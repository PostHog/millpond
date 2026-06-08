"""PyIceberg DataFile construction without footer reads.

The committer assembles `DataFile` records from writer-supplied
metadata (record_count, file_size, parquet_stats) and registers them
via `_append_snapshot_producer`. No S3 GETs per file — the whole point
of icebox vs. `add_files`.

PyIceberg 0.11.1 ergonomics (verified against `.venv`):

  DataFile.__init__ is positional via a Record-style `_data` tuple. The
  supported public constructor is the classmethod
  `DataFile.from_args(_table_format_version=int, **kwargs)` which binds
  kwargs through `super()._bind`. Keyword-only invocation is required —
  positional args silently mis-bind.

Partition tuple shape: PartitionSpec.fields() ordering dictates the
Record positional order. JSON-deserialized values come through as plain
Python int/float/str; the partition spec may want a different Iceberg
type (e.g. date partition column). Coercion happens in
`partition_tuple_from_spec` using the spec's source field type.

For v1 we hard-assume identity transforms on (year, month, day, hour)
ints. That assumption is encoded in the dict-of-ints `partition_values`
the writers send. If anyone runs `update_spec`, this breaks — the plan
calls this out as a non-goal.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import TableAlreadyExistsError
from pyiceberg.io import load_file_io
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema, assign_fresh_schema_ids
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record

from shared.bounds import encode_bounds

log = logging.getLogger(__name__)


# Partition field ids by Iceberg convention start at 1000. Names match
# what millpond/icebox_sink._add_metadata_columns stamps into the
# parquet (int32 year/month/day/hour columns), so an IdentityTransform
# off each column is what the writer's partition_values dict assumes.
# All icebox-sink consumers share this layout — no per-table tuning.
_PARTITION_FIELDS = (
    ("year", 1000),
    ("month", 1001),
    ("day", 1002),
    ("hour", 1003),
)


@dataclass(frozen=True)
class CommitResult:
    """The result of a successful iceberg-commit cycle.

    Returned as a dataclass (not a tuple) so adding fields like
    per-cycle delta counts or schema-evolution flags later doesn't
    force every caller to update positional unpacking.

    Attributes:
        snapshot_id: The committed Iceberg snapshot ID. Load-bearing
            for PG state — persisted so recovery doesn't need to
            rescan the snapshot_log.
        summary: The snapshot's spec-defined summary dict (keys like
            ``total-data-files``, ``total-records``, ``added-records``,
            etc.) extracted from the transaction's metadata in the
            same scope as the commit — no extra Lakekeeper round-trip.
            ``None`` only if a future PyIceberg API drift in the
            in-tx metadata view defeats the lookup; defensive.
    """

    snapshot_id: int
    summary: dict[str, str] | None


def partition_tuple_from_spec(
    partition_values: dict[str, Any],
    spec: PartitionSpec,
    schema: Schema,
) -> tuple[Any, ...]:
    """Build the positional partition tuple for a Record in spec order.

    Args:
        partition_values: writer-supplied {column_name: value} dict.
            Per the v1 invariant, these are int values for
            (year, month, day, hour) identity partitions.
        spec: the table's current PartitionSpec. fields() defines the
            positional order.
        schema: the table's current Schema, used to look up source
            column names by source_id when we need to bridge from the
            dict's keys (names) to the spec's fields (source_id).

    Returns:
        A tuple in spec.fields() order. Values are int (for the v1
        identity partitions on year/month/day/hour). Future non-identity
        partitions would need transform.apply() here.

    Raises:
        KeyError: if a spec field's source column name isn't in
            partition_values — would silently produce wrong partition
            placement otherwise.
    """
    name_by_source_id = {f.field_id: f.name for f in schema.fields}
    out: list[Any] = []
    for field in spec.fields:
        source_name = name_by_source_id.get(field.source_id)
        if source_name is None:
            raise KeyError(
                f"PartitionSpec field source_id {field.source_id} not present in schema"
            )
        if source_name not in partition_values:
            raise KeyError(
                f"partition_values missing required column '{source_name}' "
                f"(spec field '{field.name}', source_id {field.source_id})"
            )
        out.append(partition_values[source_name])
    return tuple(out)


def build_data_file(
    *,
    table: Table,
    file_path: str,
    record_count: int,
    file_size: int,
    partition_values: dict[str, Any],
    parquet_stats: dict[str, Any],
) -> DataFile:
    """Construct a single DataFile from writer-supplied metadata.

    Args:
        table: PyIceberg Table for the schema/spec lookup. Resolved
            once per cycle in the committer.
        file_path: full s3:// URI of the parquet file (the committer
            does NOT verify S3-existence here; that's the writer's
            invariant).
        record_count, file_size: writer-reported.
        partition_values: {column_name: value} dict.
        parquet_stats: as POSTed by the writer; keys per ParquetStats
            model. lower_bounds/upper_bounds are typed JSON keyed by
            Iceberg field id strings.

    Returns:
        A DataFile ready for producer.append_data_file().
    """
    spec = table.spec()
    schema = table.schema()
    partition_tuple = partition_tuple_from_spec(partition_values, spec, schema)

    # parquet_stats keys are stringified Iceberg field IDs (JSON wire
    # format). PyIceberg's DataFile wants int-keyed dicts. Validate that
    # every field id is known to the schema — a writer shipping stats
    # for a dropped/renamed field would otherwise produce a manifest
    # with garbage column accounting that can't be queried.
    schema_field_ids = {f.field_id for f in schema.fields}

    def _to_int_keys(d: dict[str, int], label: str) -> dict[int, int]:
        out: dict[int, int] = {}
        for k, v in d.items():
            fid = int(k)
            if fid not in schema_field_ids:
                raise KeyError(
                    f"parquet_stats.{label} references field id {fid} not in "
                    f"table schema; writer likely shipped a stale schema "
                    f"(file_path={file_path})"
                )
            out[fid] = v
        return out

    column_sizes = _to_int_keys(parquet_stats["column_sizes"], "column_sizes")
    value_counts = _to_int_keys(parquet_stats["value_counts"], "value_counts")
    null_value_counts = _to_int_keys(
        parquet_stats["null_value_counts"], "null_value_counts"
    )
    nan_value_counts = _to_int_keys(
        parquet_stats.get("nan_value_counts", {}), "nan_value_counts"
    )
    lower_bounds = encode_bounds(parquet_stats["lower_bounds"], schema)
    upper_bounds = encode_bounds(parquet_stats["upper_bounds"], schema)
    split_offsets = parquet_stats.get("split_offsets") or None

    return DataFile.from_args(
        _table_format_version=table.metadata.format_version,
        content=DataFileContent.DATA,
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        partition=Record(*partition_tuple),
        record_count=record_count,
        file_size_in_bytes=file_size,
        column_sizes=column_sizes,
        value_counts=value_counts,
        null_value_counts=null_value_counts,
        nan_value_counts=nan_value_counts,
        lower_bounds=lower_bounds,
        upper_bounds=upper_bounds,
        key_metadata=None,
        split_offsets=split_offsets,
        equality_ids=None,
        sort_order_id=None,
    )


def commit_data_files(
    *,
    table: Table,
    data_files: list[DataFile],
    branch: str = "main",
) -> CommitResult:
    """Commit a batch of DataFiles in a single Iceberg snapshot.

    Args:
        table: PyIceberg Table (loaded fresh per call by the caller).
        data_files: built by build_data_file().
        branch: snapshot branch. Defaults to "main".

    Returns:
        A ``CommitResult(snapshot_id, summary)``. ``snapshot_id`` is the
        committed Iceberg snapshot ID. ``summary`` is the snapshot's
        Iceberg-spec summary dict (with keys like ``total-data-files``,
        ``total-records``, ``added-records``, etc.) extracted from the
        transaction's updated metadata in the same scope as the commit
        — no extra Lakekeeper round-trip. ``summary`` is ``None`` only
        if a future PyIceberg API drift in the in-tx metadata view
        defeats the lookup; defensive.

    Raises:
        Whatever PyIceberg raises if the commit fails (transient FS
        errors, CommitFailedException, etc.). The caller decides how
        to classify the failure (transport vs. content) and acts on it.
    """
    if not data_files:
        # Defense in depth: callers already short-circuit when no files
        # are pending, but committing an empty snapshot here would
        # produce a zero-file snapshot in metadata.json — junk that
        # grows the manifest list without doing anything useful.
        # Refuse upstream's bad call loudly.
        raise ValueError(
            "commit_data_files: refusing to commit empty data_files list"
        )

    # Capture the snapshot id from the producer directly. Reading
    # `table.current_snapshot()` after `tx.commit` would rely on
    # PyIceberg refreshing the in-memory Table metadata in place —
    # behavior that varies across PyIceberg versions and could silently
    # return a STALE pre-commit snapshot id (a permanent lie in PG).
    # The producer's `snapshot_id` is the canonical, version-stable id
    # for the snapshot the producer just built.
    snapshot_id: int | None = None
    summary: dict[str, str] | None = None
    with table.transaction() as tx:
        with tx._append_snapshot_producer(
            snapshot_properties={},
            branch=branch,
        ) as producer:
            for df in data_files:
                producer.append_data_file(df)
            snapshot_id = producer.snapshot_id
        # After the producer context exits, the transaction's metadata
        # includes the new snapshot. Look it up via the public
        # ``tx.table_metadata.snapshot_by_id`` API — same scope, no
        # extra Lakekeeper round-trip.
        #
        # Defensive partial-extraction: pull additional_properties
        # first (the bulk of the value); only THEN try to attach the
        # operation. A failure during operation lookup keeps the
        # already-extracted dict so the committer's gauges still get
        # the cumulative+delta numbers — partial summaries beat None
        # for observability. A failure earlier (no snapshot found at
        # all, or additional_properties raises) degrades to summary=None.
        try:
            new_snapshot = tx.table_metadata.snapshot_by_id(snapshot_id)
        except Exception:
            new_snapshot = None
        if new_snapshot is not None and new_snapshot.summary is not None:
            try:
                summary = dict(new_snapshot.summary.additional_properties)
            except Exception:
                summary = None
            if summary is not None:
                try:
                    summary["posthog.icebox.operation"] = (
                        new_snapshot.summary.operation.value
                    )
                except Exception:
                    pass  # keep the additional_properties we did get

    if snapshot_id is None:
        raise RuntimeError(
            "Iceberg commit completed but producer.snapshot_id is None — "
            "this indicates a PyIceberg API change affecting "
            "_append_snapshot_producer"
        )
    return CommitResult(snapshot_id=snapshot_id, summary=summary)


def bootstrap_table_from_parquet(
    *,
    catalog: Catalog,
    namespace: str,
    table_name: str,
    parquet_s3_path: str,
) -> Table:
    """Create (namespace, table_name) in the catalog by inferring the
    schema from a staged parquet's footer. Returns the loaded Table.

    Idempotent: if a concurrent icebox replica wins the create race,
    catches TableAlreadyExistsError and loads instead. Safe to call
    from every tick that would otherwise crash on NoSuchTableError —
    the create only fires once per (namespace, table) lifetime.

    Schema derivation: read arrow_schema from the parquet footer (one
    S3 GET with a Range header — no full-file download), then apply
    `assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(...))`.
    This is the same conversion the writer does at first-flush time
    (millpond/icebox_sink._ensure_schema), so the resulting Iceberg
    field ids match the ones the writer's parquet_stats are keyed by.

    Partition spec: IdentityTransform on each of year/month/day/hour —
    matches _add_metadata_columns + partition_tuple_from_spec. The
    writer always stamps these four int32 columns; we assume their
    presence and fail loudly otherwise so a schema regression on the
    writer side surfaces clearly here rather than silently producing
    an unpartitioned table.
    """
    file_io = load_file_io(properties=catalog.properties, location=parquet_s3_path)
    with file_io.new_input(parquet_s3_path).open() as f:
        arrow_schema = pq.ParquetFile(f).schema_arrow

    ice_schema: Schema = assign_fresh_schema_ids(
        _pyarrow_to_schema_without_ids(arrow_schema)
    )

    partition_fields = []
    for name, field_id in _PARTITION_FIELDS:
        try:
            source = ice_schema.find_field(name)
        except ValueError as exc:
            raise ValueError(
                f"bootstrap_table_from_parquet: parquet at {parquet_s3_path!r} "
                f"missing required partition column {name!r}; expected an "
                f"int32 stamped by millpond/icebox_sink._add_metadata_columns"
            ) from exc
        partition_fields.append(
            PartitionField(
                source_id=source.field_id,
                field_id=field_id,
                transform=IdentityTransform(),
                name=name,
            )
        )
    spec = PartitionSpec(*partition_fields)

    log.info(
        "bootstrap_table_from_parquet: creating %s.%s (schema=%d fields, "
        "partition_spec=year/month/day/hour identity) from %s",
        namespace, table_name, len(ice_schema.fields), parquet_s3_path,
    )
    try:
        return catalog.create_table(
            (namespace, table_name),
            schema=ice_schema,
            partition_spec=spec,
        )
    except TableAlreadyExistsError:
        # Another replica beat us. Load the table they created — its
        # schema/spec might differ from ours if writers shipped
        # different shapes, but at THIS point any rows we held in PG
        # already match this parquet's schema and will commit
        # successfully or fail loudly at append time.
        log.info(
            "bootstrap_table_from_parquet: %s.%s already exists (replica "
            "race); loading instead",
            namespace, table_name,
        )
        return catalog.load_table((namespace, table_name))
