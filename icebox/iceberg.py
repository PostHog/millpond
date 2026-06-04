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
from typing import Any
from uuid import UUID

from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.typedef import Record

from shared.bounds import encode_bounds

log = logging.getLogger(__name__)


# Snapshot summary key for cycle_id — the recovery scan looks for this
# in `snapshot.summary` to identify whether a cycle's iceberg-commit
# step had actually landed in Lakekeeper.
CYCLE_ID_SUMMARY_KEY = "posthog.icebox.cycle_id"


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
    cycle_id: UUID,
    branch: str = "main",
) -> tuple[int, dict[str, str] | None]:
    """Commit a batch of DataFiles in a single Iceberg snapshot, tagging
    the snapshot with the cycle_id for recovery.

    Args:
        table: PyIceberg Table (loaded fresh at the start of the cycle).
        data_files: built by build_data_file().
        cycle_id: the icebox cycle UUID — embedded in snapshot.summary
            under posthog.icebox.cycle_id so the recovery scan can match.
        branch: snapshot branch. Defaults to "main".

    Returns:
        A ``(snapshot_id, summary)`` pair. ``snapshot_id`` is the
        committed Iceberg snapshot ID — persisted to PG so subsequent
        recovery doesn't need to rescan the snapshot_log. ``summary``
        is the snapshot's Iceberg-spec summary dict (with keys like
        ``total-data-files``, ``total-records``, ``total-files-size``,
        etc.) extracted from the transaction's updated metadata in
        the same scope as the commit — no extra Lakekeeper round-trip.
        ``summary`` is ``None`` only if PyIceberg's in-transaction
        metadata lookup unexpectedly returns no snapshot for the id we
        just committed (defensive — should never happen).

    Raises:
        Whatever PyIceberg raises if the commit fails (transient FS
        errors, CommitFailedException, etc.). The committer marks the
        cycle as failed and the recovery path re-checks Lakekeeper.
    """
    if not data_files:
        # Defense in depth: run_cycle already short-circuits when no files
        # are claimed, but committing an empty snapshot here would produce
        # a zero-file snapshot tagged with our cycle_id. On next recovery,
        # find_snapshot_for_cycle would consider this a successful commit
        # and complete the cycle, while metadata.json grows with garbage
        # no-op snapshots. Refuse upstream's bad call loudly.
        raise ValueError(
            f"commit_data_files: refusing to commit empty data_files list "
            f"for cycle_id={cycle_id}"
        )

    # Capture the snapshot id from the producer directly. Reading
    # `table.current_snapshot()` after `tx.commit` would rely on
    # PyIceberg refreshing the in-memory Table metadata in place —
    # behavior that varies across PyIceberg versions and could silently
    # return a STALE pre-commit snapshot id (a permanent lie in PG).
    # The producer's `snapshot_id` is the canonical, version-stable id
    # for the snapshot the producer just built.
    snapshot_props = {CYCLE_ID_SUMMARY_KEY: str(cycle_id)}
    snapshot_id: int | None = None
    summary: dict[str, str] | None = None
    with table.transaction() as tx:
        with tx._append_snapshot_producer(
            snapshot_properties=snapshot_props,
            branch=branch,
        ) as producer:
            for df in data_files:
                producer.append_data_file(df)
            snapshot_id = producer.snapshot_id
        # After the producer context exits, the transaction's metadata
        # includes the new snapshot. Look it up via the public
        # ``tx.table_metadata.snapshot_by_id`` API — same scope, no
        # extra Lakekeeper round-trip. Defensive try/except: if a
        # future PyIceberg rev shifts the in-tx metadata view, we
        # still get the snapshot_id back (which is the load-bearing
        # return) and the committer's gauges just hold their previous
        # value for that cycle.
        try:
            new_snapshot = tx.table_metadata.snapshot_by_id(snapshot_id)
            if new_snapshot is not None and new_snapshot.summary is not None:
                # Summary is a Pydantic model with dict-like ``additional_properties``;
                # the standard fields live there too. ``model_dump`` gives us a flat dict.
                summary = dict(new_snapshot.summary.additional_properties)
                summary["operation"] = new_snapshot.summary.operation.value
        except Exception:
            # Don't let an extraction quirk kill the commit. The
            # snapshot_id is what we need to persist; the summary is
            # observability sugar.
            summary = None

    if snapshot_id is None:
        raise RuntimeError(
            f"Iceberg commit completed but producer.snapshot_id is None — "
            f"cycle_id={cycle_id}; this indicates a PyIceberg API change "
            f"affecting _append_snapshot_producer"
        )
    return snapshot_id, summary


def find_snapshot_for_cycle(table: Table, cycle_id: UUID) -> int | None:
    """Walk the table's snapshot_log looking for our cycle_id.

    The recovery path uses this after a committer crash: did our
    Iceberg commit actually land in Lakekeeper, or do we need to retry?

    Args:
        table: a freshly-loaded Table (DON'T pass a stale handle).
        cycle_id: the UUID of the in-flight cycle we're investigating.

    Returns:
        snapshot_id if a snapshot tagged with this cycle_id exists in
        the table's snapshot_log; None if no such snapshot.

    Note:
        Walks the full snapshots() — current_snapshot() alone is not
        enough (another cycle could have committed after ours).
    """
    target = str(cycle_id)
    for snap in table.snapshots():
        if snap.summary is None:
            continue
        summary_cycle = snap.summary.get(CYCLE_ID_SUMMARY_KEY) if snap.summary else None
        if summary_cycle == target:
            return snap.snapshot_id
    return None
