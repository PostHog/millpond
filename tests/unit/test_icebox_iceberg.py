"""Tests for icebox.iceberg — DataFile construction + commit_data_files
call-shape verification.

The DataFile.from_args path is exercised by building a real DataFile
against a real PartitionSpec + Schema; no S3/Lakekeeper touch needed.
The commit_data_files path is exercised against mocked Table objects
since spinning up a real catalog in unit tests is overkill for the
call-shape verification we want here.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType, TimestamptzType

from icebox.iceberg import (
    build_data_file,
    commit_data_files,
    partition_tuple_from_spec,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _events_schema() -> Schema:
    """Match the schema millpond/iceberg.py generates for the events
    table: scalars + 4 partition cols (year/month/day/hour as IntegerType)."""
    return Schema(
        NestedField(field_id=1, name="event", field_type=StringType(), required=True),
        NestedField(field_id=2, name="distinct_id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="timestamp", field_type=TimestamptzType(), required=True),
        NestedField(field_id=4, name="year", field_type=IntegerType(), required=True),
        NestedField(field_id=5, name="month", field_type=IntegerType(), required=True),
        NestedField(field_id=6, name="day", field_type=IntegerType(), required=True),
        NestedField(field_id=7, name="hour", field_type=IntegerType(), required=True),
    )


def _events_spec() -> PartitionSpec:
    """Identity transform on each of (year, month, day, hour)."""
    return PartitionSpec(
        PartitionField(source_id=4, field_id=1000, name="year", transform=IdentityTransform()),
        PartitionField(source_id=5, field_id=1001, name="month", transform=IdentityTransform()),
        PartitionField(source_id=6, field_id=1002, name="day", transform=IdentityTransform()),
        PartitionField(source_id=7, field_id=1003, name="hour", transform=IdentityTransform()),
    )


def _valid_stats() -> dict:
    return {
        "column_sizes": {"1": 100, "2": 200, "3": 300, "4": 8, "5": 8, "6": 8, "7": 8},
        "value_counts": {"1": 1000, "2": 1000, "3": 1000, "4": 1000, "5": 1000, "6": 1000, "7": 1000},
        "null_value_counts": {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0, "6": 0, "7": 0},
        "lower_bounds": {"4": 2026, "5": 6, "6": 1, "7": 14},
        "upper_bounds": {"4": 2026, "5": 6, "6": 1, "7": 14},
    }


def _mock_table(schema=None, spec=None, format_version=2):
    """Build a MagicMock that quacks like pyiceberg.table.Table for
    the build_data_file path."""
    table = MagicMock()
    table.schema.return_value = schema or _events_schema()
    table.spec.return_value = spec or _events_spec()
    table.metadata.format_version = format_version
    return table


# ---------------------------------------------------------------------------
# partition_tuple_from_spec
# ---------------------------------------------------------------------------


def test_partition_tuple_ordered_by_spec():
    """The PartitionSpec dictates positional order, NOT the
    partition_values dict iteration order. Writers sending
    {hour: 14, year: 2026, ...} must produce (2026, 6, 1, 14)."""
    schema = _events_schema()
    spec = _events_spec()
    pv = {"hour": 14, "year": 2026, "month": 6, "day": 1}
    result = partition_tuple_from_spec(pv, spec, schema)
    assert result == (2026, 6, 1, 14)


def test_partition_tuple_raises_on_missing_column():
    """Catch the silent-misplacement bug: writer omits a partition column
    and the file would land in the wrong partition silently."""
    schema = _events_schema()
    spec = _events_spec()
    pv = {"year": 2026, "month": 6, "day": 1}  # hour missing
    with pytest.raises(KeyError, match="hour"):
        partition_tuple_from_spec(pv, spec, schema)


def test_partition_tuple_unpartitioned_spec_yields_empty_tuple():
    """An unpartitioned spec has zero fields → empty tuple. Edge case,
    not actually used in v1 but the call shouldn't crash."""
    schema = _events_schema()
    spec = PartitionSpec()  # no fields
    result = partition_tuple_from_spec({}, spec, schema)
    assert result == ()


def test_partition_tuple_raises_on_dangling_source_id():
    """The spec references a source_id not present in the schema —
    indicates a deploy-skew or a corrupt PartitionSpec. Fail loud."""
    schema = _events_schema()
    spec = PartitionSpec(
        PartitionField(source_id=999, field_id=1000, name="phantom", transform=IdentityTransform()),
    )
    with pytest.raises(KeyError, match="999"):
        partition_tuple_from_spec({"phantom": 1}, spec, schema)


# ---------------------------------------------------------------------------
# build_data_file — core construction path
# ---------------------------------------------------------------------------


def test_build_data_file_returns_datafile_with_expected_shape():
    table = _mock_table()
    df = build_data_file(
        table=table,
        file_path="s3://b/data/year=2026/month=06/day=01/hour=14/writer-0-abc.parquet",
        record_count=1000,
        file_size=4096,
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        parquet_stats=_valid_stats(),
    )
    assert isinstance(df, DataFile)
    assert df.file_path == "s3://b/data/year=2026/month=06/day=01/hour=14/writer-0-abc.parquet"
    assert df.record_count == 1000
    assert df.file_size_in_bytes == 4096
    assert df.content == DataFileContent.DATA
    assert df.file_format == FileFormat.PARQUET


def test_build_data_file_partition_record_in_spec_order():
    table = _mock_table()
    df = build_data_file(
        table=table,
        file_path="s3://b/foo.parquet",
        record_count=10,
        file_size=100,
        partition_values={"hour": 14, "year": 2026, "month": 6, "day": 1},
        parquet_stats=_valid_stats(),
    )
    assert isinstance(df.partition, Record)
    assert tuple(df.partition) == (2026, 6, 1, 14)


def test_build_data_file_stats_keys_normalized_to_int():
    """parquet_stats column maps are string-keyed on the wire (JSON).
    DataFile wants int keys."""
    table = _mock_table()
    df = build_data_file(
        table=table,
        file_path="s3://b/foo.parquet",
        record_count=10,
        file_size=100,
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        parquet_stats=_valid_stats(),
    )
    assert all(isinstance(k, int) for k in df.column_sizes.keys())
    assert all(isinstance(k, int) for k in df.value_counts.keys())
    assert all(isinstance(k, int) for k in df.null_value_counts.keys())


def test_build_data_file_bounds_encoded_as_bytes():
    """lower_bounds / upper_bounds get the encode_bounds treatment."""
    table = _mock_table()
    df = build_data_file(
        table=table,
        file_path="s3://b/foo.parquet",
        record_count=10,
        file_size=100,
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        parquet_stats=_valid_stats(),
    )
    assert all(isinstance(v, bytes) for v in df.lower_bounds.values())
    assert all(isinstance(v, bytes) for v in df.upper_bounds.values())


def test_build_data_file_split_offsets_optional():
    """split_offsets isn't always present; handle empty list / missing."""
    table = _mock_table()
    stats = _valid_stats()
    # absent
    df = build_data_file(
        table=table, file_path="s3://b/a.parquet", record_count=10, file_size=100,
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        parquet_stats=stats,
    )
    assert df.split_offsets is None
    # empty list
    stats2 = {**stats, "split_offsets": []}
    df2 = build_data_file(
        table=table, file_path="s3://b/b.parquet", record_count=10, file_size=100,
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        parquet_stats=stats2,
    )
    assert df2.split_offsets is None
    # populated
    stats3 = {**stats, "split_offsets": [0, 4096]}
    df3 = build_data_file(
        table=table, file_path="s3://b/c.parquet", record_count=10, file_size=100,
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        parquet_stats=stats3,
    )
    assert df3.split_offsets == [0, 4096]


def test_build_data_file_respects_format_version_v1():
    """If the table format_version is 1, the DataFile.from_args call
    must propagate it — v1 DataFile shape differs from v2."""
    table = _mock_table(format_version=1)
    df = build_data_file(
        table=table,
        file_path="s3://b/foo.parquet",
        record_count=10,
        file_size=100,
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        parquet_stats=_valid_stats(),
    )
    # The DataFile object exists either way — the value-level assertion
    # is that the call didn't error. v1 / v2 each produce valid DataFiles.
    assert isinstance(df, DataFile)


# ---------------------------------------------------------------------------
# commit_data_files — call shape verification
# ---------------------------------------------------------------------------


def _wire_producer_mock(snapshot_id: int | None = 12345, summary: object | None = None):
    """Build (table, tx, producer) mocks with context managers wired.

    The producer carries snapshot_id directly — commit_data_files reads
    it from the producer, NOT from table.current_snapshot(). Pass None
    for snapshot_id to simulate a PyIceberg-version mismatch where the
    producer's snapshot_id is missing.

    ``summary``: pass a mock Snapshot (with .summary.additional_properties
    + .summary.operation) to exercise the post-commit summary-extraction
    path. Default None means tx.table_metadata.snapshot_by_id returns
    None — extraction silently skipped, summary returned as None.
    """
    table = MagicMock()
    tx = MagicMock()
    producer = MagicMock()
    producer.snapshot_id = snapshot_id
    table.transaction.return_value.__enter__ = lambda self: tx
    table.transaction.return_value.__exit__ = lambda self, *a: None
    tx._append_snapshot_producer.return_value.__enter__ = lambda self: producer
    tx._append_snapshot_producer.return_value.__exit__ = lambda self, *a: None
    tx.table_metadata.snapshot_by_id.return_value = summary
    return table, tx, producer



def test_commit_data_files_appends_each_file_to_producer():
    table, _, producer = _wire_producer_mock(snapshot_id=1)
    files = [MagicMock(spec=DataFile) for _ in range(3)]
    commit_data_files(table=table, data_files=files)
    assert producer.append_data_file.call_count == 3


def test_commit_data_files_reads_snapshot_id_from_producer_not_table():
    """PE re-review #3: capturing the snapshot id from
    producer.snapshot_id (not table.current_snapshot()) avoids the
    stale-table-handle hazard. The test mocks table.current_snapshot to
    return a DIFFERENT id — the function must return the producer's id
    regardless. A regression that reads from table.current_snapshot
    would silently fail here."""
    table, _, _ = _wire_producer_mock(snapshot_id=999)
    table.current_snapshot.return_value = MagicMock(snapshot_id=42)  # wrong id

    result = commit_data_files(
        table=table, data_files=[MagicMock(spec=DataFile)],
    )
    assert result.snapshot_id == 999, (
        "commit_data_files must read snapshot_id from the producer, "
        "not from table.current_snapshot() which could be stale"
    )


def test_commit_data_files_raises_if_producer_snapshot_id_is_none():
    """If a PyIceberg upgrade removes the producer's snapshot_id
    attribute (or it starts returning None for some reason), refuse to
    record a None snapshot id to PG. The pin canary in
    test_pyiceberg_pin catches the attribute removal at install time;
    this is the second line of defense."""
    table, _, _ = _wire_producer_mock(snapshot_id=None)

    with pytest.raises(RuntimeError, match="producer.snapshot_id is None"):
        commit_data_files(
            table=table, data_files=[MagicMock(spec=DataFile)],
        )


def test_commit_data_files_respects_branch_arg():
    """Default is main but the call must propagate the arg if provided."""
    table, tx, _ = _wire_producer_mock(snapshot_id=1)

    commit_data_files(
        table=table, data_files=[MagicMock(spec=DataFile)],
        branch="staging",
    )
    assert tx._append_snapshot_producer.call_args.kwargs["branch"] == "staging"


def test_commit_data_files_passes_empty_snapshot_properties():
    """The daemon path stamps nothing into snapshot.summary — the cycle_id
    summary key is gone with the cycle abstraction."""
    table, tx, _ = _wire_producer_mock(snapshot_id=12345)

    commit_data_files(table=table, data_files=[MagicMock(spec=DataFile)])

    call_kwargs = tx._append_snapshot_producer.call_args.kwargs
    assert call_kwargs["snapshot_properties"] == {}




def test_commit_data_files_extracts_summary_when_present():
    """The post-commit summary lookup must surface the spec keys we
    chart (total-data-files etc.) into the returned dict so the
    committer can update the gauges with no extra round-trip."""
    # Build a fake Snapshot whose .summary.additional_properties carries
    # the spec keys + a real operation enum.
    from pyiceberg.table.snapshots import Operation

    fake_snapshot = MagicMock()
    fake_snapshot.summary.additional_properties = {
        "total-data-files": "42",
        "total-records": "1000000",
        "total-files-size": "987654321",
        "added-data-files": "3",
    }
    fake_snapshot.summary.operation = Operation.APPEND

    table, _, _ = _wire_producer_mock(snapshot_id=777, summary=fake_snapshot)
    result = commit_data_files(
        table=table, data_files=[MagicMock(spec=DataFile)]
    )
    assert result.snapshot_id == 777
    assert result.summary is not None
    assert result.summary["total-data-files"] == "42"
    assert result.summary["total-records"] == "1000000"
    assert result.summary["total-files-size"] == "987654321"
    # Operation key is namespaced to avoid collision with any future
    # producer-attached `operation` summary property.
    assert result.summary["posthog.icebox.operation"] == "append"


def test_commit_data_files_preserves_partial_summary_when_operation_fails():
    """Operation extraction failure must NOT discard the
    additional_properties we already pulled. Partial summary beats
    None — the cumulative + delta gauges still get values, only the
    operation label is missing."""
    # Build a hand-rolled summary object: real attribute access for
    # additional_properties, raise on .operation. MagicMock's auto-attr
    # short-circuits PropertyMock here, so use a plain class.
    class _PartialSummary:
        additional_properties = {
            "total-data-files": "100",
            "added-records": "500",
        }

        @property
        def operation(self):
            raise AttributeError("simulated PyIceberg API drift")

    fake_snapshot = MagicMock()
    fake_snapshot.summary = _PartialSummary()
    table, _, _ = _wire_producer_mock(snapshot_id=42, summary=fake_snapshot)
    result = commit_data_files(
        table=table, data_files=[MagicMock(spec=DataFile)]
    )
    assert result.snapshot_id == 42
    assert result.summary is not None
    assert result.summary["total-data-files"] == "100"
    assert result.summary["added-records"] == "500"
    assert "posthog.icebox.operation" not in result.summary


def test_commit_data_files_returns_none_summary_when_lookup_fails():
    """A future PyIceberg API shift in the in-tx metadata view must
    NOT kill the commit. The snapshot_id load-bearing return still
    flows; summary is None and the committer's gauges hold their last
    value for that cycle."""

    # Wire a snapshot mock whose .summary raises on attribute access
    class _BoomSummary:
        @property
        def additional_properties(self):
            raise RuntimeError("simulated PyIceberg API drift")

    fake_snapshot = MagicMock()
    fake_snapshot.summary = _BoomSummary()
    table, _, _ = _wire_producer_mock(snapshot_id=555, summary=fake_snapshot)
    result = commit_data_files(
        table=table, data_files=[MagicMock(spec=DataFile)]
    )
    assert result.snapshot_id == 555
    assert result.summary is None




# ---------------------------------------------------------------------------
# bootstrap_table_from_parquet
# ---------------------------------------------------------------------------


def _stage_parquet_bytes() -> bytes:
    """Build the smallest parquet that mirrors what
    millpond/icebox_sink._add_metadata_columns stamps: a few data
    columns plus year/month/day/hour int32 partition columns and an
    _inserted_at timestamp."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "team_id": pa.array([1, 2], type=pa.int64()),
        "event": pa.array(["a", "b"], type=pa.string()),
        "_inserted_at": pa.array([0, 0], type=pa.timestamp("us", tz="UTC")),
        "year": pa.array([2026, 2026], type=pa.int32()),
        "month": pa.array([6, 6], type=pa.int32()),
        "day": pa.array([8, 8], type=pa.int32()),
        "hour": pa.array([16, 16], type=pa.int32()),
    })
    buf = pa.BufferOutputStream()
    with pq.ParquetWriter(buf, table.schema) as w:
        w.write_table(table)
    return buf.getvalue().to_pybytes()


def _fake_catalog_with_parquet(parquet_bytes: bytes):
    """Catalog double whose `properties` are empty and whose FileIO
    resolves to a single in-memory parquet, regardless of path. Returns
    (catalog, create_table_calls) so tests can inspect what was created.
    """
    import io
    from unittest.mock import MagicMock

    catalog = MagicMock()
    catalog.properties = {}
    return catalog, _patch_file_io_to_return(parquet_bytes)


def _patch_file_io_to_return(parquet_bytes: bytes):
    """Return a monkeypatch helper bound at call time."""
    return parquet_bytes


def test_bootstrap_table_creates_table_with_inferred_schema_and_partition_spec(
    monkeypatch,
):
    """Round-trip: bootstrap_table_from_parquet reads the parquet
    footer, derives an Iceberg schema with deterministic field ids,
    builds the year/month/day/hour identity PartitionSpec, and calls
    catalog.create_table with both."""
    import io
    from unittest.mock import MagicMock

    from icebox import iceberg as ib_mod

    parquet_bytes = _stage_parquet_bytes()

    # Patch load_file_io so we don't actually hit S3 — return a FileIO
    # whose new_input(path).open() returns a BytesIO of our parquet.
    fake_input = MagicMock()
    fake_input.open.return_value.__enter__ = lambda self: io.BytesIO(parquet_bytes)
    fake_input.open.return_value.__exit__ = lambda self, *a: None
    fake_io = MagicMock()
    fake_io.new_input.return_value = fake_input
    monkeypatch.setattr(ib_mod, "load_file_io", lambda properties, location: fake_io)

    catalog = MagicMock()
    catalog.properties = {}
    created_table = MagicMock()
    catalog.create_table.return_value = created_table

    result = ib_mod.bootstrap_table_from_parquet(
        catalog=catalog,
        namespace="kafka",
        table_name="ai_events",
        parquet_s3_path="s3://b/foo.parquet",
    )

    assert result is created_table
    # create_table called exactly once with (identifier, schema, partition_spec).
    catalog.create_table.assert_called_once()
    call = catalog.create_table.call_args
    assert call.args == (("kafka", "ai_events"),)
    assert "schema" in call.kwargs and "partition_spec" in call.kwargs
    schema = call.kwargs["schema"]
    spec = call.kwargs["partition_spec"]

    # Schema has every data column the parquet had.
    names = {f.name for f in schema.fields}
    assert {"team_id", "event", "_inserted_at", "year", "month", "day", "hour"} <= names

    # PartitionSpec: 4 identity transforms on the int32 partition cols.
    from pyiceberg.transforms import IdentityTransform
    assert len(spec.fields) == 4
    spec_by_name = {pf.name: pf for pf in spec.fields}
    for name, expected_field_id in (
        ("year", 1000), ("month", 1001), ("day", 1002), ("hour", 1003),
    ):
        assert name in spec_by_name
        pf = spec_by_name[name]
        assert pf.field_id == expected_field_id
        assert isinstance(pf.transform, IdentityTransform)
        # source_id points at the partition column itself (identity),
        # NOT at _inserted_at.
        assert schema.find_field(pf.source_id).name == name


def test_bootstrap_table_returns_loaded_table_on_replica_race(monkeypatch):
    """If another replica wins the create race, catalog.create_table
    raises TableAlreadyExistsError; the helper falls back to load_table
    and returns its result. Multiple icebox replicas can call this
    concurrently without one crashing out."""
    import io
    from unittest.mock import MagicMock

    from pyiceberg.exceptions import TableAlreadyExistsError

    from icebox import iceberg as ib_mod

    fake_input = MagicMock()
    fake_input.open.return_value.__enter__ = lambda self: io.BytesIO(_stage_parquet_bytes())
    fake_input.open.return_value.__exit__ = lambda self, *a: None
    fake_io = MagicMock()
    fake_io.new_input.return_value = fake_input
    monkeypatch.setattr(ib_mod, "load_file_io", lambda properties, location: fake_io)

    catalog = MagicMock()
    catalog.properties = {}
    catalog.create_table.side_effect = TableAlreadyExistsError("losing the race")
    raced_table = MagicMock()
    catalog.load_table.return_value = raced_table

    result = ib_mod.bootstrap_table_from_parquet(
        catalog=catalog,
        namespace="kafka",
        table_name="ai_events",
        parquet_s3_path="s3://b/foo.parquet",
    )

    assert result is raced_table
    catalog.load_table.assert_called_once_with(("kafka", "ai_events"))


def test_bootstrap_table_rejects_parquet_missing_partition_column(monkeypatch):
    """A parquet without year/month/day/hour stamped on isn't safe to
    bootstrap from — the partition_values dict the writer ships would
    fail to map to any field in the resulting table. Fail loudly."""
    import io
    from unittest.mock import MagicMock

    import pyarrow as pa
    import pyarrow.parquet as pq

    from icebox import iceberg as ib_mod

    # Same shape as _stage_parquet_bytes but without the "hour" column.
    table = pa.table({
        "team_id": pa.array([1], type=pa.int64()),
        "year": pa.array([2026], type=pa.int32()),
        "month": pa.array([6], type=pa.int32()),
        "day": pa.array([8], type=pa.int32()),
        # NOTE: no "hour"
    })
    buf = pa.BufferOutputStream()
    with pq.ParquetWriter(buf, table.schema) as w:
        w.write_table(table)
    parquet_bytes = buf.getvalue().to_pybytes()

    fake_input = MagicMock()
    fake_input.open.return_value.__enter__ = lambda self: io.BytesIO(parquet_bytes)
    fake_input.open.return_value.__exit__ = lambda self, *a: None
    fake_io = MagicMock()
    fake_io.new_input.return_value = fake_input
    monkeypatch.setattr(ib_mod, "load_file_io", lambda properties, location: fake_io)

    catalog = MagicMock()
    catalog.properties = {}

    with pytest.raises(ValueError, match="hour"):
        ib_mod.bootstrap_table_from_parquet(
            catalog=catalog,
            namespace="kafka",
            table_name="ai_events",
            parquet_s3_path="s3://b/foo.parquet",
        )
    catalog.create_table.assert_not_called()
