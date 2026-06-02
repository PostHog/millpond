"""Tests for icebox.iceberg — DataFile construction + cycle_id recovery scan.

The DataFile.from_args path is exercised by building a real DataFile
against a real PartitionSpec + Schema; no S3/Lakekeeper touch needed.
The commit_data_files / find_snapshot_for_cycle paths are exercised
against mocked Table objects since spinning up a real catalog in unit
tests is overkill for the call-shape verification we want here.

The end-to-end committer test (real Lakekeeper, real S3) lives elsewhere.
"""
from __future__ import annotations

from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, LongType, NestedField, StringType, TimestamptzType
from pyiceberg.typedef import Record

from icebox.iceberg import (
    CYCLE_ID_SUMMARY_KEY,
    build_data_file,
    commit_data_files,
    find_snapshot_for_cycle,
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


def _wire_producer_mock(snapshot_id: int | None = 12345):
    """Build (table, tx, producer) mocks with context managers wired.

    The producer carries snapshot_id directly — commit_data_files reads
    it from the producer, NOT from table.current_snapshot(). Pass None
    to simulate a PyIceberg-version mismatch where the producer's
    snapshot_id is missing."""
    table = MagicMock()
    tx = MagicMock()
    producer = MagicMock()
    producer.snapshot_id = snapshot_id
    table.transaction.return_value.__enter__ = lambda self: tx
    table.transaction.return_value.__exit__ = lambda self, *a: None
    tx._append_snapshot_producer.return_value.__enter__ = lambda self: producer
    tx._append_snapshot_producer.return_value.__exit__ = lambda self, *a: None
    return table, tx, producer


def test_commit_data_files_uses_cycle_id_summary_key():
    """The snapshot summary MUST be tagged with our cycle_id under
    posthog.icebox.cycle_id. Recovery walks the snapshot_log scanning
    for this key — wrong key name → cycles look 'lost' and committer
    retries forever."""
    table, tx, _ = _wire_producer_mock(snapshot_id=12345)

    cycle = uuid4()
    dummy_df = MagicMock(spec=DataFile)
    result = commit_data_files(table=table, data_files=[dummy_df], cycle_id=cycle)

    tx._append_snapshot_producer.assert_called_once()
    call_kwargs = tx._append_snapshot_producer.call_args.kwargs
    assert call_kwargs["snapshot_properties"] == {CYCLE_ID_SUMMARY_KEY: str(cycle)}
    assert result == 12345


def test_commit_data_files_appends_each_file_to_producer():
    table, _, producer = _wire_producer_mock(snapshot_id=1)
    files = [MagicMock(spec=DataFile) for _ in range(3)]
    commit_data_files(table=table, data_files=files, cycle_id=uuid4())
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
        table=table, data_files=[MagicMock(spec=DataFile)], cycle_id=uuid4(),
    )
    assert result == 999, (
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
            table=table, data_files=[MagicMock(spec=DataFile)], cycle_id=uuid4(),
        )


def test_commit_data_files_respects_branch_arg():
    """Default is main but the call must propagate the arg if provided."""
    table, tx, _ = _wire_producer_mock(snapshot_id=1)

    commit_data_files(
        table=table, data_files=[MagicMock(spec=DataFile)], cycle_id=uuid4(),
        branch="staging",
    )
    assert tx._append_snapshot_producer.call_args.kwargs["branch"] == "staging"


# ---------------------------------------------------------------------------
# find_snapshot_for_cycle — recovery scan
# ---------------------------------------------------------------------------


def _stub_snapshot(snapshot_id: int, cycle_id: UUID | None) -> MagicMock:
    snap = MagicMock()
    snap.snapshot_id = snapshot_id
    snap.summary = (
        {CYCLE_ID_SUMMARY_KEY: str(cycle_id), "other.key": "x"} if cycle_id else None
    )
    return snap


def test_find_snapshot_for_cycle_returns_id_on_match():
    """Recovery's happy path: the committer crashed after Iceberg
    commit but before marking the cycle complete. The cycle_id we
    expect is in the snapshot_log — we found our snapshot, can mark
    cycle complete."""
    cid = uuid4()
    table = MagicMock()
    table.snapshots.return_value = [
        _stub_snapshot(101, cycle_id=uuid4()),  # an older cycle
        _stub_snapshot(102, cycle_id=cid),  # ours
        _stub_snapshot(103, cycle_id=None),  # tag-less (e.g. via add_files)
    ]
    assert find_snapshot_for_cycle(table, cid) == 102


def test_find_snapshot_for_cycle_returns_none_when_absent():
    """Recovery's other path: the committer crashed BEFORE Iceberg
    commit landed. cycle_id not in snapshot_log → we retry."""
    cid = uuid4()
    table = MagicMock()
    table.snapshots.return_value = [
        _stub_snapshot(101, cycle_id=uuid4()),
        _stub_snapshot(102, cycle_id=uuid4()),
    ]
    assert find_snapshot_for_cycle(table, cid) is None


def test_find_snapshot_for_cycle_handles_empty_log():
    """A fresh icebox with no committed snapshots yet."""
    table = MagicMock()
    table.snapshots.return_value = []
    assert find_snapshot_for_cycle(table, uuid4()) is None


def test_find_snapshot_for_cycle_walks_full_log_not_just_current():
    """If our snapshot is anywhere in the log, we find it — even if a
    later cycle's snapshot is current. current_snapshot() alone is not
    sufficient because parallel writers might commit after us before
    our recovery scan runs."""
    cid = uuid4()
    table = MagicMock()
    table.snapshots.return_value = [
        _stub_snapshot(101, cycle_id=cid),  # ours (older)
        _stub_snapshot(102, cycle_id=uuid4()),  # someone else's (current)
    ]
    assert find_snapshot_for_cycle(table, cid) == 101


def test_find_snapshot_for_cycle_handles_snapshot_with_none_summary():
    """Older snapshots may have summary=None (or just lack our key).
    Skip them silently rather than crashing."""
    cid = uuid4()
    table = MagicMock()
    table.snapshots.return_value = [
        _stub_snapshot(101, cycle_id=None),
        _stub_snapshot(102, cycle_id=cid),
    ]
    assert find_snapshot_for_cycle(table, cid) == 102


# ---------------------------------------------------------------------------
# Review-driven: defensive guards
# ---------------------------------------------------------------------------


def test_commit_data_files_empty_list_raises():
    """PE #6: an empty data_files list would still commit a no-op
    Iceberg snapshot tagged with our cycle_id. Recovery would then
    consider this a successful commit and metadata.json would grow
    forever with zero-file snapshots. Refuse loudly."""
    table = MagicMock()
    with pytest.raises(ValueError, match="empty data_files list"):
        commit_data_files(table=table, data_files=[], cycle_id=uuid4())
    # And: no transaction was opened
    table.transaction.assert_not_called()


def test_build_data_file_rejects_unknown_field_id_in_column_sizes():
    """QE re-review: a writer shipping stats for field id 99 against a
    schema with only ids 1..7 would silently produce a manifest with
    garbage column_sizes for the wrong field. Validate at construction."""
    table = _mock_table()
    stats = _valid_stats()
    stats["column_sizes"]["99"] = 4096  # field id 99 not in events schema
    with pytest.raises(KeyError, match="99"):
        build_data_file(
            table=table,
            file_path="s3://b/foo.parquet",
            record_count=10,
            file_size=100,
            partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
            parquet_stats=stats,
        )


def test_build_data_file_rejects_unknown_field_id_in_value_counts():
    """Same defense across all the field-id-keyed stat dicts."""
    table = _mock_table()
    stats = _valid_stats()
    stats["value_counts"]["42"] = 1000
    with pytest.raises(KeyError, match="42"):
        build_data_file(
            table=table,
            file_path="s3://b/foo.parquet",
            record_count=10,
            file_size=100,
            partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
            parquet_stats=stats,
        )
