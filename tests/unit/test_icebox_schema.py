"""Tests for icebox.schema — DDL and Pydantic row models.

DDL strings are validated structurally (they contain the expected
identifiers/keywords). Pydantic models are validated for type strictness
and that they accept dicts AND object-attribute access (via
from_attributes=True), since asyncpg returns Record objects and psycopg
returns row tuples that can be wrapped as namespace objects.
"""
from __future__ import annotations

import re
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from icebox import schema as ddl

# ---------------------------------------------------------------------------
# DDL — structural assertions
# ---------------------------------------------------------------------------


def test_no_ddl_references_table_name_column():
    """Permanent per-schema-design invariant: each icebox owns ONE
    Iceberg table and runs in its own PG schema. No DDL has a
    `table_name` column because per-table routing happens at the
    deployment layer, not at the row layer. A future PR that adds
    `table_name` (e.g., trying to consolidate iceboxes back into one
    process) reintroduces the multi-table-routing complexity we
    deliberately avoided."""
    blob = "\n".join(ddl.ALL_DDL).lower()
    assert "table_name" not in blob, (
        "no DDL should reference a table_name column; per-schema design "
        "expresses per-table routing as deployment topology, not "
        "row-level state. If you're adding this, see the multi-table "
        "discussion notes."
    )


def test_no_ddl_references_iceberg_namespace_or_table_columns():
    """Same invariant from the wire-format angle: RegisterFileRequest
    has no iceberg_namespace/iceberg_table fields (each icebox knows
    which table it serves from its own config). The DDL must mirror
    this."""
    blob = "\n".join(ddl.ALL_DDL).lower()
    assert "iceberg_namespace" not in blob
    assert "iceberg_table" not in blob


def test_status_has_singleton_check():
    """status is a singleton; CHECK(id=1) prevents accidental rows."""
    sql = ddl.CREATE_STATUS.lower()
    assert "check (id = 1)" in sql


def test_status_seed_row_is_idempotent():
    """ON CONFLICT DO NOTHING means re-running ALL_DDL doesn't error."""
    assert "on conflict do nothing" in ddl.SEED_STATUS_ROW.lower()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


def test_all_ddl_is_tuple_not_list():
    """Immutable so callers can't accidentally append to it."""
    assert isinstance(ddl.ALL_DDL, tuple)


# ---------------------------------------------------------------------------
# v6 polling-daemon DDL + row model
# ---------------------------------------------------------------------------


def test_all_ddl_includes_polling_daemon_table_and_index():
    """The new icebox_files table runs alongside the cycle-era tables
    until the cycle code is deleted. Both must come up cleanly via
    apply_migrations."""
    blob = "\n".join(ddl.ALL_DDL).lower()
    assert "create table if not exists icebox_files" in blob
    assert "icebox_files_pending_idx" in blob


def test_icebox_files_table_before_its_index():
    """A partial index on a non-existent table would error."""
    files_idx = ddl.ALL_DDL.index(ddl.CREATE_ICEBOX_FILES)
    idx_idx = ddl.ALL_DDL.index(ddl.CREATE_ICEBOX_FILES_PENDING_IDX)
    assert files_idx < idx_idx


def test_icebox_files_has_required_columns():
    sql = ddl.CREATE_ICEBOX_FILES.lower()
    for col in (
        "file_path",
        "writer_ordinal",
        "kafka_offsets",
        "partition_values",
        "record_count",
        "file_size",
        "parquet_stats",
        "inserted_at",
        "result",
        "result_at",
        "iceberg_snapshot_id",
    ):
        assert col in sql, f"missing column {col} from icebox_files DDL"


def test_icebox_files_drops_cycle_era_columns():
    """v6 drops schema_version, schema_fingerprint, cycle_id, staged_at,
    committed_at — replaced by the `result` enum + inserted_at. Catching
    a regression that re-introduces them keeps the cycle abstraction
    from leaking back in."""
    sql = ddl.CREATE_ICEBOX_FILES.lower()
    # The DDL block uses a column name regex: must not have these as
    # standalone columns. `schema_version` and `schema_fingerprint`
    # don't otherwise appear; `cycle_id` etc. are caught by the same
    # negative-presence assertion.
    for col in (
        "schema_version",
        "schema_fingerprint",
        "cycle_id",
        "staged_at",
        "committed_at",
    ):
        assert col not in sql, f"icebox_files DDL must not contain {col}"


def test_icebox_files_result_enum_check_constraint():
    """The CHECK guards against an UPDATE setting result to a typo'd
    value like 'commited' (silent no-op against the SELECT predicate)."""
    sql = ddl.CREATE_ICEBOX_FILES.lower()
    assert "check (result in ('pending', 'committed', 'failed'))" in sql


def test_icebox_files_has_unique_file_path():
    """ON CONFLICT (file_path) DO NOTHING is how the writer's replay
    stays idempotent."""
    sql = ddl.CREATE_ICEBOX_FILES.lower()
    assert re.search(r"file_path\s+text\s+not null\s+unique", sql)


def test_icebox_files_pending_idx_is_partial_on_result():
    """The partial index is bounded by O(pending), not O(history) — the
    daemon's hot SELECT scales with backlog, not lifetime row count."""
    sql = ddl.CREATE_ICEBOX_FILES_PENDING_IDX.lower()
    assert "where result = 'pending'" in sql


def test_icebox_pending_file_row_minimal():
    """Required fields only; result defaults to nothing on construction
    (we accept whatever PG returns)."""
    row = ddl.IceboxPendingFileRow(
        id=1,
        file_path="s3://bucket/a.parquet",
        writer_ordinal=0,
        kafka_offsets={"0": 100},
        partition_values={"day": 19000},
        record_count=42,
        file_size=1234,
        parquet_stats={},
        inserted_at=datetime.now(UTC),
        result="pending",
    )
    assert row.result_at is None
    assert row.iceberg_snapshot_id is None


def test_icebox_pending_file_row_committed():
    row = ddl.IceboxPendingFileRow(
        id=1,
        file_path="s3://bucket/a.parquet",
        writer_ordinal=0,
        kafka_offsets={"0": 100},
        partition_values={},
        record_count=1,
        file_size=1,
        parquet_stats={},
        inserted_at=datetime.now(UTC),
        result="committed",
        result_at=datetime.now(UTC),
        iceberg_snapshot_id=999,
    )
    assert row.iceberg_snapshot_id == 999


def test_icebox_pending_file_row_kafka_offsets_must_be_string_keyed():
    """jsonb requires string keys; the Pydantic type pins it at the
    boundary so callers don't accidentally produce int keys."""
    with pytest.raises(Exception):
        ddl.IceboxPendingFileRow(
            id=1,
            file_path="s3://...",
            writer_ordinal=0,
            kafka_offsets={20: 100},  # int key, not str
            partition_values={},
            record_count=0,
            file_size=0,
            parquet_stats={},
            inserted_at=datetime.now(UTC),
            result="pending",
        )
