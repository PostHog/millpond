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


def test_all_ddl_includes_tables_and_indexes():
    """Sanity: the ALL_DDL tuple contains the three tables, the indexes,
    and the status seed row, in dependency order. Schema creation
    itself runs in postgres_sync.ensure_schema_exists BEFORE
    apply_migrations, NOT as part of ALL_DDL — see the schema.py
    module docstring."""
    blob = "\n".join(ddl.ALL_DDL).lower()
    assert "create table if not exists commit_cycles" in blob
    assert "create table if not exists files" in blob
    assert "create table if not exists status" in blob
    # Indexes
    assert "commit_cycles_incomplete_idx" in blob
    assert "files_unclaimed_idx" in blob
    assert "files_in_flight_idx" in blob
    # Seed
    assert "insert into status" in blob


def test_all_ddl_uses_unqualified_table_names():
    """Per-schema isolation depends on every table reference being
    unqualified — `commit_cycles` resolves to <schema>.commit_cycles
    via the session's search_path. A regression that re-introduces an
    `icebox.` prefix would break the per-deployment isolation."""
    blob = "\n".join(ddl.ALL_DDL).lower()
    assert "icebox.commit_cycles" not in blob
    assert "icebox.files" not in blob
    assert "icebox.status" not in blob


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


def test_commit_cycles_table_before_files_table():
    """files.cycle_id FK references commit_cycles.cycle_id, so the
    parent table must exist first."""
    cc_idx = ddl.ALL_DDL.index(ddl.CREATE_COMMIT_CYCLES)
    files_idx = ddl.ALL_DDL.index(ddl.CREATE_FILES)
    assert cc_idx < files_idx


def test_indexes_come_after_their_tables():
    """A partial index on a non-existent table would error."""
    cc_idx = ddl.ALL_DDL.index(ddl.CREATE_COMMIT_CYCLES)
    cc_part_idx = ddl.ALL_DDL.index(ddl.CREATE_COMMIT_CYCLES_INCOMPLETE_IDX)
    assert cc_idx < cc_part_idx

    files_idx = ddl.ALL_DDL.index(ddl.CREATE_FILES)
    files_unc_idx = ddl.ALL_DDL.index(ddl.CREATE_FILES_UNCLAIMED_IDX)
    files_inf_idx = ddl.ALL_DDL.index(ddl.CREATE_FILES_IN_FLIGHT_IDX)
    assert files_idx < files_unc_idx
    assert files_idx < files_inf_idx


def test_commit_cycles_has_required_columns():
    """The state-machine columns described in the plan must all be present."""
    sql = ddl.CREATE_COMMIT_CYCLES.lower()
    for col in (
        "cycle_id",
        "started_at",
        "iceberg_snapshot_id",
        "kafka_committed_at",
        "completed_at",
    ):
        assert col in sql, f"missing column {col} from commit_cycles DDL"


def test_files_table_has_required_columns():
    """All POST-body fields + bookkeeping columns must be present."""
    sql = ddl.CREATE_FILES.lower()
    for col in (
        "file_path",
        "writer_ordinal",
        "kafka_offsets",
        "partition_values",
        "record_count",
        "file_size",
        "schema_version",
        "schema_fingerprint",
        "parquet_stats",
        "cycle_id",
        "staged_at",
        "committed_at",
        "iceberg_snapshot_id",
    ):
        assert col in sql, f"missing column {col} from files DDL"


def test_files_has_unique_file_path():
    """UNIQUE(file_path) makes idempotent replay work — same path
    twice gets dedupped at INSERT time, returning 409 to the client."""
    sql = ddl.CREATE_FILES.lower()
    assert re.search(r"file_path\s+text\s+not null\s+unique", sql), \
        "files.file_path must be UNIQUE"


def test_files_has_fk_to_commit_cycles():
    """Without the FK, an files row could reference a cycle_id
    that no commit_cycles row exists for."""
    sql = ddl.CREATE_FILES.lower()
    assert "references commit_cycles" in sql


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


def test_commit_cycle_row_from_dict_minimal():
    """Only cycle_id + started_at are required; the rest get filled
    in as the state machine advances."""
    row = ddl.CommitCycleRow(cycle_id=uuid4(), started_at=datetime.now(UTC))
    assert row.iceberg_snapshot_id is None
    assert row.kafka_committed_at is None
    assert row.completed_at is None


def test_commit_cycle_row_from_dict_full():
    """A fully-completed cycle has all four state columns set."""
    cid = uuid4()
    now = datetime.now(UTC)
    row = ddl.CommitCycleRow(
        cycle_id=cid,
        started_at=now,
        iceberg_snapshot_id=42,
        kafka_committed_at=now,
        completed_at=now,
    )
    assert row.cycle_id == cid
    assert row.iceberg_snapshot_id == 42


def test_commit_cycle_row_from_attributes():
    """Pydantic from_attributes lets us pass a namespace-like object
    (mimicking asyncpg Record / psycopg row.namedtuple)."""

    class Stub:
        cycle_id = uuid4()
        started_at = datetime.now(UTC)
        iceberg_snapshot_id = 99
        kafka_committed_at = None
        completed_at = None

    row = ddl.CommitCycleRow.model_validate(Stub(), from_attributes=True)
    assert row.iceberg_snapshot_id == 99


def test_icebox_file_row_from_dict():
    row = ddl.IceboxFileRow(
        id=1,
        file_path="s3://bucket/data/year=2026/month=06/day=02/hour=10/writer-0-abc.parquet",
        writer_ordinal=0,
        kafka_offsets={"20": 1234567},
        partition_values={"year": 2026, "month": 6, "day": 2, "hour": 10},
        record_count=1000,
        file_size=4096,
        schema_version="v1",
        schema_fingerprint="abc123",
        parquet_stats={"column_sizes": {"1": 1024}},
        cycle_id=None,
        staged_at=datetime.now(UTC),
    )
    assert row.committed_at is None
    assert row.kafka_offsets == {"20": 1234567}


def test_icebox_file_row_rejects_missing_required():
    """schema_fingerprint is required — without it, the writer can
    submit incompatible parquet that the committer would silently
    register."""
    with pytest.raises(Exception):  # ValidationError or similar
        ddl.IceboxFileRow(
            id=1,
            file_path="s3://...",
            writer_ordinal=0,
            kafka_offsets={},
            partition_values={},
            record_count=0,
            file_size=0,
            schema_version="v1",
            # schema_fingerprint omitted!
            parquet_stats={},
            staged_at=datetime.now(UTC),
        )


def test_icebox_file_row_kafka_offsets_keys_are_strings():
    """kafka_offsets is jsonb-keyed; JSON requires string keys. The
    Pydantic type is dict[str, int] to enforce this at the boundary."""
    with pytest.raises(Exception):
        ddl.IceboxFileRow(
            id=1,
            file_path="s3://...",
            writer_ordinal=0,
            # int keys would slip into PG but break Pydantic round-trip
            kafka_offsets={20: 1234567},
            partition_values={"year": 2026},
            record_count=0,
            file_size=0,
            schema_version="v1",
            schema_fingerprint="abc",
            parquet_stats={},
            staged_at=datetime.now(UTC),
        )


def test_icebox_status_row_consecutive_failures_int():
    row = ddl.IceboxStatusRow(
        id=1,
        last_success_at=datetime.now(UTC),
        consecutive_failures=0,
        last_cycle_at=datetime.now(UTC),
        last_committer_heartbeat=datetime.now(UTC),
    )
    assert row.consecutive_failures == 0


def test_icebox_status_row_optional_timestamps():
    """Fresh icebox install: status row has no timestamps until the
    first cycle runs."""
    row = ddl.IceboxStatusRow(id=1, consecutive_failures=0)
    assert row.last_success_at is None
    assert row.last_committer_heartbeat is None


def test_all_ddl_is_tuple_not_list():
    """Immutable so callers can't accidentally append to it."""
    assert isinstance(ddl.ALL_DDL, tuple)
