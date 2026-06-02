"""PG schema (DDL) + Pydantic models for the icebox PG row shapes.

DDL strings are constants here so the migration runner in
postgres_sync.py can apply them idempotently on container startup
(CREATE TABLE IF NOT EXISTS). Pydantic models mirror the row shapes
so handlers and the committer can construct typed objects from raw
asyncpg/psycopg records without manual field plucking.

DDL is split into separate statements (one per CREATE) so the runner
can execute them one at a time with clear error attribution if a
migration partially fails.
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# DDL — applied via psycopg in the migration runner
# ---------------------------------------------------------------------------

CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS icebox"

CREATE_COMMIT_CYCLES = """
CREATE TABLE IF NOT EXISTS icebox.commit_cycles (
    cycle_id             uuid PRIMARY KEY,
    started_at           timestamptz NOT NULL DEFAULT now(),
    iceberg_snapshot_id  bigint,
    kafka_committed_at   timestamptz,
    completed_at         timestamptz
)
"""

CREATE_COMMIT_CYCLES_INCOMPLETE_IDX = """
CREATE INDEX IF NOT EXISTS commit_cycles_incomplete_idx
    ON icebox.commit_cycles (started_at)
    WHERE completed_at IS NULL
"""

CREATE_FILES = """
CREATE TABLE IF NOT EXISTS icebox.files (
    id                   bigserial PRIMARY KEY,
    file_path            text NOT NULL UNIQUE,
    writer_ordinal       int NOT NULL,
    kafka_offsets        jsonb NOT NULL,
    partition_values     jsonb NOT NULL,
    record_count         bigint NOT NULL,
    file_size            bigint NOT NULL,
    schema_version       text NOT NULL,
    schema_fingerprint   text NOT NULL,
    parquet_stats        jsonb NOT NULL,
    cycle_id             uuid REFERENCES icebox.commit_cycles(cycle_id),
    staged_at            timestamptz NOT NULL DEFAULT now(),
    committed_at         timestamptz,
    iceberg_snapshot_id  bigint
)
"""

CREATE_FILES_UNCLAIMED_IDX = """
CREATE INDEX IF NOT EXISTS files_unclaimed_idx
    ON icebox.files (staged_at)
    WHERE committed_at IS NULL AND cycle_id IS NULL
"""

CREATE_FILES_IN_FLIGHT_IDX = """
CREATE INDEX IF NOT EXISTS files_in_flight_idx
    ON icebox.files (cycle_id)
    WHERE committed_at IS NULL AND cycle_id IS NOT NULL
"""

CREATE_STATUS = """
CREATE TABLE IF NOT EXISTS icebox.status (
    id                            int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_success_at               timestamptz,
    consecutive_failures          int NOT NULL DEFAULT 0,
    last_cycle_at                 timestamptz,
    last_committer_heartbeat      timestamptz
)
"""

SEED_STATUS_ROW = "INSERT INTO icebox.status (id) VALUES (1) ON CONFLICT DO NOTHING"

# Order matters: schema first, parent tables before children, indexes after
# their tables. The runner executes these in this exact order.
ALL_DDL: tuple[str, ...] = (
    CREATE_SCHEMA,
    CREATE_COMMIT_CYCLES,
    CREATE_COMMIT_CYCLES_INCOMPLETE_IDX,
    CREATE_FILES,
    CREATE_FILES_UNCLAIMED_IDX,
    CREATE_FILES_IN_FLIGHT_IDX,
    CREATE_STATUS,
    SEED_STATUS_ROW,
)


# ---------------------------------------------------------------------------
# Pydantic row models — mirror PG row shapes for typed access
# ---------------------------------------------------------------------------


class CommitCycleRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    cycle_id: UUID
    started_at: datetime
    iceberg_snapshot_id: int | None = None
    kafka_committed_at: datetime | None = None
    completed_at: datetime | None = None


class IceboxFileRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    file_path: str
    writer_ordinal: int
    kafka_offsets: dict[str, int]
    partition_values: dict[str, int]
    record_count: int
    file_size: int
    schema_version: str
    schema_fingerprint: str
    parquet_stats: dict
    cycle_id: UUID | None = None
    staged_at: datetime
    committed_at: datetime | None = None
    iceberg_snapshot_id: int | None = None


class IceboxStatusRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    last_success_at: datetime | None = None
    consecutive_failures: int
    last_cycle_at: datetime | None = None
    last_committer_heartbeat: datetime | None = None
