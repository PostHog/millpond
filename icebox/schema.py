"""PG schema (DDL) + Pydantic models for the icebox PG row shapes.

DDL strings are constants here so the migration runner in
postgres_sync.py can apply them idempotently on container startup
(CREATE TABLE IF NOT EXISTS).

DDL is split into separate statements (one per CREATE) so the runner
can execute them one at a time with clear error attribution if a
migration partially fails.
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

# ---------------------------------------------------------------------------
# DDL — applied via psycopg in the migration runner
# ---------------------------------------------------------------------------
#
# All references below are UNQUALIFIED (no `icebox.` prefix). Each
# icebox deployment owns its own PG schema (configured via
# ICEBOX_PG_SCHEMA, default "icebox") and runs all its connections
# with `options=-csearch_path=<schema>` so unqualified references
# resolve to that schema. This is what makes "one icebox per
# (topic, table) sharing a backing PG instance" cleanly isolated —
# events runs against icebox_events.icebox_files, person runs against
# icebox_person.icebox_files, and neither can accidentally read the
# other's rows because the unqualified `icebox_files` only matches the
# schema in their own connection's search_path.
#
# The CREATE SCHEMA itself runs in ensure_schema_exists (with the
# schema name interpolated as a validated identifier) BEFORE these
# DDLs are applied. Hence no CREATE SCHEMA entry in ALL_DDL.

# `result` is an explicit enum-as-text rather than a separate
# `committed_at`/`failed_at` pair so the SELECT predicate stays simple
# (`result='pending'`) and adding new states later is a non-event.
#
# `icebox_files_pending_idx` is a partial index on `inserted_at`
# filtered by `result='pending'`. The daemon's hot SELECT is bounded
# by O(pending), not O(history), and the index doesn't bloat as
# `result='committed'` rows accumulate over the table's lifetime.

CREATE_ICEBOX_FILES = """
CREATE TABLE IF NOT EXISTS icebox_files (
    id                   bigserial PRIMARY KEY,
    file_path            text NOT NULL UNIQUE,
    writer_ordinal       int NOT NULL,
    kafka_offsets        jsonb NOT NULL,
    partition_values     jsonb NOT NULL,
    record_count         bigint NOT NULL,
    file_size            bigint NOT NULL,
    parquet_stats        jsonb NOT NULL,
    inserted_at          timestamptz NOT NULL DEFAULT now(),
    result               text NOT NULL DEFAULT 'pending'
        CHECK (result IN ('pending', 'committed', 'failed')),
    result_at            timestamptz,
    iceberg_snapshot_id  bigint
)
"""

CREATE_ICEBOX_FILES_PENDING_IDX = """
CREATE INDEX IF NOT EXISTS icebox_files_pending_idx
    ON icebox_files (inserted_at)
    WHERE result = 'pending'
"""

# Singleton status row: the daemon stamps last_committer_heartbeat
# every tick; /healthz reads it for the k8s liveness probe.
CREATE_STATUS = """
CREATE TABLE IF NOT EXISTS status (
    id                            int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_committer_heartbeat      timestamptz
)
"""

SEED_STATUS_ROW = "INSERT INTO status (id) VALUES (1) ON CONFLICT DO NOTHING"

# Order matters: tables before indexes (the partial index references
# the table). CREATE SCHEMA is NOT in this tuple — it runs in
# postgres_sync.ensure_schema_exists before the migration runner
# connects.
ALL_DDL: tuple[str, ...] = (
    CREATE_ICEBOX_FILES,
    CREATE_ICEBOX_FILES_PENDING_IDX,
    CREATE_STATUS,
    SEED_STATUS_ROW,
)


# ---------------------------------------------------------------------------
# Pydantic row models
# ---------------------------------------------------------------------------


class IceboxPendingFileRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    file_path: str
    writer_ordinal: int
    kafka_offsets: dict[str, int]
    partition_values: dict[str, int]
    record_count: int
    file_size: int
    parquet_stats: dict
    inserted_at: datetime
    result: str  # 'pending' | 'committed' | 'failed'
    result_at: datetime | None = None
    iceberg_snapshot_id: int | None = None


class IceboxStatusRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    last_committer_heartbeat: datetime | None = None
