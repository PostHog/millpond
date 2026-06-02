"""Async asyncpg pool + query helpers for the FastAPI side.

The REST API runs as a normal FastAPI async server; all PG access from
handlers goes through this module. The committer thread does NOT touch
the asyncpg pool — it uses postgres_sync.py.

Pool sizing: asyncpg_pool_min=2, asyncpg_pool_max=8 by default. POST
/v1/files is fast (single INSERT); GET /v1/status is a couple of
SELECTs; GET /readyz is one ping. 8 conns is generous for a single
worker process.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import asyncpg

from icebox.config import Config
from shared.models import RegisteredFile, RegisterFileRequest, StatusResponse

log = logging.getLogger(__name__)


async def build_asyncpg_pool(cfg: Config) -> asyncpg.Pool:
    """Construct the asyncpg pool the FastAPI handlers share.

    Initialized lazily — connections open on first use. Caller is
    expected to call .close() on shutdown.
    """
    pool = await asyncpg.create_pool(
        host=cfg.pg_host,
        port=cfg.pg_port,
        database=cfg.pg_database,
        user=cfg.pg_username,
        password=cfg.pg_password,
        ssl=cfg.pg_sslmode if cfg.pg_sslmode != "disable" else None,
        min_size=cfg.asyncpg_pool_min,
        max_size=cfg.asyncpg_pool_max,
    )
    return pool


# ---------------------------------------------------------------------------
# POST /v1/files
# ---------------------------------------------------------------------------


INSERT_FILE_SQL = """
INSERT INTO icebox.files (
    file_path, writer_ordinal, kafka_offsets, partition_values,
    record_count, file_size, schema_version, schema_fingerprint,
    parquet_stats
) VALUES (
    $1, $2, $3::jsonb, $4::jsonb, $5, $6, $7, $8, $9::jsonb
)
ON CONFLICT (file_path) DO NOTHING
RETURNING id, staged_at
"""

LOOKUP_EXISTING_SQL = """
SELECT id, staged_at FROM icebox.files WHERE file_path = $1
"""


async def insert_file(
    pool: asyncpg.Pool,
    req: RegisterFileRequest,
) -> tuple[RegisteredFile, bool]:
    """Insert a row in icebox.files.

    Returns:
        (RegisteredFile, was_new): was_new=True ⇒ 201; False ⇒ 409
        (replay; same file_path already registered). The RegisteredFile
        body is identical in both cases, so writers can treat 409 as
        success.

    INSERT ON CONFLICT DO NOTHING + RETURNING means the happy path is
    a single round-trip. On conflict, a second SELECT fetches the
    existing row to return its id/queued_at — so 409 costs an extra
    round-trip, but 409s are rare (writer crash + replay).
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            INSERT_FILE_SQL,
            req.file_path,
            req.writer_ordinal,
            json.dumps({k: v for k, v in req.kafka_offsets.items()}),
            json.dumps({k: v for k, v in req.partition_values.items()}),
            req.record_count,
            req.file_size,
            req.schema_version,
            req.schema_fingerprint,
            req.parquet_stats.model_dump_json(),
        )
        if row is not None:
            return RegisteredFile(row_id=row["id"], queued_at=row["staged_at"]), True
        # Conflict: look up the existing row for the same shape response.
        existing = await conn.fetchrow(LOOKUP_EXISTING_SQL, req.file_path)
        if existing is None:
            raise RuntimeError(
                f"INSERT...DO NOTHING returned no row AND lookup returned no row "
                f"for file_path={req.file_path!r}: this should be impossible "
                f"without a concurrent DELETE, which icebox never does"
            )
        return RegisteredFile(row_id=existing["id"], queued_at=existing["staged_at"]), False


# ---------------------------------------------------------------------------
# GET /v1/status — backpressure + observability
# ---------------------------------------------------------------------------


# Hot-path status query — runs once per POST (heartbeat-stale check is
# the FIRST backpressure check, so this is the per-POST gate). Must
# stay cheap. Specifically: NO subquery against icebox.commit_cycles —
# that table grows ~1 row per cadence and an unindexed MAX would become
# a sequential scan over time.
STATUS_QUERY_SQL = """
SELECT
    (SELECT count(*) FROM icebox.files
        WHERE committed_at IS NULL AND cycle_id IS NULL)
        AS pending_files,
    (SELECT EXTRACT(EPOCH FROM (now() - min(staged_at))) FROM icebox.files
        WHERE committed_at IS NULL AND cycle_id IS NULL)
        AS oldest_pending_age_seconds,
    s.last_success_at, s.last_cycle_at, s.last_committer_heartbeat,
    s.consecutive_failures
FROM icebox.status s
WHERE s.id = 1
"""

# Augmented status — only used by GET /v1/status (operator-facing
# observability), NOT by the POST hot path. Runs the
# last_committed_iceberg_snapshot subquery in a second round-trip via
# read_status_full().
LAST_COMMITTED_SNAPSHOT_SQL = """
SELECT max(iceberg_snapshot_id) AS last_committed_iceberg_snapshot
FROM icebox.commit_cycles
WHERE iceberg_snapshot_id IS NOT NULL
"""


async def read_status(pool: asyncpg.Pool) -> StatusResponse:
    """Cheap status snapshot — hot path. Used by the POST handler's
    backpressure checks AND by /readyz.

    last_committed_iceberg_snapshot is NOT included here (it's the only
    field whose subquery doesn't have an index and grows linearly with
    cycle history). Use `read_status_full` from the observability
    endpoint if that field is needed.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(STATUS_QUERY_SQL)
    if row is None:
        raise RuntimeError(
            "icebox.status row missing — DDL migration must run before "
            "the API can serve traffic (apply_migrations seeds row id=1)"
        )
    return StatusResponse(
        pending_files=row["pending_files"] or 0,
        oldest_pending_age_seconds=(
            float(row["oldest_pending_age_seconds"])
            if row["oldest_pending_age_seconds"] is not None
            else None
        ),
        last_success_at=row["last_success_at"],
        last_cycle_at=row["last_cycle_at"],
        last_committer_heartbeat=row["last_committer_heartbeat"],
        consecutive_failures=row["consecutive_failures"] or 0,
        last_committed_iceberg_snapshot=None,  # set by read_status_full
    )


async def read_status_full(pool: asyncpg.Pool) -> StatusResponse:
    """Augmented status snapshot — observability path (GET /v1/status).
    Does an extra round-trip for last_committed_iceberg_snapshot, which
    is fine for the human-facing endpoint but not for per-POST checks."""
    base = await read_status(pool)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(LAST_COMMITTED_SNAPSHOT_SQL)
    last_snapshot = row["last_committed_iceberg_snapshot"] if row else None
    return StatusResponse(
        pending_files=base.pending_files,
        oldest_pending_age_seconds=base.oldest_pending_age_seconds,
        last_success_at=base.last_success_at,
        last_cycle_at=base.last_cycle_at,
        last_committer_heartbeat=base.last_committer_heartbeat,
        consecutive_failures=base.consecutive_failures,
        last_committed_iceberg_snapshot=last_snapshot,
    )


# ---------------------------------------------------------------------------
# Backpressure-decision helpers — used inside the POST handler
# ---------------------------------------------------------------------------


def is_heartbeat_stale(
    last_heartbeat: datetime | None,
    *,
    now: datetime,
    cadence_seconds: int,
    stale_multiple: float,
) -> bool:
    """Did the committer thread fail to write a heartbeat within
    `stale_multiple × cadence_seconds`?

    None last_heartbeat ⇒ fresh install or first-cycle pending; treat
    as NOT stale (don't preemptively 503).

    Pure function so it's trivially unit-testable.
    """
    if last_heartbeat is None:
        return False
    threshold_seconds = cadence_seconds * stale_multiple
    age = (now - last_heartbeat).total_seconds()
    return age > threshold_seconds


def should_reject_for_queue_depth(
    pending_files: int,
    *,
    max_pending: int,
) -> bool:
    """Backpressure: if there are too many unclaimed files queued, the
    writer should slow down (429). Threshold is configurable via
    cfg.committer_max_pending_files.
    """
    return pending_files >= max_pending


def should_reject_for_degraded(
    consecutive_failures: int,
    *,
    degraded_threshold: int,
) -> bool:
    """Backpressure: if the committer has hit the degraded threshold,
    we 503 incoming writes (the data is durable in PG either way, but
    we want to signal the writer to slow down)."""
    return consecutive_failures >= degraded_threshold
