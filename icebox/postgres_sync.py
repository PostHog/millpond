"""Sync psycopg connection + state-machine queries for the committer.

The committer runs in a dedicated thread (NOT an asyncio task) because
PyIceberg's commit path is synchronous; calling it from within FastAPI's
asyncio loop would block incoming POST handlers. See ICEBOX-PLAN.md
"Async-vs-sync inside the icebox process".

This module exposes:
  - `build_psycopg_pool` — connection pool sized small (defaults to
    psycopg_pool_min=1, max=2). The committer rarely needs more than
    one connection at a time; the second slot is for the migration
    runner and the (sync) heartbeat writer if any other sync caller
    ever pops up.
  - `apply_migrations` — applies ALL_DDL in dependency order with
    per-statement error attribution.
  - State-machine helpers: claim_files, insert_cycle,
    mark_iceberg_committed, mark_kafka_committed, complete_cycle,
    incomplete_cycles, update_heartbeat, record_failure, record_success.

SQL invariants:
  - All UPDATEs target one row by primary key (cycle_id or file id) so
    they're index-only and lock-bounded.
  - The "claim" UPDATE uses FOR UPDATE SKIP LOCKED so two recovering
    committers don't double-claim the same unclaimed files.
  - All state-mutating helpers run inside a single transaction the
    caller opens and commits; this lets the committer batch
    cycle-progression + status updates atomically when it makes sense.
"""
from __future__ import annotations

import logging
from collections.abc import Sequence
from uuid import UUID

import psycopg
from psycopg_pool import ConnectionPool

from icebox.config import Config
from icebox.schema import ALL_DDL, CommitCycleRow

log = logging.getLogger(__name__)


def build_psycopg_pool(cfg: Config) -> ConnectionPool:
    """Construct the sync psycopg connection pool the committer uses.

    Sized per cfg.psycopg_pool_{min,max}; defaults are 1 and 2 (the
    committer is single-threaded — one connection at a time — and the
    second slot is a buffer for sync admin operations).
    """
    conninfo = psycopg.conninfo.make_conninfo(
        host=cfg.pg_host,
        port=cfg.pg_port,
        dbname=cfg.pg_database,
        user=cfg.pg_username,
        password=cfg.pg_password,
        sslmode=cfg.pg_sslmode,
    )
    pool = ConnectionPool(
        conninfo,
        min_size=cfg.psycopg_pool_min,
        max_size=cfg.psycopg_pool_max,
        open=False,  # caller opens explicitly to fail-loud at boot
    )
    return pool


def apply_migrations(conn: psycopg.Connection) -> None:
    """Apply icebox.* schema DDL idempotently.

    Each statement is a CREATE IF NOT EXISTS or INSERT ON CONFLICT DO
    NOTHING, so this is safe to re-run on every container start.

    Runs each statement separately so PG error messages can attribute
    failures to a specific DDL statement, not a multi-statement batch.
    """
    with conn.cursor() as cur:
        for i, stmt in enumerate(ALL_DDL):
            preview = stmt.strip().split("\n", 1)[0][:80]
            log.info("apply_migrations: %d/%d: %s", i + 1, len(ALL_DDL), preview)
            try:
                cur.execute(stmt)
            except psycopg.Error as e:
                raise RuntimeError(
                    f"DDL statement {i + 1}/{len(ALL_DDL)} failed: {preview}: {e}"
                ) from e
    conn.commit()


# ---------------------------------------------------------------------------
# Cycle state machine — SQL constants + helpers
# ---------------------------------------------------------------------------


CLAIM_FILES_SQL = """
WITH candidates AS (
    SELECT id FROM icebox.files
    WHERE cycle_id IS NULL AND committed_at IS NULL
    ORDER BY staged_at
    LIMIT %(max_files)s
    FOR UPDATE SKIP LOCKED
)
UPDATE icebox.files f
SET cycle_id = %(cycle_id)s
FROM candidates c
WHERE f.id = c.id
RETURNING f.id
"""


def claim_files(
    conn: psycopg.Connection,
    *,
    cycle_id: UUID,
    max_files: int,
) -> list[int]:
    """Atomically claim up to `max_files` unclaimed files for this cycle.

    Returns the list of file ids claimed. Empty list means there's
    nothing pending (vacuous cycle — committer should not proceed).

    Uses FOR UPDATE SKIP LOCKED so two recovering committers running
    concurrently don't double-claim the same rows. v1 runs a single
    committer replica but the SKIP LOCKED is cheap insurance.
    """
    with conn.cursor() as cur:
        cur.execute(CLAIM_FILES_SQL, {"cycle_id": cycle_id, "max_files": max_files})
        return [row[0] for row in cur.fetchall()]


INSERT_CYCLE_SQL = """
INSERT INTO icebox.commit_cycles (cycle_id) VALUES (%(cycle_id)s)
"""


def insert_cycle(conn: psycopg.Connection, *, cycle_id: UUID) -> None:
    """Insert a fresh commit_cycles row at the start of a cycle.

    started_at gets the PG default `now()`. No other state columns are
    populated until the cycle progresses.
    """
    with conn.cursor() as cur:
        cur.execute(INSERT_CYCLE_SQL, {"cycle_id": cycle_id})


MARK_ICEBERG_COMMITTED_SQL = """
UPDATE icebox.commit_cycles
SET iceberg_snapshot_id = %(snapshot_id)s
WHERE cycle_id = %(cycle_id)s
"""


def mark_iceberg_committed(
    conn: psycopg.Connection,
    *,
    cycle_id: UUID,
    snapshot_id: int,
) -> None:
    """Record the Iceberg snapshot ID after a successful PyIceberg
    commit. After this point, recovery can skip the Iceberg-commit step
    on retry.
    """
    with conn.cursor() as cur:
        cur.execute(
            MARK_ICEBERG_COMMITTED_SQL,
            {"cycle_id": cycle_id, "snapshot_id": snapshot_id},
        )


MARK_KAFKA_COMMITTED_SQL = """
UPDATE icebox.commit_cycles
SET kafka_committed_at = now()
WHERE cycle_id = %(cycle_id)s
"""


def mark_kafka_committed(conn: psycopg.Connection, *, cycle_id: UUID) -> None:
    """Record Kafka offset-commit success. After this, the cycle just
    needs file-row finalization."""
    with conn.cursor() as cur:
        cur.execute(MARK_KAFKA_COMMITTED_SQL, {"cycle_id": cycle_id})


COMPLETE_CYCLE_SQL = """
UPDATE icebox.commit_cycles
SET completed_at = now()
WHERE cycle_id = %(cycle_id)s
"""

MARK_FILES_COMMITTED_SQL = """
UPDATE icebox.files
SET committed_at = now(), iceberg_snapshot_id = %(snapshot_id)s
WHERE cycle_id = %(cycle_id)s
"""


def complete_cycle(
    conn: psycopg.Connection,
    *,
    cycle_id: UUID,
    snapshot_id: int,
) -> None:
    """Mark the cycle complete and propagate the snapshot_id to its
    files. Called as the final step after Iceberg + Kafka commits."""
    with conn.cursor() as cur:
        cur.execute(
            MARK_FILES_COMMITTED_SQL,
            {"cycle_id": cycle_id, "snapshot_id": snapshot_id},
        )
        cur.execute(COMPLETE_CYCLE_SQL, {"cycle_id": cycle_id})


INCOMPLETE_CYCLES_SQL = """
SELECT cycle_id, started_at, iceberg_snapshot_id, kafka_committed_at, completed_at
FROM icebox.commit_cycles
WHERE completed_at IS NULL
ORDER BY started_at
LIMIT %(limit)s
"""

# Safety bound for recovery scans. If we ever return this many rows
# we're in trouble (stuck cycles accumulating) and ops should page.
# Surfaced as a function-arg default below so tests can pin smaller.
INCOMPLETE_CYCLES_LIMIT_DEFAULT = 100


def incomplete_cycles(
    conn: psycopg.Connection,
    *,
    limit: int = INCOMPLETE_CYCLES_LIMIT_DEFAULT,
) -> list[CommitCycleRow]:
    """Return rows for cycles that haven't reached completed_at — the
    recovery scan.

    Indexed by commit_cycles_incomplete_idx (partial index on
    completed_at IS NULL), so even in deep history this is O(in-flight)
    not O(history).
    """
    with conn.cursor() as cur:
        cur.execute(INCOMPLETE_CYCLES_SQL, {"limit": limit})
        rows = cur.fetchall()
    return [
        CommitCycleRow(
            cycle_id=r[0],
            started_at=r[1],
            iceberg_snapshot_id=r[2],
            kafka_committed_at=r[3],
            completed_at=r[4],
        )
        for r in rows
    ]


UPDATE_HEARTBEAT_SQL = """
UPDATE icebox.status SET last_committer_heartbeat = now() WHERE id = 1
"""


def update_heartbeat(conn: psycopg.Connection) -> None:
    """Stamp the committer's liveness heartbeat. The async API reads
    this and rejects POSTs with 503 if the heartbeat is stale (more
    than cfg.committer_heartbeat_stale_multiple × cadence_seconds old).
    """
    with conn.cursor() as cur:
        cur.execute(UPDATE_HEARTBEAT_SQL)


RECORD_FAILURE_SQL = """
UPDATE icebox.status
SET consecutive_failures = consecutive_failures + 1
WHERE id = 1
"""

RECORD_SUCCESS_SQL = """
UPDATE icebox.status
SET consecutive_failures = 0,
    last_success_at = now(),
    last_cycle_at = now()
WHERE id = 1
"""


def record_failure(conn: psycopg.Connection) -> None:
    """Increment consecutive_failures. Crossing the degraded threshold
    flips POST handlers to 503-degraded mode."""
    with conn.cursor() as cur:
        cur.execute(RECORD_FAILURE_SQL)


def record_success(conn: psycopg.Connection) -> None:
    """Reset consecutive_failures to 0 and stamp last_success_at +
    last_cycle_at to now()."""
    with conn.cursor() as cur:
        cur.execute(RECORD_SUCCESS_SQL)


FILES_FOR_CYCLE_SQL = """
SELECT id, file_path, writer_ordinal, kafka_offsets, partition_values,
       record_count, file_size, schema_version, schema_fingerprint,
       parquet_stats, cycle_id, staged_at, committed_at, iceberg_snapshot_id
FROM icebox.files
WHERE cycle_id = %(cycle_id)s
ORDER BY id
"""


def files_for_cycle(
    conn: psycopg.Connection,
    *,
    cycle_id: UUID,
) -> Sequence[tuple]:
    """Return all rows claimed by `cycle_id`. The committer builds
    DataFiles from each row. Used both in the steady-state path AND in
    the recovery path."""
    with conn.cursor() as cur:
        cur.execute(FILES_FOR_CYCLE_SQL, {"cycle_id": cycle_id})
        return cur.fetchall()


RELEASE_CYCLE_CLAIM_SQL = """
UPDATE icebox.files
SET cycle_id = NULL
WHERE cycle_id = %(cycle_id)s AND committed_at IS NULL
"""

DELETE_CYCLE_ROW_SQL = """
DELETE FROM icebox.commit_cycles WHERE cycle_id = %(cycle_id)s
"""


def release_cycle_claim(conn: psycopg.Connection, *, cycle_id: UUID) -> None:
    """Release a failed cycle's file claims so the next cycle can re-claim.

    Used when the cycle failed before reaching mark_iceberg_committed:
    no Iceberg snapshot exists, so the files are safe to re-batch into
    a fresh cycle_id. If we already recorded an iceberg_snapshot_id on
    the cycle row, the recovery path completes the cycle instead of
    releasing.
    """
    with conn.cursor() as cur:
        cur.execute(RELEASE_CYCLE_CLAIM_SQL, {"cycle_id": cycle_id})


def delete_cycle_row(conn: psycopg.Connection, *, cycle_id: UUID) -> None:
    """Delete a commit_cycles row that never produced a snapshot. Used
    in the released-no-iceberg recovery branch: the cycle didn't
    happen from Iceberg's perspective, so the bookkeeping row is
    just a zombie. Deleting it prevents accumulation against the
    incomplete_cycles LIMIT.
    """
    with conn.cursor() as cur:
        cur.execute(DELETE_CYCLE_ROW_SQL, {"cycle_id": cycle_id})
