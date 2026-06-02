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

import hashlib
import logging
from collections.abc import Sequence
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg_pool import ConnectionPool

from icebox.config import Config
from icebox.schema import ALL_DDL, CommitCycleRow

log = logging.getLogger(__name__)




def ensure_database_exists(cfg: Config) -> None:
    """Tactical boot-time bootstrap: if the configured PG database
    doesn't exist, create it.

    Without this, a fresh icebox deployment boot-loops on
    "database does not exist" because the migration runner runs
    AGAINST the configured database — it can't create the database
    it's connecting to.

    The proper place for this is Terraform (provision the database
    when provisioning the icebox). This is a stopgap so the dev
    rollout doesn't require coordinating two PRs.

    Flow:
      1. Connect to the `postgres` system database (always exists
         in PG) and SELECT from pg_database to check existence.
      2. If the target DB exists, return.
      3. If not, CREATE DATABASE via the same `postgres` connection.
      4. Race tolerance: if CREATE DATABASE returns 42P04 (duplicate),
         treat as success (another icebox replica won the race).
      5. Insufficient-privilege / connect failures re-raise — those
         are signals for ops, not states we can paper over here.

    The two-step approach (check + create) is used instead of
    "try connect, fall back on error" because psycopg's
    OperationalError from `connect()` doesn't carry sqlstate for
    connection-time failures; string-matching the message is
    fragile across versions and locales.
    """
    conninfo_postgres = psycopg.conninfo.make_conninfo(
        host=cfg.pg_host,
        port=cfg.pg_port,
        dbname="postgres",
        user=cfg.pg_username,
        password=cfg.pg_password,
        sslmode=cfg.pg_sslmode,
        connect_timeout=10,
    )
    # CREATE DATABASE cannot run inside a transaction block; we open
    # the system connection with autocommit so the optional CREATE
    # below works. The check query is also fine under autocommit.
    with psycopg.connect(conninfo_postgres, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (cfg.pg_database,),
            )
            if cur.fetchone() is not None:
                log.info(
                    "ensure_database_exists: database %r already exists, "
                    "no bootstrap needed",
                    cfg.pg_database,
                )
                return

            log.warning(
                "ensure_database_exists: database %r does not exist; "
                "attempting CREATE DATABASE. This is a TACTICAL BOOT-"
                "TIME BOOTSTRAP — proper provisioning belongs in Terraform.",
                cfg.pg_database,
            )
            stmt = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cfg.pg_database))
            try:
                cur.execute(stmt)
            except psycopg.errors.DuplicateDatabase:
                # Race: another icebox replica created it between the
                # SELECT and the CREATE. Treat as success.
                log.info(
                    "ensure_database_exists: database %r was created "
                    "concurrently — proceeding",
                    cfg.pg_database,
                )
                return
            except psycopg.errors.InsufficientPrivilege as e:
                # The icebox PG user lacks CREATEDB. Bootstrap can't
                # paper over privilege — surface a clear, actionable
                # error so operators don't have to dig through libpq
                # stack traces. PE-review #5.
                raise RuntimeError(
                    f"ensure_database_exists: user {cfg.pg_username!r} "
                    f"lacks CREATEDB privilege (cannot create database "
                    f"{cfg.pg_database!r}). Operator: run "
                    f"`ALTER ROLE {cfg.pg_username} CREATEDB;` as a "
                    f"superuser, or provision the database manually via "
                    f"Terraform. Original error: {e}"
                ) from e
    log.info("ensure_database_exists: created database %r", cfg.pg_database)


def ensure_schema_exists(cfg: Config) -> None:
    """Create the icebox-owned schema if it doesn't exist.

    Each icebox deployment owns its own PG schema (events runs against
    `icebox_events`, person against `icebox_person`, etc.). The schema
    name is interpolated below as a validated identifier — the config
    loader enforces `[a-zA-Z_][a-zA-Z0-9_]{0,62}` at boot, so this is
    safe even though we can't parameterize the schema name via
    psycopg's bind syntax (PG protocol doesn't allow it for DDL).

    Runs AFTER ensure_database_exists; the schema lives inside that
    database. Connects with the icebox user's credentials, which must
    have CREATE-on-database privilege. Insufficient-privilege errors
    re-raise — that's an ops signal, not something we can paper over.
    """
    conninfo = psycopg.conninfo.make_conninfo(
        host=cfg.pg_host,
        port=cfg.pg_port,
        dbname=cfg.pg_database,
        user=cfg.pg_username,
        password=cfg.pg_password,
        sslmode=cfg.pg_sslmode,
        connect_timeout=10,
    )
    # Autocommit because we follow the same pattern as
    # ensure_database_exists (one-off short-lived bootstrap conn).
    # CREATE SCHEMA itself runs fine in or out of a transaction.
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            # PE-review #8: short-circuit if the schema already exists.
            # Every pod restart otherwise pays a CREATE SCHEMA round-trip
            # (cheap server-side, but unnecessary catalog touch).
            cur.execute(
                "SELECT 1 FROM information_schema.schemata "
                "WHERE schema_name = %s",
                (cfg.pg_schema,),
            )
            if cur.fetchone() is not None:
                log.debug(
                    "ensure_schema_exists: schema %r already exists in "
                    "database %r (no-op)",
                    cfg.pg_schema, cfg.pg_database,
                )
                return

            stmt = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(cfg.pg_schema)
            )
            try:
                cur.execute(stmt)
            except psycopg.errors.InsufficientPrivilege as e:
                # The icebox user lacks CREATE-on-database. Surface a
                # clear, actionable error. PE-review #5.
                raise RuntimeError(
                    f"ensure_schema_exists: user {cfg.pg_username!r} "
                    f"lacks CREATE privilege on database "
                    f"{cfg.pg_database!r} (cannot create schema "
                    f"{cfg.pg_schema!r}). Operator: run "
                    f"`GRANT CREATE ON DATABASE {cfg.pg_database} TO "
                    f"{cfg.pg_username};` as a superuser. "
                    f"Original error: {e}"
                ) from e
    log.info(
        "ensure_schema_exists: created schema %r in database %r",
        cfg.pg_schema,
        cfg.pg_database,
    )


def build_psycopg_pool(cfg: Config) -> ConnectionPool:
    """Construct the sync psycopg connection pool the committer uses.

    Sized per cfg.psycopg_pool_{min,max}; defaults are 1 and 2 (the
    committer is single-threaded — one connection at a time — and the
    second slot is a buffer for sync admin operations).

    Every connection in this pool is automatically pinned to the
    configured schema via `options=-csearch_path=<schema>` in the
    conninfo. That lets all SQL stay UNQUALIFIED — `commit_cycles`
    resolves to `<schema>.commit_cycles` per the session's search_path.
    Schema names are validated as identifiers at config load
    (see icebox/config.py:_SAFE_PG_IDENTIFIER) so the f-string here
    is injection-safe.
    """
    conninfo = psycopg.conninfo.make_conninfo(
        host=cfg.pg_host,
        port=cfg.pg_port,
        dbname=cfg.pg_database,
        user=cfg.pg_username,
        password=cfg.pg_password,
        sslmode=cfg.pg_sslmode,
        options=f"-csearch_path={cfg.pg_schema}",
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
    SELECT id FROM files
    WHERE cycle_id IS NULL AND committed_at IS NULL
    ORDER BY staged_at
    LIMIT %(max_files)s
    FOR UPDATE SKIP LOCKED
)
UPDATE files f
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
INSERT INTO commit_cycles (cycle_id) VALUES (%(cycle_id)s)
"""


def insert_cycle(conn: psycopg.Connection, *, cycle_id: UUID) -> None:
    """Insert a fresh commit_cycles row at the start of a cycle.

    started_at gets the PG default `now()`. No other state columns are
    populated until the cycle progresses.
    """
    with conn.cursor() as cur:
        cur.execute(INSERT_CYCLE_SQL, {"cycle_id": cycle_id})


MARK_ICEBERG_COMMITTED_SQL = """
UPDATE commit_cycles
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
UPDATE commit_cycles
SET kafka_committed_at = now()
WHERE cycle_id = %(cycle_id)s
"""


def mark_kafka_committed(conn: psycopg.Connection, *, cycle_id: UUID) -> None:
    """Record Kafka offset-commit success. After this, the cycle just
    needs file-row finalization."""
    with conn.cursor() as cur:
        cur.execute(MARK_KAFKA_COMMITTED_SQL, {"cycle_id": cycle_id})


COMPLETE_CYCLE_SQL = """
UPDATE commit_cycles
SET completed_at = now()
WHERE cycle_id = %(cycle_id)s
"""

MARK_FILES_COMMITTED_SQL = """
UPDATE files
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
FROM commit_cycles
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
UPDATE status SET last_committer_heartbeat = now() WHERE id = 1
"""


def update_heartbeat(conn: psycopg.Connection) -> None:
    """Stamp the committer's liveness heartbeat. The async API reads
    this and rejects POSTs with 503 if the heartbeat is stale (more
    than cfg.committer_heartbeat_stale_multiple × cadence_seconds old).
    """
    with conn.cursor() as cur:
        cur.execute(UPDATE_HEARTBEAT_SQL)


RECORD_FAILURE_SQL = """
UPDATE status
SET consecutive_failures = consecutive_failures + 1
WHERE id = 1
"""

RECORD_SUCCESS_SQL = """
UPDATE status
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
FROM files
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
UPDATE files
SET cycle_id = NULL
WHERE cycle_id = %(cycle_id)s AND committed_at IS NULL
"""

DELETE_CYCLE_ROW_SQL = """
DELETE FROM commit_cycles WHERE cycle_id = %(cycle_id)s
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


# Advisory-lock key namespace for the icebox committer.
#
# PG advisory locks are 64-bit signed ints, shared across the whole
# database. Multiple iceboxes share one PG instance (one schema per
# deployment); the LOCK ID must be PER-SCHEMA so an events icebox and
# a person icebox can each hold their own lock without conflict.
#
# Derivation: SHA-256 of f"posthog.icebox.committer.{schema}", take
# the first 8 bytes, interpret as signed int8 (PG advisory_lock
# parameter type). Stable across runs, distinct per schema.
#
# Per the icebox plan, the deployment uses Recreate strategy +
# replicas=1. This lock is the ONLY runtime defense against two
# committers racing — if topology guarantees are violated (chart
# accidentally flipped to RollingUpdate), the lock is what prevents
# two committers from both running cycles. NOT a "secondary defense":
# if it's bypassed, OCC contention is the failure mode the entire
# icebox exists to eliminate.
#
# DO NOT version-tag this derivation and DO NOT change the prefix
# string. Either change rotates every pod's lock id; during the
# transition both old-pod-with-old-id and new-pod-with-new-id believe
# they hold the singleton lock — exactly the failure mode the lock
# prevents. If you ever need to coordinate a rotation, write a
# documented migration playbook (drain old pods, then deploy new) —
# don't bake rotation into a knob.


def committer_advisory_lock_id(schema: str) -> int:
    """Derive the 64-bit signed advisory-lock id for a given schema.

    Pure function: same schema → same lock id, forever. Tests pin
    EXACT values for every deployed schema — any change to this
    derivation (algorithm, prefix string, byte order, truncation
    length) fails CI loudly."""
    digest = hashlib.sha256(
        f"posthog.icebox.committer.{schema}".encode()
    ).digest()
    # Take 8 bytes, interpret as signed int8 (PG advisory_lock takes
    # int8). big-endian for stability across architectures.
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


TRY_ADVISORY_LOCK_SQL = "SELECT pg_try_advisory_lock(%(key)s)"
UNLOCK_ADVISORY_LOCK_SQL = "SELECT pg_advisory_unlock(%(key)s)"


def try_acquire_committer_lock(
    conn: psycopg.Connection,
    *,
    lock_id: int,
) -> bool:
    """Try to acquire the singleton committer advisory lock.

    Returns True on success (caller now holds the lock until the
    connection closes or pg_advisory_unlock is called). Returns False
    if another committer already holds it.

    Use on the committer thread's PG connection. The lock is
    session-scoped: it's automatically released when this connection
    is closed (e.g., pool eviction, process exit). That's the
    primary recovery mechanism — a dead committer's lock evaporates
    with its TCP connection.

    Raises `RuntimeError` if the query returns no row or a NULL value
    (transport corruption, catalog issue). The previous behavior was
    to coerce NULL to False, which would misclassify a transport
    error as "lock held by another committer" — six pods spinning on
    a phantom lock while PG is actually degraded. PE-review #6.
    """
    with conn.cursor() as cur:
        cur.execute(TRY_ADVISORY_LOCK_SQL, {"key": lock_id})
        row = cur.fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(
            f"pg_try_advisory_lock({lock_id}) returned no row or NULL — "
            f"PG protocol or catalog error, NOT 'lock held'. Caller "
            f"should treat this as a transport failure, retry, and "
            f"page ops if it persists."
        )
    return bool(row[0])


def release_committer_lock(
    conn: psycopg.Connection,
    *,
    lock_id: int,
) -> None:
    """Explicit release. Optional — closing the connection releases
    automatically — but exposing this lets the committer call it on
    graceful shutdown so a fast pod restart doesn't have to wait for
    TCP timeout on the dead connection."""
    with conn.cursor() as cur:
        cur.execute(UNLOCK_ADVISORY_LOCK_SQL, {"key": lock_id})


def delete_cycle_row(conn: psycopg.Connection, *, cycle_id: UUID) -> None:
    """Delete a commit_cycles row that never produced a snapshot. Used
    in the released-no-iceberg recovery branch: the cycle didn't
    happen from Iceberg's perspective, so the bookkeeping row is
    just a zombie. Deleting it prevents accumulation against the
    incomplete_cycles LIMIT.
    """
    with conn.cursor() as cur:
        cur.execute(DELETE_CYCLE_ROW_SQL, {"cycle_id": cycle_id})
