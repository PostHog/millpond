"""Sync psycopg connection + polling-daemon queries.

This module exposes:
  - `build_psycopg_pool` — connection pool sized small (defaults to
    psycopg_pool_min=1, max=2). The daemon rarely needs more than one
    connection at a time; the second slot covers state-gauge refreshes
    that run on a separate transaction.
  - `apply_migrations` — applies ALL_DDL in dependency order with
    per-statement error attribution.
  - `claim_pending_batch`, `mark_committed`, `mark_failed`,
    `update_heartbeat` — daemon helpers.
"""
from __future__ import annotations

import logging
from collections.abc import Sequence

import psycopg
from psycopg import sql
from psycopg_pool import ConnectionPool

from icebox.config import Config
from icebox.schema import ALL_DDL, IceboxPendingFileRow

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


# ---------------------------------------------------------------------------
# v6 polling-daemon helpers. Operate on the `icebox_files` table only;
# the cycle-era helpers above will be removed once the new daemon is
# in place. See docs/icebox-self-healing-recovery.md.
# ---------------------------------------------------------------------------


# Hot-path SELECT: pending rows older than the age filter, batch-limited,
# row-locked via FOR UPDATE SKIP LOCKED so a second daemon (rollout
# overlap, or future multi-replica) takes a disjoint slice.
#
# The interval is parameterized so tests can pin a small value without
# wall-clock waiting. Production callers pass `cfg.age_filter_seconds`.
CLAIM_PENDING_BATCH_SQL = """
SELECT id, file_path, writer_ordinal, kafka_offsets, partition_values,
       record_count, file_size, parquet_stats, inserted_at,
       result, result_at, iceberg_snapshot_id
FROM icebox_files
WHERE result = 'pending'
  AND inserted_at < now() - make_interval(secs => %(age_seconds)s)
ORDER BY inserted_at
LIMIT %(batch_size)s
FOR UPDATE SKIP LOCKED
"""


def claim_pending_batch(
    conn: psycopg.Connection,
    *,
    batch_size: int,
    age_seconds: float,
) -> list[IceboxPendingFileRow]:
    """Lock up to `batch_size` pending rows older than `age_seconds`.

    Returns the locked rows as IceboxPendingFileRow models. The caller
    is in the same transaction; row locks survive until the tx commits
    or rolls back.

    Empty list means there's nothing eligible — the daemon's tick
    treats this as a vacuous tick (heartbeat and return).
    """
    with conn.cursor() as cur:
        cur.execute(
            CLAIM_PENDING_BATCH_SQL,
            {"batch_size": batch_size, "age_seconds": age_seconds},
        )
        rows = cur.fetchall()
    return [
        IceboxPendingFileRow(
            id=r[0],
            file_path=r[1],
            writer_ordinal=r[2],
            kafka_offsets=r[3],
            partition_values=r[4],
            record_count=r[5],
            file_size=r[6],
            parquet_stats=r[7],
            inserted_at=r[8],
            result=r[9],
            result_at=r[10],
            iceberg_snapshot_id=r[11],
        )
        for r in rows
    ]


MARK_COMMITTED_SQL = """
UPDATE icebox_files
SET result='committed', result_at=now(), iceberg_snapshot_id=%(snapshot_id)s
WHERE id = ANY(%(ids)s)
"""


def mark_committed(
    conn: psycopg.Connection,
    *,
    ids: Sequence[int],
    snapshot_id: int,
) -> None:
    """Mark a batch of rows as successfully committed to Iceberg.

    Stamps `iceberg_snapshot_id` for traceability. Caller is inside a
    transaction.
    """
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            MARK_COMMITTED_SQL,
            {"snapshot_id": snapshot_id, "ids": list(ids)},
        )


MARK_FAILED_SQL = """
UPDATE icebox_files
SET result='failed', result_at=now()
WHERE id = ANY(%(ids)s)
"""


def mark_failed(
    conn: psycopg.Connection,
    *,
    ids: Sequence[int],
) -> None:
    """Mark a batch of rows as failed-non-transport. The daemon's caller
    advances Kafka offsets past the batch separately; these rows stay
    in PG as the audit trail (see v6 doc "Failed-row runbook").
    """
    if not ids:
        return
    with conn.cursor() as cur:
        cur.execute(MARK_FAILED_SQL, {"ids": list(ids)})
