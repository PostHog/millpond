#!/usr/bin/env python3
"""DuckLake maintenance operations.

Standalone script for running DuckLake maintenance tasks (snapshot expiry,
file cleanup, orphan deletion, checkpoint) from a K8s CronJob or manually.

Requires the same env vars as the tools/justfile:
  DUCKLAKE_RDS_HOST, DUCKLAKE_RDS_PORT, DUCKLAKE_RDS_DATABASE,
  DUCKLAKE_RDS_USERNAME, DUCKLAKE_RDS_PASSWORD, DUCKLAKE_DATA_PATH,
  DUCKDB_S3_REGION
  (plus optional DUCKDB_S3_ENDPOINT, DUCKDB_S3_USE_SSL, DUCKDB_S3_URL_STYLE)

S3 credentials are optional:
  - Set BOTH DUCKDB_S3_ACCESS_KEY_ID and DUCKDB_S3_SECRET_ACCESS_KEY for the
    static-keys path (used by megaduck/viaduck where creds come from an IAM
    user synced via ExternalSecret).
  - Leave BOTH unset to use DuckDB's credential_chain provider, which picks up
    creds from the standard AWS chain (IRSA, instance profile, env, shared
    config). This is the path for per-tenant ducklings, where the pod is
    associated with a PodIdentityAssociation and no static-key Secret exists.
  - Setting exactly one of the two is a misconfiguration and refused at
    startup.

Optional:
  PUSHGATEWAY_URL — Prometheus Pushgateway address (e.g. http://pushgateway:9091).
                     If unset, metrics are not pushed and the script runs without instrumentation.
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
import re
import sys
import threading
import time
from collections import defaultdict
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Literal

import duckdb
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

log = logging.getLogger("maintenance")


def _log_version() -> None:
    try:
        v = version("millpond")
    except PackageNotFoundError:
        v = "0.0.0+unknown"
    parts = [f"millpond {v} (maintenance)"]
    image = os.environ.get("MILLPOND_IMAGE")
    if image:
        parts.append(f"image={image}")
    log.info(" ".join(parts))


_SETTING_VALUE_RE = re.compile(r"^[a-zA-Z0-9_.:/\-@+=]+$")

# Tiered compaction spec.
# Bin semantics on DuckLake 1.4.x: min_file_size is inclusive, max_file_size is
# exclusive — verified empirically. Files at exactly the boundary fall into the
# higher tier. Targets are MiB (binary) so they line up with the byte literals.
_MIB = 1024 * 1024
TIERS = {
    1: {"min": None, "max": 1 * _MIB, "target": "128MiB"},  # < 1 MiB    -> ~128 MiB
    2: {"min": 1 * _MIB, "max": 10 * _MIB, "target": "128MiB"},  # [1, 10) MiB -> ~128 MiB
    3: {"min": 10 * _MIB, "max": 64 * _MIB, "target": "128MiB"},  # [10, 64) MiB -> ~128 MiB
}

# DuckLake's `ducklake_set_option('target_file_size', ...)` persists in the
# catalog across sessions and cannot be unset (NULL is rejected). When the
# option was unset before we touched it, restore to this documented default
# rather than leaving the catalog at whatever the last tier set.
DEFAULT_TARGET_FILE_SIZE = "128MiB"

# Single source of truth for the DuckLake ATTACH name. DuckLake creates a
# Postgres metadata schema named ``__ducklake_metadata_<attach_name>``, so the
# attach name and the schema name must always be derived from the same value
# or queries silently target the wrong schema.
ATTACH_NAME = "lake"
METADATA_SCHEMA = f"__ducklake_metadata_{ATTACH_NAME}"

# Direct Postgres ATTACH name used for `postgres_execute` / `postgres_query`
# calls; distinct from the DuckLake-catalog ATTACH (ATTACH_NAME).
PG_ATTACH_NAME = "pg"

# Name of the DuckDB S3 SECRET created at connect() time. Single source of
# truth so a future multi-region/cross-account scheme can scope additional
# SECRETs by name without dredging up every literal.
S3_SECRET_NAME = "s3"

# Postgres schema containing the DuckLake catalog tables. DuckLake 1.5.x on
# Postgres stores catalog tables in `public` directly (verified on megaduck);
# the `__ducklake_metadata_lake` name is a DuckDB-side namespace alias that
# the DuckLake extension exposes for `conn.execute()` queries but does NOT
# exist as an actual Postgres schema. Any SQL sent through `postgres_execute`
# or `postgres_query` (which bypass the DuckLake extension and hit Postgres
# directly) must use PG_CATALOG_SCHEMA, not METADATA_SCHEMA.
PG_CATALOG_SCHEMA = "public"

# ---------------------------------------------------------------------------
# Catalog index recipes (ensure-indexes)
# ---------------------------------------------------------------------------
#
# Why this exists: the DuckLake catalog schema ships without secondary
# indexes, and its per-snapshot listing/compaction predicates seq-scan
# otherwise. Learned the hard way on megaduck (2026-08): 677M
# ducklake_file_column_stats rows meant a `WHERE table_id = ...` listing took
# minutes per query, and the ClickHouse DuckLake reader's changed-snapshot
# guard retried until it flapped. Every entry is CREATE INDEX CONCURRENTLY
# IF NOT EXISTS: additive, idempotent, and never blocks catalog writers.
#
# CONCURRENTLY cannot run through duckdb's postgres_execute (the extension
# wraps it in BEGIN ... which Postgres rejects), so ensure_indexes() talks to
# the catalog over a direct psycopg connection with autocommit.
#
# Names match the indexes already live on megaduck so adoption there is a
# no-op; the set is the union of the ClickHouse-reader and compaction/metrics
# access patterns.
CATALOG_INDEXES: tuple[tuple[str, str], ...] = (
    # Snapshot-visibility reads (the hot path for every table listing):
    #   WHERE table_id = ? AND begin_snapshot <= ? AND (end_snapshot IS NULL OR ? < end_snapshot)
    ("ducklake_data_file_snapshot_read_idx", "ducklake_data_file (table_id, begin_snapshot, end_snapshot)"),
    ("ducklake_delete_file_snapshot_read_idx", "ducklake_delete_file (table_id, begin_snapshot, end_snapshot)"),
    # Live-file scans (end_snapshot IS NULL) ordered by size, used by tiered
    # compaction and the metrics daemon:
    (
        "ducklake_data_file_compaction_idx",
        "ducklake_data_file (table_id, end_snapshot, file_size_bytes) WHERE end_snapshot IS NULL",
    ),
    (
        "ducklake_data_file_compaction_order_idx",
        "ducklake_data_file (table_id, end_snapshot, file_size_bytes, begin_snapshot, row_id_start, data_file_id) "
        "WHERE end_snapshot IS NULL",
    ),
    ("ducklake_delete_file_table_idx", "ducklake_delete_file (table_id, end_snapshot) WHERE end_snapshot IS NULL"),
    (
        "ducklake_delete_file_metrics_idx",
        "ducklake_delete_file (table_id, end_snapshot, file_size_bytes) WHERE end_snapshot IS NULL",
    ),
    # Per-file lookups by data_file_id (ClickHouse file listing joins, orphan
    # healing):
    ("ducklake_file_column_stats_file_idx", "ducklake_file_column_stats (data_file_id)"),
    ("ducklake_file_partition_value_file_idx", "ducklake_file_partition_value (data_file_id)"),
    # Table-scoped per-file reads: `WHERE table_id = ? AND data_file_id IN
    # (...)` (the ClickHouse reader's stats/partition fetches):
    ("ducklake_file_column_stats_table_file_idx", "ducklake_file_column_stats (table_id, data_file_id)"),
    ("ducklake_file_partition_value_table_file_idx", "ducklake_file_partition_value (table_id, data_file_id)"),
)

# Companion SQL file: header conventions plus runtime-loadable macros.
MAINTENANCE_SQL_PATH = Path(__file__).resolve().parent / "ducklake_maintenance.sql"

# Stable identifier for `pg_try_advisory_lock`. The lock guards mutual
# exclusion *between maintenance invocations*: it is held by the `pg`
# ATTACH connection, not by the catalog connection DuckLake uses
# internally for ducklake_* function calls, so it does NOT serialize
# against arbitrary other writers (e.g. the millpond ingest pods).
ADVISORY_LOCK_KEY_SQL = "hashtext('millpond-ducklake-maintenance')::bigint"


def _setup_logging(verbose: bool = False) -> None:
    level = "DEBUG" if verbose else os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )


def _require(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return val


def _escape_libpq(value: str | None) -> str:
    """Escape a value for a libpq connection string.

    Wraps in single quotes and backslash-escapes internal single quotes and
    backslashes, per the libpq connstring grammar (NOT the Postgres SQL
    parser's grammar — they're different parsers with different rules).
    """
    if value is None:
        return "''"
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _sanitize_setting_value(val: str) -> str:
    if not _SETTING_VALUE_RE.match(val):
        raise ValueError(f"Illegal character in DuckDB setting value: {val!r}")
    return val


def _positive_int(s: str) -> int:
    n = int(s)
    if n < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {n}")
    return n


def connect(debug: bool = False) -> duckdb.DuckDBPyConnection:
    """Connect to DuckLake using environment variables.

    debug=True re-enables DuckDB's HTTP logging and the postgres extension's
    query debug output. Both are off by default because they add per-call
    overhead that compounds across 30k+ S3 deletes and many catalog reads.
    """
    conn = duckdb.connect()

    # DuckDB defaults to spilling under '.tmp' relative to CWD; in the millpond
    # image that path is on the read-only rootfs, so any compaction that needs
    # to spill crashes. /tmp is the writable emptyDir in the cron pod.
    conn.execute("SET temp_directory = '/tmp/duckdb_spill'")

    # Static keys are optional. If both DUCKDB_S3_ACCESS_KEY_ID and
    # DUCKDB_S3_SECRET_ACCESS_KEY are set, we use PROVIDER config (the
    # megaduck/viaduck path: an IAM user's static creds synced from Secrets
    # Manager). If both are absent, we use PROVIDER credential_chain so DuckDB
    # picks up creds from the standard AWS chain (IRSA, instance profile, env
    # vars, etc.) — the per-tenant duckling path, where the pod is associated
    # with a PodIdentityAssociation and there's no static-key Secret to sync.
    # Refuse a partial config: a single key set by itself is a misconfiguration
    # operators should hear about immediately, not silently swap to a
    # different credential source. Note: an empty-string env var (`""`) is
    # treated as unset, matching the way K8s ExternalSecrets sometimes mount
    # an empty value when a remote secret key is missing.
    s3_key = os.environ.get("DUCKDB_S3_ACCESS_KEY_ID")
    s3_secret = os.environ.get("DUCKDB_S3_SECRET_ACCESS_KEY")
    if bool(s3_key) != bool(s3_secret):
        key_state = "set" if s3_key else "unset"
        secret_state = "set" if s3_secret else "unset"
        raise RuntimeError(
            "DUCKDB_S3_ACCESS_KEY_ID and DUCKDB_S3_SECRET_ACCESS_KEY must be set together or both omitted; "
            f"got DUCKDB_S3_ACCESS_KEY_ID={key_state}, DUCKDB_S3_SECRET_ACCESS_KEY={secret_state}"
        )
    use_credential_chain = not s3_key

    # Region: required when on credential_chain. The legacy us-east-1 default
    # is kept for the static-keys path because megaduck/viaduck both run there
    # and have run without setting the var explicitly, but silently defaulting
    # a per-tenant duckling in (say) eu-west-1 to us-east-1 would resolve creds
    # to a region whose bucket may not exist. Fail loudly instead.
    if use_credential_chain:
        s3_region = _require("DUCKDB_S3_REGION")
    else:
        s3_region = os.environ.get("DUCKDB_S3_REGION", "us-east-1")
    s3_defaults = {
        "s3_region": s3_region,
        "s3_endpoint": f"s3.{s3_region}.amazonaws.com",
        "s3_use_ssl": "true",
        "s3_url_style": "vhost",
    }

    for key in (
        "DUCKDB_S3_ENDPOINT",
        "DUCKDB_S3_ACCESS_KEY_ID",
        "DUCKDB_S3_SECRET_ACCESS_KEY",
        "DUCKDB_S3_USE_SSL",
        "DUCKDB_S3_URL_STYLE",
        "DUCKDB_S3_REGION",
    ):
        val = os.environ.get(key)
        setting = key.lower().replace("duckdb_", "")
        if val is not None:
            conn.execute(f"SET {setting} = '{_sanitize_setting_value(val)}'")
        elif setting in s3_defaults:
            conn.execute(f"SET {setting} = '{_sanitize_setting_value(s3_defaults[setting])}'")

    conn.execute("LOAD httpfs")
    conn.execute("LOAD ducklake")
    conn.execute("LOAD postgres")
    conn.execute(f"SET enable_http_logging = {str(debug).lower()}")
    conn.execute(f"SET pg_debug_show_queries = {str(debug).lower()}")

    # On a high-frequency write workload (millpond ingest pods writing
    # continuously), the snapshot_id sequence advances rapidly. A large
    # compaction commit retries up to ducklake_max_retry_count times on
    # snapshot ID collision before giving up. The default of 10 is far too
    # low when millpond is committing hundreds of snapshots per minute.
    retry_count = int(os.environ.get("COMPACTION_MAX_RETRY_COUNT", "200"))
    conn.execute(f"SET ducklake_max_retry_count = {retry_count}")

    # Modern DuckLake (1.5.x, pinned by millpond) reads S3 through the SECRET
    # manager from both the catalog driver and ad-hoc httpfs ops (`glob`,
    # `read_parquet`, ATTACH against `ducklake:s3://...`). The legacy
    # `SET s3_*` block above is kept for httpfs-pre-secret compatibility, but
    # the SECRET below is what actually authenticates the runtime path on
    # 1.5.x.
    s3_endpoint_val = os.environ.get("DUCKDB_S3_ENDPOINT", s3_defaults["s3_endpoint"])
    s3_use_ssl_val = os.environ.get("DUCKDB_S3_USE_SSL", s3_defaults["s3_use_ssl"])
    s3_url_style_val = os.environ.get("DUCKDB_S3_URL_STYLE", s3_defaults["s3_url_style"])
    for v in (s3_endpoint_val, s3_use_ssl_val, s3_url_style_val, s3_region):
        if v:
            _sanitize_setting_value(v)
    if use_credential_chain:
        # No KEY_ID/SECRET — credential_chain delegates to the AWS SDK chain
        # (IRSA token at /var/run/secrets/eks.amazonaws.com/..., ECS container
        # role, EC2 instance profile, env vars, shared config). DuckDB
        # validates the chain at CREATE time: if no provider resolves, CREATE
        # itself raises `Secret Validation Failure: ... Credential Chain`, so
        # an empty IRSA / missing PodIdentityAssociation surfaces here rather
        # than as an opaque HTTP 403 on the first httpfs op. The resolved
        # temporary creds (e.g. an STS token from IRSA) are cached on the
        # SECRET for the connection lifetime and NOT refreshed — fine for the
        # short-lived compactor CronJob, but long-running daemons need
        # external refresh (see ducklake_metrics.py's docstring).
        try:
            conn.execute(
                f"CREATE OR REPLACE SECRET {S3_SECRET_NAME} ("
                f"TYPE s3, PROVIDER credential_chain, "
                f"REGION '{s3_region}', ENDPOINT '{s3_endpoint_val}', "
                f"URL_STYLE '{s3_url_style_val}', USE_SSL {s3_use_ssl_val})"
            )
        except duckdb.Error as e:
            raise RuntimeError(
                "S3 credential_chain resolution failed at CREATE SECRET; verify the pod has a working "
                "PodIdentityAssociation / IRSA / instance-profile / env-var / shared-config credential chain. "
                f"Original error: {e}"
            ) from e
    else:
        # Static keys path: validate to keep the f-string injection-safe
        # alongside the rest of the inputs already validated above.
        _sanitize_setting_value(s3_key)
        _sanitize_setting_value(s3_secret)
        conn.execute(
            f"CREATE OR REPLACE SECRET {S3_SECRET_NAME} ("
            f"TYPE s3, PROVIDER config, "
            f"KEY_ID '{s3_key}', SECRET '{s3_secret}', "
            f"REGION '{s3_region}', ENDPOINT '{s3_endpoint_val}', "
            f"URL_STYLE '{s3_url_style_val}', USE_SSL {s3_use_ssl_val})"
        )

    rds_host = _require("DUCKLAKE_RDS_HOST")
    rds_port = os.environ.get("DUCKLAKE_RDS_PORT", "5432")
    rds_database = os.environ.get("DUCKLAKE_RDS_DATABASE", "ducklake")
    rds_username = os.environ.get("DUCKLAKE_RDS_USERNAME", "ducklake")
    rds_password = _require("DUCKLAKE_RDS_PASSWORD")
    data_path = _require("DUCKLAKE_DATA_PATH")

    pg_connstr = (
        f"host={rds_host} port={rds_port} "
        f"dbname={_escape_libpq(rds_database)} user={_escape_libpq(rds_username)} "
        f"password={_escape_libpq(rds_password)}"
    )
    pg_connstr_sql = pg_connstr.replace("'", "''")
    conn.execute(f"""
        ATTACH 'ducklake:postgres:{pg_connstr_sql}' AS {ATTACH_NAME} (
            DATA_PATH '{data_path.replace("'", "''")}'
        )
    """)
    # Direct Postgres ATTACH for postgres_execute / postgres_query; needed by
    # the catalog-maintenance recipes that touch ctid or run DML the duckdb
    # postgres extension doesn't expose duckdb-side.
    conn.execute(f"ATTACH '{pg_connstr_sql}' AS {PG_ATTACH_NAME} (TYPE postgres)")

    if MAINTENANCE_SQL_PATH.exists():
        # File is executed verbatim by both ducklake_maintenance.py and the duckdb CLI's
        # `.read` meta-command (the `just shell` recipe), so it must contain no
        # templating placeholders — references to `__ducklake_metadata_lake`
        # are written literally to keep both paths consistent.
        conn.execute(MAINTENANCE_SQL_PATH.read_text())
        log.debug("Loaded SQL macros from %s", MAINTENANCE_SQL_PATH)
    else:
        log.warning("ducklake_maintenance.sql not found at %s; macros unavailable", MAINTENANCE_SQL_PATH)

    log.info(
        "Connected: metadata=%s:%s/%s data=%s",
        rds_host,
        rds_port,
        rds_database,
        data_path,
    )
    return conn


def expire(conn: duckdb.DuckDBPyConnection, days: int, dry_run: bool) -> None:
    """Expire snapshots older than N days."""
    log.info("Expiring snapshots older than %d days (dry_run=%s)", days, dry_run)
    if not dry_run:
        # Mutating path: mutex against expire-snapshots / cleanup-family
        # invocations. Racing queue-INSERTs from two expiry paths produce
        # duplicate ducklake_files_scheduled_for_deletion rows, which poison
        # cleanup-all via NoSuchKey (see dedup_deletions).
        _acquire_advisory_lock(conn)
    result = conn.execute(
        f"CALL ducklake_expire_snapshots('{ATTACH_NAME}', "
        f"older_than => now() - INTERVAL '{days} days', "
        f"dry_run => {str(dry_run).lower()})"
    ).fetchall()
    for row in result:
        log.info("expire: %s", row)


def _pg_query_one(conn: duckdb.DuckDBPyConnection, sql: str) -> tuple:
    """Run a SELECT via the pg ATTACH and return the first row."""
    return conn.execute(f"SELECT * FROM postgres_query('{PG_ATTACH_NAME}', {_sql_string_literal(sql)})").fetchone()


def _pg_execute(conn: duckdb.DuckDBPyConnection, sql: str) -> None:
    """Run a DML statement via the pg ATTACH."""
    conn.execute(f"CALL postgres_execute('{PG_ATTACH_NAME}', {_sql_string_literal(sql)})")


def expire_snapshots(
    conn: duckdb.DuckDBPyConnection, days: int, batch_size: int, num_batches: int | None, dry_run: bool
) -> None:
    """Postgres-native snapshot expiry that bypasses ducklake_expire_snapshots.

    DuckLake's built-in OOMs on large catalogs because it loads all matching
    snapshot rows into a DuckDB in-memory vector at bind time, before deleting
    anything. This function replicates the same DELETE cascade entirely via
    postgres_execute, in bounded snapshot_id-ordered batches.

    Per batch (ONE postgres_execute call = ONE transaction):
      1. SELECT next `batch_size` expired snapshot_ids (ORDER BY snapshot_id),
         always excluding MAX(snapshot_id) — the head snapshot is never
         expired, no matter how old. This mirrors the guard in the fork
         built-in (ducklake_expire_snapshots.cpp): DuckLake resolves ALL
         state from the newest snapshot row, so expiring it bricks the
         catalog (an idle tenant older than --days would otherwise lose
         every snapshot).
      2. Find dead DATA files: end_snapshot in batch, and NO snapshot outside
         this batch bridges [begin, end)
         → INSERT paths into ducklake_files_scheduled_for_deletion
         → DELETE cascade: column_stats → partition_value → variant_stats → data_file
      3. Find dead DELETE VECTORS: same liveness check against ducklake_delete_file
         → INSERT paths into ducklake_files_scheduled_for_deletion
         → DELETE from ducklake_delete_file
      4. DELETE ducklake_snapshot_changes, then ducklake_snapshot rows

    After all batches: run cleanup-all to physically delete S3 objects.

    Liveness is STRUCTURAL, matching the cascade in the fork: a file
    survives if ANY snapshot row in [begin_snapshot, end_snapshot) survives
    this run's deletions — expressed as "a bridging snapshot NOT IN this
    batch's id list" (ids are processed in ascending order, so every id
    below the batch was deleted by an earlier batch or is being deleted
    now; ids above it are retained). The previous predicate mixed a
    snapshot_time-vs-cutoff test into the bridging check, which could
    classify a file as dead while a later-batch or --num-batches-spared
    snapshot still referenced it — queueing live data for S3 deletion.

    Atomicity: each batch's INSERT + DELETE cascade + snapshot DELETE
    ships as one multi-statement postgres_execute call — one transaction. A
    crash between batches leaves whole batches applied or not applied —
    never a snapshot row whose data-file rows are missing (readers of a
    surviving snapshot would otherwise see its files vanish). Re-running
    converges: the batch SELECT simply finds the remaining expired ids.

    Schema note: SQL sent via _pg_execute/_pg_query_one hits Postgres directly
    and must use PG_CATALOG_SCHEMA ('public'), not METADATA_SCHEMA
    ('__ducklake_metadata_lake'). The latter is a DuckDB-side alias exposed by
    the DuckLake extension and does not exist as an actual Postgres schema.
    """
    s = PG_CATALOG_SCHEMA  # short alias; all SQL here goes through postgres_execute
    _acquire_advisory_lock(conn)

    # Capture the expiry cutoff once as a Postgres timestamptz literal so that
    # every statement in this run uses the identical boundary. Without this,
    # each postgres_execute call re-evaluates NOW() in its own transaction and
    # a snapshot right at the boundary could be classified as "expired" by the
    # batch SELECT but "live" by the NOT EXISTS liveness check (or vice versa).
    cutoff_text = _pg_query_one(conn, f"SELECT (NOW() - INTERVAL '{days} days')::text")[0]
    cutoff_sql = f"TIMESTAMPTZ '{cutoff_text}'"

    head_guard = f"snapshot_id != (SELECT MAX(snapshot_id) FROM {s}.ducklake_snapshot)"

    if dry_run:
        row = _pg_query_one(
            conn,
            f"SELECT COUNT(*) FROM {s}.ducklake_snapshot WHERE snapshot_time < {cutoff_sql} AND {head_guard}",
        )
        log.info(
            "expire-snapshots dry-run: %d snapshots older than %d days (batch_size=%d); "
            "run without --dry-run to see per-batch dead file counts",
            row[0],
            days,
            batch_size,
        )
        return

    total_snapshots = 0
    total_dead_data_files = 0
    total_dead_delete_vectors = 0
    batch_num = 0

    while True:
        batch_select = (
            f"SELECT snapshot_id FROM {s}.ducklake_snapshot "
            f"WHERE snapshot_time < {cutoff_sql} AND {head_guard} "
            f"ORDER BY snapshot_id LIMIT {batch_size}"
        )
        rows = conn.execute(
            f"SELECT snapshot_id FROM postgres_query('{PG_ATTACH_NAME}', {_sql_string_literal(batch_select)})"
        ).fetchall()
        if not rows:
            log.info("expire-snapshots: no expired snapshots remaining, done")
            break
        if num_batches is not None and batch_num >= num_batches:
            log.info("expire-snapshots: reached --num-batches limit (%d), stopping", num_batches)
            break

        batch_num += 1
        snapshot_ids = [row[0] for row in rows]
        id_list = ", ".join(str(i) for i in snapshot_ids)

        # WHERE clause for both data-file and delete-vector dead checks.
        # A file/vector is dead when its end_snapshot is in this expired
        # batch AND no snapshot OUTSIDE this batch bridges [begin_snapshot,
        # end_snapshot). Structural, matching the cascade in the fork — NOT
        # a time comparison: ids are deleted in ascending order, so at the
        # moment a file's end_snapshot is processed, every potential bridge
        # below it is already gone (earlier batch) or in this batch's
        # id_list; a later-batch id or --num-batches survivor that bridges
        # keeps the file alive (it is re-examined if that id expires later).
        # Excluding the batch by id also makes the predicate evaluate
        # identically before and after this transaction's own snapshot
        # DELETEs, so statement order within the batch txn is irrelevant.
        # Concurrent writers only append ids above MAX, which can never
        # fall inside an old [begin, end) window.
        def _dead_where(alias: str) -> str:
            return (
                f"{alias}.end_snapshot IN ({id_list}) "
                f"AND NOT EXISTS ("
                f"  SELECT 1 FROM {s}.ducklake_snapshot live "
                f"  WHERE live.snapshot_id >= {alias}.begin_snapshot "
                f"  AND live.snapshot_id < {alias}.end_snapshot "
                f"  AND live.snapshot_id NOT IN ({id_list})"
                f")"
            )

        # --- Dead data files ---
        dead_data_where = _dead_where("df")
        dead_data_full_subq = (
            f"SELECT df.data_file_id, df.path, df.path_is_relative "
            f"FROM {s}.ducklake_data_file df WHERE {dead_data_where}"
        )

        dead_data_count = _pg_query_one(conn, f"SELECT COUNT(*) FROM ({dead_data_full_subq}) _d")[0]

        # --- Dead delete vectors (positional delete files) ---
        dead_dv_where = _dead_where("dv")
        dead_dv_full_subq = (
            f"SELECT dv.delete_file_id AS data_file_id, dv.path, dv.path_is_relative "
            f"FROM {s}.ducklake_delete_file dv WHERE {dead_dv_where}"
        )

        dead_dv_count = _pg_query_one(conn, f"SELECT COUNT(*) FROM ({dead_dv_full_subq}) _d")[0]

        log.info(
            "expire-snapshots: batch %d: snapshot_ids [%d..%d] (%d), dead_data_files=%d dead_delete_vectors=%d",
            batch_num,
            snapshot_ids[0],
            snapshot_ids[-1],
            len(snapshot_ids),
            dead_data_count,
            dead_dv_count,
        )

        # One postgres_execute call = one transaction for the whole batch:
        # queue-INSERTs, file-row cascade, and snapshot DELETEs commit
        # together or not at all (see docstring "Atomicity"). The dead-file
        # sets are materialized ONCE into ON COMMIT DROP temp tables (same
        # pattern as repair-partition-values) so the expensive
        # ducklake_data_file scan runs once per batch instead of once per
        # referencing statement. Queue INSERTs stay textually before the
        # file DELETEs to preserve the schedule-before-drop invariant if
        # this is ever split up again. SET LOCAL bounds a blocked/runaway
        # batch so the advisory lock cannot be held forever.
        _pg_execute(
            conn,
            f"SET LOCAL statement_timeout = '{_EXPIRE_SNAPSHOTS_STATEMENT_TIMEOUT}'; "
            f"SET LOCAL lock_timeout = '{_EXPIRE_SNAPSHOTS_LOCK_TIMEOUT}'; "
            f"CREATE TEMP TABLE _expire_dead_data ON COMMIT DROP AS {dead_data_full_subq}; "
            f"CREATE TEMP TABLE _expire_dead_dv ON COMMIT DROP AS {dead_dv_full_subq}; "
            f"INSERT INTO {s}.ducklake_files_scheduled_for_deletion "
            f"  (data_file_id, path, path_is_relative, schedule_start) "
            f"SELECT d.data_file_id, d.path, d.path_is_relative, NOW() "
            f"FROM _expire_dead_data d "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {s}.ducklake_files_scheduled_for_deletion x "
            f"  WHERE x.data_file_id = d.data_file_id"
            f"); "
            f"INSERT INTO {s}.ducklake_files_scheduled_for_deletion "
            f"  (data_file_id, path, path_is_relative, schedule_start) "
            f"SELECT d.data_file_id, d.path, d.path_is_relative, NOW() "
            f"FROM _expire_dead_dv d "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {s}.ducklake_files_scheduled_for_deletion x "
            f"  WHERE x.data_file_id = d.data_file_id"
            f"); "
            f"DELETE FROM {s}.ducklake_file_column_stats "
            f"  WHERE data_file_id IN (SELECT data_file_id FROM _expire_dead_data); "
            f"DELETE FROM {s}.ducklake_file_partition_value "
            f"  WHERE data_file_id IN (SELECT data_file_id FROM _expire_dead_data); "
            f"DELETE FROM {s}.ducklake_file_variant_stats "
            f"  WHERE data_file_id IN (SELECT data_file_id FROM _expire_dead_data); "
            f"DELETE FROM {s}.ducklake_data_file "
            f"  WHERE data_file_id IN (SELECT data_file_id FROM _expire_dead_data); "
            f"DELETE FROM {s}.ducklake_delete_file "
            f"  WHERE delete_file_id IN (SELECT data_file_id FROM _expire_dead_dv); "
            f"DELETE FROM {s}.ducklake_snapshot_changes WHERE snapshot_id IN ({id_list}); "
            f"DELETE FROM {s}.ducklake_snapshot WHERE snapshot_id IN ({id_list})",
        )

        total_snapshots += len(snapshot_ids)
        total_dead_data_files += dead_data_count
        total_dead_delete_vectors += dead_dv_count

    log.info(
        "expire-snapshots: done: %d snapshots expired, %d dead data files + %d delete vectors "
        "scheduled for deletion (%d batches, batch_size=%d, num_batches=%s); run cleanup-all to delete S3 objects",
        total_snapshots,
        total_dead_data_files,
        total_dead_delete_vectors,
        batch_num,
        batch_size,
        str(num_batches) if num_batches is not None else "unlimited",
    )


def _scheduled_for_deletion_count(conn: duckdb.DuckDBPyConnection) -> int:
    """Queue depth of ducklake_files_scheduled_for_deletion."""
    return conn.execute(f"SELECT COUNT(*) FROM {METADATA_SCHEMA}.ducklake_files_scheduled_for_deletion").fetchone()[0]


def _log_cleanup_throughput(
    operation: str,
    files_processed: int,
    elapsed_s: float,
    queue_depth_after: int,
) -> None:
    """Emit one structured line with cleanup throughput stats.

    Single line, key=value pairs, grep-friendly. ``files_processed`` is taken
    directly from the count of rows ``ducklake_cleanup_old_files`` returned
    rather than from a queue-depth delta — the delta is wrong if any other
    writer enqueues deletions during the call (and the maintenance advisory
    lock by design only mutexes maintenance invocations, not arbitrary
    writers). ``queue_depth_after`` gives a "how much remains" signal but
    isn't used in the rate.
    """
    rate = files_processed / elapsed_s if elapsed_s > 0 else 0.0
    log.info(
        "%s throughput: files_processed=%d elapsed_s=%.1f rate_obj_s=%.1f queue_depth_after=%d",
        operation,
        files_processed,
        elapsed_s,
        rate,
        queue_depth_after,
    )


def cleanup(conn: duckdb.DuckDBPyConnection, days: int, dry_run: bool) -> None:
    """Delete files scheduled for deletion older than N days."""
    if not dry_run:
        # Mutating path: mutex against other maintenance invocations (the
        # extension-side maintenance in ingest pods does NOT take this lock;
        # see _acquire_advisory_lock docstring). Same-session re-acquisition
        # by nested callers (maintain -> expire/cleanup, fsck -> cleanup_all_safe)
        # is fine: pg advisory locks are reentrant per session.
        _acquire_advisory_lock(conn)
    log.info("Cleaning up files older than %d days (dry_run=%s)", days, dry_run)
    t0 = time.monotonic()
    result = conn.execute(
        f"CALL ducklake_cleanup_old_files('{ATTACH_NAME}', "
        f"older_than => now() - INTERVAL '{days} days', "
        f"dry_run => {str(dry_run).lower()})"
    ).fetchall()
    elapsed = time.monotonic() - t0
    for row in result:
        log.info("cleanup: %s", row)
    if not dry_run:
        # Skip throughput log on dry_run: ducklake_cleanup_old_files returns the
        # would-be-deleted rows in dry-run mode, so len(result) is the preview
        # count, not actually-processed work — claiming a rate from that would
        # be misleading.
        _log_cleanup_throughput("cleanup", len(result), elapsed, _scheduled_for_deletion_count(conn))


def ensure_indexes(dry_run: bool) -> None:
    """Create the catalog's secondary indexes (CATALOG_INDEXES) idempotently.

    Runs over a direct psycopg connection with autocommit: CREATE INDEX
    CONCURRENTLY cannot run inside a transaction, and duckdb's postgres_execute
    wraps everything in BEGIN (verified against megaduck 2026-08). Each
    statement is `IF NOT EXISTS`, so the routine is safe to run on every
    maintenance pass and cheap when the catalog is already indexed.
    """
    import psycopg  # local import: only this subcommand needs a raw pg driver

    log.info(
        "%sensure-indexes: %d index(es) against catalog schema %s",
        "[dry-run] " if dry_run else "",
        len(CATALOG_INDEXES),
        PG_CATALOG_SCHEMA,
    )
    if dry_run:
        for name, definition in CATALOG_INDEXES:
            log.info(
                "[dry-run] CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s.%s",
                name,
                PG_CATALOG_SCHEMA,
                definition,
            )
        return

    dsn = (
        f"host={_require('DUCKLAKE_RDS_HOST')} "
        f"port={os.environ.get('DUCKLAKE_RDS_PORT', '5432')} "
        f"dbname={os.environ.get('DUCKLAKE_RDS_DATABASE', 'ducklake')} "
        f"user={os.environ.get('DUCKLAKE_RDS_USERNAME', 'ducklake')} "
        f"password={_require('DUCKLAKE_RDS_PASSWORD')} "
        "sslmode=require"
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            for name, definition in CATALOG_INDEXES:
                statement = f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} ON {PG_CATALOG_SCHEMA}.{definition}"
                log.info("ensure-indexes: %s", name)
                t0 = time.monotonic()
                cur.execute(statement)
                log.info("ensure-indexes: %s done in %.1fs", name, time.monotonic() - t0)


def cleanup_all(conn: duckdb.DuckDBPyConnection) -> None:
    """Delete all files scheduled for deletion regardless of age.

    Deliberately no dry_run parameter: ducklake_cleanup_old_files with
    cleanup_all => true has no preview form, and the previous "accept
    --dry-run, log 'skipping', do nothing" behavior taught operators they
    had previewed something when they had previewed nothing. The CLI now
    rejects --dry-run outright; age-gated previews exist as cleanup-dry-run
    and the full pipeline preview as fsck-dry-run.
    """
    _acquire_advisory_lock(conn)
    log.info("Cleaning up all files scheduled for deletion")
    t0 = time.monotonic()
    result = conn.execute(f"CALL ducklake_cleanup_old_files('{ATTACH_NAME}', cleanup_all => true)").fetchall()
    elapsed = time.monotonic() - t0
    for row in result:
        log.info("cleanup-all: %s", row)
    _log_cleanup_throughput("cleanup-all", len(result), elapsed, _scheduled_for_deletion_count(conn))


def _sql_string_literal(s: str) -> str:
    """Quote a Python string as a SQL string literal (single-quote-doubled)."""
    return "'" + s.replace("'", "''") + "'"


def _acquire_advisory_lock(conn: duckdb.DuckDBPyConnection) -> None:
    """Take the maintenance advisory lock or raise if another session holds it.

    The lock is taken on the `pg` ATTACH and released automatically when that
    connection closes (including on crash); no explicit release needed for
    single-subcommand invocations. Any subcommand that mutates the catalog
    should call this before doing so.
    """
    inner_sql = f"SELECT pg_try_advisory_lock({ADVISORY_LOCK_KEY_SQL}) AS acquired"
    held = conn.execute(
        f"SELECT acquired FROM postgres_query('{PG_ATTACH_NAME}', {_sql_string_literal(inner_sql)})"
    ).fetchone()[0]
    if not held:
        raise RuntimeError(
            "Another maintenance session is holding the advisory lock; aborting. "
            "If you're sure no other invocation is running, the previous holder's "
            "connection may not have closed cleanly — wait a few seconds and retry."
        )
    log.info("Acquired advisory lock %s", ADVISORY_LOCK_KEY_SQL)


def dedup_deletions(conn: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Drop duplicate rows from ducklake_files_scheduled_for_deletion.

    The same path can land in the queue across multiple snapshots (DuckLake
    bug c5); combined with c1, the second visit poisons cleanup-all because
    the S3 DELETE returns NoSuchKey and rolls back the whole transaction.
    Keep one row per distinct path (the lowest ctid) and drop the rest.

    The DELETE is ctid-based and runs through the duckdb postgres extension
    (`postgres_execute`); duckdb-side DML can't see Postgres system columns.
    """
    dups = conn.execute("SELECT count_pending_dups()").fetchone()[0]
    log.info("dedup-deletions: %d duplicate rows in queue (dry_run=%s)", dups, dry_run)
    if dry_run or dups == 0:
        return
    _acquire_advisory_lock(conn)
    delete_sql = (
        f"DELETE FROM {PG_CATALOG_SCHEMA}.ducklake_files_scheduled_for_deletion "
        f"WHERE ctid NOT IN ("
        f"SELECT MIN(ctid) FROM {PG_CATALOG_SCHEMA}.ducklake_files_scheduled_for_deletion GROUP BY path"
        f")"
    )
    conn.execute(f"CALL postgres_execute('{PG_ATTACH_NAME}', {_sql_string_literal(delete_sql)})")
    after = conn.execute("SELECT count_pending_dups()").fetchone()[0]
    log.info("dedup-deletions: queue now has %d duplicate rows", after)


# Bounds on the purge deletes so a blocked or runaway statement can't hold
# the maintenance advisory lock forever. Same pattern as the
# repair-partition-values timeout constants.
_PURGE_ORPHAN_STATS_STATEMENT_TIMEOUT = "300s"
_PURGE_ORPHAN_STATS_LOCK_TIMEOUT = "30s"

# Bounds for one expire-snapshots batch transaction (INSERT + full cascade +
# snapshot DELETE for up to --batch-size snapshots). Generous statement
# timeout: the dead-file subqueries scan ducklake_data_file, which reaches
# tens of millions of rows on large catalogs.
_EXPIRE_SNAPSHOTS_STATEMENT_TIMEOUT = "600s"
_EXPIRE_SNAPSHOTS_LOCK_TIMEOUT = "30s"
# Upper bound on --batch-size: the batch id list is interpolated as a literal
# into every statement of the batch transaction (including a per-row NOT IN
# inside the bridging check) — unbounded batches produce multi-megabyte SQL.
_EXPIRE_SNAPSHOTS_MAX_BATCH_SIZE = 10_000


def _orphan_stats_predicate(alias: str) -> str:
    """WHERE fragment selecting stats rows whose table has no live version.

    NOT EXISTS, never NOT IN: stats-table `table_id` is nullable and the
    stats tables have no PK. NOT IN goes three-valued on NULLs — it skips
    NULL-key rows (leaving permanent residue the monitoring counts as
    orphaned), and if the live set ever contained a NULL table_id it would
    silently delete NOTHING. NOT EXISTS treats NULL-key rows as orphaned,
    which is correct: they are unreadable and pure commit-path weight.

    Kept in lockstep with the `ducklake_stats_rows_orphaned` metric in
    ducklake-metrics-daemon — metric and purge must count the same rows.
    """
    return (
        f"NOT EXISTS ("
        f"SELECT 1 FROM {PG_CATALOG_SCHEMA}.ducklake_table t "
        f"WHERE t.table_id = {alias}.table_id AND t.end_snapshot IS NULL"
        f")"
    )


def purge_orphan_stats(conn: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Delete global-stats rows left behind by dropped tables.

    DROP TABLE only end-snapshots the `ducklake_table` row; the table's rows
    in `ducklake_table_stats` / `ducklake_table_column_stats` linger until
    snapshot expiry deletes the last table version. The commit path reads
    BOTH stats tables in full on every commit attempt, so on catalogs with
    heavy DROP+CREATE churn the orphans directly inflate every writer's
    commit latency (observed: a catalog at 99.3% orphaned column-stats rows
    committed 30-50x slower until purged).

    Safe to run at any time, concurrently with writers:
      - Orphanhood is permanent — table_ids are allocated monotonically and
        never reused, there is no undrop, and post-drop writes are rejected
        at commit. Nothing can un-orphan a row.
      - BOTH deletes run in ONE postgres_execute call = ONE REPEATABLE READ
        transaction. This is load-bearing, not style: the commit path's
        conflict read (GetSnapshotAndStatsAndChanges) does
        `ducklake_table_stats LEFT JOIN ducklake_table_column_stats` with a
        WHERE that only filters table_stats columns, and
        TransformGlobalStatsRow does an UNGUARDED GetValue on column_id — a
        reader-visible state of "table_stats row present, column_stats rows
        gone" turns every contended commit into an InternalException. One
        transaction means readers see either both tables purged or neither.
        Within it, table_stats is deleted first to match the DeleteSnapshots
        cascade order (same cross-table lock order → no lock-order-inversion
        deadlock with concurrent expiry); the intermediate no-parent
        column_stats state is invisible anyway (single txn) and harmless to
        the LEFT JOIN even if it weren't.
      - Live tables' stats rows are never touched, so concurrent stats
        UPDATEs from committing writers don't contend with the deletes.
      - A concurrent ducklake_expire_snapshots cascade (which does NOT take
        our advisory lock) deleting the same rows can surface as a
        serialization error or deadlock; the tool raises and a re-run is
        fully convergent (rows already gone, predicate re-evaluates).

    Column-stats rows are matched on table_id only: a row with NULL
    column_id (observed in the wild) is still dead weight once its table is
    gone. VACUUM cannot run here (postgres_execute wraps every call in a
    transaction block), so after a large purge the commit path keeps
    scanning dead pages until autovacuum catches up — for multi-100k purges
    run `VACUUM (ANALYZE)` on both stats tables manually to realize the
    full win immediately.
    """
    s = PG_CATALOG_SCHEMA
    counts_sql = (
        f"SELECT "
        f"(SELECT COUNT(*) FROM {s}.ducklake_table_stats ts "
        f" WHERE {_orphan_stats_predicate('ts')}) AS table_rows, "
        f"(SELECT COUNT(*) FROM {s}.ducklake_table_column_stats cs "
        f" WHERE {_orphan_stats_predicate('cs')}) AS column_rows"
    )
    table_rows, column_rows = _pg_query_one(conn, counts_sql)
    log.info(
        "purge-orphan-stats: %d orphaned table-stats rows, %d orphaned column-stats rows (dry_run=%s)",
        table_rows,
        column_rows,
        dry_run,
    )
    if dry_run or (table_rows == 0 and column_rows == 0):
        return

    _acquire_advisory_lock(conn)
    # One call = one transaction (see docstring); SET LOCAL bounds a blocked
    # or runaway delete so the advisory lock can't be held forever — same
    # pattern as repair-partition-values.
    _pg_execute(
        conn,
        f"SET LOCAL statement_timeout = '{_PURGE_ORPHAN_STATS_STATEMENT_TIMEOUT}'; "
        f"SET LOCAL lock_timeout = '{_PURGE_ORPHAN_STATS_LOCK_TIMEOUT}'; "
        f"DELETE FROM {s}.ducklake_table_stats ts WHERE {_orphan_stats_predicate('ts')}; "
        f"DELETE FROM {s}.ducklake_table_column_stats cs WHERE {_orphan_stats_predicate('cs')}",
    )

    after_table, after_column = _pg_query_one(conn, counts_sql)
    log.info(
        "purge-orphan-stats: deleted ~%d table-stats and ~%d column-stats rows "
        "(%d/%d orphans remain from churn during the run)",
        max(0, table_rows - after_table),
        max(0, column_rows - after_column),
        after_table,
        after_column,
    )
    if table_rows + column_rows >= 100_000:
        log.info(
            "purge-orphan-stats: large purge — run VACUUM (ANALYZE) on "
            "%s.ducklake_table_stats and %s.ducklake_table_column_stats to reclaim "
            "dead pages now instead of waiting for autovacuum",
            s,
            s,
        )


# ---------------------------------------------------------------------------
# repair-partition-values: recurring catalog cleanup
# ---------------------------------------------------------------------------
# Workaround for an upstream DuckLake bug in ducklake_add_data_files(): when a
# partition spec applies multiple transforms to one source column (events:
# year/month/day on `timestamp`; persons: year/month on `_timestamp`), every
# ducklake_file_partition_value row lands at the HIGHEST partition_key_index
# instead of being spread across the spec's columns. Tier-3 compaction then
# fails with "Files have different hive partition path", and partition pruning
# silently misses files.
#
# A dagster duckling backfill fix-up reduces new-write bleeding (PostHog repo,
# posthog/dags/events_backfill_to_duckling.py:_fixup_partition_values_for_added_files),
# but it's per-batch and may not be deployed everywhere. This subcommand is the
# catalog-side backstop: wired as the first step of the 15-minute compaction
# cronjob, it cleans up any rot (historical or freshly-introduced) on every
# tick. The cheap pre-flight EXISTS query short-circuits when there's nothing
# to do, so clean catalogs pay near-zero cost.
#
# REMOVE this subcommand (and the matching just-recipes) once the upstream
# DuckLake source fix is deployed across every customer warehouse AND every
# customer's catalog has had at least one clean pre-flight (no rot detected).

# REMOVE-WHEN: upstream-ducklake-fix-deployed
# Per-kind partition spec. "events" / "persons" are the logical kinds; the
# actual catalog table name can carry a per-team suffix (events_<suffix> /
# persons_<suffix>) per DuckgresServerTeam.table_suffix on the posthog side.
# The spec is identical across suffix variants since the partition layout
# doesn't change. Must mirror the live catalog
# (ducklake_partition_column.transform for the active partition_id). Asserted
# at runtime before any DML — drift fails loud.
_REPAIR_PARTITION_VALUE_SPEC: dict[str, tuple[tuple[int, str], ...]] = {
    "events": ((0, "year"), (1, "month"), (2, "day")),
    "persons": ((0, "year"), (1, "month")),
}

# Catalog-derived table names must be plain identifiers before they are ever
# interpolated into SQL (defense-in-depth: interpolation sites ALSO escape
# via _sql_string_literal, but a quote-bearing name has no business in this
# repair pipeline at all).
_CATALOG_TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")

# Bounds on the per-table repair statement so a runaway query can't hold the
# advisory lock forever and block all other maintenance.
_REPAIR_PARTITION_VALUE_STATEMENT_TIMEOUT = "300s"
_REPAIR_PARTITION_VALUE_LOCK_TIMEOUT = "30s"


def _repair_partition_values_path_shape_predicate(
    table_kind: Literal["events", "persons"], *, column: str = "path"
) -> str:
    """SQL regex that matches the kind's expected hive layout — numeric year/
    month[/day] segments followed by a single .parquet file name. Tighter than
    a LIKE chain: it rejects non-numeric segments (e.g. `day=ab`) at the
    targets stage. Without this, `substring(path from '<col>=([0-9]+)')`
    silently returns NULL on a non-matching path, which would produce NULL
    partition_value rows — a third class of catalog rot worse than the bug
    we're repairing. `column` defaults to bare `path`; pass the aliased form
    (e.g. `df.path`) when the surrounding SELECT uses a table alias."""
    if table_kind == "events":
        return f"({column} ~ '/year=[0-9]+/month=[0-9]+/day=[0-9]+/[^/]+\\.parquet$')"
    if table_kind == "persons":
        return f"({column} ~ '/year=[0-9]+/month=[0-9]+/[^/]+\\.parquet$')"
    raise ValueError(f"unknown table_kind: {table_kind!r}")


def _repair_partition_values_discover_tables(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    """Find every live posthog.events* / posthog.persons* table in this catalog.

    Returns a list of (kind, table_name), where kind is "events"/"persons" and
    table_name is the actual catalog name (potentially suffixed). LIKE escapes
    the underscore because PG treats _ as a single-char wildcard — without the
    escape, "events" itself would match the "events_%" pattern.
    """
    discover_sql = (
        f"SELECT t.table_name "
        f"FROM {PG_CATALOG_SCHEMA}.ducklake_table t "
        f"JOIN {PG_CATALOG_SCHEMA}.ducklake_schema sch "
        f"  ON sch.schema_id = t.schema_id AND sch.end_snapshot IS NULL "
        f"WHERE sch.schema_name = 'posthog' "
        f"  AND t.end_snapshot IS NULL "
        f"  AND ("
        f"    t.table_name = 'events' OR t.table_name LIKE 'events\\_%' ESCAPE '\\' "
        f"    OR t.table_name = 'persons' OR t.table_name LIKE 'persons\\_%' ESCAPE '\\'"
        f"  ) "
        f"ORDER BY t.table_name"
    )
    rows = conn.execute(
        f"SELECT * FROM postgres_query('{PG_ATTACH_NAME}', {_sql_string_literal(discover_sql)})"
    ).fetchall()
    discovered: list[tuple[str, str]] = []
    for (table_name,) in rows:
        # Catalog-derived names are attacker/typo-controlled via ordinary
        # DDL and get interpolated into later postgres_query/postgres_execute
        # statements. A name outside the strict identifier shape (e.g.
        # containing a quote) is by definition not a millpond-written
        # events_*/persons_* table, so it is skipped LOUDLY rather than
        # repaired — never interpolated.
        if not _CATALOG_TABLE_NAME_RE.fullmatch(table_name):
            log.warning(
                "repair-partition-values: skipping table with non-identifier name %r "
                "(not millpond-written; refusing to interpolate it into SQL)",
                table_name,
            )
            continue
        if table_name == "events" or table_name.startswith("events_"):
            discovered.append(("events", table_name))
        elif table_name == "persons" or table_name.startswith("persons_"):
            discovered.append(("persons", table_name))
    return discovered


def _repair_partition_values_pre_flight_any_rot(conn: duckdb.DuckDBPyConnection) -> bool:
    """Cheap one-shot EXISTS across all in-scope posthog events/persons tables.

    Returns True iff at least one file's fpv INDEX SET is not exactly
    {0..N-1} for its kind (events {0,1,2}, persons {0,1}), or ANY fpv row in
    scope has a NULL partition_value. The array-set check mirrors
    _count_broken / _execute's post-condition and catches BOTH missing rows
    AND the collapsed-index shape (N rows all stacked on the top index —
    ducklake_add_data_files rot with the RIGHT row count at the WRONG
    indexes). A bare row-count check passed that shape, so the pre-flight
    short-circuited "clean" over real rot and _execute never ran — exactly
    the corruption the partition-value-corruption dashboard metric flags.
    EXISTS short-circuits at the first matching row inside the LIMIT 1
    subquery — keeps this snappy on clean catalogs.

    Important because this subcommand runs as the first step of the
    15-minute compaction cronjob: most invocations will be against clean
    catalogs and should exit before doing N per-table count queries.
    """
    # Restrict to files a real _execute run would actually touch:
    #   - Table must be PARTITIONED (has a live ducklake_partition_info row) —
    #     otherwise events_nrt / other unpartitioned events*/persons* variants
    #     produce an empty fpv index set and trip the HAVING branch forever.
    #   - Path must be S3-absolute — lake-relative files (ducklake-{uuid}.parquet
    #     at the lake root) don't fit the hive layout the repair targets.
    #   - Path must match the kind-specific hive layout _execute expects
    #     (events: year/month/day; persons: year/month) — some persons files
    #     were written under a legacy `/year=/month=/day=/…` path that this
    #     tool can't currently parse (persons has no `day` partition column),
    #     so they're out of _execute's scope and shouldn't fire pre-flight.
    # Without these filters this always returns TRUE on any catalog with
    # unpartitioned variants, lake-relative files, or those legacy paths,
    # defeating the cron short-circuit.
    events_path_shape = _repair_partition_values_path_shape_predicate("events", column="df.path")
    persons_path_shape = _repair_partition_values_path_shape_predicate("persons", column="df.path")
    # Same index-set aggregate + expected arrays as _count_broken and
    # _execute's post-condition — all three MUST agree, or the pre-flight
    # short-circuits over rot the repair would fix (or the reverse: fires
    # forever over files the repair won't touch).
    index_set_agg = (
        "COALESCE(array_agg(fpv.partition_key_index ORDER BY fpv.partition_key_index) "
        "FILTER (WHERE fpv.partition_key_index IS NOT NULL), '{}'::bigint[])"
    )
    expected = {
        kind: "ARRAY[" + ",".join(str(idx) for idx, _ in spec) + "]::bigint[]"
        for kind, spec in _REPAIR_PARTITION_VALUE_SPEC.items()
    }
    pre_flight_sql = (
        "SELECT EXISTS ( "
        "  SELECT 1 FROM ( "
        "    SELECT df.data_file_id, df.table_id, t.table_name, "
        f"           {index_set_agg} AS index_set, "
        "           BOOL_OR(fpv.partition_value IS NULL) AS has_null "
        f"    FROM {PG_CATALOG_SCHEMA}.ducklake_data_file df "
        f"    JOIN {PG_CATALOG_SCHEMA}.ducklake_table t USING (table_id) "
        f"    JOIN {PG_CATALOG_SCHEMA}.ducklake_schema sch "
        "      ON sch.schema_id = t.schema_id AND sch.end_snapshot IS NULL "
        f"    JOIN {PG_CATALOG_SCHEMA}.ducklake_partition_info pi "
        "      ON pi.table_id = t.table_id AND pi.end_snapshot IS NULL "
        f"    LEFT JOIN {PG_CATALOG_SCHEMA}.ducklake_file_partition_value fpv "
        "      ON fpv.data_file_id = df.data_file_id AND fpv.table_id = df.table_id "
        "    WHERE sch.schema_name = 'posthog' "
        "      AND t.end_snapshot IS NULL "
        "      AND df.end_snapshot IS NULL "
        # Match the discovery gate (_CATALOG_TABLE_NAME_RE): a table whose
        # name isn't a plain identifier is skipped by the repair, so it must
        # not make pre-flight report rot the plan will never touch (else the
        # cron alternates has_rot=true / repaired-nothing forever).
        "      AND t.table_name ~ '^[A-Za-z0-9_]+$' "
        "      AND df.path LIKE 's3://%' "
        "      AND df.path NOT LIKE '%/full/%' "
        "      AND ( "
        f"        ((t.table_name = 'events' OR t.table_name LIKE 'events\\_%' ESCAPE '\\') "
        f"          AND {events_path_shape}) "
        f"        OR ((t.table_name = 'persons' OR t.table_name LIKE 'persons\\_%' ESCAPE '\\') "
        f"          AND {persons_path_shape}) "
        "      ) "
        "    GROUP BY df.data_file_id, df.table_id, t.table_name "
        # Index-SET comparison, not row count: the collapsed shape (3 rows all
        # at index 2) has the right count at the wrong indexes, and the
        # non-DISTINCT array_agg also catches duplicated rows per index.
        f"    HAVING (t.table_name LIKE 'events%' AND {index_set_agg} IS DISTINCT FROM {expected['events']}) "
        f"        OR (t.table_name LIKE 'persons%' AND {index_set_agg} IS DISTINCT FROM {expected['persons']}) "
        "        OR BOOL_OR(fpv.partition_value IS NULL) "
        "    LIMIT 1 "
        "  ) s "
        ") AS has_rot"
    )
    row = conn.execute(
        f"SELECT * FROM postgres_query('{PG_ATTACH_NAME}', {_sql_string_literal(pre_flight_sql)})"
    ).fetchone()
    if row is None:
        raise RuntimeError("repair-partition-values pre-flight: EXISTS query returned no row")
    return bool(row[0])


def _repair_partition_values_log_outliers(conn: duckdb.DuckDBPyConnection, tables: list[tuple[str, str]]) -> None:
    # Lake-relative files (`ducklake-{uuid}.parquet` at the lake root) and the
    # persons /full/ singleton don't fit the hive layout this subcommand
    # repairs. They're known and intentional; just log counts so the operator
    # sees they exist (they're skipped by the repair filters).
    if not tables:
        return
    table_names_sql = ", ".join(_sql_string_literal(tn) for _, tn in tables)
    counts_sql = (
        f"SELECT t.table_name, "
        f"  COUNT(*) FILTER (WHERE df.path NOT LIKE 's3://%') AS lake_relative, "
        f"  COUNT(*) FILTER (WHERE df.path LIKE '%/full/%')   AS full_singleton "
        f"FROM {PG_CATALOG_SCHEMA}.ducklake_data_file df "
        f"JOIN {PG_CATALOG_SCHEMA}.ducklake_table t USING (table_id) "
        f"JOIN {PG_CATALOG_SCHEMA}.ducklake_schema sch "
        f"  ON sch.schema_id = t.schema_id AND sch.end_snapshot IS NULL "
        f"WHERE df.end_snapshot IS NULL AND t.end_snapshot IS NULL "
        f"  AND sch.schema_name = 'posthog' "
        f"  AND t.table_name IN ({table_names_sql}) "
        f"GROUP BY t.table_name "
        f"HAVING COUNT(*) FILTER (WHERE df.path NOT LIKE 's3://%') > 0 "
        f"    OR COUNT(*) FILTER (WHERE df.path LIKE '%/full/%') > 0"
    )
    rows = conn.execute(
        f"SELECT * FROM postgres_query('{PG_ATTACH_NAME}', {_sql_string_literal(counts_sql)})"
    ).fetchall()
    for table_name, lake_relative, full_singleton in rows:
        log.warning(
            "repair-partition-values: %s has %d lake-relative + %d /full/ file(s) "
            "(skipped — not in scope of this repair)",
            table_name,
            lake_relative,
            full_singleton,
        )


def _repair_partition_values_resolve(
    conn: duckdb.DuckDBPyConnection, table_kind: Literal["events", "persons"], table_name: str
) -> tuple[int, int] | None:
    """Look up (table_id, partition_id) for posthog.<table_name> and verify the
    live partition_column spec matches _REPAIR_PARTITION_VALUE_SPEC[kind].

    Returns None if the table was discovered earlier but has since gone away
    (race against ALTER/DROP, or end_snapshot was set between discover and
    resolve). Raises if the live spec doesn't match what we'd write.
    """
    lookup_sql = (
        f"SELECT t.table_id, pi.partition_id "
        f"FROM {PG_CATALOG_SCHEMA}.ducklake_table t "
        f"JOIN {PG_CATALOG_SCHEMA}.ducklake_schema sch "
        f"  ON sch.schema_id = t.schema_id AND sch.end_snapshot IS NULL "
        f"JOIN {PG_CATALOG_SCHEMA}.ducklake_partition_info pi "
        f"  ON pi.table_id = t.table_id AND pi.end_snapshot IS NULL "
        f"WHERE sch.schema_name = 'posthog' "
        f"  AND t.table_name = {_sql_string_literal(table_name)} "
        f"  AND t.end_snapshot IS NULL"
    )
    rows = conn.execute(
        f"SELECT * FROM postgres_query('{PG_ATTACH_NAME}', {_sql_string_literal(lookup_sql)})"
    ).fetchall()
    if len(rows) == 0:
        log.warning(
            "repair-partition-values: posthog.%s was discovered but no live partition_info found — "
            "table may have been dropped or unpartitioned between discovery and resolve. Skipping.",
            table_name,
        )
        return None
    if len(rows) != 1:
        raise RuntimeError(
            f"repair-partition-values: expected exactly one live partition_info for "
            f"posthog.{table_name}, got {len(rows)}"
        )
    table_id, partition_id = rows[0]

    # Assert live spec matches our hardcoded expectation for this kind.
    spec_sql = (
        f"SELECT partition_key_index, transform "
        f"FROM {PG_CATALOG_SCHEMA}.ducklake_partition_column "
        f"WHERE partition_id = {int(partition_id)} AND table_id = {int(table_id)} "
        f"ORDER BY partition_key_index"
    )
    spec_rows = conn.execute(
        f"SELECT * FROM postgres_query('{PG_ATTACH_NAME}', {_sql_string_literal(spec_sql)})"
    ).fetchall()
    actual = tuple((int(idx), str(transform)) for idx, transform in spec_rows)
    expected = _REPAIR_PARTITION_VALUE_SPEC[table_kind]
    if actual != expected:
        raise RuntimeError(
            f"repair-partition-values: live catalog spec for posthog.{table_name} "
            f"(kind={table_kind}, partition_id={partition_id}) is {actual}; expected {expected}. "
            f"Update _REPAIR_PARTITION_VALUE_SPEC and redeploy before re-running. "
            f"Inspect with: SELECT partition_key_index, transform FROM public.ducklake_partition_column "
            f"WHERE partition_id = {int(partition_id)} AND table_id = {int(table_id)} ORDER BY partition_key_index;"
        )
    return int(table_id), int(partition_id)


def _repair_partition_values_count_broken(
    conn: duckdb.DuckDBPyConnection,
    table_kind: Literal["events", "persons"],
    table_id: int,
    partition_id: int,
    spec: tuple[tuple[int, str], ...],
) -> tuple[int, int]:
    """Return (wrong_indexes, null_values) for files this table needs repaired.

    A file is "wrong_indexes" if its actual fpv partition_key_index set is NOT
    exactly {0,1,...,len(spec)-1}, OR its partition_id is NULL on a legacy
    team_id=... path. A file is "null_values" if ANY of its fpv rows has a
    NULL partition_value — separately reported because that's a third class of
    rot (e.g. from a hypothetical write that registered a path the substring
    regex couldn't parse). Lake-relative + /full/ files are deliberately
    excluded (warned about separately). Scoped by table_id (so per-team
    suffixed tables don't bleed into each other); the legacy-path filter uses
    kind because the S3 layout is keyed on the LOGICAL kind, not the suffixed
    table_name."""
    expected_index_set = "ARRAY[" + ",".join(str(idx) for idx, _ in spec) + "]"
    path_shape_predicate = _repair_partition_values_path_shape_predicate(table_kind, column="df.path")
    count_sql = (
        f"WITH targets AS ( "
        f"  SELECT df.data_file_id "
        f"  FROM {PG_CATALOG_SCHEMA}.ducklake_data_file df "
        f"  WHERE df.table_id = {table_id} "
        f"    AND df.end_snapshot IS NULL "
        f"    AND df.path NOT LIKE '%/full/%' "
        f"    AND {path_shape_predicate} "
        f"    AND ( "
        f"      df.partition_id = {partition_id} "
        f"      OR (df.partition_id IS NULL "
        f"          AND df.path LIKE '%/backfill/{table_kind}/team_id=%') "
        f"    ) "
        f"), "
        f"state AS ( "
        f"  SELECT t.data_file_id, "
        f"         COALESCE( "
        f"           array_agg(fpv.partition_key_index ORDER BY fpv.partition_key_index) "
        f"           FILTER (WHERE fpv.partition_key_index IS NOT NULL), "
        f"           '{{}}'::bigint[] "
        f"         ) AS indexes, "
        f"         BOOL_OR(fpv.partition_value IS NULL) AS has_null "
        f"  FROM targets t "
        f"  LEFT JOIN {PG_CATALOG_SCHEMA}.ducklake_file_partition_value fpv "
        f"    ON fpv.data_file_id = t.data_file_id AND fpv.table_id = {table_id} "
        f"  GROUP BY t.data_file_id "
        f") "
        f"SELECT "
        f"  COUNT(*) FILTER (WHERE indexes IS DISTINCT FROM {expected_index_set}::bigint[]) AS wrong_indexes, "
        f"  COUNT(*) FILTER (WHERE has_null) AS null_values "
        f"FROM state"
    )
    row = conn.execute(f"SELECT * FROM postgres_query('{PG_ATTACH_NAME}', {_sql_string_literal(count_sql)})").fetchone()
    if row is None:
        raise RuntimeError(
            "repair-partition-values: count query returned no row (Postgres protocol invariant violated)"
        )
    return int(row[0]), int(row[1])


def _repair_partition_values_execute(
    conn: duckdb.DuckDBPyConnection,
    table_kind: Literal["events", "persons"],
    table_id: int,
    partition_id: int,
    spec: tuple[tuple[int, str], ...],
) -> None:
    """Race-safe per-table repair, executed as ONE postgres_execute call.

    DuckDB's postgres extension wraps every postgres_execute call in
    ``BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; ...; COMMIT;``. All
    statements in this call therefore share one snapshot — concurrent dagster
    commits that land DURING the call are invisible to every read in this
    call (including the trailing post-condition), and our own writes are
    visible to that post-condition. No operator-side freeze of dagster is
    required.

    Statements (one call, one snapshot):
      1. SET LOCAL statement_timeout + lock_timeout — bound runaway so the
         advisory lock can't be held forever.
      2. CREATE TEMP TABLE _repair_targets ON COMMIT DROP AS SELECT … —
         materialize Class A (NULL partition_id at legacy team_id= path) +
         Class B (partition_id set, partial fpv rows) + Era 3 (N fpv rows
         all at highest index) into a temp table. ON COMMIT DROP means it
         auto-cleans at the end of this txn, so the next per-table call
         sees no leftover state.
      3. wCTE INSERT — relabel UPDATEs partition_id on the Class A subset,
         deleted DELETEs existing fpv rows for the target set, INSERT
         re-emits canonical fpv rows parsed from each target's path. All
         three driven off _repair_targets, scoped consistently with the
         post-condition. relabel + deleted are unreferenced from INSERT;
         PG §7.8.2 guarantees data-modifying CTEs in WITH run exactly once
         "to completion, independently of whether the primary query reads
         all (or indeed any) of their output." A defensive reference
         (WHERE EXISTS) would silently filter Class A files whose DELETE
         returns 0 rows.
      4. DO block — re-derives fpv state for _repair_targets including
         our own writes; RAISE EXCEPTION if any row still has wrong indexes
         or NULL partition_value. The exception surfaces to Python as a
         duckdb error; the orchestrator catches it and continues to the
         next table.

    Scoped by table_id; the legacy-path predicate uses kind because the
    dagster backfill writes to s3://.../backfill/<kind>/<team_id>/...
    regardless of whether the catalog table carries a suffix. The hive-
    shape predicate rejects non-conforming paths at the targets stage —
    without it, ``substring(...)`` would silently return NULL on a non-
    matching path and we'd insert NULL partition_value rows.

    Silent skip worth knowing: a NULL-partition_id file at a path that does
    NOT match the legacy ``/backfill/<kind>/team_id=`` layout is excluded
    from _repair_targets. This tool is scoped to the dagster-bug rot; other
    NULL-partition_id origins (if any ever appear) are not in scope.

    A dagster commit whose data_file_id was allocated below our visible MAX
    but committed AFTER our snapshot is invisible to this call and goes
    unrepaired. A re-run picks it up — convergent."""
    expected_index_set = "ARRAY[" + ",".join(str(idx) for idx, _ in spec) + "]"
    targets_path_shape = _repair_partition_values_path_shape_predicate(table_kind, column="df.path")
    insert_branches = " UNION ALL ".join(
        f"SELECT t.data_file_id, {table_id}, {idx}, "
        f"(substring(t.path from '{col_name}=([0-9]+)'))::INT::TEXT "
        f"FROM _repair_targets t"
        for idx, col_name in spec
    )
    repair_sql = (
        f"SET LOCAL statement_timeout = '{_REPAIR_PARTITION_VALUE_STATEMENT_TIMEOUT}'; "
        f"SET LOCAL lock_timeout = '{_REPAIR_PARTITION_VALUE_LOCK_TIMEOUT}'; "
        f"CREATE TEMP TABLE _repair_targets ON COMMIT DROP AS "
        f"  SELECT df.data_file_id, df.path "
        f"  FROM {PG_CATALOG_SCHEMA}.ducklake_data_file df "
        f"  WHERE df.table_id = {table_id} "
        f"    AND df.end_snapshot IS NULL "
        f"    AND df.path NOT LIKE '%/full/%' "
        f"    AND {targets_path_shape} "
        f"    AND ( "
        f"      df.partition_id = {partition_id} "
        f"      OR (df.partition_id IS NULL "
        f"          AND df.path LIKE '%/backfill/{table_kind}/team_id=%') "
        f"    ); "
        f"WITH relabel AS ( "
        f"  UPDATE {PG_CATALOG_SCHEMA}.ducklake_data_file "
        f"  SET partition_id = {partition_id} "
        f"  WHERE data_file_id IN (SELECT data_file_id FROM _repair_targets) "
        f"    AND partition_id IS NULL "
        f"), "
        f"deleted AS ( "
        f"  DELETE FROM {PG_CATALOG_SCHEMA}.ducklake_file_partition_value "
        f"  WHERE table_id = {table_id} "
        f"    AND data_file_id IN (SELECT data_file_id FROM _repair_targets) "
        f") "
        f"INSERT INTO {PG_CATALOG_SCHEMA}.ducklake_file_partition_value "
        f"  (data_file_id, table_id, partition_key_index, partition_value) "
        f"{insert_branches}; "
        f"DO $repair$ "
        f"DECLARE wrong INT; nulls INT; "
        f"BEGIN "
        f"  WITH state AS ( "
        f"    SELECT t.data_file_id, "
        f"           COALESCE( "
        f"             array_agg(fpv.partition_key_index ORDER BY fpv.partition_key_index) "
        f"             FILTER (WHERE fpv.partition_key_index IS NOT NULL), "
        f"             '{{}}'::bigint[] "
        f"           ) AS indexes, "
        f"           BOOL_OR(fpv.partition_value IS NULL) AS has_null "
        f"    FROM _repair_targets t "
        f"    LEFT JOIN {PG_CATALOG_SCHEMA}.ducklake_file_partition_value fpv "
        f"      ON fpv.data_file_id = t.data_file_id AND fpv.table_id = {table_id} "
        f"    GROUP BY t.data_file_id "
        f"  ) "
        f"  SELECT COUNT(*) FILTER (WHERE indexes IS DISTINCT FROM {expected_index_set}::bigint[]), "
        f"         COUNT(*) FILTER (WHERE has_null) "
        f"    INTO wrong, nulls FROM state; "
        f"  IF wrong > 0 OR nulls > 0 THEN "
        f"    RAISE EXCEPTION 'repair-partition-values post-condition failed for table_id={table_id}: "
        f"wrong_indexes=% null_values=%', wrong, nulls; "
        f"  END IF; "
        f"END $repair$;"
    )
    conn.execute(f"CALL postgres_execute('{PG_ATTACH_NAME}', {_sql_string_literal(repair_sql)})")


def repair_partition_values(conn: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Recurring catalog repair for the ducklake_add_data_files partition index bug.

    Designed to run as the first step of the 15-minute compaction cronjob, so
    every invocation must be cheap on clean catalogs and safe alongside the
    dagster backfill. See the module-level repair-partition-values block above
    for full context.

    Order of operations:
      1. Acquire millpond's session-scoped maintenance advisory lock (unless
         dry_run) so concurrent millpond maintenance jobs don't race. The lock
         does NOT serialize against dagster — REPEATABLE READ does that.
      2. Discover every live posthog.events* / posthog.persons* table in this
         catalog (covers DuckgresServerTeam.table_suffix variants).
      3. Cheap pre-flight EXISTS: short-circuit and return clean if no file in
         scope has wrong fpv shape or NULL partition_value. This is the hot
         path for cron-driven invocations on already-clean catalogs.
      4. Per-table plan: resolve (table_id, partition_id), verify the live
         catalog spec matches our hardcoded expectation, count broken files.
      5. Per-table repair: one postgres_execute call per table (race-safe via
         REPEATABLE READ snapshot — see _repair_partition_values_execute).

    Per-table failures (post-condition raise, lock timeout, statement timeout,
    network blip) are logged and the loop continues — one bad table doesn't
    block the rest. The orchestrator raises iff every planned table failed
    (the tool achieved nothing); otherwise warns and returns clean.

    Convergent: safe to re-run on already-repaired catalogs (pre-flight
    short-circuits) or on catalogs without events/persons tables (exits after
    discover). A data_file_id that dagster allocates below our snapshot's
    visible MAX but commits AFTER our snapshot is invisible to this run;
    the next 15-minute tick picks it up.
    """
    log.info("repair-partition-values: starting (dry_run=%s)", dry_run)

    if not dry_run:
        _acquire_advisory_lock(conn)

    discovered = _repair_partition_values_discover_tables(conn)
    if not discovered:
        log.info("repair-partition-values: no posthog.events* / posthog.persons* tables in this catalog, exiting")
        return
    log.info(
        "repair-partition-values: discovered %d table(s): %s",
        len(discovered),
        ", ".join(f"{name} ({kind})" for kind, name in discovered),
    )

    if not _repair_partition_values_pre_flight_any_rot(conn):
        log.info("repair-partition-values: pre-flight clean (no rot detected in scope), exiting")
        return

    _repair_partition_values_log_outliers(conn, discovered)

    plan: list[tuple[Literal["events", "persons"], str, int, int, tuple[tuple[int, str], ...], int, int]] = []
    for table_kind, table_name in discovered:
        spec = _REPAIR_PARTITION_VALUE_SPEC[table_kind]
        try:
            resolved = _repair_partition_values_resolve(conn, table_kind, table_name)
            if resolved is None:
                continue
            table_id, partition_id = resolved
            wrong_indexes, null_values = _repair_partition_values_count_broken(
                conn, table_kind, table_id, partition_id, spec
            )
        except Exception as exc:
            log.warning(
                "repair-partition-values: table=%s kind=%s planning failed; skipping (likely out of scope — "
                "different partition spec, dropped table, or transient catalog read). error=%s",
                table_name,
                table_kind,
                exc,
            )
            continue
        log.info(
            "repair-partition-values: table=%s kind=%s table_id=%d partition_id=%d "
            "pre_wrong_indexes=%d pre_null_values=%d",
            table_name,
            table_kind,
            table_id,
            partition_id,
            wrong_indexes,
            null_values,
        )
        if wrong_indexes > 0 or null_values > 0:
            plan.append((table_kind, table_name, table_id, partition_id, spec, wrong_indexes, null_values))

    if not plan:
        log.info("repair-partition-values: nothing to repair after planning, exiting")
        return

    if dry_run:
        log.info("repair-partition-values: dry_run=True, skipping execute")
        return

    failed: list[str] = []
    for table_kind, table_name, table_id, partition_id, spec, pre_wrong, pre_null in plan:
        try:
            _repair_partition_values_execute(conn, table_kind, table_id, partition_id, spec)
            post_wrong, post_null = _repair_partition_values_count_broken(
                conn, table_kind, table_id, partition_id, spec
            )
        except Exception as exc:
            log.error(
                "repair-partition-values: table=%s kind=%s table_id=%d partition_id=%d "
                "execute failed; continuing. error=%s",
                table_name,
                table_kind,
                table_id,
                partition_id,
                exc,
            )
            failed.append(table_name)
            continue
        log.info(
            "repair-partition-values: table=%s kind=%s table_id=%d partition_id=%d "
            "pre_wrong_indexes=%d pre_null_values=%d post_wrong_indexes=%d post_null_values=%d",
            table_name,
            table_kind,
            table_id,
            partition_id,
            pre_wrong,
            pre_null,
            post_wrong,
            post_null,
        )

    if failed and len(failed) == len(plan):
        raise RuntimeError(f"repair-partition-values: all {len(plan)} planned table(s) failed: {', '.join(failed)}")
    if failed:
        log.warning(
            "repair-partition-values: %d/%d table(s) failed: %s (next cron tick will retry)",
            len(failed),
            len(plan),
            ", ".join(failed),
        )


def _heal_orphans_b1_counts(conn: duckdb.DuckDBPyConnection, data_path: str) -> tuple[int, int]:
    """Run heal-orphans's B1 gate counts against an already-populated _orphans.

    Returns ``(total_live_data_files, would_be_live)``. Filters
    ducklake_data_file to live rows (``end_snapshot IS NULL``) and normalizes
    both sides to absolute form so cross-table mismatches in storage form
    don't slip past the gate. Extracted from heal_orphans so production and
    tests share the same query — without this, regressions to the
    ``LIKE 's3://%' OR LIKE '/%'`` branch silently slip past test fixtures
    that only use one or the other.
    """
    return conn.execute(
        f"""
        WITH orphan_abs AS (
            SELECT CASE WHEN path LIKE 's3://%' OR path LIKE '/%' THEN path
                        ELSE rtrim(?, '/') || '/' || path END AS abs_path
            FROM _orphans
        ),
        live_data_abs AS (
            SELECT CASE WHEN path LIKE 's3://%' OR path LIKE '/%' THEN path
                        ELSE rtrim(?, '/') || '/' || path END AS abs_path
            FROM {METADATA_SCHEMA}.ducklake_data_file
            WHERE end_snapshot IS NULL
        )
        SELECT
            (SELECT COUNT(*) FROM {METADATA_SCHEMA}.ducklake_data_file
             WHERE end_snapshot IS NULL) AS total_live,
            (SELECT COUNT(*) FROM live_data_abs
             WHERE abs_path IN (SELECT abs_path FROM orphan_abs)) AS would_be_live
        """,
        [data_path, data_path],
    ).fetchone()


def heal_orphans(conn: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Delete catalog rows whose S3 key no longer exists.

    Five-step gated procedure (addresses lead-QE punch list B1/B2/B3/H1/H4):

      1. Take the advisory lock (skipped on --dry-run since nothing
         mutates and the dry-run output is informational only).
      2. Materialize the orphan set into a TEMP TABLE so subsequent
         safety gates and the final DELETE all see the same snapshot.
         Done under the lock so the snapshot is stable for the whole
         procedure — without this ordering another maintenance
         invocation could change ducklake_files_scheduled_for_deletion
         between scan and DELETE, invalidating the gates.
      3. Safety gate B1: prove `ducklake_data_file` is non-empty AND that
         none of the orphan paths are referenced as live data files. A
         vacuous pass (gate succeeds because the lake is empty) is not
         allowed.
      4. Safety gate B3: prove no positional delete vector
         (`ducklake_delete_file`) points at an orphan `data_file_id`.
         If one does, the file is still live for vector lookups — abort.
      5. One `postgres_execute` DELETE matching on `path` (UUIDv7-unique
         per quirk r3); single statement, atomic at the Postgres layer.
    """
    data_path = _require("DUCKLAKE_DATA_PATH")
    log.info("heal-orphans: scanning for catalog-side orphans (dry_run=%s)", dry_run)

    if not dry_run:
        _acquire_advisory_lock(conn)

    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _orphans AS SELECT data_file_id, path FROM find_catalog_orphans(?)",
        [data_path],
    )
    n_orphans = conn.execute("SELECT COUNT(*) FROM _orphans").fetchone()[0]
    log.info("heal-orphans: %d catalog rows reference S3 paths that no longer exist", n_orphans)
    if n_orphans == 0:
        return

    # B1: positive-proof gate. Both clauses must hold. The query lives in
    # _heal_orphans_b1_counts so production and tests share one source.
    total_live_data_files, would_be_live = _heal_orphans_b1_counts(conn, data_path)
    if total_live_data_files == 0:
        raise RuntimeError(
            "heal-orphans safety gate B1 failed: ducklake_data_file has zero "
            "live rows (end_snapshot IS NULL). Refusing to operate on a "
            "vacuous catalog."
        )
    if would_be_live > 0:
        raise RuntimeError(
            f"heal-orphans safety gate B1 failed: {would_be_live} of the "
            f"{n_orphans} 'orphan' paths still appear as live rows in "
            "ducklake_data_file. Aborting — these are not orphans."
        )

    # B3: any LIVE positional-delete vector pointing at an orphan id is a hard
    # abort. The delete-vector table references the data file by
    # data_file_id, so a match here means the file is still live for vector
    # lookups. Historical (end_snapshot IS NOT NULL) delete vectors are no
    # longer live and must not block heal-orphans, mirroring the B1 gate.
    delete_vector_refs = conn.execute(
        f"SELECT COUNT(*) FROM {METADATA_SCHEMA}.ducklake_delete_file "
        f"WHERE end_snapshot IS NULL "
        f"  AND data_file_id IN (SELECT data_file_id FROM _orphans)"
    ).fetchone()[0]
    if delete_vector_refs > 0:
        raise RuntimeError(
            f"heal-orphans safety gate B3 failed: {delete_vector_refs} positional "
            "delete vector(s) reference 'orphan' data_file_ids. Aborting — those "
            "files are still live for delete-vector lookups."
        )

    log.info("heal-orphans: safety gates B1+B3 passed; %d rows queued for delete", n_orphans)
    if dry_run:
        return

    # Lock already held from step 1; materialize the path list out of the temp
    # table and ship it as a single DELETE through postgres_execute. The duckdb
    # postgres extension autocommits per statement, so this one DELETE is
    # atomic at the upstream Postgres layer (per quirk r4).
    paths = [row[0] for row in conn.execute("SELECT path FROM _orphans").fetchall()]
    path_list = ", ".join(_sql_string_literal(p) for p in paths)
    delete_sql = f"DELETE FROM {PG_CATALOG_SCHEMA}.ducklake_files_scheduled_for_deletion WHERE path IN ({path_list})"
    conn.execute(f"CALL postgres_execute('{PG_ATTACH_NAME}', {_sql_string_literal(delete_sql)})")

    after = conn.execute(
        "SELECT COUNT(*) FROM _orphans o "
        f"JOIN {METADATA_SCHEMA}.ducklake_files_scheduled_for_deletion s ON o.path = s.path"
    ).fetchone()[0]
    if after != 0:
        log.warning("heal-orphans: %d orphan rows remain in the queue after DELETE", after)
    else:
        log.info("heal-orphans: %d orphan rows removed from the queue", n_orphans)


def cleanup_all_safe(conn: duckdb.DuckDBPyConnection, max_iterations: int) -> None:
    """Loop dedup + heal-orphans + cleanup-all until cleanup-all exits clean.

    Each crashed `ducklake_cleanup_old_files` (DuckLake bug c1: a NoSuchKey
    on S3 DELETE rolls back the txn but the S3 deletes already-committed are
    permanent) creates fresh catalog-side orphans. The orchestrator heals
    those between attempts so the next cleanup-all sees a clean queue.

    The advisory lock is acquired once for the whole orchestration so all
    three steps share mutual exclusion.
    """
    _acquire_advisory_lock(conn)
    for attempt in range(1, max_iterations + 1):
        log.info("cleanup-all-safe: attempt %d / %d", attempt, max_iterations)
        dedup_deletions(conn, dry_run=False)
        heal_orphans(conn, dry_run=False)
        try:
            cleanup_all(conn)
            log.info("cleanup-all-safe: cleanup-all succeeded on attempt %d", attempt)
            return
        except duckdb.IOException as e:
            log.warning(
                "cleanup-all-safe: cleanup-all crashed on attempt %d (%s); looping to heal fresh orphans",
                attempt,
                e,
            )
    raise RuntimeError(f"cleanup-all-safe exhausted {max_iterations} iterations without a clean cleanup-all run")


def fsck(conn: duckdb.DuckDBPyConnection, dry_run: bool, max_iterations: int) -> None:
    """End-to-end "lake catalog is healthy" recipe.

    Runs `cleanup-all-safe` (dedup + heal-orphans + cleanup-all in a loop)
    followed by `ducklake_delete_orphaned_files` to mop up S3-side orphans
    from any prior interrupted writes. Tiered compaction is intentionally
    out of scope for this recipe; run `compact-to-tier-N` separately.

    Dry-run delegates to the dry-run forms of each step rather than counting
    queue rows manually, so the B1/B3 safety gates inside heal-orphans
    actually run. A real fsck that would abort because of a failed gate
    aborts the dry-run too — operators see the same outcome.
    """
    if dry_run:
        log.info("fsck dry-run: starting")
        dedup_deletions(conn, dry_run=True)
        heal_orphans(conn, dry_run=True)
        orphans(conn, dry_run=True)
        log.info("fsck dry-run: done")
        return

    cleanup_all_safe(conn, max_iterations)
    orphans(conn, dry_run=False)


def find_orphans(conn: duckdb.DuckDBPyConnection) -> None:
    """List catalog rows whose S3 key no longer exists.

    Pure SELECT via the `find_catalog_orphans(data_path)` macro. Logs the
    summary on stderr and prints `data_file_id<TAB>path` rows on stdout,
    so the output is grep / wc / xargs-friendly.
    """
    data_path = _require("DUCKLAKE_DATA_PATH")
    rows = conn.execute(
        "SELECT data_file_id, path FROM find_catalog_orphans(?)",
        [data_path],
    ).fetchall()
    log.info("find-orphans: %d catalog rows reference S3 paths that no longer exist", len(rows))
    for data_file_id, path in rows:
        print(f"{data_file_id}\t{path}")


def orphans(conn: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Find and delete orphaned S3 files."""
    if not dry_run:
        # Mutating path: mutex against other maintenance invocations (the
        # extension-side maintenance in ingest pods does NOT take this lock;
        # see _acquire_advisory_lock docstring). Same-session re-acquisition
        # by nested callers (maintain -> expire/cleanup, fsck -> cleanup_all_safe)
        # is fine: pg advisory locks are reentrant per session.
        _acquire_advisory_lock(conn)
    log.info("Deleting orphaned files (dry_run=%s)", dry_run)
    result = conn.execute(
        f"CALL ducklake_delete_orphaned_files('{ATTACH_NAME}', dry_run => {str(dry_run).lower()})"
    ).fetchall()
    for row in result:
        log.info("orphans: %s", row)


def checkpoint(conn: duckdb.DuckDBPyConnection) -> None:
    """Run CHECKPOINT (integrated merge + expire + cleanup)."""
    # Always mutating (no dry-run form exists for CHECKPOINT).
    _acquire_advisory_lock(conn)
    log.info("Running CHECKPOINT")
    conn.execute(f"CHECKPOINT {ATTACH_NAME}")
    log.info("CHECKPOINT complete")


def maintain(conn: duckdb.DuckDBPyConnection, days: int, dry_run: bool) -> None:
    """Full maintenance: expire snapshots then cleanup files.

    No direct DML of its own; the advisory lock is taken (reentrantly, same
    session) inside expire() and cleanup().
    """
    expire(conn, days, dry_run)
    cleanup(conn, days, dry_run)


def _bytes_to_human(stored_value: str) -> str | None:
    """Convert a DuckLake-stored byte-count string back to a units-suffixed form.

    DuckLake persists ``target_file_size`` as raw bytes (e.g. ``'67108864'``)
    but ``ducklake_set_option`` rejects that form on input — it needs a
    KiB/MiB/GiB suffix. Pick the largest 1024^i unit that divides the value
    cleanly. Returns None when the input is not a clean integer or has no
    clean power-of-1024 representation; the caller should fall back to a
    safe default in that case.
    """
    try:
        n = int(stored_value)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    for unit, scale in (("TiB", 2**40), ("GiB", 2**30), ("MiB", 2**20), ("KiB", 2**10)):
        if n >= scale and n % scale == 0:
            return f"{n // scale}{unit}"
    return None


@contextlib.contextmanager
def _scoped_target_file_size(conn: duckdb.DuckDBPyConnection, value: str):
    """Set target_file_size for the body, restore the prior catalog value on exit.

    Reads the prior GLOBAL value before the body runs and restores it in the
    finally block. Operators who have intentionally configured a non-default
    global target_file_size keep it; this command's tier-specific override
    only applies during the wrapped body.

    Three subtleties from DuckLake 1.4 internals:

    * ``ducklake_options`` returns one row per GLOBAL/SCHEMA/TABLE scope, so
      filter to ``scope = 'GLOBAL'`` — the unfiltered fetchone in the
      original implementation could return ``('',)`` from a TABLE-scope row.
    * DuckLake persists the value as a raw byte count (e.g. ``'67108864'``),
      but ``ducklake_set_option`` rejects that form on input — it needs a
      KiB/MiB/GiB suffix. Convert via ``_bytes_to_human`` before restoring.
    * If the prior value isn't a clean power of 1024, we can't represent it
      with a units suffix; log a warning and fall back to
      ``DEFAULT_TARGET_FILE_SIZE`` rather than leaving the catalog at the
      tier-specific value we set during the body.
    """
    _sanitize_setting_value(value)
    prior_row = conn.execute(
        f"SELECT value FROM ducklake_options('{ATTACH_NAME}') "
        f"WHERE option_name = 'target_file_size' AND scope = 'GLOBAL'"
    ).fetchone()
    # Distinguish three cases so we only warn on a genuine conversion failure
    # (a prior GLOBAL value of 134217728 converts to '128MiB' which equals
    # DEFAULT_TARGET_FILE_SIZE — without this distinction we'd emit a noisy
    # warning on every compaction in a healthy default install).
    if prior_row is None or not prior_row[0]:
        restore = DEFAULT_TARGET_FILE_SIZE
    else:
        converted = _bytes_to_human(prior_row[0])
        if converted is None:
            log.warning(
                "target_file_size GLOBAL value %r could not be converted to a "
                "units-suffixed form; falling back to %s on restore",
                prior_row[0],
                DEFAULT_TARGET_FILE_SIZE,
            )
            restore = DEFAULT_TARGET_FILE_SIZE
        else:
            restore = converted
    _sanitize_setting_value(restore)
    conn.execute(f"CALL ducklake_set_option('{ATTACH_NAME}', 'target_file_size', '{value}')")
    try:
        yield
    finally:
        conn.execute(f"CALL ducklake_set_option('{ATTACH_NAME}', 'target_file_size', '{restore}')")
        log.info("target_file_size restored to %s", restore)


def _set_compaction_tuning(conn: duckdb.DuckDBPyConnection, threads: int, memory_limit: str) -> None:
    """Bound DuckDB resource use for compaction.

    Defaults are conservative to keep ducklake_merge_adjacent_files within a
    cron pod's memory limit: empirically the merge plan still over-uses memory
    relative to a pure streaming op (see DuckLake bug c8), so 2 threads / 4 GB
    is the safe floor that succeeded where 12 threads / 20 GB OOMKilled.
    Operators can raise via --threads / --memory-limit when the lake fits.
    """
    _sanitize_setting_value(memory_limit)
    conn.execute(f"SET threads = {threads}")
    conn.execute(f"SET memory_limit = '{memory_limit}'")
    # Skip the implicit sort to preserve insert order; not needed for a merge
    # that already orders by (begin_snapshot, row_id_start, data_file_id).
    conn.execute("SET preserve_insertion_order = false")
    # Default 30s is too tight for the multi-MB GETs/PUTs that compaction
    # drives; 10 min covers the worst-case S3 hiccup without livelocking.
    conn.execute("SET http_timeout = 600000")
    log.info(
        "compaction tuning: threads=%d memory_limit=%s preserve_insertion_order=false http_timeout=600000ms",
        threads,
        memory_limit,
    )


def _rss_bytes() -> int | None:
    """Current process RSS, or None if unreadable.

    Reads /proc/self/status (the prod container is Linux); falls back to
    getrusage peak RSS elsewhere (macOS dev — note: peak, not current).
    RSS is the number that matters for compaction OOMs: DuckLake's merge
    keeps a large working set OUTSIDE DuckDB's memory accounting (bug c8),
    so the cgroup kills the pod while duckdb's own gauge looks healthy.
    """
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    try:
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return peak if sys.platform == "darwin" else peak * 1024
    except Exception:
        return None


def _net_bytes() -> tuple[int, int] | None:
    """Current cumulative (rx_bytes, tx_bytes) for eth0, or None if unreadable.

    Reads /proc/self/net/dev (Linux only). The heartbeat diffs two readings
    across the interval to compute MB/s — distinguishes the S3 read phase
    (high RX, rising RSS) from the catalog-scan phase (low network, flat RSS)
    without manual pod exec sampling.
    """
    try:
        with open("/proc/self/net/dev") as f:
            for line in f:
                if "eth0:" in line:
                    fields = line.split()
                    return int(fields[1]), int(fields[9])
    except (OSError, ValueError, IndexError):
        pass
    return None


def _heartbeat_line(
    conn: duckdb.DuckDBPyConnection,
    label: str,
    elapsed: float,
    prev_net: tuple[int, int] | None,
    interval_s: float,
) -> tuple[str, tuple[int, int] | None]:
    """One heartbeat log line: elapsed, RSS, and network rate."""
    parts = [f"{label}: {elapsed:.0f}s elapsed"]
    rss = _rss_bytes()
    if rss is not None:
        parts.append(f"rss={rss / 1024**3:.1f}GiB")
    cur_net = _net_bytes()
    if cur_net is not None and prev_net is not None and interval_s > 0:
        # max(0, ...) guards against 32-bit counter wrap (~4 GiB rolls over at
        # ~200 MB/s in 20s); a negative delta produces a misleading log line.
        rx_mbs = max(0, cur_net[0] - prev_net[0]) / interval_s / 1024**2
        tx_mbs = max(0, cur_net[1] - prev_net[1]) / interval_s / 1024**2
        parts.append(f"net=↓{rx_mbs:.1f}/↑{tx_mbs:.1f} MiB/s")
    return ", ".join(parts), cur_net


def _start_heartbeat(conn: duckdb.DuckDBPyConnection, label: str, interval_s: float = 60.0) -> threading.Event:
    """Log a heartbeat line every `interval_s` while a long blocking call runs
    on `conn` from the main thread. Returns the Event to .set() when done
    (use try/finally). Daemon thread; safe to leak on crash.
    """
    stop = threading.Event()
    start_t = time.monotonic()

    def _tick() -> None:
        prev_net = _net_bytes()
        while not stop.wait(timeout=interval_s):
            line, prev_net = _heartbeat_line(conn, label, time.monotonic() - start_t, prev_net, interval_s)
            log.info("%s", line)

    threading.Thread(target=_tick, daemon=True).start()
    return stop


def _enumerate_compaction_tables(
    conn: duckdb.DuckDBPyConnection, min_b: int | None, max_b: int
) -> list[tuple[str, str]]:
    """Tables with >= 2 live files in the tier's size band, biggest backlog first.

    Candidate-driven (from ducklake_data_file), NOT information_schema: a
    catalog can carry thousands of live tables (dlt/Fivetran staging) with
    zero tier candidates — enumerating all of them costs a bind-time
    candidate scan per table per tier against a PG catalog with a history
    of read-tax problems, and alphabetical order let trivially-small
    early-alphabet tables (2-file cosmetic merges) eat the global file
    budget every run while the real backlog starved. Observed in prod:
    3,074 live tables, the budget exhausted on five 2-file billing tables,
    and the 15k-candidate events table never reached. Candidate-count DESC
    serves the most backlogged table first; ties break by name for
    determinism. Single-candidate tables can't merge and are excluded.

    Catalog-derived names are ordinary-DDL-controlled and get interpolated
    into the per-table CALL statements below, so anything outside the
    conservative identifier shape is skipped LOUDLY rather than quoted
    heroically (same defense as repair-partition-values discovery).
    """
    where = [
        "df.end_snapshot IS NULL",
        "t.end_snapshot IS NULL",
        "sch.end_snapshot IS NULL",
        f"df.file_size_bytes < {max_b}",
        # Mirror the compactor's own selection: merge_adjacent skips any file
        # carrying live delete files, so counting them here would let an
        # unmergeable-but-huge table rank first and no-op at the top of every
        # run (rank inflated by files the extension refuses to touch).
        (
            f"NOT EXISTS (SELECT 1 FROM {METADATA_SCHEMA}.ducklake_delete_file dl "
            "WHERE dl.data_file_id = df.data_file_id AND dl.end_snapshot IS NULL)"
        ),
    ]
    if min_b is not None:
        where.append(f"df.file_size_bytes >= {min_b}")
    # NOTE: COUNT(*) >= 2 is per-TABLE; the extension merges per
    # (partition_id, partition_values) group, so 2 candidates in different
    # partitions still yield a zero-group no-op CALL. That's a wasted bind
    # scan, not a correctness issue — do not read >= 2 as a mergeability
    # guarantee. Zero-group CALLs are counted and logged by the caller.
    rows = conn.execute(
        "SELECT sch.schema_name, t.table_name "
        f"FROM {METADATA_SCHEMA}.ducklake_data_file df "
        f"JOIN {METADATA_SCHEMA}.ducklake_table t USING (table_id) "
        f"JOIN {METADATA_SCHEMA}.ducklake_schema sch ON sch.schema_id = t.schema_id "
        f"WHERE {' AND '.join(where)} "
        "GROUP BY sch.schema_name, t.table_name "
        "HAVING COUNT(*) >= 2 "
        "ORDER BY COUNT(*) DESC, sch.schema_name, t.table_name"
    ).fetchall()
    tables: list[tuple[str, str]] = []
    for schema_name, table_name in rows:
        # _CATALOG_TABLE_NAME_RE (strict [A-Za-z0-9_]+, fullmatch): these names
        # are interpolated into single-quoted CALL args, and a millpond-written
        # table is always a plain identifier — anything else is skipped, never
        # quoted heroically.
        if not _CATALOG_TABLE_NAME_RE.fullmatch(schema_name) or not _CATALOG_TABLE_NAME_RE.fullmatch(table_name):
            log.warning(
                "compact: skipping table with non-identifier name %r.%r (not millpond-written)",
                schema_name,
                table_name,
            )
            continue
        tables.append((schema_name, table_name))
    return tables


def _merge_adjacent_call(schema_name: str | None, table_name: str, args: list[str], file_budget: int) -> str:
    """Build the per-table ducklake_merge_adjacent_files CALL."""
    call_args = [*args, f"max_compacted_files => {file_budget}"]
    if schema_name is None:
        return f"CALL ducklake_merge_adjacent_files('{ATTACH_NAME}', '{table_name}', {', '.join(call_args)})"
    return (
        f"CALL ducklake_merge_adjacent_files('{ATTACH_NAME}', '{table_name}', "
        f"schema => '{schema_name}', {', '.join(call_args)})"
    )


def compact(
    conn: duckdb.DuckDBPyConnection,
    tier: int,
    table: str | None,
    dry_run: bool,
    threads: int,
    memory_limit: str,
    max_compacted_files: int,
    max_outputs_per_call: int | None = None,
) -> int:
    """Compact files in tier N (1, 2, or 3) for the catalog or one table.

    Returns the number of tables whose per-table merge FAILED (0 when
    everything succeeded, and always 0 for the single-table form, which
    propagates its error raw instead). main() exports this as the
    maintenance_compact_tables_failed gauge so a permanently-poisoned
    table is alertable even though partial failure deliberately exits 0.

    max_outputs_per_call decouples per-TRANSACTION size from the per-RUN
    budget. The fork CALL's `max_compacted_files =>` bounds OUTPUT files
    (txn size ~ outputs x target_file_size), while this function's run
    budget accounts INPUT files consumed. Unset (default): one CALL per
    table with the legacy grant — byte-identical behavior to before the
    knob existed. Set: each CALL is capped at this many outputs and a
    table is called REPEATEDLY (until its grant or the run budget is
    consumed, or a call produces nothing) — many small always-committable
    transactions per run instead of one. Built for the megaduck source
    catalog, where a single large merge txn cannot commit inside the
    activeDeadline (documented DeadlineExceeded history) but dozens of
    ~10-output txns per run converge its tier-3 backlog.
    """
    spec = TIERS[tier]
    min_b, max_b, target = spec["min"], spec["max"], spec["target"]
    scope = f"table '{table}'" if table else "catalog-wide"
    range_str = f"[{min_b or 0}, {max_b}) bytes"
    log.info(
        "Compact tier %d (%s): merge files %s into ~%s targets, max %d files/run (dry_run=%s)",
        tier,
        scope,
        range_str,
        target,
        max_compacted_files,
        dry_run,
    )

    where = ["end_snapshot IS NULL", f"file_size_bytes < {max_b}"]
    if min_b is not None:
        where.append(f"file_size_bytes >= {min_b}")
    if table:
        if not _SETTING_VALUE_RE.match(table):
            raise ValueError(f"Illegal character in table name: {table!r}")
        where.append(
            f"table_id IN (SELECT table_id FROM {METADATA_SCHEMA}.ducklake_table WHERE table_name = '{table}')"
        )
    candidate_count, candidate_bytes = conn.execute(
        f"SELECT COUNT(*), COALESCE(SUM(file_size_bytes), 0) "
        f"FROM {METADATA_SCHEMA}.ducklake_data_file WHERE {' AND '.join(where)}"
    ).fetchone()
    log.info(
        "compact tier-%d candidates: %d files, %d bytes total",
        tier,
        candidate_count,
        candidate_bytes,
    )

    if dry_run:
        return 0
    if candidate_count == 0:
        log.info("compact tier-%d: nothing to do, skipping merge", tier)
        return 0

    # Bound each run: the prod backlog (600k+ tier-1 candidates) is far too
    # large for one merge transaction — it would blow the cron pod's
    # activeDeadlineSeconds and produce one giant catalog commit. A capped
    # run finishes in bounded time and the cron schedule grinds the backlog
    # down incrementally.
    args = [f"max_file_size => {max_b}"]
    if min_b is not None:
        args.append(f"min_file_size => {min_b}")

    _set_compaction_tuning(conn, threads, memory_limit)
    t0 = time.monotonic()
    result: list[tuple] = []
    failed: list[str] = []
    table_total = 1
    noop_tables = 0

    if table:
        # Single-table invocation. Errors propagate raw, exactly as before.
        # max_outputs_per_call applies here too (review finding: silently
        # ignoring the pod-wide env on a manual `compact --table` run would
        # issue exactly the giant single txn the knob exists to prevent).
        heartbeat = _start_heartbeat(conn, f"compact tier-{tier} merge")
        try:
            with _scoped_target_file_size(conn, target):
                remaining = max_compacted_files
                while remaining > 0:
                    call_cap = remaining if max_outputs_per_call is None else min(max_outputs_per_call, remaining)
                    sql = _merge_adjacent_call(None, table, args, call_cap)
                    rows = conn.execute(sql).fetchall()
                    if any(len(r) < 4 for r in rows):
                        raise ValueError(f"unexpected merge result shape: {rows[:2]!r}")
                    if not rows:
                        break
                    consumed = sum(r[2] for r in rows)
                    if consumed <= 0:
                        raise ValueError(f"merge returned rows but zero inputs consumed: {rows[:2]!r}")
                    result.extend(rows)
                    remaining -= consumed
                    if max_outputs_per_call is None:
                        break
        finally:
            heartbeat.set()
    else:
        # Catalog-wide compaction, one CALL PER TABLE. The catalog-scope form
        # of ducklake_merge_adjacent_files aborts the ENTIRE run when any one
        # table can't compact — e.g. a merge group mixing hive-path
        # conventions after an add_data_files backfill registered foreign
        # paths into a live table (seen in production on `events`: the
        # compactor groups by logical partition_values, then asserts all
        # files share one hive directory string), or the orphaned
        # inlined-data/schema-version classes. Per-table calls isolate the
        # failure: every healthy table still compacts and the broken one is
        # reported. Mirrors the battle-tested standalone posthog maintenance
        # script, which loops tables for exactly this reason.
        tables = _enumerate_compaction_tables(conn, min_b, max_b)
        table_total = len(tables)
        log.info(
            "compact tier-%d: %d table(s) with >= 2 tier candidates, biggest backlog first",
            tier,
            table_total,
        )
        if not tables and candidate_count > 0:
            # Candidates exist but no table qualified: every candidate is a
            # per-table singleton (nothing to merge with — benign), or the
            # holders were skipped by the identifier gate (per-table WARNs
            # above say which). INFO, not a failure.
            log.info(
                "compact tier-%d: %d candidate file(s) but no table has >= 2 — nothing mergeable this run",
                tier,
                candidate_count,
            )
        with _scoped_target_file_size(conn, target):
            # max_compacted_files stays a GLOBAL budget across the run — the
            # same bound as the old catalog-scope call — so run duration
            # stays predictable. Leftover tables wait for the next cron tick.
            #
            # Per-table GRANT cap: on multi-table runs no single table may
            # claim more than half the budget in one CALL. Backlog-DESC
            # ordering without the cap re-created starvation in mirror image —
            # a whale whose ingest outpaces the drain rate would consume the
            # entire budget every tick and tables #2..N would never be
            # served. Biggest backlog still gets the biggest cut; it just
            # can't take the whole pie.
            remaining = max_compacted_files
            for schema_name, table_name in tables:
                if remaining <= 0:
                    log.info(
                        "compact tier-%d: file budget exhausted; remaining tables wait for the next run",
                        tier,
                    )
                    break
                grant = remaining if table_total == 1 else min(remaining, max(1, max_compacted_files // 2))
                # Inner loop: legacy mode (max_outputs_per_call unset) makes
                # exactly ONE call with the whole grant — unchanged behavior.
                # Capped mode re-calls the same table with small per-txn
                # output caps until the grant/budget is consumed or a call
                # produces nothing (table drained for this tier).
                call_idx = 0
                while grant > 0 and remaining > 0:
                    # min() with the grant: when the remaining grant is
                    # smaller than the cap, degrade to exactly legacy
                    # semantics — never issue a larger per-txn cap than the
                    # legacy single call would have.
                    call_cap = grant if max_outputs_per_call is None else min(max_outputs_per_call, grant)
                    sql = _merge_adjacent_call(schema_name, table_name, args, call_cap)
                    heartbeat = _start_heartbeat(
                        conn,
                        f"compact tier-{tier} {schema_name}.{table_name}"
                        + (f" call {call_idx + 1}" if call_idx else ""),
                    )
                    try:
                        rows = conn.execute(sql).fetchall()
                        # Validate shape BEFORE mutating shared state: if a fork/
                        # version drift changes the CALL's result schema, this
                        # table's accounting fails loudly but the run-wide
                        # aggregation below never sees malformed rows.
                        if any(len(r) < 4 for r in rows):
                            raise ValueError(f"unexpected merge result shape: {rows[:2]!r}")
                        if not rows:
                            if call_idx == 0:
                                # >= 2 in-band files but zero merge groups:
                                # candidates are per-partition singletons,
                                # row-id-non-contiguous, or otherwise
                                # unmergeable. Counted so a catalog full of
                                # these is visible, not a silent perpetual
                                # no-op. (An empty LATER call just means the
                                # table drained — not a no-op table.)
                                noop_tables += 1
                            break
                        consumed = sum(r[2] for r in rows)
                        if consumed <= 0:
                            # Progress guard: termination of this loop rests
                            # on every productive call consuming >= 2 inputs
                            # (a merge group by definition). A fork bump that
                            # reorders result columns or emits zero-input
                            # rows must fail THIS table loudly, not re-issue
                            # identical merge txns until the activeDeadline
                            # kills the pod.
                            raise ValueError(f"merge returned rows but zero inputs consumed: {rows[:2]!r}")
                        result.extend(rows)
                        remaining -= consumed
                        grant -= consumed
                    except duckdb.CatalogException:
                        # Table vanished between enumeration and the CALL
                        # (dropped mid-run) — benign, not a failure.
                        log.warning(
                            "compact tier-%d: %s.%s disappeared before merge (dropped?); skipping",
                            tier,
                            schema_name,
                            table_name,
                        )
                        break
                    except Exception:
                        # Continue-on-error relies on the connection SURVIVING
                        # the failed CALL (verified on duckdb 1.5.2 and 1.5.5,
                        # even for InternalException, and pinned end-to-end by
                        # test_compaction_isolation_integration). If a future
                        # duckdb bump invalidates the instance on INTERNAL
                        # errors, every table fails, the failure summary +
                        # gauge spike, and the target_file_size restore raises
                        # — noisy, not silent. A mid-loop failure keeps the
                        # progress already committed by earlier calls.
                        failed.append(f"{schema_name}.{table_name}")
                        log.exception(
                            "compact tier-%d: %s.%s failed; continuing with remaining tables",
                            tier,
                            schema_name,
                            table_name,
                        )
                        break
                    finally:
                        heartbeat.set()
                    call_idx += 1
                    if max_outputs_per_call is None:
                        break
    elapsed = time.monotonic() - t0

    # Aggregate result rows (one per output group: schema, table, input_files, output_files)
    # into one log line per table rather than one line per group.
    totals: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0, 0])
    for schema, tbl, inputs, outputs in result:
        totals[(schema, tbl)][0] += 1  # groups
        totals[(schema, tbl)][1] += inputs  # input files
        totals[(schema, tbl)][2] += outputs  # output files
    for (schema, tbl), (groups, inputs, outputs) in totals.items():
        log.info(
            "compact tier-%d: %s.%s: %d groups, %d files → %d output files",
            tier,
            schema,
            tbl,
            groups,
            inputs,
            outputs,
        )
    total_groups = sum(v[0] for v in totals.values())
    total_inputs = sum(v[1] for v in totals.values())
    total_outputs = sum(v[2] for v in totals.values())
    log.info(
        "compact tier-%d: %d groups total, %d files → %d output files, duration=%.1fs",
        tier,
        total_groups,
        total_inputs,
        total_outputs,
        elapsed,
    )
    if noop_tables:
        log.info(
            "compact tier-%d: %d/%d table(s) had >= 2 in-band files but zero merge groups "
            "(per-partition singletons / non-contiguous row-ids)",
            tier,
            noop_tables,
            table_total,
        )
    if failed:
        # Per-table failures NEVER raise, no matter how many. An
        # all-failed raise sounds like a systemic-failure detector, but
        # candidate-driven enumeration converges on exactly the failed set:
        # healthy tables drain out of the enumeration once compacted while
        # poisoned tables never do, so failed == table_total is this
        # system's steady state under any persistent poison — and raising
        # would wedge the recipe chain (later tiers + cleanup-all) forever,
        # the exact incident this loop exists to prevent. The WARN + the
        # maintenance_compact_tables_failed gauge carry the alert; genuinely
        # systemic failures still fail the run elsewhere (a dead catalog
        # fails the candidate query above; dead object storage fails
        # cleanup-all right after).
        log.warning(
            "compact tier-%d: %d/%d table(s) failed: %s",
            tier,
            len(failed),
            table_total,
            ", ".join(failed),
        )
    return len(failed)


def compact_probe(conn: duckdb.DuckDBPyConnection, table: str, max_compacted_files: int) -> None:
    """Merge up to N adjacent files in one table without changing target_file_size."""
    if not _SETTING_VALUE_RE.match(table):
        raise ValueError(f"Illegal character in table name: {table!r}")
    log.info("compact-probe: table=%s max_compacted_files=%d", table, max_compacted_files)
    heartbeat = _start_heartbeat(conn, f"compact-probe {table}")
    try:
        result = conn.execute(
            f"CALL ducklake_merge_adjacent_files('{ATTACH_NAME}', '{table}', "
            f"max_compacted_files => {max_compacted_files})"
        ).fetchall()
    finally:
        heartbeat.set()
    for row in result:
        log.info("compact-probe: %s", row)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DuckLake maintenance operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--verbose", action="store_true", help="Debug logging")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DuckDB HTTP + postgres query debug logging (high overhead; off by default)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # expire
    p = sub.add_parser("expire", help="Expire snapshots older than N days")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dry-run", action="store_true")

    # expire-snapshots (postgres-native, no DuckDB memory overhead)
    p = sub.add_parser(
        "expire-snapshots",
        help="Postgres-native snapshot expiry (avoids ducklake_expire_snapshots OOM)",
    )
    p.add_argument("--days", type=int, default=7, help="Expire snapshots older than N days (default 7)")
    p.add_argument(
        "--batch-size",
        type=_positive_int,
        default=1000,
        help="Snapshot IDs to process per iteration (default 1000)",
    )
    p.add_argument(
        "--num-batches",
        type=_positive_int,
        default=None,
        help="Stop after processing this many batches (default: unlimited)",
    )
    p.add_argument("--dry-run", action="store_true", help="Report counts without making changes")

    # cleanup
    p = sub.add_parser("cleanup", help="Delete files scheduled for deletion")
    p.add_argument("--days", type=int, default=1)
    p.add_argument("--dry-run", action="store_true")

    # cleanup-all
    # No --dry-run on purpose: there is no preview form of cleanup_all =>
    # true, and silently accepting the flag (old behavior) made operators
    # believe they had previewed something. Use cleanup-dry-run (age-gated
    # subset) or fsck-dry-run instead.
    sub.add_parser(
        "cleanup-all",
        help="Delete all scheduled files (no dry-run; see cleanup-dry-run / fsck-dry-run)",
        description=(
            "Delete ALL files scheduled for deletion regardless of age. There is no dry-run "
            "mode: ducklake_cleanup_old_files with cleanup_all => true has no preview form. "
            "Preview the age-gated subset with `cleanup --dry-run`, or the full maintenance "
            "pipeline with `fsck --dry-run`."
        ),
    )

    # dedup-deletions
    p = sub.add_parser(
        "dedup-deletions",
        help="Drop duplicate rows from ducklake_files_scheduled_for_deletion (workaround for DuckLake bug c5)",
    )
    p.add_argument("--dry-run", action="store_true")

    # purge-orphan-stats
    p = sub.add_parser(
        "purge-orphan-stats",
        help="Delete global-stats rows for dropped tables (commit-path tax; see ducklake_stats_rows_orphaned metric)",
    )
    p.add_argument("--dry-run", action="store_true")

    # REMOVE-WHEN: upstream-ducklake-fix-deployed
    # repair-partition-values (one-shot per customer; remove with the upstream DuckLake source fix)
    p = sub.add_parser(
        "repair-partition-values",
        help="Repair ducklake_file_partition_value rows wrecked by the ducklake_add_data_files bug",
    )
    p.add_argument("--dry-run", action="store_true")

    # find-orphans
    sub.add_parser(
        "find-orphans",
        help="List ducklake_files_scheduled_for_deletion rows whose S3 key no longer exists",
    )

    # heal-orphans
    p = sub.add_parser(
        "heal-orphans",
        help="Delete catalog rows whose S3 key no longer exists (gated; see B1/B3 safety checks)",
    )
    p.add_argument("--dry-run", action="store_true")

    # cleanup-all-safe
    p = sub.add_parser(
        "cleanup-all-safe",
        help="Orchestrator: dedup + heal-orphans + cleanup-all in a loop until cleanup-all exits clean",
    )
    p.add_argument(
        "--max-iterations",
        type=_positive_int,
        default=10,
        help="Maximum dedup/heal/cleanup-all iterations before giving up (default 10)",
    )

    # fsck
    p = sub.add_parser(
        "fsck",
        help="cleanup-all-safe + ducklake_delete_orphaned_files (catalog-healthy end-to-end recipe)",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--max-iterations",
        type=_positive_int,
        default=10,
        help="Maximum cleanup-all-safe iterations before giving up (default 10)",
    )

    # orphans
    p = sub.add_parser("orphans", help="Delete orphaned S3 files")
    p.add_argument("--dry-run", action="store_true")

    # maintain
    p = sub.add_parser("maintain", help="Full maintenance (expire + cleanup)")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dry-run", action="store_true")

    # ensure-indexes
    p = sub.add_parser(
        "ensure-indexes",
        help="Create the catalog's secondary indexes (CATALOG_INDEXES) idempotently, CONCURRENTLY",
    )
    p.add_argument("--dry-run", action="store_true")

    # checkpoint
    sub.add_parser("checkpoint", help="CHECKPOINT (merge + expire + cleanup)")

    # compact
    p = sub.add_parser("compact", help="Tiered compaction (merge small files into larger ones)")
    p.add_argument("--tier", type=int, choices=[1, 2, 3], required=True)
    p.add_argument("--table", default="", help="Limit to one table; empty = catalog-wide")
    p.add_argument("--dry-run", action="store_true")
    # Defaults are env-overridable so the K8s CronJob (which runs the bare
    # `just compact-all-tiers` recipe with no CLI args) can be tuned per
    # environment from chart values without an image rebuild.
    p.add_argument(
        "--threads",
        type=_positive_int,
        default=_positive_int(os.environ.get("COMPACTION_THREADS", "2")),
        help="DuckDB threads during the merge (default $COMPACTION_THREADS or 2; raise cautiously, see bug c8)",
    )
    p.add_argument(
        "--memory-limit",
        default=os.environ.get("COMPACTION_MEMORY_LIMIT", "4GB"),
        help="DuckDB memory_limit during the merge (default $COMPACTION_MEMORY_LIMIT or 4GB)",
    )
    p.add_argument(
        "--max-compacted-files",
        type=_positive_int,
        # Fallback matches the justfile's exported default: a direct python
        # invocation must not be 1250x more aggressive than `just compact-*`.
        default=_positive_int(os.environ.get("COMPACTION_MAX_FILES", "80")),
        help="Cap on INPUT files merged per run (default $COMPACTION_MAX_FILES or 80)",
    )
    p.add_argument(
        "--max-outputs-per-call",
        type=_positive_int,
        default=(
            _positive_int(os.environ["COMPACTION_MAX_OUTPUTS_PER_CALL"])
            if os.environ.get("COMPACTION_MAX_OUTPUTS_PER_CALL")
            else None
        ),
        help=(
            "Cap OUTPUT files per merge CALL/transaction (txn size ~ this x "
            "target_file_size) and re-call each table until its grant is "
            "consumed. Unset = legacy single-call-per-table behavior. "
            "(default $COMPACTION_MAX_OUTPUTS_PER_CALL or unset)"
        ),
    )

    # compact-probe
    p = sub.add_parser("compact-probe", help="Probe: merge a few adjacent files in one table")
    p.add_argument("--table", required=True)
    p.add_argument("--max-compacted-files", type=_positive_int, default=2)

    return parser


def _push(registry: CollectorRegistry, pushgateway: str) -> None:
    """Push metrics to the pushgateway, logging but not raising on failure."""
    try:
        push_to_gateway(pushgateway, job="ducklake-maintenance", registry=registry)
    except Exception:
        log.exception("Failed to push metrics to %s", pushgateway)


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    _log_version()

    pushgateway = os.environ.get("PUSHGATEWAY_URL")
    registry = CollectorRegistry()
    start_time = Gauge(
        "maintenance_start_time",
        "Unix timestamp when the maintenance operation started",
        ["operation"],
        registry=registry,
    )
    duration = Gauge(
        "maintenance_duration_seconds",
        "Duration of the maintenance operation",
        ["operation", "status"],
        registry=registry,
    )
    # Partial compaction failures deliberately exit 0 (the recipe chain must
    # still run later tiers + cleanup-all), so a permanently-poisoned table
    # would otherwise be invisible to metrics. Alert on sustained nonzero.
    compact_tables_failed = Gauge(
        "maintenance_compact_tables_failed",
        "Tables whose per-table merge failed in the last compact run",
        ["tier"],
        registry=registry,
    )
    operation = args.command
    if hasattr(args, "days") and args.days < 1:
        parser.error("--days must be >= 1")
    if getattr(args, "batch_size", None) is not None and args.batch_size > _EXPIRE_SNAPSHOTS_MAX_BATCH_SIZE:
        parser.error(
            f"--batch-size must be <= {_EXPIRE_SNAPSHOTS_MAX_BATCH_SIZE}: the id list is interpolated "
            "into every batch statement, and huge batches produce multi-MB SQL and giant NOT-IN lists"
        )

    start_time.labels(operation=operation).set(time.time())
    if pushgateway:
        _push(registry, pushgateway)

    t0 = time.monotonic()
    status = "success"
    conn = None
    # ensure-indexes speaks raw psycopg to the catalog and needs neither the
    # DuckDB session nor its S3 wiring — skip the duckdb connect for it.
    if args.command != "ensure-indexes":
        conn = connect(debug=args.debug)
    try:
        match args.command:
            case "expire":
                expire(conn, args.days, args.dry_run)
            case "expire-snapshots":
                expire_snapshots(conn, args.days, args.batch_size, args.num_batches, args.dry_run)
            case "cleanup":
                cleanup(conn, args.days, args.dry_run)
            case "cleanup-all":
                cleanup_all(conn)
            case "dedup-deletions":
                dedup_deletions(conn, args.dry_run)
            case "purge-orphan-stats":
                purge_orphan_stats(conn, args.dry_run)
            case "repair-partition-values":  # REMOVE-WHEN: upstream-ducklake-fix-deployed
                repair_partition_values(conn, args.dry_run)
            case "find-orphans":
                find_orphans(conn)
            case "heal-orphans":
                heal_orphans(conn, args.dry_run)
            case "cleanup-all-safe":
                cleanup_all_safe(conn, args.max_iterations)
            case "fsck":
                fsck(conn, args.dry_run, args.max_iterations)
            case "orphans":
                orphans(conn, args.dry_run)
            case "ensure-indexes":
                ensure_indexes(args.dry_run)
            case "maintain":
                maintain(conn, args.days, args.dry_run)
            case "checkpoint":
                checkpoint(conn)
            case "compact":
                n_failed = compact(
                    conn,
                    args.tier,
                    args.table or None,
                    args.dry_run,
                    args.threads,
                    args.memory_limit,
                    args.max_compacted_files,
                    max_outputs_per_call=args.max_outputs_per_call,
                )
                compact_tables_failed.labels(tier=str(args.tier)).set(n_failed)
            case "compact-probe":
                compact_probe(conn, args.table, args.max_compacted_files)
    except Exception:
        status = "error"
        log.exception("Maintenance operation %s failed", operation)
        raise
    finally:
        elapsed = time.monotonic() - t0
        duration.labels(operation=operation, status=status).set(elapsed)
        if conn is not None:
            conn.close()
        if pushgateway:
            _push(registry, pushgateway)
        log.info("Operation %s finished: status=%s duration=%.1fs", operation, status, elapsed)


if __name__ == "__main__":
    main()
