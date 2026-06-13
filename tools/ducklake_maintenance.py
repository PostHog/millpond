#!/usr/bin/env python3
"""DuckLake maintenance operations.

Standalone script for running DuckLake maintenance tasks (snapshot expiry,
file cleanup, orphan deletion, checkpoint) from a K8s CronJob or manually.

Requires the same env vars as the tools/justfile:
  DUCKLAKE_RDS_HOST, DUCKLAKE_RDS_PORT, DUCKLAKE_RDS_DATABASE,
  DUCKLAKE_RDS_USERNAME, DUCKLAKE_RDS_PASSWORD, DUCKLAKE_DATA_PATH,
  DUCKDB_S3_REGION, DUCKDB_S3_ACCESS_KEY_ID, DUCKDB_S3_SECRET_ACCESS_KEY
  (plus optional DUCKDB_S3_ENDPOINT, DUCKDB_S3_USE_SSL, DUCKDB_S3_URL_STYLE)

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
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

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
    1: {"min": None, "max": 1 * _MIB, "target": "5MiB"},  # < 1 MiB    -> ~5 MiB
    2: {"min": 1 * _MIB, "max": 10 * _MIB, "target": "32MiB"},  # [1, 10) MiB -> ~32 MiB
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

    # S3 config from env vars
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

    # The legacy SET s3_* settings above are honored by the ducklake catalog
    # driver but not by ad-hoc httpfs ops (`glob('s3://...')`,
    # `read_parquet('s3://...')`) in DuckDB 1.4 — those go through the SECRET
    # manager. Mirror the same credentials into a SECRET so every recipe in
    # this script (and any operator-typed glob) authenticates correctly.
    s3_endpoint_val = os.environ.get("DUCKDB_S3_ENDPOINT", s3_defaults["s3_endpoint"])
    s3_use_ssl_val = os.environ.get("DUCKDB_S3_USE_SSL", s3_defaults["s3_use_ssl"])
    s3_url_style_val = os.environ.get("DUCKDB_S3_URL_STYLE", s3_defaults["s3_url_style"])
    s3_key = os.environ.get("DUCKDB_S3_ACCESS_KEY_ID", "")
    s3_secret = os.environ.get("DUCKDB_S3_SECRET_ACCESS_KEY", "")
    for v in (s3_endpoint_val, s3_use_ssl_val, s3_url_style_val, s3_key, s3_secret, s3_region):
        if v:
            _sanitize_setting_value(v)
    conn.execute(
        f"CREATE OR REPLACE SECRET s3 ("
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
    result = conn.execute(
        f"CALL ducklake_expire_snapshots('{ATTACH_NAME}', "
        f"older_than => now() - INTERVAL '{days} days', "
        f"dry_run => {str(dry_run).lower()})"
    ).fetchall()
    for row in result:
        log.info("expire: %s", row)


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


def cleanup_all(conn: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Delete all files scheduled for deletion regardless of age."""
    if dry_run:
        log.info("cleanup-all has no dry-run mode; skipping")
        return
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
        f"DELETE FROM {METADATA_SCHEMA}.ducklake_files_scheduled_for_deletion "
        f"WHERE ctid NOT IN ("
        f"SELECT MIN(ctid) FROM {METADATA_SCHEMA}.ducklake_files_scheduled_for_deletion GROUP BY path"
        f")"
    )
    conn.execute(f"CALL postgres_execute('{PG_ATTACH_NAME}', {_sql_string_literal(delete_sql)})")
    after = conn.execute("SELECT count_pending_dups()").fetchone()[0]
    log.info("dedup-deletions: queue now has %d duplicate rows", after)


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
        "CREATE OR REPLACE TEMP TABLE _orphans AS "
        "SELECT data_file_id, path FROM find_catalog_orphans(?)",
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
    delete_sql = (
        f"DELETE FROM {METADATA_SCHEMA}.ducklake_files_scheduled_for_deletion "
        f"WHERE path IN ({path_list})"
    )
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
            cleanup_all(conn, dry_run=False)
            log.info("cleanup-all-safe: cleanup-all succeeded on attempt %d", attempt)
            return
        except duckdb.IOException as e:
            log.warning(
                "cleanup-all-safe: cleanup-all crashed on attempt %d (%s); "
                "looping to heal fresh orphans",
                attempt,
                e,
            )
    raise RuntimeError(
        f"cleanup-all-safe exhausted {max_iterations} iterations without a clean cleanup-all run"
    )


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
    log.info("Deleting orphaned files (dry_run=%s)", dry_run)
    result = conn.execute(
        f"CALL ducklake_delete_orphaned_files('{ATTACH_NAME}', dry_run => {str(dry_run).lower()})"
    ).fetchall()
    for row in result:
        log.info("orphans: %s", row)


def checkpoint(conn: duckdb.DuckDBPyConnection) -> None:
    """Run CHECKPOINT (integrated merge + expire + cleanup)."""
    log.info("Running CHECKPOINT")
    conn.execute(f"CHECKPOINT {ATTACH_NAME}")
    log.info("CHECKPOINT complete")


def maintain(conn: duckdb.DuckDBPyConnection, days: int, dry_run: bool) -> None:
    """Full maintenance: expire snapshots then cleanup files."""
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
    # DuckDB only computes query progress when the progress bar is enabled;
    # print-off keeps it out of the logs. This is what lets the heartbeat's
    # cross-thread query_progress() poll return a percentage instead of -1.
    conn.execute("SET enable_progress_bar = true")
    conn.execute("SET enable_progress_bar_print = false")
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


_progress_poll_warned = False


def _heartbeat_line(conn: duckdb.DuckDBPyConnection, label: str, elapsed: float) -> str:
    """One heartbeat log line: elapsed, merge percentage + ETA when DuckDB
    reports one (sub-1% readings are noise on huge merges), and process RSS.

    query_progress() failures degrade to elapsed-only (warned once): the
    heartbeat is decoration and must never kill the operation it watches.
    """
    global _progress_poll_warned
    parts = [f"{label}: {elapsed:.0f}s elapsed"]
    try:
        pct = float(conn.query_progress())
    except Exception:
        pct = -1.0
        if not _progress_poll_warned:
            _progress_poll_warned = True
            log.warning("query_progress() poll failed; heartbeat continues without percentages", exc_info=True)
    if pct >= 100:
        parts.append("~100% merged")
    elif pct >= 1:
        eta = elapsed * (100.0 - pct) / pct
        parts.append(f"~{pct:.0f}% merged, est. {eta / 60:.0f}m remaining")
    elif pct >= 0:
        # Sub-1% readings are rendered verbatim rather than suppressed: when the
        # merge cap is much smaller than the total file population, DuckLake's
        # progress denominator may cover the full catalog rather than the capped
        # subset, so the reading stays <1% for the whole run. A slowly climbing
        # "~0.4% merged" is still a liveness signal and distinguishes a live
        # merge from a hung one. No ETA: extrapolating from a fraction-of-percent
        # would produce absurdly large numbers.
        parts.append(f"~{pct:.1f}% merged (sub-1%; ETA unavailable)")
    rss = _rss_bytes()
    if rss is not None:
        parts.append(f"rss={rss / 1024**3:.1f}GiB")
    return ", ".join(parts)


def _start_heartbeat(conn: duckdb.DuckDBPyConnection, label: str, interval_s: float = 60.0) -> threading.Event:
    """Log a heartbeat line every `interval_s` while a long blocking call runs
    on `conn` from the main thread. Returns the Event to .set() when done
    (use try/finally). Daemon thread; safe to leak on crash.

    Cross-thread query_progress() is a lock-free atomic read of executor
    state (it's how Jupyter's progress bar works) — worst case is a stale
    reading, never corruption. The connection's single query slot belongs to
    the watched call, so the percentage can't be someone else's query.
    """
    stop = threading.Event()
    start_t = time.monotonic()

    def _tick() -> None:
        while not stop.wait(timeout=interval_s):
            log.info("%s", _heartbeat_line(conn, label, time.monotonic() - start_t))

    threading.Thread(target=_tick, daemon=True).start()
    return stop


def compact(
    conn: duckdb.DuckDBPyConnection,
    tier: int,
    table: str | None,
    dry_run: bool,
    threads: int,
    memory_limit: str,
    max_compacted_files: int,
) -> None:
    """Compact files in tier N (1, 2, or 3) for the catalog or one table."""
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
        return
    if candidate_count == 0:
        log.info("compact tier-%d: nothing to do, skipping merge", tier)
        return

    # Bound each run: the prod backlog (600k+ tier-1 candidates) is far too
    # large for one merge transaction — it would blow the cron pod's
    # activeDeadlineSeconds and produce one giant catalog commit. A capped
    # run finishes in bounded time and the cron schedule grinds the backlog
    # down incrementally.
    args = [f"max_file_size => {max_b}", f"max_compacted_files => {max_compacted_files}"]
    if min_b is not None:
        args.append(f"min_file_size => {min_b}")
    if table:
        sql = f"CALL ducklake_merge_adjacent_files('{ATTACH_NAME}', '{table}', {', '.join(args)})"
    else:
        sql = f"CALL ducklake_merge_adjacent_files('{ATTACH_NAME}', {', '.join(args)})"

    _set_compaction_tuning(conn, threads, memory_limit)
    heartbeat = _start_heartbeat(conn, f"compact tier-{tier} merge")
    try:
        with _scoped_target_file_size(conn, target):
            result = conn.execute(sql).fetchall()
    finally:
        heartbeat.set()
    for row in result:
        log.info("compact tier-%d: %s", tier, row)


def compact_probe(conn: duckdb.DuckDBPyConnection, table: str, max_compacted_files: int) -> None:
    """Merge up to N adjacent files in one table without changing target_file_size."""
    if not _SETTING_VALUE_RE.match(table):
        raise ValueError(f"Illegal character in table name: {table!r}")
    log.info("compact-probe: table=%s max_compacted_files=%d", table, max_compacted_files)
    result = conn.execute(
        f"CALL ducklake_merge_adjacent_files('{ATTACH_NAME}', '{table}', "
        f"max_compacted_files => {max_compacted_files})"
    ).fetchall()
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

    # cleanup
    p = sub.add_parser("cleanup", help="Delete files scheduled for deletion")
    p.add_argument("--days", type=int, default=1)
    p.add_argument("--dry-run", action="store_true")

    # cleanup-all
    p = sub.add_parser("cleanup-all", help="Delete all scheduled files")
    p.add_argument("--dry-run", action="store_true")

    # dedup-deletions
    p = sub.add_parser(
        "dedup-deletions",
        help="Drop duplicate rows from ducklake_files_scheduled_for_deletion (workaround for DuckLake bug c5)",
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
        default=_positive_int(os.environ.get("COMPACTION_MAX_FILES", "100000")),
        help="Cap on files merged per run (default $COMPACTION_MAX_FILES or 100000)",
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
    operation = args.command
    if hasattr(args, "days") and args.days < 1:
        parser.error("--days must be >= 1")

    start_time.labels(operation=operation).set(time.time())
    if pushgateway:
        _push(registry, pushgateway)

    t0 = time.monotonic()
    status = "success"
    conn = None
    conn = connect(debug=args.debug)
    try:
        match args.command:
            case "expire":
                expire(conn, args.days, args.dry_run)
            case "cleanup":
                cleanup(conn, args.days, args.dry_run)
            case "cleanup-all":
                cleanup_all(conn, args.dry_run)
            case "dedup-deletions":
                dedup_deletions(conn, args.dry_run)
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
            case "maintain":
                maintain(conn, args.days, args.dry_run)
            case "checkpoint":
                checkpoint(conn)
            case "compact":
                compact(
                    conn,
                    args.tier,
                    args.table or None,
                    args.dry_run,
                    args.threads,
                    args.memory_limit,
                    args.max_compacted_files,
                )
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
