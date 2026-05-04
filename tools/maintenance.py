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
import time
from importlib.metadata import PackageNotFoundError, version

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
    if value is None:
        return "''"
    return "'" + value.replace("'", "''") + "'"


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


def cleanup(conn: duckdb.DuckDBPyConnection, days: int, dry_run: bool) -> None:
    """Delete files scheduled for deletion older than N days."""
    log.info("Cleaning up files older than %d days (dry_run=%s)", days, dry_run)
    result = conn.execute(
        f"CALL ducklake_cleanup_old_files('{ATTACH_NAME}', "
        f"older_than => now() - INTERVAL '{days} days', "
        f"dry_run => {str(dry_run).lower()})"
    ).fetchall()
    for row in result:
        log.info("cleanup: %s", row)


def cleanup_all(conn: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Delete all files scheduled for deletion regardless of age."""
    if dry_run:
        log.info("cleanup-all has no dry-run mode; skipping")
        return
    log.info("Cleaning up all files scheduled for deletion")
    result = conn.execute(f"CALL ducklake_cleanup_old_files('{ATTACH_NAME}', cleanup_all => true)").fetchall()
    for row in result:
        log.info("cleanup-all: %s", row)


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


@contextlib.contextmanager
def _scoped_target_file_size(conn: duckdb.DuckDBPyConnection, value: str):
    """Set target_file_size for the body, restore to DEFAULT_TARGET_FILE_SIZE on exit.

    We don't try to read-and-restore the prior value: ``ducklake_options`` can
    return multiple rows (GLOBAL/SCHEMA/TABLE scopes) and the byte-count
    string DuckLake stores ('134217728') round-trips as an empty value here,
    causing the restore SET to ParserException. Always restoring to the
    documented steady-state default is robust and keeps the catalog at a known
    value regardless of starting state.
    """
    _sanitize_setting_value(value)
    conn.execute(f"CALL ducklake_set_option('{ATTACH_NAME}', 'target_file_size', '{value}')")
    try:
        yield
    finally:
        conn.execute(
            f"CALL ducklake_set_option('{ATTACH_NAME}', 'target_file_size', '{DEFAULT_TARGET_FILE_SIZE}')"
        )
        log.info("target_file_size restored to %s", DEFAULT_TARGET_FILE_SIZE)


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


def compact(
    conn: duckdb.DuckDBPyConnection,
    tier: int,
    table: str | None,
    dry_run: bool,
    threads: int,
    memory_limit: str,
) -> None:
    """Compact files in tier N (1, 2, or 3) for the catalog or one table."""
    spec = TIERS[tier]
    min_b, max_b, target = spec["min"], spec["max"], spec["target"]
    scope = f"table '{table}'" if table else "catalog-wide"
    range_str = f"[{min_b or 0}, {max_b}) bytes"
    log.info(
        "Compact tier %d (%s): merge files %s into ~%s targets (dry_run=%s)",
        tier,
        scope,
        range_str,
        target,
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

    args = [f"max_file_size => {max_b}"]
    if min_b is not None:
        args.append(f"min_file_size => {min_b}")
    if table:
        sql = f"CALL ducklake_merge_adjacent_files('{ATTACH_NAME}', '{table}', {', '.join(args)})"
    else:
        sql = f"CALL ducklake_merge_adjacent_files('{ATTACH_NAME}', {', '.join(args)})"

    _set_compaction_tuning(conn, threads, memory_limit)
    with _scoped_target_file_size(conn, target):
        result = conn.execute(sql).fetchall()
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
    p.add_argument(
        "--threads",
        type=_positive_int,
        default=2,
        help="DuckDB threads during the merge (default 2; raise cautiously, see DuckLake bug c8)",
    )
    p.add_argument(
        "--memory-limit",
        default="4GB",
        help="DuckDB memory_limit during the merge (default 4GB)",
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
            case "orphans":
                orphans(conn, args.dry_run)
            case "maintain":
                maintain(conn, args.days, args.dry_run)
            case "checkpoint":
                checkpoint(conn)
            case "compact":
                compact(conn, args.tier, args.table or None, args.dry_run, args.threads, args.memory_limit)
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
