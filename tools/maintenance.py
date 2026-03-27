#!/usr/bin/env python3
"""DuckLake maintenance operations.

Standalone script for running DuckLake maintenance tasks (snapshot expiry,
file cleanup, orphan deletion, checkpoint) from a K8s CronJob or manually.

Requires the same env vars as the tools/justfile:
  DUCKLAKE_RDS_HOST, DUCKLAKE_RDS_PORT, DUCKLAKE_RDS_DATABASE,
  DUCKLAKE_RDS_USERNAME, DUCKLAKE_RDS_PASSWORD, DUCKLAKE_DATA_PATH,
  DUCKDB_S3_REGION, DUCKDB_S3_ACCESS_KEY_ID, DUCKDB_S3_SECRET_ACCESS_KEY
  (plus optional DUCKDB_S3_ENDPOINT, DUCKDB_S3_USE_SSL, DUCKDB_S3_URL_STYLE)
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys

import duckdb

log = logging.getLogger("maintenance")

_SETTING_VALUE_RE = re.compile(r"^[a-zA-Z0-9_.:/\-@+=]+$")


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
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _sanitize_setting_value(val: str) -> str:
    if not _SETTING_VALUE_RE.match(val):
        raise ValueError(f"Illegal character in DuckDB setting value: {val!r}")
    return val


def connect() -> duckdb.DuckDBPyConnection:
    """Connect to DuckLake using environment variables."""
    conn = duckdb.connect()

    # S3 config from env vars
    for key in (
        "DUCKDB_S3_ENDPOINT",
        "DUCKDB_S3_ACCESS_KEY_ID",
        "DUCKDB_S3_SECRET_ACCESS_KEY",
        "DUCKDB_S3_USE_SSL",
        "DUCKDB_S3_URL_STYLE",
        "DUCKDB_S3_REGION",
    ):
        val = os.environ.get(key)
        if val is not None:
            setting = key.lower().replace("duckdb_", "")
            conn.execute(f"SET {setting} = '{_sanitize_setting_value(val)}'")

    conn.execute("LOAD httpfs")
    conn.execute("LOAD ducklake")
    conn.execute("LOAD postgres")

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
        ATTACH 'ducklake:postgres:{pg_connstr_sql}' AS lake (
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
        f"CALL ducklake_expire_snapshots('lake', "
        f"older_than => now() - INTERVAL '{days} days', "
        f"dry_run => {str(dry_run).lower()})"
    ).fetchall()
    for row in result:
        log.info("expire: %s", row)


def cleanup(conn: duckdb.DuckDBPyConnection, days: int, dry_run: bool) -> None:
    """Delete files scheduled for deletion older than N days."""
    log.info("Cleaning up files older than %d days (dry_run=%s)", days, dry_run)
    result = conn.execute(
        f"CALL ducklake_cleanup_old_files('lake', "
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
    result = conn.execute(
        "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)"
    ).fetchall()
    for row in result:
        log.info("cleanup-all: %s", row)


def orphans(conn: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    """Find and delete orphaned S3 files."""
    log.info("Deleting orphaned files (dry_run=%s)", dry_run)
    result = conn.execute(
        f"CALL ducklake_delete_orphaned_files('lake', "
        f"dry_run => {str(dry_run).lower()})"
    ).fetchall()
    for row in result:
        log.info("orphans: %s", row)


def checkpoint(conn: duckdb.DuckDBPyConnection) -> None:
    """Run CHECKPOINT (integrated merge + expire + cleanup)."""
    log.info("Running CHECKPOINT")
    conn.execute("CHECKPOINT lake")
    log.info("CHECKPOINT complete")


def maintain(conn: duckdb.DuckDBPyConnection, days: int, dry_run: bool) -> None:
    """Full maintenance: expire snapshots then cleanup files."""
    expire(conn, days, dry_run)
    cleanup(conn, days, dry_run)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DuckLake maintenance operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--verbose", action="store_true", help="Debug logging")

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

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    conn = connect()
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
    finally:
        conn.close()


if __name__ == "__main__":
    main()
