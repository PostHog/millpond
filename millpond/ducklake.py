import logging
import os
import re
from urllib.parse import urlparse

import duckdb
import pyarrow as pa

from millpond import schema
from millpond.config import Config

log = logging.getLogger(__name__)

_SETTING_VALUE_RE = re.compile(r"^[a-zA-Z0-9_.:/\-@+=]+$")


def _sanitize_setting_value(val: str) -> str:
    """Validate a DuckDB SET value to prevent SQL injection."""
    if not _SETTING_VALUE_RE.match(val):
        raise ValueError(f"Illegal character in DuckDB setting value: {val!r}")
    return val


def connect(cfg: Config) -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB with httpfs and ducklake, attach the catalog."""
    conn = duckdb.connect(cfg.ducklake_connection)

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

    # Parse the PG URL into a libpq connection string for DuckLake.
    # The 'postgres:' prefix tells DuckLake to use the Postgres extension
    # for metadata storage rather than a local DuckDB file.
    # See: https://ducklake.select/docs/stable/duckdb/usage/connecting
    parsed = urlparse(cfg.ducklake_metadata_url)
    pg_connstr = (
        f"host={parsed.hostname} port={parsed.port or 5432} "
        f"dbname={parsed.path.lstrip('/')} user={parsed.username} password={parsed.password}"
    )
    conn.execute(f"""
        ATTACH 'ducklake:postgres:{pg_connstr}' AS lake (
            DATA_PATH '{cfg.ducklake_data_path}'
        )
    """)

    log.info("DuckLake connected: metadata=%s data=%s", cfg.ducklake_metadata_url, cfg.ducklake_data_path)
    return conn


_tables_ensured: set[str] = set()


def _ensure_table(conn: duckdb.DuckDBPyConnection, table_name: str, batch: pa.Table) -> None:
    """Create the DuckLake table from Arrow schema if it doesn't exist. Cached after first call."""
    if table_name in _tables_ensured:
        return
    conn.register("_schema_batch", batch.slice(0, 0))  # empty batch, just schema
    try:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS lake.main.{table_name} AS "
            "SELECT *, NOW() AS _inserted_at FROM _schema_batch WHERE false"
        )
    finally:
        conn.unregister("_schema_batch")
    _tables_ensured.add(table_name)


def write(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    batch: pa.Table,
    schema_mgr: "schema.SchemaManager | None" = None,
) -> None:
    """Write an Arrow table to DuckLake with _inserted_at timestamp."""
    _ensure_table(conn, table_name, batch)
    if schema_mgr is not None:
        schema_mgr.evolve(batch.schema)
    conn.register("_arrow_batch", batch)
    try:
        conn.execute(f"INSERT INTO lake.main.{table_name} BY NAME (SELECT *, NOW() AS _inserted_at FROM _arrow_batch)")
    finally:
        conn.unregister("_arrow_batch")
