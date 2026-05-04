import logging
import os
import re

import duckdb
import pyarrow as pa

from millpond import schema
from millpond.config import Config

log = logging.getLogger(__name__)

_SETTING_VALUE_RE = re.compile(r"^[a-zA-Z0-9_.:/\-@+=]+$")


def _escape_libpq(value: str | None) -> str:
    """Escape a value for a libpq connection string.

    Wraps in single quotes and backslash-escapes internal single quotes and
    backslashes, per the libpq connstring grammar:

      https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

    Note this is *not* the same parser as Postgres SQL string literals — the
    SQL parser uses ``''`` for embedded quotes and is governed by
    ``standard_conforming_strings``; the libpq connstring parser is a
    separate grammar that has always required ``\\'`` and ``\\\\``.
    """
    if value is None:
        return "''"
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


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

    # Partitioned INSERTs hold concurrent write buffers per partition value.
    # With high-cardinality partition keys this can exhaust memory.
    # Disabling insertion order allows DuckDB to process partitions sequentially.
    conn.execute("SET preserve_insertion_order = false")

    # Build a libpq connection string for DuckLake.
    # The 'postgres:' prefix tells DuckLake to use the Postgres extension
    # for metadata storage rather than a local DuckDB file.
    # See: https://ducklake.select/docs/stable/duckdb/usage/connecting
    pg_connstr = (
        f"host={cfg.rds_host} port={cfg.rds_port} "
        f"dbname={_escape_libpq(cfg.rds_database)} user={_escape_libpq(cfg.rds_username)} "
        f"password={_escape_libpq(cfg.rds_password)}"
    )
    # Double single quotes for DuckDB SQL string literal — the libpq layer
    # inside DuckLake sees the unescaped quotes after DuckDB parses the string.
    pg_connstr_sql = pg_connstr.replace("'", "''")
    conn.execute(f"""
        ATTACH 'ducklake:postgres:{pg_connstr_sql}' AS lake (
            DATA_PATH '{cfg.ducklake_data_path.replace("'", "''")}'
        )
    """)

    log.info(
        "DuckLake connected: metadata=%s:%s/%s data=%s",
        cfg.rds_host,
        cfg.rds_port,
        cfg.rds_database,
        cfg.ducklake_data_path,
    )
    return conn


def _validate_partition_expr(expr: str) -> str:
    """Validate a partition expression to prevent SQL injection."""
    from millpond.config import SAFE_PARTITION_EXPR

    if not SAFE_PARTITION_EXPR.match(expr):
        raise ValueError(f"Partition expression contains unsafe characters: {expr!r}")
    return expr


# Assumes single connection for pod lifetime. Must be cleared if connection is ever recycled.
_tables_ensured: set[str] = set()


def reset_table_cache() -> None:
    """Clear the table creation cache. Call when the DuckDB connection is recycled."""
    _tables_ensured.clear()


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check if a table exists in the DuckLake catalog."""
    result = conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog = 'lake' AND table_schema = 'main' AND table_name = ?",
        [table_name],
    ).fetchone()
    return result is not None


def _ensure_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    batch: pa.Table,
    partition_by: str | None = None,
) -> None:
    """Create the DuckLake table if it doesn't exist. Cached after first call.

    Handles concurrent creation by multiple pods: if CREATE or ALTER fails
    with a serialization/catalog error, we check if the table now exists
    and treat that as success.
    """
    if table_name in _tables_ensured:
        return

    if _table_exists(conn, table_name):
        log.info("Table %s already exists", table_name)
        _tables_ensured.add(table_name)
        return

    conn.register("_schema_batch", batch.slice(0, 0))  # empty batch, just schema
    try:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS lake.main.{table_name} AS "
            "SELECT *, NOW() AS _inserted_at FROM _schema_batch WHERE false"
        )
    except duckdb.Error as e:
        # Another pod may have created the table concurrently
        if _table_exists(conn, table_name):
            log.info("Table %s created by another pod, continuing", table_name)
        else:
            raise RuntimeError(f"Failed to create table {table_name}: {e}") from e
    finally:
        conn.unregister("_schema_batch")

    if partition_by is not None:
        _validate_partition_expr(partition_by)
        try:
            conn.execute(f"ALTER TABLE lake.main.{table_name} SET PARTITIONED BY ({partition_by})")
            log.info("Table %s partitioned by: %s", table_name, partition_by)
        except duckdb.Error as e:
            # Another pod may have already set partitioning — verify table exists and continue
            if _table_exists(conn, table_name):
                log.info("Table %s partition may have been set by another pod, continuing: %s", table_name, e)
            else:
                raise RuntimeError(f"Failed to partition table {table_name}: {e}") from e

    _tables_ensured.add(table_name)


def write(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    batch: pa.Table,
    schema_mgr: "schema.SchemaManager | None" = None,
    partition_by: str | None = None,
) -> None:
    """Write an Arrow table to DuckLake with _inserted_at timestamp."""
    _ensure_table(conn, table_name, batch, partition_by)
    if schema_mgr is not None:
        schema_mgr.evolve(batch.schema)
    conn.register("_arrow_batch", batch)
    try:
        conn.execute(f"INSERT INTO lake.main.{table_name} BY NAME (SELECT *, NOW() AS _inserted_at FROM _arrow_batch)")
    finally:
        conn.unregister("_arrow_batch")
