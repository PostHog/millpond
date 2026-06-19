from __future__ import annotations

import logging
import os
import re

import duckdb
import pyarrow as pa

from millpond import schema
from millpond.config import Config

log = logging.getLogger(__name__)

_SETTING_VALUE_RE = re.compile(r"^[a-zA-Z0-9_.:/\-@+=]+$")

# Column names reserved as metadata. DuckLake itself only writes
# `_inserted_at`; year/month/day/hour stay reserved because partition
# expressions commonly derive them, and tables were created under a
# regime that reserved them — accepting them now would silently change
# collision behavior for replayed data.
RESERVED_COLUMNS: frozenset[str] = frozenset({"_inserted_at", "year", "month", "day", "hour"})


def check_reserved_collision(batch_schema: pa.Schema, reserved: frozenset[str]) -> None:
    """Raise early on source-schema collision with reserved metadata columns.

    `_inserted_at` is appended at write time (with `year/month/day/hour`
    reserved alongside it). If a source column has the same name, the
    append step explodes deep in the stack (duplicate column on the
    post-write projection). Catch it at the top of `write()` with a
    clear message instead.
    """
    collisions = sorted(name for name in batch_schema.names if name in reserved)
    if collisions:
        raise ValueError(
            f"Source schema column(s) {collisions!r} collide with "
            f"DuckLake-reserved metadata column names; rename them "
            f"upstream or filter them out before write()."
        )


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


def _table_exists(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    schema_name: str = "main",
) -> bool:
    """Check if a table exists in the DuckLake catalog."""
    result = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_catalog = 'lake' AND table_schema = ? AND table_name = ?",
        [schema_name, table_name],
    ).fetchone()
    return result is not None


def _ensure_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    batch: pa.Table,
    tables_ensured: set[str],
    partition_by: str | None = None,
    schema_name: str = "main",
) -> None:
    """Create the DuckLake table if it doesn't exist. Caller-owned cache.

    Handles concurrent creation by multiple pods: if CREATE or ALTER fails
    with a serialization/catalog error, we check if the table now exists
    and treat that as success. `tables_ensured` is owned by the caller
    (a Sink instance) so cache lifetime tracks the connection's.

    Multi-writer DDL safety: `CREATE TABLE IF NOT EXISTS` and the
    `_table_exists` re-check on error make CREATE idempotent across pods.
    `ADD COLUMN IF NOT EXISTS` in schema.SchemaManager makes evolution
    idempotent too. A pod with a stale `_known_columns` view that races
    against another writer's ADD COLUMN will either succeed (the INSERT
    `BY NAME` tolerates the extra column existing) or fail and trip the
    write-retry path that invalidates the schema cache.
    """
    if table_name in tables_ensured:
        return

    if _table_exists(conn, table_name, schema_name):
        log.info("Table %s.%s already exists", schema_name, table_name)
        tables_ensured.add(table_name)
        return

    conn.register("_schema_batch", batch.slice(0, 0))  # empty batch, just schema
    try:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS lake.{schema_name}.{table_name} AS "
            "SELECT *, NOW() AS _inserted_at FROM _schema_batch WHERE false"
        )
    except duckdb.Error as e:
        # Another pod may have created the table concurrently
        if _table_exists(conn, table_name, schema_name):
            log.info("Table %s.%s created by another pod, continuing", schema_name, table_name)
        else:
            raise RuntimeError(f"Failed to create table {schema_name}.{table_name}: {e}") from e
    finally:
        conn.unregister("_schema_batch")

    if partition_by is not None:
        _validate_partition_expr(partition_by)
        try:
            conn.execute(f"ALTER TABLE lake.{schema_name}.{table_name} SET PARTITIONED BY ({partition_by})")
            log.info("Table %s.%s partitioned by: %s", schema_name, table_name, partition_by)
        except duckdb.Error as e:
            # Another pod may have already set partitioning — verify table exists and continue
            if _table_exists(conn, table_name, schema_name):
                log.info(
                    "Table %s.%s partition may have been set by another pod, continuing: %s",
                    schema_name,
                    table_name,
                    e,
                )
            else:
                raise RuntimeError(f"Failed to partition table {schema_name}.{table_name}: {e}") from e

    tables_ensured.add(table_name)


def write(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    batch: pa.Table,
    tables_ensured: set[str],
    schema_mgr: schema.SchemaManager | None = None,
    partition_by: str | None = None,
    schema_name: str = "main",
) -> None:
    """Write an Arrow table to DuckLake with _inserted_at timestamp."""
    check_reserved_collision(batch.schema, RESERVED_COLUMNS)
    _ensure_table(conn, table_name, batch, tables_ensured, partition_by, schema_name)
    if schema_mgr is not None:
        schema_mgr.evolve(batch.schema)
    conn.register("_arrow_batch", batch)
    try:
        conn.execute(
            f"INSERT INTO lake.{schema_name}.{table_name} BY NAME (SELECT *, NOW() AS _inserted_at FROM _arrow_batch)"
        )
    finally:
        conn.unregister("_arrow_batch")


class DuckLakeSink:
    """The sink: owns the DuckDB connection, table cache, and schema state.

    Thin wrapper around the module-level `connect`/`write` helpers and the
    existing `schema.SchemaManager`. main.py only calls `write()`,
    `reset_caches()`, and `close()`; schema evolution via DuckLake DDL is
    none of its business. `write()` must not be called with a zero-row
    batch (main.py gates on `pending_records > 0`); `reset_caches()` is
    invoked only by main.py's write-retry loop after a failure; `close()`
    is called exactly once at pod shutdown.

    The table cache and SchemaManager are instance state — each Sink owns
    its own — so multiple Sink instances in the same process (tests, future
    features) don't trample one another.
    """

    def __init__(self, cfg: Config):
        # Explicit guards rather than assert: `python -O` strips asserts and
        # would forward None to connect(), producing a cryptic libpq
        # "host=None" failure instead of a clear startup error. All fields
        # below are read either by connect() building the Postgres
        # connstring or by this constructor.
        for name in (
            "ducklake_schema",
            "ducklake_table",
            "ducklake_connection",
            "ducklake_data_path",
            "rds_host",
            "rds_port",
            "rds_database",
            "rds_username",
            "rds_password",
        ):
            if getattr(cfg, name) is None:
                raise RuntimeError(f"DuckLakeSink requires cfg.{name}; config.load() should have enforced this")
        self._cfg = cfg
        self._conn = connect(cfg)
        self._schema_name = cfg.ducklake_schema
        self._table_name = cfg.ducklake_table
        self._partition_by = cfg.partition_by
        self._tables_ensured: set[str] = set()
        self._schema_mgr = schema.SchemaManager(self._conn, self._table_name, self._schema_name)

    def write(self, batch: pa.Table) -> None:
        write(
            self._conn,
            self._table_name,
            batch,
            self._tables_ensured,
            self._schema_mgr,
            self._partition_by,
            self._schema_name,
        )

    def reset_caches(self) -> None:
        self._tables_ensured.clear()
        self._schema_mgr.invalidate()

    def close(self) -> None:
        self._conn.close()
