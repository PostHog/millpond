from __future__ import annotations

import logging
import os
import re

import duckdb
import pyarrow as pa

from millpond import metrics, schema
from millpond.config import Config
from millpond.schema import variant_column_name

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

    Deliberately fatal, unlike the non-fatal VARIANT companion drops in
    _drop_protected_columns: silently dropping a payload `_inserted_at` would
    re-stamp replayed data and change its partition placement, which the
    reserved-columns contract predating dual-write chose to surface loudly.
    """
    collisions = sorted(name for name in batch_schema.names if name in reserved)
    if collisions:
        raise ValueError(
            f"Source schema column(s) {collisions!r} collide with "
            f"DuckLake-reserved metadata column names; rename them "
            f"upstream or filter them out before write()."
        )


def _drop_protected_columns(batch: pa.Table, protected_lower: frozenset[str] | set[str]) -> pa.Table:
    """Drop batch columns whose lowercased name is a sink-managed VARIANT target.

    Matching is case-insensitive because DuckDB resolves identifiers
    case-insensitively — a ``PROPERTIES_VARIANT`` key would land in the same
    catalog column as ``properties_variant``. Bumps
    ``variant_companion_columns_dropped_total`` once per dropped field name
    (records still land, so this is not a ``records_skipped_total`` reason).

    NB: reserved metadata names (``_inserted_at``, ``year``…) deliberately do
    NOT go through this drop path — check_reserved_collision keeps its fatal
    policy because silently re-stamping ``_inserted_at`` on replayed data would
    change partition placement; that trade-off predates dual-write.
    """
    if not protected_lower:
        return batch
    drop = [n for n in batch.schema.names if n.lower() in protected_lower]
    if not drop:
        return batch
    for name in drop:
        log.warning(
            "Dropping source column %r that collides with a VARIANT dual-write "
            "target; dual-write continues from the configured source when present",
            name,
        )
        metrics.variant_companion_columns_dropped_total.inc()
    return batch.drop_columns(drop)


def drop_variant_companion_columns(
    batch: pa.Table,
    variant_columns: tuple[str, ...] | None,
) -> pa.Table:
    """Strip config-managed ``{source}_variant`` columns from the batch non-fatally.

    Kafka payloads can carry a literal ``properties_variant`` key. Leaving it in
    the batch would let evolve() ADD it as VARCHAR (silent corruption when a
    later dual-write projects VARIANT into it) or collide with the INSERT
    projection (crash loop). Drop companions before ensure_table/evolve/insert;
    keep dual-writing from the real source column when present. write() also
    drops any column whose *live* table type is VARIANT, protecting writers
    whose local config is absent or stale (mixed fleet).
    """
    protected = {variant_column_name(s).lower() for s in variant_columns or ()}
    return _drop_protected_columns(batch, protected)


def _quote_ident(name: str) -> str:
    """Double-quote a DuckDB identifier, escaping embedded quotes."""
    return '"' + name.replace('"', '""') + '"'


def build_insert_select_sql(
    column_names: list[str],
    variant_columns: tuple[str, ...] | None,
) -> str:
    """Build the SELECT list for ``INSERT ... BY NAME (SELECT ... FROM batch)``.

    When no dual-write companions are projected this batch, returns the
    historical shape ``*, NOW() AS _inserted_at`` so opt-in VARIANT dual-write
    does not rewrite every writer's INSERT. When at least one source is in
    ``variant_columns`` (the *ready* set from ensure_variant_columns), expands
    an explicit column list and projects companions:

        try_cast(try_cast("properties" AS JSON) AS VARIANT) AS "properties_variant"

    Source matching is case-insensitive (DuckDB resolves identifiers
    case-insensitively, so a ``Properties`` batch column feeds the same catalog
    column as configured ``properties``); the companion alias always uses the
    configured source's casing, projected at most once per source. Identifiers
    are quote-escaped (``"`` → ``""``) so a Kafka key containing a quote cannot
    break the INSERT or inject SQL — the sole gate on these names, which is why
    no SAFE_IDENTIFIER filter re-runs here: silently dropping a ready source
    would leave a permanently-NULL companion with no signal. ``try_cast`` nulls
    malformed JSON on the VARIANT side only; the original string column still
    lands.
    """
    active = {s.lower(): s for s in (variant_columns or ())}
    if not any(name.lower() in active for name in column_names):
        return "*, NOW() AS _inserted_at"

    parts: list[str] = []
    projected: set[str] = set()
    for name in column_names:
        parts.append(_quote_ident(name))
        source = active.get(name.lower())
        if source is not None and source not in projected:
            projected.add(source)
            vname = variant_column_name(source)
            qname = _quote_ident(name)
            parts.append(f"try_cast(try_cast({qname} AS JSON) AS VARIANT) AS {_quote_ident(vname)}")
    parts.append("NOW() AS _inserted_at")
    return ", ".join(parts)


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

    # DuckLake retries failed commits up to ducklake_max_retry_count
    # times before surfacing the error. Default is 10, which is too low
    # for multi-writer deployments — losers of the snapshot-id allocation
    # race burn 10 retries fast and surface as
    # `ducklake_snapshot_pkey` duplicate-key violations. Loaded from
    # DUCKLAKE_MAX_RETRY_COUNT env (default 100).
    conn.execute(f"SET ducklake_max_retry_count = {int(cfg.ducklake_max_retry_count)}")

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
    variant_columns: tuple[str, ...] | None = None,
) -> int:
    """Write an Arrow table to DuckLake with _inserted_at timestamp.

    Returns the number of records actually written (0 when the batch is
    skipped whole) so callers can keep records_written_total honest.

    When ``variant_columns`` is set, each listed source column present in the
    batch is dual-written: the original column is kept as-is, and a companion
    ``{name}_variant`` column receives ``try_cast(try_cast(col AS JSON) AS
    VARIANT)``. DuckDB auto-shreds VARIANT sub-fields on Parquet write.

    Dual-write is best-effort per source: companions that cannot be ensured as
    VARIANT are omitted from the INSERT projection (string column still lands).
    Payload fields named like dual-write targets are stripped non-fatally so a
    poison key cannot crash-loop the partition or poison the table as VARCHAR.
    """
    check_reserved_collision(batch.schema, RESERVED_COLUMNS)
    if variant_columns and schema_mgr is None:
        # Dual-write requires SchemaManager for ADD COLUMN + type checks.
        # DuckLakeSink always supplies one; module-level callers that enable
        # dual-write must too (no divergent raw DDL path).
        raise ValueError("VARIANT dual-write requires a SchemaManager (schema_mgr is None)")
    # Drop sink-managed companion names before CREATE/evolve so a Kafka key
    # named properties_variant cannot land as VARCHAR or collide on INSERT.
    had_columns = batch.num_columns > 0
    batch = drop_variant_companion_columns(batch, variant_columns)
    if schema_mgr is not None:
        # Also drop any column whose *live* table type is VARIANT. Only the
        # sink creates VARIANT columns (evolve maps nested Arrow types to
        # JSON), so every VARIANT column is sink-managed. This protects
        # writers whose variant_columns config is absent or stale (mixed
        # fleet): INSERT BY NAME would otherwise implicitly cast the raw JSON
        # text into a string-wrapped variant — silent corruption invisible to
        # `companion.field` queries.
        batch = _drop_protected_columns(batch, schema_mgr.live_variant_column_names())
    if had_columns and batch.num_columns == 0:
        # Every field was a companion collision (poison producer). SELECT *
        # over a zero-column relation errors, so skip the flush instead of
        # crash-looping the partition on the same batch. These records ARE
        # lost, hence records_skipped_total (unlike the per-column drops).
        # Gated on had_columns: a batch that arrives with zero columns did
        # not collide with anything and must keep failing loudly below.
        log.warning(
            "Skipping batch of %d record(s): all columns collided with VARIANT dual-write targets",
            batch.num_rows,
        )
        metrics.records_skipped_total.labels(reason="variant_companion_collision").inc(batch.num_rows)
        return 0
    _ensure_table(conn, table_name, batch, tables_ensured, partition_by, schema_name)
    ready_sources: tuple[str, ...] | None = None
    if schema_mgr is not None:
        schema_mgr.evolve(batch.schema)
        if variant_columns:
            ready = schema_mgr.ensure_variant_columns(variant_columns, set(batch.schema.names))
            ready_sources = tuple(s for s in variant_columns if s in ready) or None
    select_list = build_insert_select_sql(list(batch.schema.names), ready_sources)
    insert_sql = f"INSERT INTO lake.{schema_name}.{table_name} BY NAME (SELECT {select_list} FROM _arrow_batch)"
    conn.register("_arrow_batch", batch)
    try:
        try:
            conn.execute(insert_sql)
        except duckdb.Error:
            if ready_sources is None:
                raise
            # The VARIANT projection can fail at WRITE time on values the
            # cast itself accepted: DuckDB shreds VARIANT into typed Parquet
            # columns, and a JSON integer above INT64_MAX arrives as UINT64
            # and overflows the shredded column ("Type UINT64 with value ...
            # out of range for INT64" — 2026-08-12 incident, every NRT pod
            # crash-looped on one tenant's payload). try_cast cannot guard
            # it: the value is valid VARIANT, only unshreddable.
            #
            # Retry the same batch string-only. The failed INSERT committed
            # nothing, the string column still lands, and the companion is
            # NULL for these rows — the same degradation ensure_variant_columns
            # applies for a wrong-typed companion, rather than a poison batch
            # wedging the partition forever. Re-raise if the retry also
            # fails: then the projection was not the cause.
            log.warning(
                "VARIANT dual-write INSERT failed for %d record(s); retrying string-only "
                "(companion NULL for this batch). Sources: %s",
                batch.num_rows,
                ", ".join(ready_sources),
                exc_info=True,
            )
            metrics.errors_total.labels(type="variant_write").inc()
            metrics.variant_write_fallback_total.inc()
            fallback_sql = (
                f"INSERT INTO lake.{schema_name}.{table_name} BY NAME "
                f"(SELECT {build_insert_select_sql(list(batch.schema.names), None)} FROM _arrow_batch)"
            )
            conn.execute(fallback_sql)
    finally:
        conn.unregister("_arrow_batch")
    return batch.num_rows


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
        self._variant_columns = cfg.variant_columns

    def write(self, batch: pa.Table) -> int:
        return write(
            self._conn,
            self._table_name,
            batch,
            self._tables_ensured,
            self._schema_mgr,
            self._partition_by,
            self._schema_name,
            self._variant_columns,
        )

    def reset_caches(self) -> None:
        self._tables_ensured.clear()
        self._schema_mgr.invalidate()

    def close(self) -> None:
        self._conn.close()
