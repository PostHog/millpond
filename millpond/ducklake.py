from __future__ import annotations

import logging
import os
import re

import duckdb
import orjson
import pyarrow as pa
import pyarrow.compute as pc

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


# DuckDB shreds VARIANT into typed Parquet columns on write, and an integer in
# (INT64_MAX, UINT64_MAX] becomes a UINT64 variant that overflows the INT64
# shredded column — failing the INSERT even though try_cast accepted the value
# and sub-field access returns it. On 2026-08-12 that crash-looped every prod
# NRT consumer: offsets never advance, so one poison batch wedges the partition
# forever. Verified empirically: values BELOW/AT INT64_MAX (ns timestamps,
# snowflake ids), above UINT64_MAX (kept as a VARIANT string), and negatives
# all shred fine. Only this window is dangerous.
_INT64_MAX = 2**63 - 1
_UINT64_MAX = 2**64 - 1

# Cheap vectorized prefilter, run in Arrow to decide which rows are worth
# parsing. It only has to be a SUPERSET of the danger window, and it assumes no
# JSON structure — it matches inside strings too. Precision comes from the JSON
# parse below, never from this pattern: an earlier attempt to make a regex the
# predicate over raw JSON text was wrong in both directions at once (flagging
# ubiquitous 19-digit ids while missing bare top-level numbers).
#
# Shaped to the window's decimal form so the common case skips the parse
# entirely: every value in (2**63-1, 2**64-1] is either 19 digits starting with
# 9, or 20 digits starting with 1. Nanosecond timestamps (19 digits, leading 1)
# therefore do not trip it — with a plain `[0-9]{19,20}` they did, and the parse
# pass ran on essentially every flush.
_MAYBE_UNSHREDDABLE_DIGITS = r"(9[0-9]{18}|1[0-9]{19})"

# Suffix for the hidden per-source column that carries sanitized JSON. Kept out
# of the INSERT's output list, so the original source column still lands byte
# for byte — the string column stays authoritative.
_VARIANT_SRC_PREFIX = "__millpond_variant_src_"


def _is_unshreddable_value_error(exc: BaseException) -> bool:
    """True for DuckDB's out-of-range conversion error on a VARIANT write.

    Matches the shape reported for the integer widths seen in practice ("Type
    UINT64 with value ... can't be cast because the value is out of range for
    the destination type INT64"), and nothing else — commit contention and IO
    failures must stay retryable.
    """
    msg = str(exc)
    return "out of range for the destination type" in msg and "can't be cast" in msg


def _coerce_unshreddable_ints(raw: str | None) -> str | None:
    """Rewrite integers DuckDB cannot shred as strings, preserving everything else.

    Returns ``raw`` unchanged (same object) when nothing needs fixing, so
    untouched rows keep their exact bytes. Malformed JSON is returned as-is —
    ``try_cast`` nulls its companion anyway.

    Every JSON object key is kept. Restricting *which* keys DuckDB shreds is
    a parquet-writer concern (``variant_shred_key_prefix`` /
    ``variant_shred_keys``), not a document-rewrite: dropping keys here would
    make ``companion.custom_prop`` NULL.
    """
    if raw is None:
        return None
    try:
        doc = orjson.loads(raw)
    except orjson.JSONDecodeError:
        return raw

    changed = False

    def walk(node):
        nonlocal changed
        if isinstance(node, bool):
            return node
        if isinstance(node, int) and _INT64_MAX < node <= _UINT64_MAX:
            changed = True
            return str(node)
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(v) for v in node]
        return node

    fixed = walk(doc)
    if not changed:
        return raw
    return orjson.dumps(fixed).decode()


def sanitize_variant_sources(batch: pa.Table, sources: tuple[str, ...] | None) -> tuple[pa.Table, dict[str, str]]:
    """Add hidden sanitized columns for sources carrying unshreddable integers.

    Returns the batch (with any hidden columns appended) and a map of source →
    column the VARIANT projection should read from. Sources needing no fix are
    absent from the map and project from the original column, so the common
    path adds one vectorized regex scan and nothing else.

    Two stages on purpose. The Arrow-side prefilter is a cheap superset test
    over the whole column at once; only the rows it flags — normally none — are
    parsed and rewritten in Python, which is what makes the precise fix
    affordable on a 256MB batch. The original column is never modified: the
    string column must land exactly as received. Keys are never dropped —
    the companion is the full document.
    """
    if not sources:
        return batch, {}

    by_lower = {name.lower(): name for name in batch.schema.names}
    src_columns: dict[str, str] = {}
    for source in sources:
        name = by_lower.get(source.lower())
        if name is None:
            continue
        column = batch.column(name)
        if not pa.types.is_string(column.type) and not pa.types.is_large_string(column.type):
            # Non-string sources (a batch whose values were all numeric, say)
            # cannot be regex-scanned and cannot carry a JSON document; the
            # backstop covers the rare unsigned-integer overflow.
            continue
        flagged = pc.match_substring_regex(column, _MAYBE_UNSHREDDABLE_DIGITS)
        if not pc.any(flagged, min_count=0).as_py():
            continue
        values = column.to_pylist()
        fixed = [_coerce_unshreddable_ints(v) for v in values]
        n_coerced = sum(1 for before, after in zip(values, fixed, strict=True) if before is not after)
        if not n_coerced:
            continue
        log.warning(
            "Coerced unshreddable integer(s) to strings in %d row(s) of %r for the VARIANT "
            "companion; the source column is unchanged",
            n_coerced,
            name,
        )
        metrics.variant_values_coerced_total.inc(n_coerced)
        hidden = f"{_VARIANT_SRC_PREFIX}{source}"
        batch = batch.append_column(hidden, pa.array(fixed, type=pa.string()))
        src_columns[source] = hidden
    return batch, src_columns


def build_insert_select_sql(
    column_names: list[str],
    variant_columns: tuple[str, ...] | None,
    variant_src: dict[str, str] | None = None,
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

    ``variant_src`` (from sanitize_variant_sources) redirects a source's VARIANT
    cast to a hidden sanitized column. Hidden columns are only ever read inside
    the cast, never emitted as output columns, so ``INSERT ... BY NAME`` never
    sees a column the table does not have.
    """
    active = {s.lower(): s for s in (variant_columns or ())}
    hidden = set((variant_src or {}).values())
    present = [name for name in column_names if name not in hidden]
    # The star form would emit hidden columns as output columns, which the
    # target table does not have — only safe when there are none.
    if not hidden and not any(name.lower() in active for name in present):
        return "*, NOW() AS _inserted_at"

    parts: list[str] = []
    projected: set[str] = set()
    for name in present:
        parts.append(_quote_ident(name))
        source = active.get(name.lower())
        if source is not None and source not in projected:
            projected.add(source)
            vname = variant_column_name(source)
            qname = _quote_ident((variant_src or {}).get(source, name))
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


# `$` is a JSON key character (PostHog reserved properties) and must be legal
# in the shred-allowlist SET strings. The S3 setting charset above is
# narrower on purpose — those values are interpolated into SET s3_*.
_SHRED_SETTING_VALUE_RE = re.compile(r"^[a-zA-Z0-9_$:.,+\-]+$")


def _sanitize_shred_setting_value(val: str) -> str:
    """Validate a variant shred-allowlist SET string."""
    if not _SHRED_SETTING_VALUE_RE.match(val):
        raise ValueError(f"Illegal character in VARIANT shred setting value: {val!r}")
    return val


def _apply_variant_shred_settings(
    conn: duckdb.DuckDBPyConnection,
    key_prefix: object,
    extra_keys: object,
) -> None:
    """SET the parquet writer's shred allowlist. No-op when unset.

    ``key_prefix`` / ``extra_keys`` are ``object`` so a MagicMock cfg in
    tests (which has neither field as a real value) is ignored rather than
    interpolated into SET.
    """
    prefix = key_prefix if isinstance(key_prefix, str) and key_prefix else None
    keys: tuple[str, ...] | None = None
    if isinstance(extra_keys, (tuple, list)) and extra_keys:
        keys = tuple(str(k) for k in extra_keys)
    if prefix is None and keys is None:
        return

    def _set(name: str, value: str) -> bool:
        safe = _sanitize_shred_setting_value(value)
        try:
            conn.execute(f"SET {name} = '{safe}'")
            return True
        except duckdb.Error as e:
            log.warning(
                "DuckDB rejected SET %s=%r (%s); VARIANT companions will auto-shred "
                "every distinct key. This is the 2026-08 OOM path unless the "
                "PostHog duckdb fork (variant_shred_key_prefix) is loaded.",
                name,
                value,
                e,
            )
            return False

    applied = True
    if prefix is not None:
        applied = _set("variant_shred_key_prefix", prefix) and applied
    if keys is not None:
        applied = _set("variant_shred_keys", ",".join(keys)) and applied
    if applied:
        log.info("VARIANT shred allowlist: prefix=%r extra_keys=%s", prefix, ",".join(keys) if keys else "()")


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

    # Shred allowlist: full VARIANT document, typed Parquet columns only for
    # matching keys. Stock DuckDB 1.5.5 ignores these (unknown setting) and
    # auto-shreds every key — the 2026-08 OOM. The PostHog fork honors them
    # in the parquet writer. Fail open on an unknown setting so official
    # wheels still start; log loudly when a filter was requested.
    _apply_variant_shred_settings(conn, cfg.variant_key_prefix, cfg.variant_keys)

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
    VARIANT)``. The companion is the full document. Which keys DuckDB shreds
    into typed Parquet columns is a writer setting (``variant_shred_key_prefix``
    / ``variant_shred_keys``, applied in ``connect``), not a rewrite of the
    value.

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
    # Rewrite integers DuckDB cannot shred (into hidden columns; the source
    # columns themselves are untouched) before they reach the VARIANT cast.
    # Every key stays in the companion — shred allowlisting is a SET on the
    # connection, not a document filter.
    batch, variant_src = sanitize_variant_sources(batch, ready_sources)
    select_list = build_insert_select_sql(list(batch.schema.names), ready_sources, variant_src)
    insert_sql = f"INSERT INTO lake.{schema_name}.{table_name} BY NAME (SELECT {select_list} FROM _arrow_batch)"
    conn.register("_arrow_batch", batch)
    try:
        try:
            conn.execute(insert_sql)
        except duckdb.Error as e:
            # Backstop only. sanitize_variant_sources rewrites the values
            # DuckDB cannot shred, so this should never fire; it exists
            # because the alternative to an unrecognized unshreddable value is
            # the 2026-08-12 crash loop (offsets never advance, so one poison
            # batch wedges the partition forever). The residual case it covers
            # is a non-string source column holding an unsigned integer above
            # INT64_MAX, which the Arrow-side scan cannot reach.
            #
            # Deliberately narrow: only the out-of-range conversion signature
            # is absorbed. Commit contention, S3 flaps and every other write
            # failure must reach main.py's _write_with_retry, which classifies
            # them, retries with backoff, and calls reset_caches() to reload
            # the schema cache — recovery this function must not pre-empt.
            if ready_sources is None or not _is_unshreddable_value_error(e):
                raise
            log.warning(
                "VARIANT dual-write INSERT hit an unshreddable value the sanitizer did not "
                "reach (%d record(s), sources: %s); retrying string-only. This costs the "
                "whole batch its companions and abandons the partly-written Parquet file, "
                "so treat a nonzero variant_write_fallback_total as a gap in "
                "sanitize_variant_sources, not routine degradation.",
                batch.num_rows,
                ", ".join(ready_sources),
                exc_info=True,
            )
            fallback_sql = (
                f"INSERT INTO lake.{schema_name}.{table_name} BY NAME "
                f"(SELECT {build_insert_select_sql(list(batch.schema.names), None, variant_src)} "
                f"FROM _arrow_batch)"
            )
            conn.execute(fallback_sql)
            # Only now is the string-only write real: counting before the
            # retry inflates both metrics by one per outer-retry attempt
            # during an outage where nothing landed at all.
            metrics.errors_total.labels(type="variant_write").inc()
            metrics.variant_write_fallback_total.inc()
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
