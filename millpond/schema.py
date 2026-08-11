"""Schema evolution for DuckLake tables.

Compares incoming Arrow schema against the existing DuckLake table schema
and issues DDL to reconcile:
  - New columns → ALTER TABLE ADD COLUMN IF NOT EXISTS
  - Wider types → ALTER TABLE ALTER COLUMN SET DATA TYPE (DuckLake enforces widening-only)
  - Incompatible changes → logged, metricked, skipped

Schema is cached per table to avoid repeated PRAGMA round-trips.
"""

import logging
import re

import duckdb
import pyarrow as pa

from millpond import metrics

# Column names safe to embed in generated SQL. Field names that don't
# match are skipped with a `records_skipped_total{reason="unsafe_field_name"}`
# metric bump.
SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Dual-write suffix for VARIANT companion columns. Source `properties` →
# sink `properties_variant`. Shared with ducklake.write so the ADD COLUMN
# path and the INSERT projection never disagree on the derived name.
VARIANT_COLUMN_SUFFIX = "_variant"


def variant_column_name(source: str) -> str:
    """Derived VARIANT companion name for a dual-written source column."""
    return f"{source}{VARIANT_COLUMN_SUFFIX}"


log = logging.getLogger(__name__)

# PyArrow type → DuckDB SQL type
_ARROW_TO_DUCKDB: dict[str, str] = {
    "bool": "BOOLEAN",
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "uint8": "UTINYINT",
    "uint16": "USMALLINT",
    "uint32": "UINTEGER",
    "uint64": "UBIGINT",
    "float16": "FLOAT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "string": "VARCHAR",
    "large_string": "VARCHAR",
    "utf8": "VARCHAR",
    "large_utf8": "VARCHAR",
    "binary": "BLOB",
    "large_binary": "BLOB",
    "date32": "DATE",
    "timestamp[ns]": "TIMESTAMP",
    "timestamp[us]": "TIMESTAMP",
    "timestamp[ms]": "TIMESTAMP",
    "timestamp[s]": "TIMESTAMP",
    "timestamp[ns, tz=UTC]": "TIMESTAMPTZ",
    "timestamp[us, tz=UTC]": "TIMESTAMPTZ",
    "timestamp[ms, tz=UTC]": "TIMESTAMPTZ",
    "timestamp[s, tz=UTC]": "TIMESTAMPTZ",
}


def _arrow_type_to_duckdb(arrow_type: pa.DataType) -> str:
    """Map a PyArrow type to a DuckDB SQL type string."""
    type_str = str(arrow_type)
    if type_str in _ARROW_TO_DUCKDB:
        return _ARROW_TO_DUCKDB[type_str]
    # Structs, lists, maps → JSON
    if pa.types.is_struct(arrow_type) or pa.types.is_list(arrow_type) or pa.types.is_map(arrow_type):
        return "JSON"
    return "VARCHAR"


# DuckDB information_schema may return different type names than our _ARROW_TO_DUCKDB mapping.
# Normalize to our canonical names to prevent spurious ALTER TABLE on every flush.
_INFO_SCHEMA_TO_CANONICAL: dict[str, str] = {
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
}


def _normalize_duckdb_type(type_name: str) -> str:
    """Normalize a DuckDB type name from information_schema to our canonical form."""
    if type_name in _INFO_SCHEMA_TO_CANONICAL:
        return _INFO_SCHEMA_TO_CANONICAL[type_name]
    # VARIANT spelling varies by catalog ("VARIANT" / "Variant"); compare
    # case-insensitively so ensure_variant_columns never re-ADD every flush.
    if type_name.upper() == "VARIANT":
        return "VARIANT"
    return type_name


class SchemaManager:
    """Tracks the DuckLake table schema and evolves it as needed."""

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        schema_name: str = "main",
    ):
        # `schema_name` defaults to "main" so existing callers (and the
        # in-process tests that hand-construct a SchemaManager) keep
        # writing into DuckDB's default schema without changes.
        # Production callers should set it from cfg.ducklake_schema.
        self._conn = conn
        self._table_name = table_name
        self._schema_name = schema_name
        self._known_columns: dict[str, str] = {}  # column_name -> duckdb_type
        self._initialized = False

    def _load_table_schema(self) -> None:
        """Load current column names and types from DuckLake."""
        try:
            result = self._conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_catalog = 'lake' AND table_schema = ? AND table_name = ?",
                [self._schema_name, self._table_name],
            ).fetchall()
            self._known_columns = {row[0]: _normalize_duckdb_type(row[1]) for row in result}
            self._initialized = True
        except duckdb.CatalogException:
            # Table doesn't exist yet
            self._known_columns = {}
            self._initialized = True

    def _ensure_schema_loaded(self) -> bool:
        """Load/reload known columns if needed. Returns False if the table is empty/missing."""
        if not self._initialized:
            self._load_table_schema()
        if not self._known_columns:
            # Table was just created by _ensure_table, or does not exist yet.
            self._load_table_schema()
            if not self._known_columns:
                return False
        return True

    def _column_type(self, column_name: str) -> str | None:
        """Cached type for a column, matched case-insensitively.

        The cache is keyed by catalog-stored casing while callers hold
        config-cased names; DuckDB resolves identifiers case-insensitively, so
        an exact-case get() would miss (and re-ADD) a case-differing column.
        """
        lname = column_name.lower()
        for name, typ in self._known_columns.items():
            if name.lower() == lname:
                return typ
        return None

    def _set_column_type(self, column_name: str, duckdb_type: str) -> None:
        """Update the cache under the existing (catalog-cased) key if one matches.

        A plain ``dict[column_name] = type`` could create a second entry that
        differs only in case, leaving _column_type to return the stale one.
        """
        lname = column_name.lower()
        for name in self._known_columns:
            if name.lower() == lname:
                self._known_columns[name] = duckdb_type
                return
        self._known_columns[column_name] = duckdb_type

    def _add_column(self, column_name: str, duckdb_type: str, require_verified: bool = False) -> bool:
        """ADD COLUMN IF NOT EXISTS, re-read the live schema, and update the cache.

        The live schema is re-read (via the shared _load_table_schema path)
        rather than trusted because ADD IF NOT EXISTS is a no-op when the
        column already exists under any type (e.g. created by another writer) —
        caching the requested type would silently mask the mismatch.

        When the advisory re-read itself fails after a successful ADD:
        - ``require_verified=False`` (evolve's ordinary columns): trust the ADD
          and cache the requested type — at worst a stale wrong type triggers a
          widening attempt on a later flush.
        - ``require_verified=True`` (VARIANT companions): return False so the
          caller degrades for this flush — projecting into an unverified column
          risks silently corrupting it via implicit casts.

        Returns True when the column is known (or trusted) to exist as
        ``duckdb_type``; on failure logs, bumps ``errors_total{type="schema"}``,
        and returns False so callers degrade rather than crash-loop the write
        path.
        """
        log.info("Schema evolution: adding column %s (%s)", column_name, duckdb_type)
        try:
            self._conn.execute(
                f"ALTER TABLE lake.{self._schema_name}.{self._table_name} "
                f'ADD COLUMN IF NOT EXISTS "{column_name}" {duckdb_type}'
            )
        except duckdb.Error as e:
            log.warning("Failed to add column %s: %s", column_name, e)
            metrics.errors_total.labels(type="schema").inc()
            return False
        try:
            self._load_table_schema()
        except duckdb.Error as e:
            # Transient catalog error on an advisory re-read must not fail the
            # flush (the ADD itself succeeded).
            metrics.errors_total.labels(type="schema").inc()
            if require_verified:
                log.warning("Cannot verify type of %s after ADD COLUMN: %s; degrading", column_name, e)
                return False
            log.warning("Schema re-read failed after adding %s: %s; trusting the ADD", column_name, e)
            self._set_column_type(column_name, duckdb_type)
            metrics.schema_columns_added_total.inc()
            return True
        live = self._column_type(column_name)
        if live is None:
            log.warning("Column %s missing after ADD COLUMN IF NOT EXISTS", column_name)
            metrics.errors_total.labels(type="schema").inc()
            return False
        if live != duckdb_type:
            log.warning(
                "Column %s is %s after ADD COLUMN IF NOT EXISTS (expected %s); keeping live type",
                column_name,
                live,
                duckdb_type,
            )
            metrics.errors_total.labels(type="schema").inc()
            return False
        metrics.schema_columns_added_total.inc()
        return True

    def evolve(self, batch_schema: pa.Schema) -> None:
        """Compare incoming Arrow schema against table and issue DDL if needed."""
        if not self._ensure_schema_loaded():
            return

        for field in batch_schema:
            if field.name == "_inserted_at":
                continue

            if not SAFE_IDENTIFIER.match(field.name):
                log.warning("Skipping unsafe field name: %r", field.name)
                metrics.records_skipped_total.labels(reason="unsafe_field_name").inc()
                continue

            duckdb_type = _arrow_type_to_duckdb(field.type)

            existing = self._column_type(field.name)
            if existing is None:
                self._add_column(field.name, duckdb_type)

            elif existing != duckdb_type:
                # Type mismatch — attempt widening
                log.info(
                    "Schema evolution: widening column %s from %s to %s",
                    field.name,
                    existing,
                    duckdb_type,
                )
                try:
                    self._conn.execute(
                        f"ALTER TABLE lake.{self._schema_name}.{self._table_name} "
                        f'ALTER COLUMN "{field.name}" SET DATA TYPE {duckdb_type}'
                    )
                    self._set_column_type(field.name, duckdb_type)
                    metrics.schema_columns_widened_total.inc()
                except duckdb.Error as e:
                    # DuckLake rejects invalid promotions — log and continue
                    log.warning(
                        "Failed to widen column %s from %s to %s: %s",
                        field.name,
                        existing,
                        duckdb_type,
                        e,
                    )
                    metrics.errors_total.labels(type="schema").inc()

    def live_variant_column_names(self) -> frozenset[str]:
        """Lowercased names of VARIANT columns in the live table schema.

        Only the sink creates VARIANT columns (evolve maps nested Arrow types
        to JSON), so every VARIANT column is sink-managed. write() uses this to
        strip colliding payload fields even when the local variant_columns
        config is absent or stale (mixed fleet).
        """
        if not self._ensure_schema_loaded():
            return frozenset()
        return frozenset(name.lower() for name, typ in self._known_columns.items() if typ == "VARIANT")

    def ensure_variant_columns(
        self,
        sources: tuple[str, ...],
        present_source_names: set[str],
    ) -> frozenset[str]:
        """Ensure VARIANT dual-write companions exist; return sources ready to project.

        For each source in ``sources`` present in the batch, try to ensure a
        ``{source}_variant`` VARIANT column. Returns the subset of sources whose
        companion is confirmed VARIANT and safe to project on INSERT.

        Degrades (excludes from the returned set) rather than raising when:
        - the companion already exists as a non-VARIANT type (cannot ALTER in place)
        - ADD COLUMN fails
        - ADD IF NOT EXISTS no-ops on a wrong-typed column

        Presence matching is case-insensitive: a ``Properties`` batch key feeds
        the same DuckDB column as configured ``properties``, so it must dual-write
        rather than silently skip. Projection must only cover the returned set —
        otherwise INSERT binds a missing/wrong-typed column and crash-loops the
        flush path. Bumps ``errors_total{type="schema"}`` on every degrade so
        the misconfig is loud.
        """
        if not sources:
            return frozenset()
        if not self._ensure_schema_loaded():
            return frozenset()

        present_lower = {name.lower() for name in present_source_names}
        ready: set[str] = set()
        for source in sources:
            if source.lower() not in present_lower:
                continue
            # Unreachable via config.load() (which rejects unsafe names), but
            # kept as defense-in-depth: vname is embedded in generated DDL.
            # No records are skipped on this path, so it's a schema error.
            if not SAFE_IDENTIFIER.match(source):
                log.warning("Skipping VARIANT dual-write for unsafe source field name: %r", source)
                metrics.errors_total.labels(type="schema").inc()
                continue

            vname = variant_column_name(source)
            existing = self._column_type(vname)
            if existing == "VARIANT":
                ready.add(source)
                continue
            if existing is not None:
                log.warning(
                    "VARIANT dual-write target %s exists as %s, expected VARIANT; "
                    "degrading to string-only for source %s (DuckLake cannot ALTER "
                    "to VARIANT in place)",
                    vname,
                    existing,
                    source,
                )
                metrics.errors_total.labels(type="schema").inc()
                continue

            if self._add_column(vname, "VARIANT", require_verified=True):
                ready.add(source)
            else:
                log.warning("Degrading to string-only for source %s", source)

        return frozenset(ready)

    def invalidate(self) -> None:
        """Force a reload of the table schema on next evolve() call."""
        self._initialized = False
