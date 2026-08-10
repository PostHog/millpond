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

    def _add_column(self, column_name: str, duckdb_type: str) -> bool:
        """ADD COLUMN IF NOT EXISTS and update cache. Returns True on success.

        On failure logs, bumps ``errors_total{type="schema"}``, and returns False
        so callers can degrade rather than crash-loop the write path.
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
        self._known_columns[column_name] = duckdb_type
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

            if field.name not in self._known_columns:
                self._add_column(field.name, duckdb_type)

            elif self._known_columns[field.name] != duckdb_type:
                # Type mismatch — attempt widening
                existing = self._known_columns[field.name]
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
                    self._known_columns[field.name] = duckdb_type
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

    def _live_column_type(self, column_name: str) -> str | None:
        """Read one column's type from information_schema (normalized).

        Used after ``ADD COLUMN IF NOT EXISTS`` so the cache never trusts a
        no-op ADD that left a wrong-typed column in place. Returns None when
        the column is absent.
        """
        try:
            result = self._conn.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_catalog = 'lake' AND table_schema = ? "
                "AND table_name = ? AND column_name = ?",
                [self._schema_name, self._table_name, column_name],
            ).fetchone()
        except duckdb.CatalogException:
            return None
        if result is None:
            return None
        return _normalize_duckdb_type(result[0])

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

        Projection must only cover the returned set — otherwise INSERT binds a
        missing/wrong-typed column and crash-loops the flush path. Bumps
        ``errors_total{type="schema"}`` on every degrade so the misconfig is loud.
        """
        if not sources:
            return frozenset()
        if not self._ensure_schema_loaded():
            return frozenset()

        ready: set[str] = set()
        for source in sources:
            if source not in present_source_names:
                continue
            if not SAFE_IDENTIFIER.match(source):
                log.warning("Skipping VARIANT dual-write for unsafe source field name: %r", source)
                metrics.records_skipped_total.labels(reason="unsafe_field_name").inc()
                continue

            vname = variant_column_name(source)
            existing = self._known_columns.get(vname)
            if existing is not None:
                if existing != "VARIANT":
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
                ready.add(source)
                continue

            # Do not use _add_column's optimistic cache: ADD IF NOT EXISTS is a
            # no-op when the column already exists under any type, so we must
            # re-read the live type before recording VARIANT in the cache.
            log.info("Schema evolution: adding VARIANT dual-write column %s", vname)
            try:
                self._conn.execute(
                    f"ALTER TABLE lake.{self._schema_name}.{self._table_name} "
                    f'ADD COLUMN IF NOT EXISTS "{vname}" VARIANT'
                )
            except duckdb.Error as e:
                log.warning(
                    "Failed to add VARIANT column %s: %s; degrading to string-only for %s",
                    vname,
                    e,
                    source,
                )
                metrics.errors_total.labels(type="schema").inc()
                continue

            live = self._live_column_type(vname)
            if live != "VARIANT":
                if live is not None:
                    self._known_columns[vname] = live
                log.warning(
                    "VARIANT dual-write target %s is %s after ADD COLUMN IF NOT EXISTS "
                    "(expected VARIANT); degrading to string-only for source %s",
                    vname,
                    live,
                    source,
                )
                metrics.errors_total.labels(type="schema").inc()
                continue
            self._known_columns[vname] = "VARIANT"
            metrics.schema_columns_added_total.inc()
            ready.add(source)

        return frozenset(ready)

    def invalidate(self) -> None:
        """Force a reload of the table schema on next evolve() call."""
        self._initialized = False
