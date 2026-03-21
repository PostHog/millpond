"""Schema evolution for DuckLake tables.

Compares incoming Arrow schema against the existing DuckLake table schema
and issues DDL to reconcile:
  - New columns → ALTER TABLE ADD COLUMN IF NOT EXISTS
  - Wider types → ALTER TABLE ALTER COLUMN SET DATA TYPE (DuckLake enforces widening-only)
  - Incompatible changes → logged, metricked, skipped

Schema is cached per table to avoid repeated PRAGMA round-trips.
"""

import logging

import duckdb
import pyarrow as pa

from millpond import metrics

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


class SchemaManager:
    """Tracks the DuckLake table schema and evolves it as needed."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, table_name: str):
        self._conn = conn
        self._table_name = table_name
        self._known_columns: dict[str, str] = {}  # column_name -> duckdb_type
        self._initialized = False

    def _load_table_schema(self) -> None:
        """Load current column names and types from DuckLake."""
        try:
            result = self._conn.execute(
                f"SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_catalog = 'lake' AND table_schema = 'main' AND table_name = '{self._table_name}'"
            ).fetchall()
            self._known_columns = {row[0]: row[1] for row in result}
            self._initialized = True
        except duckdb.CatalogException:
            # Table doesn't exist yet
            self._known_columns = {}
            self._initialized = True

    def evolve(self, batch_schema: pa.Schema) -> None:
        """Compare incoming Arrow schema against table and issue DDL if needed."""
        if not self._initialized:
            self._load_table_schema()

        if not self._known_columns:
            # Table was just created by _ensure_table, reload
            self._load_table_schema()
            if not self._known_columns:
                return

        for field in batch_schema:
            if field.name == "_inserted_at":
                continue

            duckdb_type = _arrow_type_to_duckdb(field.type)

            if field.name not in self._known_columns:
                # New column
                log.info("Schema evolution: adding column %s (%s)", field.name, duckdb_type)
                try:
                    self._conn.execute(
                        f"ALTER TABLE lake.main.{self._table_name} "
                        f'ADD COLUMN IF NOT EXISTS "{field.name}" {duckdb_type}'
                    )
                    self._known_columns[field.name] = duckdb_type
                except duckdb.Error as e:
                    log.warning("Failed to add column %s: %s", field.name, e)
                    metrics.errors_total.labels(type="schema").inc()

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
                        f"ALTER TABLE lake.main.{self._table_name} "
                        f'ALTER COLUMN "{field.name}" SET DATA TYPE {duckdb_type}'
                    )
                    self._known_columns[field.name] = duckdb_type
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

    def invalidate(self) -> None:
        """Force a reload of the table schema on next evolve() call."""
        self._initialized = False
