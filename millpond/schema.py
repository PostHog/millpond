"""Schema evolution for Iceberg tables.

Compares incoming Arrow schema against the live Iceberg table schema
and issues ``Table.update_schema()`` operations to reconcile:

  - New columns               -> ``add_column(name, type)``
  - Compatible widenings      -> ``update_column(name, field_type=...)``
  - Incompatible changes      -> logged, metricked, skipped

The four partition columns (``year``/``month``/``day``/``hour``) and
``_inserted_at`` are reserved — ``iceberg.py`` owns them and source
schema evolution must not touch them.

Schema state is cached per instance to avoid round-tripping the catalog
on every flush. ``invalidate()`` forces a reload on the next ``evolve()``;
called by the write-retry path after a failed write since another writer
may have changed the schema underneath us.
"""

from __future__ import annotations

import logging
import re

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)

from millpond import iceberg as iceberg_mod
from millpond import metrics

log = logging.getLogger(__name__)

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Columns owned by iceberg.py — must never be altered by source-schema
# evolution. The arrow batch handed to write() can't shadow these without
# breaking the partition layout.
_RESERVED: frozenset[str] = frozenset({"_inserted_at", *iceberg_mod.PARTITION_COLS})


def _arrow_to_iceberg(arrow_type: pa.DataType) -> IcebergType:
    """Map a PyArrow type to an Iceberg type.

    Source data has already had nested types (struct/list/map) JSON-stringified
    upstream in arrow_converter, so by the time a field reaches us its type
    should be flat. Anything we don't recognise falls back to ``StringType``.
    """
    if pa.types.is_boolean(arrow_type):
        return BooleanType()
    # Iceberg has no signed int width below 32; widen on the way in. uint32
    # exceeds int32 range so widen to LongType to be safe.
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type) or pa.types.is_int32(arrow_type):
        return IntegerType()
    if pa.types.is_uint8(arrow_type) or pa.types.is_uint16(arrow_type):
        return IntegerType()
    if pa.types.is_int64(arrow_type) or pa.types.is_uint32(arrow_type) or pa.types.is_uint64(arrow_type):
        return LongType()
    if pa.types.is_float16(arrow_type) or pa.types.is_float32(arrow_type):
        return FloatType()
    if pa.types.is_float64(arrow_type):
        return DoubleType()
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return StringType()
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return BinaryType()
    if pa.types.is_date(arrow_type):
        return DateType()
    if pa.types.is_timestamp(arrow_type):
        return TimestamptzType() if arrow_type.tz else TimestampType()
    return StringType()


class SchemaManager:
    """Tracks the Iceberg table schema and evolves it as needed."""

    def __init__(self, catalog: Catalog, namespace: str, table_name: str):
        self._catalog = catalog
        self._namespace = namespace
        self._table_name = table_name
        self._known: dict[str, IcebergType] = {}
        self._initialized = False

    def _identifier(self) -> tuple[str, str]:
        return (self._namespace, self._table_name)

    def _load_table_schema(self) -> None:
        try:
            table = self._catalog.load_table(self._identifier())
            self._known = {f.name: f.field_type for f in table.schema().fields}
        except Exception:
            # Table not created yet; iceberg._ensure_table will land it
            # on the next write. evolve() returns early in this case and
            # picks it up after iceberg.py creates the table.
            self._known = {}
        self._initialized = True

    def evolve(self, batch_schema: pa.Schema) -> None:
        """Reconcile the live table schema with the columns in ``batch_schema``.

        Adds and widenings are collected and applied in a single
        ``update_schema()`` transaction. PyIceberg auto-commits with
        optimistic concurrency on the context manager exit; a conflict
        with another writer raises and the call lands in the broad
        except below, which invalidates the cache so the next attempt
        re-reads the (possibly-already-evolved-by-someone-else) schema.
        """
        if not self._initialized:
            self._load_table_schema()
        if not self._known:
            # Table not yet present; iceberg._ensure_table will create it
            # with the source schema on the next write. Force a reload on
            # the following evolve() call so we pick up the new state.
            self._initialized = False
            return

        additions: list[tuple[str, IcebergType]] = []
        widenings: list[tuple[str, IcebergType, IcebergType]] = []

        for field in batch_schema:
            if field.name in _RESERVED:
                continue
            if not _SAFE_IDENTIFIER.match(field.name):
                log.warning("Skipping unsafe field name: %r", field.name)
                metrics.records_skipped_total.labels(reason="unsafe_field_name").inc()
                continue

            ice_type = _arrow_to_iceberg(field.type)
            if field.name not in self._known:
                additions.append((field.name, ice_type))
            elif type(self._known[field.name]) is not type(ice_type):
                widenings.append((field.name, self._known[field.name], ice_type))

        if not additions and not widenings:
            return

        try:
            table = self._catalog.load_table(self._identifier())
            with table.update_schema() as us:
                for name, ice_type in additions:
                    us.add_column(name, ice_type)
                for name, _old, new in widenings:
                    us.update_column(name, field_type=new)
            # update_schema auto-commits on context exit. Re-mirror local
            # state from the post-commit snapshot rather than trusting our
            # planned set — Iceberg may have rejected widenings as
            # incompatible without raising (depends on PyIceberg version).
            self._load_table_schema()
            for name, ice_type in additions:
                if self._known.get(name) is not None:
                    metrics.schema_columns_added_total.inc()
                    log.info("Schema evolution: added column %s (%s)", name, ice_type)
            for name, old, new in widenings:
                current = self._known.get(name)
                if current is not None and type(current) is type(new):
                    metrics.schema_columns_widened_total.inc()
                    log.info("Schema evolution: widened column %s from %s to %s", name, old, new)
        except Exception as e:
            log.warning("Schema evolution commit failed: %s", e)
            metrics.errors_total.labels(type="schema").inc()
            self._initialized = False

    def invalidate(self) -> None:
        """Force a reload of the table schema on the next ``evolve()`` call."""
        self._initialized = False
