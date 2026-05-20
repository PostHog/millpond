"""Iceberg write path.

``connect()`` returns a long-lived PyIceberg ``Catalog`` handle, ``write()``
appends an Arrow batch via a REST commit. ``IcebergSink`` wraps both plus
an internal schema manager and conforms to the `Sink` protocol consumed by
`main.py`.

Partition layout is fixed: identity transforms on ``year`` / ``month`` /
``day`` / ``hour`` derived from ``_inserted_at`` at write time. This produces
a Hive-style layout (``year=2026/month=5/day=13/hour=14/*.parquet``) on S3
that downstream tooling can prefix-filter, at the cost of reader ergonomics
— Iceberg doesn't know the four columns are derived from the timestamp, so
queries need to filter on the partition columns explicitly for pruning. If
reader ergonomics ever bite, we can layer a second hidden-partitioning spec
on top via Iceberg's spec evolution; not needed today.
"""

from __future__ import annotations

import datetime
import logging

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
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

from millpond import metrics
from millpond.config import Config
from millpond.sink import SAFE_IDENTIFIER, check_reserved_collision

log = logging.getLogger(__name__)

# Names of the derived partition columns appended to every batch before append.
# Order matters: it defines the on-disk directory nesting in the Hive layout.
PARTITION_COLS: tuple[str, ...] = ("year", "month", "day", "hour")

# Iceberg requires every PartitionField to have a stable field_id that's
# distinct from any column's field_id. We start the partition field IDs well
# above any plausible source-schema field count so they never collide with
# columns the converter assigns. Field IDs are catalog-wide so this matters.
_PARTITION_FIELD_ID_BASE = 1000

# Columns IcebergSink manages itself; source schemas must not shadow them.
# `_inserted_at` is added at write time; the four partition cols are derived
# from it. `millpond.ducklake.RESERVED_COLUMNS` holds the same five names
# today — DuckLake reserves the partition cols defensively even though it
# doesn't produce them itself, so a deployment-time switch between
# destinations can't suddenly start accepting or rejecting batches based on
# user-column collisions.
RESERVED_COLUMNS: frozenset[str] = frozenset({"_inserted_at", *PARTITION_COLS})


def connect(
    catalog_uri: str,
    warehouse: str,
    s3_access_key_id: str,
    s3_secret_access_key: str,
    s3_region: str,
    *,
    catalog_token: str | None = None,
    s3_endpoint: str | None = None,
) -> Catalog:
    """Load the REST catalog with S3 credentials passed as catalog properties.

    PyIceberg's PyArrow S3 filesystem reads ``s3.access-key-id`` /
    ``s3.secret-access-key`` / ``s3.region`` from the catalog properties
    before falling back to the AWS env var chain (verified against
    pyiceberg 0.11.1). Passing them here keeps the IRSA token used for
    Kafka MSK auth out of the S3 client's credential resolution, exactly
    the way ``DUCKDB_S3_*`` env vars did for DuckDB.
    """
    props: dict[str, str] = {
        "type": "rest",
        "uri": catalog_uri,
        "warehouse": warehouse,
        "s3.access-key-id": s3_access_key_id,
        "s3.secret-access-key": s3_secret_access_key,
        "s3.region": s3_region,
        # Pin the FileIO implementation. Without this, some catalog
        # implementations (Lakekeeper) return responses that lead PyIceberg
        # to route writes through `FsspecFileIO`, which depends on `s3fs`
        # — not a dependency we want to carry. PyArrowFileIO uses PyArrow's
        # S3 client, which is already pulled in transitively and respects
        # the `s3.access-key-id` / `s3.endpoint` properties above.
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }
    if catalog_token:
        props["token"] = catalog_token
    if s3_endpoint:
        props["s3.endpoint"] = s3_endpoint
    cat = load_catalog("lake", **props)
    log.info("Iceberg REST catalog loaded: uri=%s warehouse=%s", catalog_uri, warehouse)
    return cat


def _now_utc_us() -> datetime.datetime:
    """Microsecond-precision UTC now, matching the timestamp type Iceberg expects.

    `datetime.datetime.now(UTC)` is microsecond-precision on every platform
    we support; the Arrow `pa.timestamp("us", tz="UTC")` type carried in
    `_inserted_at` is also microsecond, so no rounding happens on the
    way through PyIceberg's append path.
    """
    return datetime.datetime.now(datetime.UTC)


def _add_metadata_columns(batch: pa.Table) -> pa.Table:
    """Append ``_inserted_at`` plus the four partition columns.

    All rows in a single batch share the same ``_inserted_at`` value so
    a flush always lands in exactly one partition. Year/month/day/hour
    are derived from that timestamp via PyArrow compute; cast to int32
    explicitly because pc.year/month/day/hour all return int64.
    """
    now = _now_utc_us()
    ts_type = pa.timestamp("us", tz="UTC")
    ts_array = pa.array([now] * len(batch), ts_type)
    batch = batch.append_column("_inserted_at", ts_array)
    ts = batch.column("_inserted_at")
    batch = batch.append_column("year", pc.cast(pc.year(ts), pa.int32()))
    batch = batch.append_column("month", pc.cast(pc.month(ts), pa.int32()))
    batch = batch.append_column("day", pc.cast(pc.day(ts), pa.int32()))
    batch = batch.append_column("hour", pc.cast(pc.hour(ts), pa.int32()))
    return batch


def _schema_sample(source_batch: pa.Table) -> pa.Table:
    """Empty Arrow table with the on-disk schema (source cols + metadata cols)."""
    sample = source_batch.slice(0, 0)
    sample = sample.append_column("_inserted_at", pa.array([], pa.timestamp("us", tz="UTC")))
    for name in PARTITION_COLS:
        sample = sample.append_column(name, pa.array([], pa.int32()))
    return sample


def _build_partition_spec(iceberg_schema) -> PartitionSpec:
    """Identity transform on each of year/month/day/hour."""
    return PartitionSpec(
        *(
            PartitionField(
                source_id=iceberg_schema.find_field(name).field_id,
                field_id=_PARTITION_FIELD_ID_BASE + i,
                transform=IdentityTransform(),
                name=name,
            )
            for i, name in enumerate(PARTITION_COLS)
        )
    )


def _ensure_table(
    catalog: Catalog,
    namespace: str,
    table_name: str,
    source_batch: pa.Table,
    tables_ensured: dict[str, Table],
    location: str | None = None,
) -> Table:
    """Get-or-create the Iceberg table. Caller-owned cache (`tables_ensured`).

    Concurrent creation: if ``create_table`` raises with a known
    already-exists / commit-conflict signal, we fall back to ``load_table``.
    The first writer's schema wins for now — schema evolution from a
    concurrent newer-schema pod is handled by ``SchemaManager`` on
    subsequent writes, not here.
    """
    key = f"{namespace}.{table_name}"
    if (cached := tables_ensured.get(key)) is not None:
        return cached

    identifier = (namespace, table_name)
    try:
        table = catalog.load_table(identifier)
        log.info("Iceberg table %s.%s already exists, loaded", namespace, table_name)
    except NoSuchTableError:
        sample = _schema_sample(source_batch)
        # _pyarrow_to_schema_without_ids converts the PyArrow schema but
        # leaves every field_id at -1 (it's meant to feed a name-mapping
        # pass downstream). For our purposes we want sequential IDs so the
        # partition spec can reference columns by ID. assign_fresh_schema_ids
        # walks the schema and replaces -1s with monotonically increasing
        # IDs starting from 1 — the same thing create_table does internally
        # for pa.Schema inputs, but we need the IDs *before* building the
        # partition spec.
        iceberg_schema = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(sample.schema))
        spec = _build_partition_spec(iceberg_schema)
        catalog.create_namespace_if_not_exists(namespace)
        create_kwargs: dict[str, object] = {"partition_spec": spec}
        if location:
            create_kwargs["location"] = location
        try:
            table = catalog.create_table(identifier, schema=iceberg_schema, **create_kwargs)
            log.info("Created Iceberg table %s.%s with partition spec %s", namespace, table_name, PARTITION_COLS)
        except (TableAlreadyExistsError, CommitFailedException):
            # Another writer beat us to it; load what they created.
            table = catalog.load_table(identifier)
            log.info("Iceberg table %s.%s created by another writer, loaded", namespace, table_name)

    tables_ensured[key] = table
    return table


def _arrow_to_iceberg(arrow_type: pa.DataType) -> IcebergType:
    """Map a PyArrow type to an Iceberg type.

    Source data has nested types (struct/list/map) JSON-stringified upstream
    in arrow_converter, so by the time a field reaches us its type should be
    flat. Anything we don't recognise falls back to ``StringType``.
    """
    if pa.types.is_boolean(arrow_type):
        return BooleanType()
    # Iceberg has no signed int width below 32; widen on the way in. uint32
    # exceeds int32 range so widen to LongType to be safe.
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type) or pa.types.is_int32(arrow_type):
        return IntegerType()
    if pa.types.is_uint8(arrow_type) or pa.types.is_uint16(arrow_type):
        return IntegerType()
    # uint64 → LongType is a width truncation: uint64 max (2^64-1) exceeds
    # int63 max (2^63-1) so values above 2^63-1 will fail to write. This
    # matches Iceberg's type system limits (no unsigned types) and we accept
    # the trade-off — flag it in deployment docs if your producer can emit
    # values that large.
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
    """Tracks the Iceberg table schema and evolves it as needed.

    Internal to the Iceberg backend — main.py only sees `IcebergSink.write()`.
    Schema state is cached per instance to avoid round-tripping the catalog on
    every flush. ``invalidate()`` forces a reload on the next ``evolve()``;
    called by the write-retry path after a failed write since another writer
    may have changed the schema underneath us.
    """

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
        except NoSuchTableError:
            # Table not created yet; _ensure_table will land it on the next
            # write. evolve() returns early in this case and picks it up
            # after the table is created.
            self._known = {}
        except Exception:
            # Transient catalog failure (network, server 5xx). Log at WARNING
            # with the exception so an operator sees what blipped, and leave
            # _known empty. The next evolve() call will retry the load.
            log.warning(
                "Failed to load Iceberg table schema for %s.%s",
                self._namespace,
                self._table_name,
                exc_info=True,
            )
            self._known = {}
        self._initialized = True

    def evolve(self, batch_schema: pa.Schema) -> bool:
        """Reconcile the live table schema with the columns in ``batch_schema``.

        Returns True iff this call committed a schema change (caller can use
        the signal to skip a redundant post-evolve table reload). Adds and
        widenings are collected and applied in a single ``update_schema()``
        transaction.

        On commit failure (CommitFailedException, etc.) this **re-raises**
        rather than swallowing — the write-retry loop in ``main.py`` then
        invalidates caches and retries. Earlier versions caught broadly and
        continued to ``Table.append`` with a stale schema view; that defeated
        the retry contract.
        """
        if not self._initialized:
            self._load_table_schema()
        if not self._known:
            # Table not yet present; _ensure_table will create it with the
            # source schema on the next write. Force a reload on the
            # following evolve() call so we pick up the new state.
            self._initialized = False
            return False

        additions: list[tuple[str, IcebergType]] = []
        widenings: list[tuple[str, IcebergType, IcebergType]] = []

        for field in batch_schema:
            if field.name in RESERVED_COLUMNS:
                continue
            if not SAFE_IDENTIFIER.match(field.name):
                log.warning("Skipping unsafe field name: %r", field.name)
                metrics.records_skipped_total.labels(reason="unsafe_field_name").inc()
                continue

            ice_type = _arrow_to_iceberg(field.type)
            if field.name not in self._known:
                additions.append((field.name, ice_type))
            elif type(self._known[field.name]) is not type(ice_type):
                widenings.append((field.name, self._known[field.name], ice_type))

        if not additions and not widenings:
            return False

        try:
            table = self._catalog.load_table(self._identifier())
            with table.update_schema() as us:
                for name, ice_type in additions:
                    us.add_column(name, ice_type)
                for name, _old, new in widenings:
                    us.update_column(name, field_type=new)
        except Exception:
            # Bump the schema-error counter, invalidate, re-raise. The retry
            # path will reload (possibly observing another writer's evolution)
            # and try again.
            metrics.errors_total.labels(type="schema").inc()
            self._initialized = False
            raise

        # update_schema auto-commits on context exit. The commit
        # succeeded; from here on we're only updating local mirror state
        # and emitting observability. Errors past this point must NOT lose
        # the success metrics — count by intent (what we asked for and
        # what update_schema accepted) rather than by post-commit
        # observation, which can transiently fail.
        for name, ice_type in additions:
            metrics.schema_columns_added_total.inc()
            log.info("Schema evolution: added column %s (%s)", name, ice_type)
        for name, old, new in widenings:
            metrics.schema_columns_widened_total.inc()
            log.info("Schema evolution: widened column %s from %s to %s", name, old, new)

        # Re-mirror local state from the post-commit snapshot. If this
        # reload transiently fails, `_load_table_schema` clears `_known`
        # and the *next* evolve() will retry the load — metrics are
        # already counted, so no double-counting risk.
        self._load_table_schema()
        return True

    def invalidate(self) -> None:
        """Force a reload of the table schema on the next ``evolve()`` call."""
        self._initialized = False


def write(
    catalog: Catalog,
    namespace: str,
    table_name: str,
    source_batch: pa.Table,
    tables_ensured: dict[str, Table],
    schema_mgr: SchemaManager | None = None,
    location: str | None = None,
) -> None:
    """Append an Arrow batch to the Iceberg table.

    Per the Sink contract callers must not invoke ``write()`` with a
    zero-row batch (``main.py`` gates on ``pending_records > 0``).
    This function defensively short-circuits if it ever happens — an
    empty append would still incur a REST commit for no benefit. Note
    this means a never-touched Iceberg table is created lazily on the
    first non-empty batch; the DuckLake backend instead creates the
    table eagerly on any write call. The divergence is acceptable
    because main.py never exercises the empty-batch path.

    Adds ``_inserted_at`` plus the four partition columns derived from
    it, then calls ``Table.append(...)``. ``schema_mgr`` (when supplied)
    evolves the catalog schema to accommodate any new source columns in
    the batch — and only re-loads the cached Table object when evolution
    actually committed a change.
    """
    if len(source_batch) == 0:
        return
    check_reserved_collision(source_batch.schema, RESERVED_COLUMNS, "Iceberg")

    table = _ensure_table(catalog, namespace, table_name, source_batch, tables_ensured, location)
    if schema_mgr is not None and schema_mgr.evolve(source_batch.schema):
        # Schema changed; refresh the cached Table so the append sees the
        # new columns. Skip the round-trip when nothing committed (common
        # case after the first flush).
        table = catalog.load_table((namespace, table_name))
        tables_ensured[f"{namespace}.{table_name}"] = table

    batch_with_meta = _add_metadata_columns(source_batch)
    table.append(batch_with_meta)


class IcebergSink:
    """`Sink` implementation for Iceberg.

    Thin wrapper around the module-level `connect` / `write` helpers and the
    in-module `SchemaManager`. main.py only sees the Sink protocol; whether
    schema evolution happens via DuckLake DDL or Iceberg `update_schema()`
    is none of its business.

    The table cache and SchemaManager are instance state — each Sink owns
    its own — so multiple Sink instances in the same process (tests, future
    features) don't trample one another.
    """

    def __init__(self, cfg: Config):
        # Explicit guards rather than assert: `python -O` strips asserts and
        # would forward None to connect(), producing a cryptic pyiceberg
        # failure on first write instead of a clear startup error.
        for name in (
            "iceberg_catalog_uri",
            "iceberg_warehouse",
            "iceberg_namespace",
            "iceberg_table",
            "s3_access_key_id",
            "s3_secret_access_key",
            "s3_region",
        ):
            if getattr(cfg, name) is None:
                raise RuntimeError(f"IcebergSink requires cfg.{name}; config.load() should have enforced this")
        self._cfg = cfg
        self._catalog = connect(
            catalog_uri=cfg.iceberg_catalog_uri,
            warehouse=cfg.iceberg_warehouse,
            s3_access_key_id=cfg.s3_access_key_id,
            s3_secret_access_key=cfg.s3_secret_access_key,
            s3_region=cfg.s3_region,
            catalog_token=cfg.iceberg_catalog_token,
            s3_endpoint=cfg.s3_endpoint,
        )
        self._namespace = cfg.iceberg_namespace
        self._table_name = cfg.iceberg_table
        self._location = cfg.iceberg_table_location
        self._tables_ensured: dict[str, Table] = {}
        self._schema_mgr = SchemaManager(self._catalog, self._namespace, self._table_name)

    def write(self, batch: pa.Table) -> None:
        write(
            self._catalog,
            self._namespace,
            self._table_name,
            batch,
            self._tables_ensured,
            self._schema_mgr,
            self._location,
        )

    def reset_caches(self) -> None:
        # Called by _write_with_retry after a failed write — drops cached
        # table state so the next attempt re-checks the catalog. Invariant:
        # main.py is the only caller; sink internals do not self-reset.
        self._tables_ensured.clear()
        self._schema_mgr.invalidate()

    def close(self) -> None:
        # PyIceberg REST catalog has no persistent connection to close.
        pass
