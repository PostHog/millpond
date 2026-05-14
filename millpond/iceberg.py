"""Iceberg write path.

``connect()`` returns a long-lived PyIceberg ``Catalog`` handle, ``write()``
appends an Arrow batch via a REST commit.

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
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform

log = logging.getLogger(__name__)

# Names of the derived partition columns appended to every batch before append.
# Order matters: it defines the on-disk directory nesting in the Hive layout.
PARTITION_COLS: tuple[str, ...] = ("year", "month", "day", "hour")

# Iceberg requires every PartitionField to have a stable field_id that's
# distinct from any column's field_id. We start the partition field IDs well
# above any plausible source-schema field count so they never collide with
# columns the converter assigns. Field IDs are catalog-wide so this matters.
_PARTITION_FIELD_ID_BASE = 1000


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
    }
    if catalog_token:
        props["token"] = catalog_token
    if s3_endpoint:
        props["s3.endpoint"] = s3_endpoint
    cat = load_catalog("lake", **props)
    log.info("Iceberg REST catalog loaded: uri=%s warehouse=%s", catalog_uri, warehouse)
    return cat


# Module-level cache. Same pattern as the old ducklake module: assumes single
# catalog handle for the pod lifetime. ``reset_table_cache`` is the escape
# hatch invoked on retry-after-error in case a concurrent writer changed
# the table out from under us.
_tables_ensured: dict[str, Table] = {}


def reset_table_cache() -> None:
    """Clear the table cache. Call when the catalog handle is recycled."""
    _tables_ensured.clear()


def _now_utc_us() -> datetime.datetime:
    """Microsecond-precision UTC now, matching the timestamp type Iceberg expects."""
    return datetime.datetime.now(datetime.UTC).replace(microsecond=0)


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
    """Empty Arrow table with the on-disk schema (source cols + metadata cols).

    Used at table-creation time to derive the Iceberg schema without
    needing actual rows. The metadata columns are added with the exact
    dtypes ``_add_metadata_columns`` produces so the resulting Iceberg
    schema matches what writes will look like.
    """
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
    location: str | None = None,
) -> Table:
    """Get-or-create the Iceberg table; cached after first call.

    Concurrent creation: if ``create_table`` raises (another pod won the
    race), we fall back to ``load_table``. The cache then holds the
    common winner. The first writer's schema wins for now — schema
    evolution from a concurrent newer-schema pod is handled by
    ``schema.SchemaManager`` on subsequent writes, not here.
    """
    key = f"{namespace}.{table_name}"
    if (cached := _tables_ensured.get(key)) is not None:
        return cached

    identifier = (namespace, table_name)
    try:
        table = catalog.load_table(identifier)
        log.info("Iceberg table %s.%s already exists, loaded", namespace, table_name)
    except Exception:
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
        try:
            catalog.create_namespace_if_not_exists(namespace)
        except AttributeError:
            # Pre-0.7 PyIceberg didn't expose create_namespace_if_not_exists;
            # tolerate either path so the module works against older REST
            # servers without forcing a PyIceberg pin.
            try:
                catalog.create_namespace(namespace)
            except Exception:
                pass
        create_kwargs: dict[str, object] = {"partition_spec": spec}
        if location:
            create_kwargs["location"] = location
        try:
            table = catalog.create_table(identifier, schema=iceberg_schema, **create_kwargs)
            log.info("Created Iceberg table %s.%s with partition spec %s", namespace, table_name, PARTITION_COLS)
        except Exception:
            table = catalog.load_table(identifier)
            log.info("Iceberg table %s.%s created by another writer, loaded", namespace, table_name)

    _tables_ensured[key] = table
    return table


def write(
    catalog: Catalog,
    namespace: str,
    table_name: str,
    source_batch: pa.Table,
    schema_mgr=None,
    location: str | None = None,
) -> None:
    """Append an Arrow batch to the Iceberg table.

    Adds ``_inserted_at`` plus the four partition columns derived from
    it, then calls ``Table.append(...)``. ``schema_mgr`` (when supplied)
    evolves the catalog schema to accommodate any new source columns in
    the batch.
    """
    if len(source_batch) == 0:
        return  # nothing to write; skip the commit round trip

    table = _ensure_table(catalog, namespace, table_name, source_batch, location)
    if schema_mgr is not None:
        schema_mgr.evolve(source_batch.schema)
        table = catalog.load_table((namespace, table_name))
        _tables_ensured[f"{namespace}.{table_name}"] = table

    batch_with_meta = _add_metadata_columns(source_batch)
    table.append(batch_with_meta)
