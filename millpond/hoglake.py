"""Hoglake backend: write Arrow batches to a hoglake catalog via pyhoglake.

The hoglake control plane is a *service* (Kotlin/Ktor + Postgres): clients
write parquet to object storage themselves and register the files via
footer-shipping commits — the server never opens data files on the write
path. pyhoglake owns that writer path (field-id-stamped parquet, footer
stat extraction, one-commit registration, partitioned fanout appends);
this module owns everything millpond-shaped around it:

* first-write bootstrap (catalog/namespace/table ensure, concurrent-
  creation tolerant), with the partition spec and sort order declared at
  table creation from millpond's config;
* the `_inserted_at` metadata column (stamped once per flush — unlike
  DuckLake's SQL `NOW()`, every row in a flush carries the same value);
* schema evolution mirroring schema.SchemaManager's semantics: new
  batch columns become `add_column` alters, `int -> long` /
  `float -> double` mismatches become `promote_column` alters, failures
  are logged + metricked + degraded per column, never fatal to the flush;
* batch/table alignment: pyhoglake's `_align_table` REFUSES both extra
  and missing columns, so columns the table doesn't have (failed adds)
  are dropped and table columns the batch lacks are null-filled before
  `append()` (the null-fill mirrors DuckLake's `INSERT BY NAME`).

Events land as TEXT — `properties` and friends stay JSON strings, exactly
as the arrow_converter produces them. VARIANT is explicitly out of scope
for this backend.

At-least-once: `write()` raises on any failure; main.py's retry loop
backs off, calls `reset_caches()` (which drops the resolved
catalog/namespace/table handles so the next attempt re-resolves), and
Kafka offsets commit only after `write()` returns. A commit refused by
the server (409) may orphan an uploaded parquet file — that is cleanup's
problem by hoglake design, never the catalog's, and never a duplicate
row.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import httpx
import pyarrow as pa
from pyhoglake import (
    AlreadyExistsError,
    AlterOp,
    Column,
    CommitConflictError,
    ExpiredError,
    HoglakeClient,
    HoglakeError,
    NotFoundError,
    S3Config,
    UnsupportedTypeError,
    ValidationError,
    ops,
)
from pyhoglake.types import arrow_type_to_coltype, column_to_arrow_field

from millpond import metrics
from millpond.config import Config
from millpond.sink import SAFE_IDENTIFIER, check_reserved_collision

log = logging.getLogger(__name__)

# Same set as ducklake.RESERVED_COLUMNS, deliberately: a deployment-time
# destination switch must not change collision behavior (see sink.py).
# Hoglake itself only writes `_inserted_at`.
RESERVED_COLUMNS: frozenset[str] = frozenset({"_inserted_at", "year", "month", "day", "hour"})

_INSERTED_AT = "_inserted_at"
_INSERTED_AT_TYPE = pa.timestamp("us", tz="UTC")

# The server reserves the `_hog` column-name prefix for its internals
# (`_hog_row_id` is compaction's row-id carrier); DDL and appends 422 on
# it, and pyhoglake fast-fails it client-side. A payload key with that
# prefix must be dropped, not sent — otherwise one poison producer key
# wedges the partition forever (append fails, offsets never advance).
_HOG_RESERVED_PREFIX = "_hog"

# Widenings mirroring SchemaManager's ALTER COLUMN path, restricted to
# the promotions the hoglake server accepts (its matrix follows
# DuckLake's documented table): int -> long, float -> double. The
# narrower int8/int16 rungs are NOT promoted — millpond never creates
# them, and a foreign table's narrow column failing the append-side cast
# is the loud outcome we want.
_PROMOTIONS: dict[tuple[str, str], str] = {
    ("int", "long"): "long",
    ("float", "double"): "double",
}


def is_retryable(exc: BaseException) -> bool:
    """Classify a write-path failure: is a fresh attempt (after
    reset_caches) worth anything?

    Retryable — transient by nature:
      * `CommitConflictError` (OCC 409; the server says retry on a fresh
        baseline, and `retryable=True` on the class agrees)
      * `NotFoundError` (table/namespace dropped mid-run; the next
        attempt re-ensures it)
      * `IncarnationChangedError` (drop+recreate under the same name;
        reset_caches re-resolves the live incarnation — millpond's
        contract is "write to the name", so adopting the new incarnation
        on the NEXT attempt is correct; pyhoglake guarantees zero rows
        landed on the refused commit)
      * transport errors / timeouts (httpx), OSError (pyarrow S3 upload)
      * any HoglakeError with a 5xx status (503 = commit admission
        backpressure with Retry-After)
      * anything unknown (assume transient — crashing the pod after the
        retry budget is the loud fallback either way)

    Not retryable — the request itself is wrong and will stay wrong:
      * `ValidationError` (422), `UnsupportedTypeError`,
        `AlreadyExistsError` (409 on a create), `ExpiredError` (410),
        `MalformedResponseError` (wire-contract violation).

    NB: main.py's `_write_with_retry` retries EVERYTHING up to its
    budget by design (the seam contract — semantics preserved from the
    DuckLake-only era); this classifier drives metric labeling and
    operator triage, not the retry decision. A permanent 4xx burns three
    quick attempts, then crash-loops the pod — the loud failure mode.
    """
    # Non-retryable HoglakeError subclasses first; CommitConflictError is
    # the one subclass that declares itself retryable.
    if isinstance(exc, CommitConflictError):
        return True
    if isinstance(exc, ValidationError | UnsupportedTypeError | AlreadyExistsError | ExpiredError):
        return False
    if isinstance(exc, HoglakeError):
        status = getattr(exc, "status_code", None)
        if status is not None and 400 <= status < 500:
            # 404 is the exception: a dropped table is re-ensured on retry.
            return isinstance(exc, NotFoundError)
        if type(exc).__name__ == "MalformedResponseError":
            return False
        # NotFoundError / IncarnationChangedError without a status, 5xx,
        # and the base class with no status (client-side failures).
        return True
    if isinstance(exc, httpx.HTTPError | OSError):
        return True
    return True


def table_schema_for_batch(batch_schema: pa.Schema) -> pa.Schema:
    """The schema a fresh events table is created with: the batch's own
    columns (as the arrow_converter produced them — TEXT for
    properties/JSON, int64/float64/bool/string/timestamptz otherwise)
    plus the `_inserted_at` timestamptz metadata column."""
    if _INSERTED_AT in batch_schema.names:
        return batch_schema
    return batch_schema.append(pa.field(_INSERTED_AT, _INSERTED_AT_TYPE, nullable=True))


def _drop_unwritable_columns(batch: pa.Table) -> pa.Table:
    """Drop columns whose names the hoglake DDL surface would refuse.

    Mirrors SchemaManager.evolve's unsafe-field-name skip (same metric
    reason, one bump per column per flush): records still land, minus
    the field. Two gates:
      * SAFE_IDENTIFIER — millpond's own generated-DDL posture;
      * the server-reserved `_hog` prefix — a 422 at append time would
        otherwise wedge the partition on one poison key.
    """
    drop = []
    for name in batch.schema.names:
        if name.lower().startswith(_HOG_RESERVED_PREFIX):
            log.warning("Skipping hoglake-reserved field name: %r (the _hog prefix is server-reserved)", name)
        elif not SAFE_IDENTIFIER.match(name):
            log.warning("Skipping unsafe field name: %r", name)
        else:
            continue
        metrics.records_skipped_total.labels(reason="unsafe_field_name").inc()
        drop.append(name)
    return batch.drop_columns(drop) if drop else batch


class HoglakeSink:
    """The hoglake sink: owns the pyhoglake client, the resolved
    catalog/namespace/table handles, and the live-schema cache.

    main.py only calls `write()`, `reset_caches()`, and `close()`.
    `write()` must not be called with a zero-row batch (main.py gates on
    `pending_records > 0`); `reset_caches()` is invoked only by main.py's
    write-retry loop after a failure; `close()` is called exactly once at
    pod shutdown.
    """

    def __init__(self, cfg: Config):
        # Explicit guards rather than assert: `python -O` strips asserts
        # and would surface as a cryptic httpx/pyarrow failure instead of
        # a clear startup error. config.load() enforces these already;
        # this protects callers that bypass it.
        for name in (
            "hoglake_url",
            "hoglake_catalog",
            "hoglake_namespace",
            "hoglake_table",
            "hoglake_s3_access_key",
            "hoglake_s3_secret_key",
        ):
            if getattr(cfg, name) is None:
                raise RuntimeError(f"HoglakeSink requires cfg.{name}; config.load() should have enforced this")
        self._cfg = cfg
        self._client = HoglakeClient(
            cfg.hoglake_url,
            s3=S3Config(
                access_key=cfg.hoglake_s3_access_key,
                secret_key=cfg.hoglake_s3_secret_key,
                endpoint_override=cfg.hoglake_s3_endpoint,
                region=cfg.hoglake_s3_region,
            ),
        )
        # Commit author recorded on every snapshot — the pipeline
        # identity plus the pod ordinal, for multi-writer forensics.
        self._author = f"millpond/{cfg.table_label}/{cfg.ordinal}"
        # Resolved lazily on first write; reset_caches() drops them so the
        # retry path re-resolves (another pod may have created/altered the
        # table, or it may have been dropped+recreated).
        self._table = None
        self._live_columns: dict[str, Column] = {}

    # -- Sink protocol -----------------------------------------------------

    def write(self, batch: pa.Table) -> int:
        check_reserved_collision(batch.schema, RESERVED_COLUMNS, "Hoglake")
        had_columns = batch.num_columns > 0
        batch = _drop_unwritable_columns(batch)
        if had_columns and batch.num_columns == 0:
            # Every field was unwritable (poison producer). These records
            # ARE lost; skip the flush rather than crash-looping the
            # partition on the same batch forever.
            log.warning(
                "Skipping batch of %d record(s): every column had an unwritable name",
                batch.num_rows,
            )
            metrics.records_skipped_total.labels(reason="unsafe_field_name").inc(batch.num_rows)
            return 0
        batch = self._stamp_inserted_at(batch)
        table = self._ensure_table(batch.schema)
        batch = self._evolve_and_align(table, batch)
        result = table.append(batch, author=self._author)
        # One parquet per partition tuple per flush (fanout appends) —
        # the hoglake compaction-debt feed rate.
        metrics.hoglake_files_written_total.inc(len(result.files))
        return batch.num_rows

    def reset_caches(self) -> None:
        self._table = None
        self._live_columns = {}

    def close(self) -> None:
        self._client.close()

    # -- metadata column ---------------------------------------------------

    @staticmethod
    def _stamp_inserted_at(batch: pa.Table) -> pa.Table:
        """Append `_inserted_at` (timestamptz, micros, UTC), one value for
        the whole flush. DuckLake's SQL `NOW()` allows microsecond drift
        within a flush; here every row of a flush shares the timestamp —
        strictly more useful for flush forensics, and partition
        transforms bucket identically."""
        now = datetime.now(UTC)
        col = pa.repeat(pa.scalar(now, type=_INSERTED_AT_TYPE), batch.num_rows)
        return batch.append_column(pa.field(_INSERTED_AT, _INSERTED_AT_TYPE, nullable=True), col)

    # -- bootstrap ---------------------------------------------------------

    def _ensure_table(self, batch_schema: pa.Schema):
        """Resolve (or create) catalog -> namespace -> table, tolerating
        concurrent creation by other pods at every level. Cached for the
        sink's lifetime; reset_caches() drops the cache."""
        if self._table is not None:
            return self._table

        cfg = self._cfg
        try:
            catalog = self._client.catalog(cfg.hoglake_catalog)
        except NotFoundError:
            if cfg.hoglake_data_path is None:
                raise RuntimeError(
                    f"hoglake catalog {cfg.hoglake_catalog!r} does not exist and HOGLAKE_DATA_PATH "
                    f"is not set; create the catalog first or set HOGLAKE_DATA_PATH to let "
                    f"millpond create it"
                ) from None
            try:
                catalog = self._client.create_catalog(cfg.hoglake_catalog, cfg.hoglake_data_path)
                log.info("Created hoglake catalog %s (data_path=%s)", cfg.hoglake_catalog, cfg.hoglake_data_path)
            except AlreadyExistsError:
                catalog = self._client.catalog(cfg.hoglake_catalog)

        try:
            ns = catalog.namespace(cfg.hoglake_namespace)
        except NotFoundError:
            try:
                ns = catalog.create_namespace(cfg.hoglake_namespace)
                log.info("Created hoglake namespace %s", cfg.hoglake_namespace)
            except AlreadyExistsError:
                ns = catalog.namespace(cfg.hoglake_namespace)

        try:
            table = ns.table(cfg.hoglake_table)
            log.info("Hoglake table %s.%s already exists", cfg.hoglake_namespace, cfg.hoglake_table)
            self._adopt_columns(table.columns)
        except NotFoundError:
            table = self._create_table(ns, batch_schema)

        self._table = table
        return table

    def _create_table(self, ns, batch_schema: pa.Schema):
        """Create the events table from the (sanitized, stamped) batch
        schema, then declare the partition spec and sort order in one
        alter. Concurrent creation loses cleanly to the winner."""
        cfg = self._cfg
        schema = table_schema_for_batch(batch_schema)
        try:
            table = ns.create_table(cfg.hoglake_table, schema)
            log.info(
                "Created hoglake table %s.%s with %d columns",
                cfg.hoglake_namespace,
                cfg.hoglake_table,
                len(schema),
            )
        except AlreadyExistsError:
            # Another pod won the race; the winner declares the specs.
            log.info("Hoglake table %s created by another pod, continuing", cfg.hoglake_table)
            table = ns.table(cfg.hoglake_table)
            self._adopt_columns(table.columns)
            return table

        self._adopt_columns(table.columns)
        spec_ops = self._spec_ops()
        if spec_ops:
            try:
                info = table.alter(spec_ops)
                self._adopt_columns(info.columns)
                log.info("Declared hoglake table specs: %s", ", ".join(op.op for op in spec_ops))
            except CommitConflictError as e:
                # Concurrent DDL (another pod racing the same specs).
                # The specs are config-identical across the fleet, so the
                # winner declared the same thing; refresh and continue.
                log.info("Hoglake spec DDL raced another writer, continuing: %s", e)
                self._adopt_columns(table.info().columns)
        return table

    def _spec_ops(self) -> list[AlterOp]:
        """Partition-spec + sort-order alter ops with field ids resolved
        against the live columns. Partition columns must exist (fatal —
        data layout is load-bearing and the operator asked for it); sort
        fields degrade non-fatally like main._apply_sort's missing-field
        skip (the sort spec is advisory for writers, binding only for
        compaction)."""
        cfg = self._cfg
        fid = {name: col.field_id for name, col in self._live_columns.items()}
        spec_ops: list[AlterOp] = []

        if cfg.hoglake_partition_by:
            fields = []
            for column, transform, param in cfg.hoglake_partition_by:
                if column not in fid:
                    raise RuntimeError(
                        f"HOGLAKE_PARTITION_BY column {column!r} is not in the table schema "
                        f"(columns: {sorted(fid)}); partition columns must exist in the source "
                        f"events (or be _inserted_at)"
                    )
                fields.append(ops.partition_field(fid[column], transform, param))
            spec_ops.append(ops.set_partition_spec(fields))

        if cfg.sort_by:
            missing = [c for c in cfg.sort_by if c not in fid]
            if missing:
                log.warning(
                    "MILLPOND_SORT_BY field(s) %s missing from the hoglake table schema; "
                    "skipping the sort-order declaration (batches are still pre-sorted on "
                    "the fields present)",
                    missing,
                )
            else:
                sort_fields = [
                    # asc + nulls_last mirrors main._apply_sort (ascending,
                    # null_placement="at_end") so the declared order is the
                    # order millpond actually writes.
                    {"source_field_id": fid[c], "direction": "asc", "null_order": "nulls_last"}
                    for c in cfg.sort_by
                ]
                spec_ops.append(AlterOp("set_sort_order", {"sort_fields": sort_fields}))

        return spec_ops

    # -- schema evolution + alignment -------------------------------------

    def _adopt_columns(self, columns) -> None:
        self._live_columns = {c.name: c for c in columns}

    def _evolve_and_align(self, table, batch: pa.Table) -> pa.Table:
        """Reconcile the batch schema with the live table schema.

        Mirrors SchemaManager.evolve: one alter per column, per-column
        failures are logged + `errors_total{type="schema"}` + degraded —
        never fatal to the flush. Then align for pyhoglake's strict
        `_align_table`: drop batch columns the table refused, null-fill
        table columns the batch lacks (DuckLake's `INSERT BY NAME`
        equivalent). Name matching is EXACT — hoglake identifiers are
        case-sensitive, unlike DuckDB's case-insensitive resolution.
        """
        failed: list[str] = []
        for field in batch.schema:
            live = self._live_columns.get(field.name)
            if live is None:
                if not self._add_column(table, field):
                    failed.append(field.name)
                continue
            try:
                want, _params = arrow_type_to_coltype(field.type)
            except UnsupportedTypeError:
                log.warning(
                    "Column %r has no hoglake type mapping (%s); dropping it this flush", field.name, field.type
                )
                metrics.errors_total.labels(type="schema").inc()
                failed.append(field.name)
                continue
            if live.type != want:
                promoted = _PROMOTIONS.get((live.type, want))
                if promoted is not None:
                    self._promote_column(table, field.name, live.type, promoted)
                # Else: leave the column; append()'s cast to the live type
                # decides (all-null wobble casts cleanly; genuine type
                # garbage fails the flush loudly — same posture as
                # DuckLake's INSERT-side cast).

        if failed:
            batch = batch.drop_columns(failed)

        # Null-fill table columns absent from the batch (removed/renamed
        # upstream, or added by another writer).
        for name, col in self._live_columns.items():
            if name not in batch.schema.names:
                arrow_field = column_to_arrow_field(col)
                batch = batch.append_column(
                    pa.field(name, arrow_field.type, nullable=True),
                    pa.nulls(batch.num_rows, type=arrow_field.type),
                )
        return batch

    def _add_column(self, table, field: pa.Field) -> bool:
        """ADD a new column; True when the column is live afterwards."""
        try:
            type_name, _ = arrow_type_to_coltype(field.type)
        except UnsupportedTypeError:
            log.warning("Column %r has no hoglake type mapping (%s); dropping it this flush", field.name, field.type)
            metrics.errors_total.labels(type="schema").inc()
            return False
        log.info("Schema evolution: adding hoglake column %s (%s)", field.name, type_name)
        try:
            info = table.alter([ops.add_column(field.name, field.type)])
            self._adopt_columns(info.columns)
            metrics.schema_columns_added_total.inc()
            return True
        except CommitConflictError:
            # Concurrent DDL — typically another pod adding the same
            # column. Re-resolve; present means someone won the race.
            self._adopt_columns(table.info().columns)
            if field.name in self._live_columns:
                log.info("Column %s added by another writer, continuing", field.name)
                return True
            log.warning("Failed to add column %s: concurrent DDL conflict", field.name)
            metrics.errors_total.labels(type="schema").inc()
            return False
        except HoglakeError as e:
            log.warning("Failed to add column %s: %s", field.name, e)
            metrics.errors_total.labels(type="schema").inc()
            return False

    def _promote_column(self, table, name: str, from_type: str, to_type: str) -> None:
        """Widen a live column (int->long / float->double); failures are
        non-fatal — the column stays at its live type and the append-side
        cast decides per value."""
        log.info("Schema evolution: promoting hoglake column %s from %s to %s", name, from_type, to_type)
        try:
            info = table.alter([ops.promote_column(name, to_type)])
            self._adopt_columns(info.columns)
            metrics.schema_columns_widened_total.inc()
        except CommitConflictError:
            self._adopt_columns(table.info().columns)
            live = self._live_columns.get(name)
            if live is not None and live.type == to_type:
                log.info("Column %s promoted by another writer, continuing", name)
                metrics.schema_columns_widened_total.inc()
            else:
                log.warning("Failed to promote column %s to %s: concurrent DDL conflict", name, to_type)
                metrics.errors_total.labels(type="schema").inc()
        except HoglakeError as e:
            log.warning("Failed to promote column %s from %s to %s: %s", name, from_type, to_type, e)
            metrics.errors_total.labels(type="schema").inc()
