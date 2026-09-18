"""Hoglake backend: write Arrow batches to a hoglake catalog via pyhoglake.

The hoglake control plane is a *service* (Kotlin/Ktor + Postgres): clients
write parquet to object storage themselves and register the files via
footer-shipping commits — the server never opens data files on the write
path. pyhoglake owns that writer path (field-id-stamped parquet, footer
stat extraction, one-commit registration, partitioned fanout appends);
this module owns everything millpond-shaped around it:

* startup catalog resolution (in `__init__`, so a bad URL or an absent
  catalog fails before the pod claims readiness) and first-write
  bootstrap of namespace/table, concurrent-creation tolerant at every
  level;
* the partition spec and sort order: declared from millpond's config at
  table creation, VERIFIED afterwards, and reconciled against the live
  table on every later resolve — a config that disagrees with the table
  it writes to stops the pod rather than silently writing under a layout
  nobody declared;
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
import os
import tempfile
import uuid
from datetime import UTC, datetime

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyhoglake import (
    AlreadyExistsError,
    AlterOp,
    Column,
    CommitConflictError,
    ExpiredError,
    HoglakeClient,
    HoglakeError,
    IncarnationChangedError,
    MalformedResponseError,
    NotFoundError,
    S3Config,
    UnsupportedTypeError,
    ValidationError,
    ops,
    transforms,
)
from pyhoglake.types import arrow_type_to_coltype, column_to_arrow_field, columns_to_arrow_schema

from millpond import metrics
from millpond.config import Config
from millpond.sink import SAFE_IDENTIFIER, check_reserved_collision

log = logging.getLogger(__name__)

# Deliberately NARROWER than ducklake.RESERVED_COLUMNS, which also holds
# `year/month/day/hour`. Those names are load-bearing for DuckLake's
# Hive-style partitioning, which materializes a derived column per
# partition key. Hoglake partitions by Iceberg-semantics transforms
# recorded in the catalog: `month(_inserted_at)` produces a partition
# VALUE on the data file, never a `month` column. So for this backend
# those four are ordinary payload keys and reserving them would be
# fatal for no reason — `check_reserved_collision` raises, main.py's
# retry loop cannot make a ValueError go away, and one producer key
# named `month` wedges the partition forever. (The contrast that makes
# it indefensible: a WORSE-formed key, `month-of-year`, is merely
# dropped.) The asymmetry is safe for a deployment-time destination
# switch because it only runs one way: hoglake accepts every batch
# DuckLake accepted, plus four names DuckLake refused. See sink.py.
RESERVED_COLUMNS: frozenset[str] = frozenset({"_inserted_at"})

_INSERTED_AT = "_inserted_at"
_INSERTED_AT_TYPE = pa.timestamp("us", tz="UTC")

# The server reserves the `_hog` column-name prefix for its internals
# (`_hog_row_id` is compaction's row-id carrier); DDL and appends 422 on
# it, and pyhoglake fast-fails it client-side. A payload key with that
# prefix must be dropped, not sent — otherwise one poison producer key
# wedges the partition forever (append fails, offsets never advance).
_HOG_RESERVED_PREFIX = "_hog"

# The server's column-name rule is `^[A-Za-z_][A-Za-z0-9_-]{0,127}$` —
# 128 characters, total. millpond's shared SAFE_IDENTIFIER has no length
# bound (DuckDB has no such limit), so the cap is applied here, on the
# hoglake side only. Without it the two paths disagree: in steady state
# an over-long name degrades (its `add_column` 422s and the column is
# dropped for the flush), but BOOTSTRAP ships the whole schema in a
# single `create_table` — one 129-character key in the first batch 422s
# the create, so the table is never made, offsets never advance, and the
# pod crash-loops on the same batch forever.
_MAX_COLUMN_NAME_LEN = 128

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


# 4xx codes that mean "not now" rather than "not ever". Everything else
# in the 4xx range is a request the server will refuse identically
# forever. 429 is generic rate limiting; 408 is the server giving up on
# a slow request. Hoglake's own admission backpressure is a 503, which
# the 5xx rule below already covers.
_RETRYABLE_CLIENT_STATUS: frozenset[int] = frozenset({408, 429})

# Statuses whose `Retry-After` header is worth honoring: the server is
# saying "not now, try again in N seconds". 503 is hoglake's commit
# admission backpressure (`commit_queue_timeout`, `Retry-After: 1`).
_RETRY_AFTER_STATUS: frozenset[int] = frozenset({429, 503})

# Base backoff for this backend's retry ladder, matching main.py's
# DuckLake default. The doubling and its ceiling live in main.py; only
# the attempt count is configurable, because that is the part DuckLake's
# inner loop was silently providing.
_WRITE_BASE_DELAY_S = 1.0

# Namespace for the UUIDv5 commit idempotency keys. A fixed, private
# namespace means the key for a given (table, offset range) is stable
# across pods, restarts and releases — which is the only reason a retry
# from a NEW process can still be recognized as the same publication.
# Never change it: doing so re-randomizes every in-flight flush's
# identity and reopens the duplicate window for exactly one restart.
_IDEMPOTENCY_NAMESPACE = uuid.UUID("6f1b6d2e-4c5a-5f3e-9b7a-2d8c1e0a4f77")

# The server's 422 when a key is replayed with a payload that is not the
# one the receipt was written for: "idempotency_key reused with a
# different request". Matched as a substring because it is a message,
# not a code — but the condition it reports is unambiguous, and the
# alternative (treating it as a generic 422) crash-loops the pod forever
# on a flush whose rows are already in the lake.
_REUSED_KEY_MARKER = "idempotency_key reused"

# The refusals pyhoglake raises when a PREPARED file's columns do not
# match the destination's, quoted from its source so a re-align is
# attempted for the cases a re-align can actually fix:
#   * pyhoglake/client.py:901 — the strict schema/field-id comparison on
#     the ordinary (non-variant) prepare path, which is the one millpond
#     takes;
#   * pyhoglake/parquet_schema.py:173 — the same refusal on the variant
#     validation path.
# Deliberately NOT here: "prepared file partition arity differs from
# destination" (a spec change, which re-aligning columns cannot fix) and
# "data is missing table columns", which only `_align_table` raises —
# and this sink stopped calling `Table.append` when it moved to prepared
# commits, so matching it was matching a string that can no longer
# reach us.
_ALIGNMENT_REFUSALS: tuple[str, ...] = (
    "prepared Parquet schema/field IDs differ from destination",
    "prepared Parquet columns differ from destination",
)


class HoglakeSinkError(RuntimeError):
    """A refusal raised by THIS module, not by the server or the client.

    Spec reconciliation, the partition-column checks, the declaration
    post-condition and the prepared-payload invariants are millpond's own
    safety stops: they are decisions about the CONFIG and the code, and
    no amount of waiting changes either. They were previously plain
    `RuntimeError`s, which `is_retryable` classed as "unknown, assume
    transient" — so the loudest stops in the sink were also the slowest,
    burning the whole retry ladder before the operator saw the message.

    Subclasses `RuntimeError` so callers that catch the historical type
    (and the tests that assert on it) keep working. `retryable=True` is
    the one exception, for a stop a REBUILT flush really can clear — the
    partition spec changing under an already-prepared payload.
    """

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


def is_retryable(exc: BaseException) -> bool:
    """Classify a write-path failure: is a fresh attempt (after
    reset_caches) worth anything?

    WIRED IN, not advisory: `HoglakeSink.is_retryable` exposes this to
    `main._write_with_retry`, which skips the remaining budget and
    re-raises immediately when it answers False. The alternative —
    letting the loop burn its whole ladder on a 422 that cannot become
    valid by waiting — delays the crash the operator needs to see and
    buries the real error under repeated identical failures.

    Retryable — transient by nature:
      * `CommitConflictError` (OCC 409; the server says retry on a fresh
        baseline, and `retryable=True` on the class agrees)
      * `NotFoundError` (table/namespace dropped mid-run; the next
        attempt re-ensures it)
      * `IncarnationChangedError` — BOTH shapes. The server raises it as
        a 409 carrying the recreation marker; the client's own pre-flight
        re-resolve raises it with no status at all. Either way the table
        was dropped and recreated under the same name, reset_caches
        re-resolves the live incarnation, and millpond's contract is
        "write to the name" — so adopting the new incarnation on the NEXT
        attempt is correct, and the refused commit registered zero rows.
      * transport errors / timeouts (httpx), OSError (pyarrow S3 upload)
      * any HoglakeError with a 5xx status (503 `commit_queue_timeout` is
        the server's explicit "the catalog is convoyed, come back" —
        retrying the identical commit is what its own docs ask for)
      * 408 / 429 — the two 4xx codes that mean "not now"
      * a HoglakeError with NO status: the request never reached the
        server, so nothing about it has been judged
      * anything unknown (assume transient — crashing the pod after the
        retry budget is the loud fallback either way)

    Not retryable — the request itself is wrong and will stay wrong:
      * `HoglakeSinkError` — THIS sink's own safety stops (a spec that
        disagrees with config, a partition column that is not in the
        table, a declaration the server did not apply). They are
        statements about the config or the code, and a retry cannot
        change either. The one flagged `retryable=True` — the partition
        spec moving under a prepared payload — is the exception, because
        a REBUILT flush genuinely clears it.
      * `ValueError` / `KeyError` / `TypeError` — millpond's own
        validation (`check_reserved_collision`) and the pyarrow
        misuse that a schema race can produce. Same argument: waiting
        does not make a colliding column name stop colliding.
      * `ValidationError` (422), `UnsupportedTypeError`,
        `AlreadyExistsError` (409 on a create), `ExpiredError` (410 — a
        read_snapshot below the catalog's expiry floor; only a fresh plan
        fixes that), `MalformedResponseError` (wire-contract violation),
        and any other 4xx.
    """
    # This sink's own refusals, first: they carry their own verdict.
    if isinstance(exc, HoglakeSinkError):
        return exc.retryable
    # Retryable subclasses first — both are 409s, and the generic 4xx
    # rule below would otherwise swallow them.
    if isinstance(exc, CommitConflictError | IncarnationChangedError):
        return True
    if isinstance(
        exc,
        MalformedResponseError | ValidationError | UnsupportedTypeError | AlreadyExistsError | ExpiredError,
    ):
        return False
    if isinstance(exc, HoglakeError):
        status = exc.status_code
        if status is None:
            # Never reached the server (connection setup, body parse,
            # client-side guard): nothing was judged, so retry.
            return True
        if status in _RETRYABLE_CLIENT_STATUS:
            return True
        if 400 <= status < 500:
            # 404 is the exception: a dropped table is re-ensured on retry.
            return isinstance(exc, NotFoundError)
        return True
    if isinstance(exc, httpx.HTTPError | OSError):
        return True
    # OSError is checked above (pyarrow's S3 upload raises it), so these
    # are the in-process ones: a bad value, a missing key, a wrong type.
    # Nothing about waiting fixes any of them.
    if isinstance(exc, ValueError | KeyError | TypeError):
        return False
    return True


def _is_alignment_refusal(exc: BaseException) -> bool:
    """Is this a "the prepared file's columns are not the destination's"
    refusal, i.e. one a refresh-and-null-fill can actually clear?

    A KeyError always qualifies: the only thing in `_prepare` that raises
    one is the alignment's `select` reporting a name the batch lacks.
    """
    if isinstance(exc, KeyError):
        return True
    if not isinstance(exc, ValidationError):
        return False
    message = str(exc)
    return any(marker in message for marker in _ALIGNMENT_REFUSALS)


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
    the field. Three gates:
      * SAFE_IDENTIFIER — millpond's own generated-DDL posture;
      * the server's 128-character column-name cap, which
        SAFE_IDENTIFIER does not carry;
      * the server-reserved `_hog` prefix — a 422 at append time would
        otherwise wedge the partition on one poison key.
    """
    drop = []
    for name in batch.schema.names:
        if name.lower().startswith(_HOG_RESERVED_PREFIX):
            log.warning("Skipping hoglake-reserved field name: %r (the _hog prefix is server-reserved)", name)
        elif not SAFE_IDENTIFIER.match(name):
            log.warning("Skipping unsafe field name: %r", name)
        elif len(name) > _MAX_COLUMN_NAME_LEN:
            log.warning(
                "Skipping over-long field name (%d chars, hoglake allows %d): %r",
                len(name),
                _MAX_COLUMN_NAME_LEN,
                name,
            )
        else:
            continue
        metrics.records_skipped_total.labels(reason="unsafe_field_name").inc()
        drop.append(name)
    return batch.drop_columns(drop) if drop else batch


def _partition_groups(data: pa.Table, info) -> list[tuple[tuple[str | None, ...] | None, pa.Table]]:
    """Split an aligned batch by partition tuple under the table's live
    spec: one (wire-string tuple, sub-table) per distinct tuple.

    The prepared-commit path puts row-to-partition correctness on the
    caller — the server validates the STRUCTURE of what it is told (spec
    arity, key indexes) but never opens a data file at commit time, so a
    wrong value here mis-prunes reads of that file forever. The transform
    math is therefore pyhoglake's (`transforms.transform_strings`,
    Iceberg semantics, arrow-native where it can be); only the grouping
    is ours.

    Groups come out ordered by first occurrence in the batch, so file
    registration — and the server's rows-then-offset row-id assignment —
    follows input order, which is also the order MILLPOND_SORT_BY put the
    rows in. A null source value forms its own group, per Iceberg.
    """
    spec = info.partition_spec
    if spec is None or not spec.fields:
        return [(None, data)]
    columns = {c.field_id: c for c in info.columns}
    key_names = [f"__millpond_pk_{i}" for i in range(len(spec.fields))]
    key_arrays = []
    for field in spec.fields:
        column = columns.get(field.source_field_id)
        if column is None:
            raise HoglakeSinkError(
                f"partition spec of {info.namespace}.{info.name} references field_id "
                f"{field.source_field_id}, which is not a live column"
            )
        key_arrays.append(
            transforms.transform_strings(
                field.transform,
                field.transform_param,
                transforms.partition_source_array(data, [column]),
                column.type,
                column.type_params,
            )
        )
    keyed = pa.table(
        {
            **dict(zip(key_names, key_arrays, strict=True)),
            "__millpond_row": pa.array(range(data.num_rows), pa.int64()),
        }
    )
    combos = keyed.group_by(key_names).aggregate([("__millpond_row", "min")]).sort_by("__millpond_row_min")
    out: list[tuple[tuple[str | None, ...] | None, pa.Table]] = []
    for i in range(combos.num_rows):
        values = tuple(combos.column(k)[i].as_py() for k in key_names)
        mask = None
        for name, value in zip(key_names, values, strict=True):
            key_column = keyed.column(name)
            field_mask = pc.is_null(key_column) if value is None else pc.fill_null(pc.equal(key_column, value), False)
            mask = field_mask if mask is None else pc.and_(mask, field_mask)
        out.append((values, data.filter(mask)))
    return out


def _partition_tuples(spec) -> tuple[tuple[int, str, int | None], ...]:
    """A live PartitionSpec as comparable (source_field_id, transform,
    param) triples. An absent spec and an empty one are the same thing —
    the server retires a spec by setting an empty field list."""
    if spec is None or not spec.fields:
        return ()
    return tuple((f.source_field_id, f.transform, f.transform_param) for f in spec.fields)


def _sort_tuples(spec) -> tuple[tuple[int, str, str], ...]:
    """A live SortSpec as comparable (source_field_id, direction,
    null_order) triples."""
    if spec is None or not spec.fields:
        return ()
    return tuple((f.source_field_id, f.direction, f.null_order) for f in spec.fields)


def _describe_partition(spec, live_columns) -> str:
    """A live spec in HOGLAKE_PARTITION_BY's own grammar, so the operator
    can paste the fix straight into the values file."""
    names = {col.field_id: name for name, col in live_columns.items()}
    return ", ".join(
        f"{f.transform}({names.get(f.source_field_id, f'field_id={f.source_field_id}')}"
        f"{', ' + str(f.transform_param) if f.transform_param is not None else ''})"
        for f in (spec.fields if spec is not None else ())
    )


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
                raise HoglakeSinkError(f"HoglakeSink requires cfg.{name}; config.load() should have enforced this")
        self._cfg = cfg
        self._client = HoglakeClient(
            cfg.hoglake_url,
            s3=S3Config(
                access_key=cfg.hoglake_s3_access_key,
                secret_key=cfg.hoglake_s3_secret_key,
                endpoint_override=cfg.hoglake_s3_endpoint,
                region=cfg.hoglake_s3_region,
            ),
            # Otherwise pyhoglake's hardcoded 30s applies to every
            # request including the commit, and an operator with a
            # convoyed catalog has no knob at all.
            timeout=cfg.hoglake_request_timeout_s,
        )
        # Retry-After, captured off the raw response: pyhoglake maps
        # status codes to exception classes and discards headers, so the
        # server's own backoff advice (503 `commit_queue_timeout` answers
        # with `Retry-After: 1`) would be lost. Installed as an httpx
        # event hook — a private attribute of the client, so the whole
        # thing is best-effort: if pyhoglake restructures its transport
        # we silently fall back to the exponential curve rather than
        # failing to construct a sink.
        self._retry_after: float | None = None
        try:
            self._client._http.event_hooks["response"].append(self._note_response)
        except Exception:  # noqa: BLE001 - optional enhancement, never fatal
            log.debug("Could not install the Retry-After hook; backoff falls back to the exponential curve")
        # Commit author recorded on every snapshot — the pipeline
        # identity plus the pod ordinal, for multi-writer forensics.
        self._author = f"millpond/{cfg.table_label}/{cfg.ordinal}"
        # Resolved lazily on first write; reset_caches() drops them so the
        # retry path re-resolves (another pod may have created/altered the
        # table, or it may have been dropped+recreated).
        self._table = None
        self._live_columns: dict[str, Column] = {}
        # The in-flight flush's uploaded-and-not-yet-published commit
        # request, held IN MEMORY for the lifetime of the flush so a
        # retry replays it rather than building a second one. Survives
        # reset_caches(); cleared when the commit resolves.
        self._prepared: dict | None = None
        self._prepared_key: str | None = None
        self._prepared_rows: int = 0
        # STARTUP network validation. Everything else in this class is
        # lazy, and that is fine — but the catalog is the one thing whose
        # absence config.py and the README both describe as a "startup
        # error", and resolving it here is what makes that true. Before
        # this, a wrong HOGLAKE_URL, wrong S3-adjacent credentials or a
        # catalog nobody had created surfaced on the FIRST FLUSH: the pod
        # started, passed its probes, took its partitions, built lag, and
        # only then began crash-looping. One request at construction
        # turns all of that into a pod that never claims to be ready.
        self._catalog = self._resolve_catalog()

    # -- Sink protocol -----------------------------------------------------

    def write(self, batch: pa.Table, *, kafka_offsets: tuple[tuple[str, int, int], ...] | None = None) -> int:
        """Publish `batch` as ONE idempotent commit.

        `kafka_offsets` is the flush's identity — the (topic, partition,
        highest offset) triples main.py is about to commit — and it is
        what makes a retry a REPLAY instead of a second write. See
        `_flush_key`. Absent, the flush is anonymous and falls back to
        at-least-once (a lost response duplicates); main.py always
        supplies it, direct callers usually should not care.
        """
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
        if batch.num_rows == 0:
            # The Sink contract says this never happens (main.py gates on
            # pending_records > 0) — but a commit must register at least
            # one file with at least one row, so a zero-row batch would be
            # a 422 the retry loop could never clear.
            return 0

        key = self._flush_key(kafka_offsets)
        if self._prepared is not None and self._prepared_key == key:
            # A retry of a flush whose registration is already uploaded.
            # Replay it byte-identically; never rebuild it.
            return self._commit_prepared()

        batch = self._stamp_inserted_at(batch)
        table = self._ensure_table(batch.schema)
        batch = self._evolve_and_align(table, batch)
        try:
            payload = self._prepare(table, batch, key)
        except (ValidationError, KeyError) as e:
            # Concurrent-DDL race (found by the live integration suite):
            # another writer's add_column can land between this sink's
            # alignment and the pre-flight resolve, and the strict
            # alignment then refuses the batch for lacking the brand-new
            # column. Refresh, null-fill, and retry ONCE; a second
            # refusal is a real error.
            #
            # `_prepare` null-fills against its own freshly adopted
            # columns, so this is the SECOND line of defence, not the
            # first — it covers a column that appears between that
            # null-fill and pyhoglake's own pre-flight resolve one round
            # trip later. KeyError is caught with it because the
            # alignment's `select` reports a missing column that way, and
            # letting one out is worse than re-aligning once: KeyError is
            # not a pyhoglake type, so the retry loop reads it as
            # "unknown, assume transient" and spends the whole budget on
            # it.
            if not _is_alignment_refusal(e):
                raise
            self._adopt_columns(table.info().columns)
            batch = self._null_fill_missing(batch)
            payload = self._prepare(table, batch, key)
        self._prepared = payload
        self._prepared_key = key
        self._prepared_rows = batch.num_rows
        return self._commit_prepared()

    def reset_caches(self) -> None:
        """Drop the resolved table/schema handles so the next attempt
        re-resolves.

        The PREPARED PAYLOAD deliberately survives: it is the record of
        an upload that already happened, and the whole point of holding
        it is that the retry replays the same registration instead of
        minting new paths. It is cleared when its commit is resolved —
        published, or proven already published — and by close()."""
        self._table = None
        self._live_columns = {}

    def close(self) -> None:
        self._client.close()

    # -- retry policy (read by main._write_with_retry) ---------------------

    @staticmethod
    def is_retryable(exc: BaseException) -> bool:
        """See the module-level `is_retryable`. Exposed on the sink so
        the retry loop can consult it without main.py importing
        pyhoglake for a DuckLake-only deployment."""
        return is_retryable(exc)

    def write_retry_budget(self) -> tuple[int, float]:
        """(attempts, base backoff seconds) for this destination.

        DuckLake's three attempts were always the OUTER ring of a retry
        scheme whose inner loop runs `ducklake_max_retry_count` times
        (100 by default here). Hoglake has no inner loop: pyhoglake
        issues one request and raises. Three attempts against a catalog
        under commit-admission backpressure — which answers
        `Retry-After: 1` and expects to be asked again — is a crash
        loop dressed as a retry policy. HOGLAKE_MAX_RETRY_COUNT sets it;
        see config.py for how the default interacts with the liveness
        deadline."""
        return self._cfg.hoglake_max_retry_count, _WRITE_BASE_DELAY_S

    def _note_response(self, response) -> None:
        """httpx response hook: remember a Retry-After from a response
        that is telling us to back off. Deliberately narrow — only
        statuses that mean "not now" leave a hint, so a stray header on
        a 200 cannot slow the pipeline down."""
        if response.status_code not in _RETRY_AFTER_STATUS:
            return
        raw = response.headers.get("retry-after")
        if raw is None:
            return
        try:
            # Delta-seconds only. The HTTP-date form is legal but hoglake
            # never sends it, and guessing at clock skew to honor one
            # would be worse than falling back to our own curve.
            self._retry_after = float(raw.strip())
        except ValueError:
            log.debug("Ignoring non-numeric Retry-After %r", raw)

    def retry_after_hint(self) -> float | None:
        """The server's own backoff advice for the most recent refusal,
        consumed once. One-shot on purpose: a hint left over from a
        failure two flushes ago must not govern an unrelated retry."""
        hint, self._retry_after = self._retry_after, None
        return hint

    # -- idempotent publication --------------------------------------------

    def _flush_key(self, kafka_offsets) -> str:
        """The commit's idempotency key: a UUIDv5 over this table's
        identity and the Kafka offset range being flushed.

        DERIVED, not random, and that is the entire mechanism. A key is a
        name for "these rows, published to this table", so the retry of a
        flush whose response was lost carries the same name as the commit
        that may already have landed, and the server answers from its
        receipt instead of writing again.

        Why the offset range is a sound identity: a flush only happens
        with at least one new record buffered, so at least one partition's
        highest offset has advanced since the last one, and main.py
        clears the map after every flush. Two different flushes of the
        same pipeline therefore cannot produce the same triple set. The
        table identity is in the hash because receipts are scoped per
        CATALOG, not per table — two pipelines writing different tables in
        one catalog must not collide.

        Without offsets (a direct caller, not main.py) the key is random,
        which is honest: an anonymous batch has no identity to recognize
        it by on a retry, so it keeps the old at-least-once behaviour
        rather than pretending to more.
        """
        if not kafka_offsets:
            log.debug("Flush has no Kafka identity; commit falls back to at-least-once")
            return str(uuid.uuid4())
        cfg = self._cfg
        name = "\n".join(
            [
                f"{cfg.hoglake_catalog}/{cfg.hoglake_namespace}/{cfg.hoglake_table}",
                *(f"{topic}:{partition}:{offset}" for topic, partition, offset in sorted(kafka_offsets)),
            ]
        )
        return str(uuid.uuid5(_IDEMPOTENCY_NAMESPACE, name))

    def _prepare(self, table, batch: pa.Table, key: str) -> dict:
        """Write the batch's parquet, upload it, and return the commit
        request — WITHOUT publishing it.

        The split is what makes the retry safe: after this returns, the
        files exist in object storage and the request that registers them
        is a value we can hold and re-send verbatim. pyhoglake's
        `prepare_append_files` owns the upload (streamed from disk in
        chunks, so a 100MB flush never doubles in RAM the way an
        in-memory serialize does) and the registration's stats/footer
        conventions.

        Partition fanout is ours to compute because the prepared path
        puts row-to-partition correctness on the caller — transform math
        still comes from pyhoglake.transforms, so the Iceberg semantics
        have exactly one implementation.
        """
        info = table.info()
        self._adopt_columns(info.columns)
        target = columns_to_arrow_schema(info.columns)
        # Null-fill against THESE columns, not the ones the caller
        # aligned to. `_evolve_and_align` filled against the schema it
        # resolved a round trip ago; a concurrent writer's add_column
        # since then puts a name in `target` that the batch does not
        # carry, and `pa.Table.select` answers a missing name with a
        # KeyError — which is not a ValidationError, so the self-heal in
        # `write()` never saw it, and is not a pyhoglake type, so the
        # retry loop called it transient and burned the whole budget
        # before crashing the pod. Fill first, select second: the select
        # can then only ever narrow.
        batch = self._null_fill_missing(batch)
        aligned = batch.select(list(target.names)).cast(target)
        groups = _partition_groups(aligned, info)
        with tempfile.TemporaryDirectory(prefix="millpond-hoglake-") as tmp:
            files = []
            for index, (partition_values, part) in enumerate(groups):
                path = os.path.join(tmp, f"part-{index}.parquet")
                pq.write_table(part, path)
                files.append((path, partition_values))
            payload = table.prepare_append_files(files, idempotency_key=key)
        # Blind append, exactly as `Table.append` does it.
        # `prepare_append_files` pins `read_snapshot` to the catalog head
        # at prepare time, and the server's conflict scan then fails the
        # commit with a 409 if any DDL touched this table since — which
        # for millpond means "another pod added a column", the single
        # most likely thing to happen during a producer rollout. A
        # prepared payload cannot survive that: its read_snapshot is
        # frozen, so the conflict is permanent and the only way out is
        # re-uploading under a new registration. Appends never conflict
        # with appends, so dropping the field restores the semantics the
        # non-idempotent path always had, and the incarnation guard
        # (expected_table_uuid, which prepare_append_files puts on the
        # entry) remains the real safety mechanism.
        payload.pop("read_snapshot", None)
        payload["author"] = self._author
        return payload

    def _commit_prepared(self) -> int:
        """Publish the prepared request, or recognize that it is already
        published.

        Transport failures here are UNCERTAIN, never clean failures: a
        timeout or a reset means the commit may have applied and the
        answer was lost. The payload therefore stays cached and the
        exception propagates, so main.py's retry loop sends THE SAME
        request again — which the server resolves under the per-catalog
        commit lock: a receipt for this key returns the original result
        without writing, and no receipt means it really did not land.
        Either way the rows publish exactly once.
        """
        payload = self._prepared
        if payload is None:  # unreachable; an explicit raise, not an assert (python -O strips those)
            raise HoglakeSinkError("_commit_prepared called with no prepared payload")
        files = payload["appends"][0]["files"]
        try:
            self._catalog.commit_prepared(payload)
        except ValidationError as e:
            if _REUSED_KEY_MARKER not in f"{e}".lower():
                raise
            # The receipt exists and our request is not the one it was
            # written for. That can only mean this offset range was
            # already published under a DIFFERENT registration — the pod
            # crashed after the commit applied but before the offsets
            # committed, and the replay from Kafka rebuilt the flush.
            # The rows are in the lake; publishing the rebuilt copy would
            # duplicate them. Accept, and account for the upload we just
            # orphaned.
            log.warning(
                "Kafka offsets for this flush were already published to %s.%s under a different "
                "registration (idempotency key %s); treating the flush as done and orphaning %d "
                "uploaded file(s)",
                self._cfg.hoglake_namespace,
                self._cfg.hoglake_table,
                payload["idempotency_key"],
                len(files),
            )
            metrics.hoglake_commit_replays_total.labels(outcome="already_published").inc()
            metrics.hoglake_orphaned_files_total.inc(len(files))
            rows = self._prepared_rows
            self._clear_prepared()
            return rows
        # One parquet per partition tuple per flush (fanout appends) —
        # the hoglake compaction-debt feed rate.
        metrics.hoglake_files_written_total.inc(len(files))
        rows = self._prepared_rows
        self._clear_prepared()
        return rows

    def _clear_prepared(self) -> None:
        self._prepared = None
        self._prepared_key = None
        self._prepared_rows = 0

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

    def _resolve_catalog(self):
        """Resolve (or create) the catalog. Called from __init__, so every
        failure here is a startup failure with a message that says what to
        fix."""
        cfg = self._cfg
        try:
            return self._client.catalog(cfg.hoglake_catalog)
        except NotFoundError:
            if cfg.hoglake_data_path is None:
                raise HoglakeSinkError(
                    f"hoglake catalog {cfg.hoglake_catalog!r} does not exist and HOGLAKE_DATA_PATH "
                    f"is not set; create the catalog first or set HOGLAKE_DATA_PATH to let "
                    f"millpond create it"
                ) from None
            try:
                catalog = self._client.create_catalog(cfg.hoglake_catalog, cfg.hoglake_data_path)
                log.info("Created hoglake catalog %s (data_path=%s)", cfg.hoglake_catalog, cfg.hoglake_data_path)
                return catalog
            except AlreadyExistsError:
                # Another pod created it between our GET and our POST.
                return self._client.catalog(cfg.hoglake_catalog)
        except (HoglakeError, httpx.HTTPError, OSError) as e:
            # Bad URL, DNS, TLS, a control plane that is down, a proxy
            # answering HTML: all of it lands here, and all of it is a
            # deployment problem the operator can see from the message.
            raise HoglakeSinkError(
                f"cannot reach the hoglake control plane at {cfg.hoglake_url!r} to resolve "
                f"catalog {cfg.hoglake_catalog!r}: {e}"
            ) from e

    def _ensure_table(self, batch_schema: pa.Schema):
        """Resolve (or create) namespace -> table, tolerating concurrent
        creation by other pods at every level, and reconcile the live
        partition spec / sort order against config. Cached for the sink's
        lifetime; reset_caches() drops the cache.

        `self._table` is assigned LAST, deliberately: any failure in here
        — including a spec declaration the server refuses — must leave
        the cache empty so the next attempt re-checks instead of
        returning a table whose layout was never declared."""
        if self._table is not None:
            return self._table

        cfg = self._cfg
        catalog = self._catalog
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
            self._reconcile_specs(table)
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
            # Another pod won the race. The winner declares the specs —
            # but "the winner will do it" is exactly the assumption that
            # made the old create-then-alter window permanent, so the
            # loser verifies rather than trusting.
            log.info("Hoglake table %s created by another pod, continuing", cfg.hoglake_table)
            table = ns.table(cfg.hoglake_table)
            self._adopt_columns(table.columns)
            self._reconcile_specs(table)
            return table

        self._adopt_columns(table.columns)
        self._declare_specs(table)
        return table

    def _declare_specs(self, table) -> None:
        """Declare the configured partition spec + sort order in ONE
        alter, then VERIFY the result.

        Creation is two round trips and the gap between them is the
        hazard: the table exists before its layout does. Only a
        concurrent-DDL 409 is absorbed here (another pod declaring the
        same config-identical specs); anything else propagates, and
        because `_ensure_table` has not cached the table yet, the next
        attempt re-resolves, finds the table present and unspecced, and
        tries again — failing identically for as long as the config is
        wrong, which is the point. Silently writing to a table whose
        partitioning the operator asked for and never got is the failure
        mode this replaces."""
        spec_ops = self._spec_ops()
        if not spec_ops:
            return
        try:
            info = table.alter(spec_ops)
            log.info("Declared hoglake table specs: %s", ", ".join(op.op for op in spec_ops))
        except CommitConflictError as e:
            # Concurrent DDL (another pod racing the same specs). The
            # specs are config-identical across the fleet, so the winner
            # declared the same thing — which the verification below
            # proves rather than assumes.
            log.info("Hoglake spec DDL raced another writer, continuing: %s", e)
            info = table.info()
        self._adopt_columns(info.columns)
        self._verify_specs(info)

    def _reconcile_specs(self, table) -> None:
        """Compare the live partition spec / sort order of an EXISTING
        table against config.

        The spec was only ever declared at CREATE, so a deployed pipeline
        that changed HOGLAKE_PARTITION_BY silently kept the old layout,
        and a pod whose config had lost its partitioning (the
        MILLPOND_DESTINATION flip nulls the DuckLake partition var) wrote
        on as if nothing had happened. Config is the declaration of what
        this table's layout is; a pod that disagrees with the table it
        writes to stops, loudly, the same way MILLPOND_VARIANT_COLUMNS
        stops a hoglake pod at startup.

        The one divergence that is NOT an error is an unspecced table
        that config says should be specced: that is the create-then-alter
        window reopening, and declaring the spec is the recovery."""
        # No early out when both knobs are unset: "config says
        # unpartitioned, the table is partitioned" is the destination-flip
        # case, and it is the one this check exists for.
        info = table.info()
        self._adopt_columns(info.columns)
        want_partition = self._want_partition_fields()
        live_partition = _partition_tuples(info.partition_spec)
        if live_partition and live_partition != want_partition:
            raise HoglakeSinkError(
                f"live partition spec of {self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} "
                f"{_describe_partition(info.partition_spec, self._live_columns)} does not match "
                f"HOGLAKE_PARTITION_BY {self._describe_configured_partition()}. Hoglake never "
                f"re-specs a table behind your back and millpond will not write under a layout "
                f"nobody declared: set HOGLAKE_PARTITION_BY to the live spec, or point this "
                f"pipeline at a new table."
            )

        want_sort = self._want_sort_fields()
        live_sort = _sort_tuples(info.sort_spec)
        sort_known = self._cfg.sort_by is None or want_sort is not None
        if sort_known and live_sort and live_sort != (want_sort or ()):
            raise HoglakeSinkError(
                f"live sort order of {self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} does "
                f"not match MILLPOND_SORT_BY {self._cfg.sort_by!r}. The sort order is advisory for "
                f"writers but BINDING for hoglake compaction, so a mismatch means compaction "
                f"re-sorts every file this pod writes: set MILLPOND_SORT_BY to the live order, or "
                f"point this pipeline at a new table."
            )

        if want_partition and not live_partition:
            log.warning(
                "Hoglake table %s.%s exists with no partition spec but HOGLAKE_PARTITION_BY is set; "
                "declaring it now (a previous bootstrap created the table and failed before its "
                "spec landed)",
                self._cfg.hoglake_namespace,
                self._cfg.hoglake_table,
            )
            self._declare_specs(table)
        elif want_sort and not live_sort:
            log.warning(
                "Hoglake table %s.%s exists with no sort order but MILLPOND_SORT_BY is set; declaring it now",
                self._cfg.hoglake_namespace,
                self._cfg.hoglake_table,
            )
            self._declare_specs(table)

    def _verify_specs(self, info) -> None:
        """Post-condition on the declaration: the live layout IS what
        config asked for. A server that accepted the alter and applied
        something else, or a code path that skipped an op, both end here
        rather than in a silently mis-laid-out table."""
        want_partition = self._want_partition_fields()
        if want_partition and _partition_tuples(info.partition_spec) != want_partition:
            raise HoglakeSinkError(
                f"hoglake table {self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} does not "
                f"carry the partition spec that was just declared for it "
                f"(HOGLAKE_PARTITION_BY {self._describe_configured_partition()}); refusing to "
                f"write to an undeclared layout"
            )
        want_sort = self._want_sort_fields()
        if want_sort and _sort_tuples(info.sort_spec) != want_sort:
            raise HoglakeSinkError(
                f"hoglake table {self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} does not "
                f"carry the sort order that was just declared for it "
                f"(MILLPOND_SORT_BY {self._cfg.sort_by!r})"
            )

    def _want_partition_fields(self) -> tuple[tuple[int, str, int | None], ...]:
        """Configured partition spec as (source_field_id, transform,
        param) triples, resolved against the live columns."""
        cfg = self._cfg
        if not cfg.hoglake_partition_by:
            return ()
        fid = {name: col.field_id for name, col in self._live_columns.items()}
        out = []
        for column, transform, param in cfg.hoglake_partition_by:
            if column not in fid:
                raise HoglakeSinkError(
                    f"HOGLAKE_PARTITION_BY column {column!r} is not in the table schema "
                    f"(columns: {sorted(fid)}); partition columns must exist in the source "
                    f"events (or be _inserted_at)"
                )
            out.append((fid[column], transform, param))
        return tuple(out)

    def _want_sort_fields(self) -> tuple[tuple[int, str, str], ...] | None:
        """Configured sort order as (source_field_id, direction,
        null_order) triples, or None when a sort field is missing from the
        table schema — the pre-existing non-fatal degrade (the batch is
        still pre-sorted on the fields that ARE present)."""
        cfg = self._cfg
        if not cfg.sort_by:
            return ()
        fid = {name: col.field_id for name, col in self._live_columns.items()}
        missing = [c for c in cfg.sort_by if c not in fid]
        if missing:
            log.warning(
                "MILLPOND_SORT_BY field(s) %s missing from the hoglake table schema; "
                "skipping the sort-order declaration (batches are still pre-sorted on "
                "the fields present)",
                missing,
            )
            return None
        # asc + nulls_last mirrors main._apply_sort (ascending,
        # null_placement="at_end") so the declared order is the order
        # millpond actually writes.
        return tuple((fid[c], "asc", "nulls_last") for c in cfg.sort_by)

    def _describe_configured_partition(self) -> str:
        return ", ".join(
            f"{t}({c}{', ' + str(p) if p is not None else ''})" for c, t, p in (self._cfg.hoglake_partition_by or ())
        )

    def _spec_ops(self) -> list[AlterOp]:
        """Partition-spec + sort-order alter ops, built from the same
        resolved tuples the reconciliation and verification paths
        compare against — so what is declared, what is checked and what
        the error message names can never drift apart.

        Partition columns must exist (fatal — data layout is load-bearing
        and the operator asked for it); sort fields degrade non-fatally
        like main._apply_sort's missing-field skip (the sort spec is
        advisory for writers, binding only for compaction).
        """
        spec_ops: list[AlterOp] = []
        partition = self._want_partition_fields()
        if partition:
            spec_ops.append(ops.set_partition_spec([ops.partition_field(*f) for f in partition]))
        sort = self._want_sort_fields()
        if sort:
            spec_ops.append(
                AlterOp(
                    "set_sort_order",
                    {
                        "sort_fields": [
                            {"source_field_id": fid, "direction": direction, "null_order": null_order}
                            for fid, direction, null_order in sort
                        ]
                    },
                )
            )
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
        failed: list[str] = list(
            self._add_columns(table, [f for f in batch.schema if f.name not in self._live_columns])
        )
        for field in batch.schema:
            live = self._live_columns.get(field.name)
            if live is None:
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

        return self._null_fill_missing(batch)

    def _null_fill_missing(self, batch: pa.Table) -> pa.Table:
        """Null-fill live table columns absent from the batch (removed or
        renamed upstream, or added by another writer) — the equivalent of
        DuckLake's `INSERT BY NAME` filling unnamed columns with NULL."""
        for name, col in self._live_columns.items():
            if name not in batch.schema.names:
                arrow_field = column_to_arrow_field(col)
                batch = batch.append_column(
                    pa.field(name, arrow_field.type, nullable=True),
                    pa.nulls(batch.num_rows, type=arrow_field.type),
                )
        return batch

    def _add_columns(self, table, fields: list[pa.Field]) -> list[str]:
        """ADD every new column in ONE alter; return the names that are
        still not live afterwards (the caller drops those from the batch).

        `/alter` applies its op list in order, atomically, as a single
        DDL commit — one snapshot, one schema-version bump, one trip
        through the per-catalog commit lock. Issuing a commit per column
        multiplied that traffic by the width of the schema drift, and
        gave every column its own chance to lose the concurrent-DDL race
        against another pod doing the same thing.

        The batch is an optimization and never a semantics change: one
        unacceptable column fails the whole transaction (nothing
        applies), so a failure falls back to the per-column loop, where
        each column degrades on its own exactly as before.
        """
        if not fields:
            return []
        add_ops: list[AlterOp] = []
        addable: list[pa.Field] = []
        failed: list[str] = []
        for field in fields:
            try:
                type_name, _ = arrow_type_to_coltype(field.type)
            except UnsupportedTypeError:
                # Dropped before the alter is built: a column hoglake has
                # no type for must not take the other columns' DDL down
                # with it.
                log.warning(
                    "Column %r has no hoglake type mapping (%s); dropping it this flush", field.name, field.type
                )
                metrics.errors_total.labels(type="schema").inc()
                failed.append(field.name)
                continue
            log.info("Schema evolution: adding hoglake column %s (%s)", field.name, type_name)
            add_ops.append(ops.add_column(field.name, field.type))
            addable.append(field)

        if not add_ops:
            return failed
        if len(add_ops) > 1:
            try:
                info = table.alter(add_ops)
                self._adopt_columns(info.columns)
                metrics.schema_columns_added_total.inc(len(add_ops))
                return failed
            except HoglakeError as e:
                log.info("Batched add of %d column(s) failed (%s); retrying per column", len(add_ops), e)
        for field in addable:
            if not self._add_column(table, field):
                failed.append(field.name)
        return failed

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
