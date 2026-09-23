import logging
import random
import signal
import sys
import time
from importlib.metadata import PackageNotFoundError, version

import pyarrow as pa
import pyarrow.compute as pc
from confluent_kafka import TopicPartition

from millpond import (
    arrow_converter,
    backpressure,
    config,
    consumer,
    include_values,
    logging_config,
    metrics,
    server,
)
from millpond import sink as sink_mod

log = logging.getLogger(__name__)

_LAG_SAMPLE_INTERVAL_S = 60.0  # how often to query watermark offsets for lag metrics
_HEARTBEAT_INTERVAL_S = 60.0  # periodic log when idle (well under 480s liveness timeout)
# Longest a single consume() may block. record_poll() runs only after consume
# returns, and server.health marks the process dead at max_poll_age_s=480 —
# so a consume timeout derived from a large FLUSH_INTERVAL_MS (e.g. 10min)
# would starve the liveness probe on a quiet topic and SIGKILL the pod.
# 60s also keeps the idle heartbeat cadence honest.
_CONSUME_MAX_BLOCK_S = 60.0


def _consume_timeout(remaining: float) -> float:
    """Consume timeout for the main loop: the time left until the flush
    interval fires, floored at 0.1s so we always poll, capped at
    _CONSUME_MAX_BLOCK_S so liveness (record_poll) and the idle heartbeat
    keep running on a quiet topic. Flush triggers are re-checked every
    loop iteration, so the cap never delays a flush."""
    return min(max(remaining, 0.1), _CONSUME_MAX_BLOCK_S)


_WRITE_MAX_RETRIES = 3
_WRITE_BASE_DELAY_S = 1.0
_COMMIT_MAX_RETRIES = 3
_COMMIT_BASE_DELAY_S = 0.5
# Ceiling on a server-supplied Retry-After. The consume loop is single
# threaded, so a backoff is also a poll gap: server.health marks the
# process dead at max_poll_age_s=480, and record_poll only runs between
# consume() calls. 30s keeps the whole retry ladder well inside that.
_RETRY_AFTER_MAX_S = 30.0
# Upward spread on every backoff step, as a fraction of the step. A
# fleet refused by one 503 otherwise wakes in lockstep and re-forms the
# convoy it was backing off from, on every rung of the ladder. Applied
# upward only, so a server-supplied Retry-After stays a floor.
_RETRY_JITTER = 0.25

# Module-level set tracking which "missing sort fields" patterns we've
# already warned about. Without this, a misconfigured sort against a
# high-volume topic would log once per flush — at production cadence,
# tens of thousands of log lines / hour. The key is the comma-joined
# missing-fields tuple, so distinct misconfigurations still each get
# one warning. Pod restart resets the set (the lifetime tied to the
# process is intentional — operators get a fresh warning on each
# restart, which signals a likely persistent misconfiguration).
_sort_missing_fields_warned: set[str] = set()
# Same dedup, for sort keys whose Arrow type has no usable sort kernel.
_sort_unsortable_warned: set[str] = set()
# Same dedup again, for a filter field pinned `uuid` whose configured values
# are not all UUIDs. Static values are refused at config load, so this only
# fires for a dynamic include-values poll — and the poll keeps serving the
# same bad value, so the condition is permanent and the log would be one line
# per flush carrying the whole values tuple.
#
# Keyed by (field, value count, first bad value) and deliberately NOT by the
# values themselves: an authoritative include-values source rebuilds its list
# on every membership change, so a set keyed on the tuple would retain a full
# copy of every historical set with nothing to evict it — 100 changes at 10k
# team ids is ~80 MiB of dead strings in a 512Mi pod. The count and the first
# offender are what distinguish one report from the next; a new bad value, or
# a changed list size, still gets its own line.
_uuid_filter_values_warned: set[tuple[str, int, str]] = set()

# One-entry memo for the parsed uuid filter values. `_apply_filter` runs per
# CONSUMED batch, so re-parsing the include set each time is per-batch work
# proportional to the set size (~2.3 ms at 10k values) for an input that
# changes at most every few minutes. The include-values source swaps an
# immutable tuple by attribute assignment and static config holds one forever,
# so the common case is the SAME object every call and the identity check in
# `_parse_uuid_filter_values` settles it without walking either tuple.
_uuid_filter_values_memo: tuple | None = None


def _convert_batch(values: list[bytes]) -> pa.Table | None:
    """Convert raw message values to Arrow, timing the conversion."""
    t0 = time.monotonic()
    table = arrow_converter.convert(values)
    if table is not None:
        duration = time.monotonic() - t0
        metrics.arrow_conversion_seconds.observe(duration)
    return table


def _coerce_columns(table: pa.Table, cfg: config.Config) -> pa.Table:
    """Pin `cfg.typed_columns` to their target types after JSON→Arrow conversion.

    Returns the input unchanged when no typed columns are configured. Applied
    right after conversion and before the keep-filter, so the typed columns flow
    through filtering, sorting, table creation, and schema evolution. See
    arrow_converter.coerce_typed_columns for why this is necessary when writing
    into a table with already-typed columns (TIMESTAMPTZ, BIGINT, …).

    The `uuid` target is the one whose Arrow type is an EXTENSION type
    (`pa.uuid()`), and pyarrow's compute kernels do not accept one: both
    downstream stages therefore work on its STORAGE array rather than the
    column — `_apply_filter`/`_apply_drop_filter` compare the 16 raw bytes,
    `_apply_sort` sorts them (see `_sortable_key_projection`). "Flows through"
    is true of a uuid-pinned column only because of those two allowances; it
    is not automatic.
    """
    if cfg.typed_columns is None:
        return table
    return arrow_converter.coerce_typed_columns(table, cfg.typed_columns)


def _apply_filter(table: pa.Table, cfg: config.Config, values: tuple[int, ...] | tuple[str, ...] | None) -> pa.Table:
    """Apply the configured keep-filter: drop records whose value in
    `filter_keep_field` is not in `values` (the CURRENT include set from
    the configured IncludeValuesSource — static or polled; passing it
    explicitly per batch is what makes the dynamic source take effect
    without any state on the config object).

    Two reason labels on `records_skipped_total`:
      - `filter_field_missing`: the configured field is absent from this
        batch's schema, null for that row, or the column's Arrow type is
        incompatible with the configured value type (i.e. the column
        exists but the allowlist can't be applied against it — a schema
        anomaly worth tracking distinctly).
      - `filter_excluded`: column present and non-null but value not in
        the configured allowlist. Expected steady-state drop reason.

    Cast direction matters: we cast the (small) value array to the
    column's type, not the (potentially large) column to a fixed type.
    Three things fall out of that:
      1. Schema drift across batches (one batch's `team_id` is `int64`,
         the next is `string` because all values were null upstream) is
         handled by construction — each call evaluates the cast against
         the live column type, not against a type chosen at config load.
      2. The hot path makes one pass over the column (the `table.filter`
         at the end). No `pc.cast(column, ...)`, no `pc.is_null(column)` +
         `pc.invert` + `pc.and_kleene` triple-pass — `pc.is_in` already
         returns null for null inputs, and `column.null_count` is O(1).
      3. A column type that can't accept the configured values at all
         (struct, list, types narrower than the values) raises at the
         single cast site, where we catch it instead of letting it kill
         the consume loop.

    No-op when filter not configured. The drop-direction filter is
    implemented separately in _apply_drop_filter, applied AFTER this one
    (keep ∩ ¬drop).
    """
    if cfg.filter_keep_field is None or values is None:
        return table

    field = cfg.filter_keep_field
    if field not in table.column_names:
        metrics.records_skipped_total.labels(reason="filter_field_missing").inc(len(table))
        return table.slice(0, 0)

    column = table[field]

    # Column-type allowlist. PyArrow's `safe=True` cast happily coerces
    # ints to bool, float, timestamp, date, etc. — every one of which
    # silently produces a semantically-wrong match. Restrict the filter
    # to integer and string columns explicitly so any other column type
    # surfaces as `filter_field_missing` rather than a quiet, wrong match.
    # Bool, float, timestamp, date, decimal, struct, list, map, etc. all
    # land here.
    if column.type == arrow_converter.UUID_TYPE:
        # A uuid-pinned column is an extension type, which the allowlist below
        # would refuse — and refusing an ALLOWLIST field drops the whole batch,
        # silently, every flush. Compare on the storage bytes instead: the
        # filter values are UUID text, `uuid_bytes` turns them into the same 16
        # big-endian bytes the column holds, and `is_in` over the storage array
        # is the identical membership test. Config load refuses a non-UUID
        # filter value for a uuid-pinned field, so a bad value here came from a
        # DYNAMIC include-values poll; it cannot match anything under any
        # encoding, so it is dropped from the allowlist rather than used to
        # condemn the batch — the good values still admit their rows.
        value_array, invalid = _parse_uuid_filter_values(values)
        if invalid:
            _warn_bad_uuid_filter_values(field, values, invalid, "they cannot match and are ignored")
            metrics.errors_total.labels(type="filter_value_invalid").inc()
        if len(value_array) == 0:
            # Nothing on the allowlist is a UUID, so nothing can match. Fail
            # CLOSED, as the allowlist always does — but under its OWN reason,
            # not `filter_field_missing`, which is about the batch's schema and
            # would make a bad include set indistinguishable from a renamed
            # column on the dashboards.
            metrics.records_skipped_total.labels(reason="filter_value_invalid").inc(len(table))
            return table.slice(0, 0)
        return _finish_keep_filter(table, field, _uuid_storage(column), value_array, label=arrow_converter.uuid_text)

    if not (
        pa.types.is_integer(column.type) or pa.types.is_string(column.type) or pa.types.is_large_string(column.type)
    ):
        log.warning(
            "Filter field %r has unsupported type %s; supported types are integer and string. "
            "Treating batch as field-missing.",
            field,
            column.type,
        )
        metrics.records_skipped_total.labels(reason="filter_field_missing").inc(len(table))
        return table.slice(0, 0)

    try:
        # Default safe=True: any overflow or non-coercible source value
        # raises rather than silently producing nulls. Catching here keeps
        # a config-vs-schema mismatch (e.g. int values exceeding an int32
        # column's range, or non-numeric strings against an int column)
        # from crashing the pod.
        value_array = pa.array(values).cast(column.type)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as e:
        log.warning(
            "Filter values %r incompatible with column %r type %s (%s); treating batch as field-missing",
            values,
            field,
            column.type,
            e,
        )
        metrics.records_skipped_total.labels(reason="filter_field_missing").inc(len(table))
        return table.slice(0, 0)

    return _finish_keep_filter(table, field, column, value_array)


def _uuid_storage(column):
    """The `fixed_size_binary(16)` storage behind a `pa.uuid()` column.

    Zero copy, and it keeps the null mask — the extension array's validity
    bitmap IS the storage array's, so `null_count` and `is_in`'s null handling
    behave exactly as they do for the column itself.
    """
    combined = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    return combined.storage


def _finish_keep_filter(table: pa.Table, field: str, column, value_array, label=None) -> pa.Table:
    """The keep-filter's shared tail: build the mask, count the two skip
    reasons, return the filtered table. Factored out so the uuid path and the
    integer/string path cannot drift on the accounting.

    `column` is the array the membership test runs against — the table's own
    column for integer/string fields, its STORAGE array for a uuid one.
    `label` is None on the integer/string path, where `filtered[field]` is
    already materialized and `str` is the right rendering; the uuid path passes
    `uuid_text` and pays one extra filter pass over the STORAGE array, because
    `value_counts` has no kernel for the extension type and a bytes repr is not
    a label anybody can match against a UUID.
    """
    # is_in returns null for null inputs; fill_null(False) excludes them
    # from the keep mask. Null rows are counted separately as
    # filter_field_missing below.
    keep_mask = pc.fill_null(pc.is_in(column, value_array), False)
    null_count = column.null_count
    n_original = len(table)
    filtered = table.filter(keep_mask)
    n_excluded = n_original - len(filtered) - null_count

    if null_count > 0:
        metrics.records_skipped_total.labels(reason="filter_field_missing").inc(null_count)
    if n_excluded > 0:
        metrics.records_skipped_total.labels(reason="filter_excluded").inc(n_excluded)
    if len(filtered) > 0:
        # Per-value match counts on the KEPT rows only (bounded by the
        # include set). value_counts is one pass over the minority the filter
        # retained; on the default path that minority is already materialized
        # as `filtered[field]`, so re-filtering `column` would be a second
        # pass over the hot path for nothing.
        counted = column.filter(keep_mask) if label is not None else filtered[field]
        render = label or str
        for chunk in pc.value_counts(counted).to_pylist():
            metrics.filter_matched_total.labels(value=render(chunk["values"])).inc(chunk["counts"])
    return filtered


def _parse_uuid_filter_values(values) -> tuple[pa.Array, tuple]:
    """Split configured filter values into comparable bytes and the rejects.

    Returns `(fixed_size_binary(16) array of the values that ARE UUIDs, tuple
    of the ones that are not)`. A value that is not a UUID cannot match a uuid
    column under any encoding, so it is dropped from the comparison rather
    than used to condemn the whole batch: the allowlist still admits what it
    can, the denylist still denies what it can, and only a set with NO usable
    value left falls back to the filter's failure direction.

    Memoized on the values object (see `_uuid_filter_values_memo`), identity
    first: the include-values source hands back the same immutable tuple every
    batch until the set actually changes, so the hit costs one pointer compare
    rather than a walk of 10k strings.
    """
    global _uuid_filter_values_memo
    memo = _uuid_filter_values_memo
    if memo is not None and (memo[0] is values or memo[0] == values):
        return memo[1], memo[2]
    parsed: list[bytes] = []
    invalid: list = []
    for value in values:
        try:
            parsed.append(arrow_converter.uuid_bytes(value))
        except ValueError:
            invalid.append(value)
    result = (values, pa.array(parsed, type=arrow_converter.UUID_STORAGE_TYPE), tuple(invalid))
    _uuid_filter_values_memo = result
    return result[1], result[2]


def _warn_bad_uuid_filter_values(field: str, values, invalid: tuple, outcome: str) -> None:
    """Warn once per (field, value count, first offender) that some configured
    filter values are not UUIDs.

    The static values are refused at config load, so this is always a dynamic
    include-values poll serving a non-UUID — and the poll keeps serving it, so
    the condition is permanent. One line per flush would be the loudest thing
    in the log for the lifetime of the pod; the counters are the always-on
    signal. See `_uuid_filter_values_warned` for why the key is not the values.
    """
    key = (field, len(values), str(invalid[0]))
    if key in _uuid_filter_values_warned:
        return
    _uuid_filter_values_warned.add(key)
    log.warning(
        "%d of %d filter value(s) for uuid-typed column %r are not UUIDs (first: %r); %s",
        len(invalid),
        len(values),
        field,
        invalid[0],
        outcome,
    )


def _apply_drop_filter(table: pa.Table, cfg: config.Config) -> pa.Table:
    """Apply the drop-direction filter (denylist): drop records whose value
    in `filter_drop_field` is in `filter_drop_values`; keep everything else.
    Runs AFTER the keep-filter — the composed semantics are keep ∩ ¬drop
    (e.g. the CP-driven include set minus an operator blacklist).

    Failure semantics are the OPPOSITE of the keep-filter, deliberately:
    an allowlist that can't evaluate fails closed (dropping the batch is
    the conservative reading of "only deliver what's on the list"), but a
    denylist that can't evaluate fails OPEN — keeping the batch leaks the
    blacklisted minority temporarily, while dropping it would turn a
    schema hiccup into data loss for every tenant. Unevaluable batches
    WARN and pass through unchanged. Null field values are kept for the
    same reason (a null can't match the blacklist).

    Dropped rows count under `records_skipped_total{reason="filter_dropped"}`.
    """
    if cfg.filter_drop_field is None or cfg.filter_drop_values is None:
        return table

    field = cfg.filter_drop_field
    if field not in table.column_names:
        log.warning("Drop-filter field %r missing from batch; keeping batch unchanged (fail-open)", field)
        return table

    column = table[field]
    if column.type == arrow_converter.UUID_TYPE:
        # As in the keep-filter: compare the 16 storage bytes, because the
        # allowlist below refuses an extension type and a denylist that stops
        # denying leaks exactly the rows it exists to remove. Values that are
        # not UUIDs cannot match and are dropped from the denylist; a denylist
        # with none left denies nothing, matching this filter's direction.
        value_array, invalid = _parse_uuid_filter_values(cfg.filter_drop_values)
        if invalid:
            _warn_bad_uuid_filter_values(field, cfg.filter_drop_values, invalid, "they cannot match and are ignored")
            metrics.errors_total.labels(type="filter_value_invalid").inc()
        if len(value_array) == 0:
            # Nothing on the denylist is a UUID, so nothing is denied.
            # Fail-open, as this filter always does.
            return table
        column = _uuid_storage(column)
    elif not (
        pa.types.is_integer(column.type) or pa.types.is_string(column.type) or pa.types.is_large_string(column.type)
    ):
        log.warning(
            "Drop-filter field %r has unsupported type %s; keeping batch unchanged (fail-open)",
            field,
            column.type,
        )
        return table
    else:
        try:
            value_array = pa.array(cfg.filter_drop_values).cast(column.type)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as e:
            log.warning(
                "Drop-filter values %r incompatible with column %r type %s (%s); keeping batch unchanged (fail-open)",
                cfg.filter_drop_values,
                field,
                column.type,
                e,
            )
            return table

    # is_in returns false-or-null for null inputs depending on pyarrow
    # version; fill_null(False) normalizes either way, keeping null rows
    # (a null can't match the blacklist — fail-open, see docstring).
    drop_mask = pc.fill_null(pc.is_in(column, value_array), False)
    filtered = table.filter(pc.invert(drop_mask))
    n_dropped = len(table) - len(filtered)
    if n_dropped > 0:
        metrics.records_skipped_total.labels(reason="filter_dropped").inc(n_dropped)
    return filtered


def _apply_sort(table: pa.Table, cfg: config.Config) -> pa.Table:
    """Sort the consolidated batch by `cfg.sort_by` (ascending) before write.

    Returns the input unchanged when sort is unconfigured or when one or
    more sort fields are missing from the batch schema. In the missing-
    field case:
      - Log a warning *once per distinct missing-fields pattern* (the
        pod-lifetime dedup guards against per-flush log floods at
        production cadence).
      - Increment `sort_skipped_total{reason="field_missing"}` by the
        record count so operators can detect missing-field sort gaps
        from metrics alone.

    A sort key whose Arrow type has no sort kernel is the third skip case
    (`sort_skipped_total{reason="unsortable_type"}`). Extension types are
    handled before it rather than by it: `MILLPOND_TYPED_COLUMNS=<c>:uuid`
    makes a column `pa.uuid()`, for which pyarrow raises "Sorting not
    supported for type extension<arrow.uuid>" — so the key columns are
    replaced by their STORAGE arrays for the duration of the sort. The
    storage of `pa.uuid()` is `fixed_size_binary(16)` holding the UUID's
    big-endian bytes, which sorts bytewise and therefore in exactly the same
    order as the canonical text; the indices come back positional and apply
    unchanged to the original table. The `unsortable_type` arm stays as the
    backstop for a type with neither a kernel nor sortable storage — a flush
    that loses its layout improvement, never one that dies with its offsets
    uncommitted.

    Apply order matters: this runs in `_flush()` after `pa.concat_tables`
    consolidates the pending buffer but before `sink.write()`. Sink-side
    partition columns (computed by the ducklake extension) are not yet
    present and so are not in scope for the sort, which is by design —
    operators specify sort keys against the source schema, not the
    lake's derived columns.
    """
    if cfg.sort_by is None:
        return table

    missing = [f for f in cfg.sort_by if f not in table.column_names]
    if missing:
        key = ",".join(missing)
        if key not in _sort_missing_fields_warned:
            log.warning("Sort field(s) missing from batch schema: %s; skipping sort for affected flushes", missing)
            _sort_missing_fields_warned.add(key)
        metrics.sort_skipped_total.labels(reason="field_missing").inc(len(table))
        return table

    # PyArrow's sort_indices is stable; null_placement defaults to "at_end"
    # which is the sensible default for ascending sort by a key with null
    # values. Take() rewrites the table once — a full copy at flush size
    # (~256 MB at production batches). That's the cost we pay for the
    # write-side layout improvement; it's expected to be small relative
    # to the sink.write() that follows.
    sort_keys = [(field, "ascending") for field in cfg.sort_by]
    try:
        indices = pc.sort_indices(_sortable_key_projection(table, cfg.sort_by), sort_keys=sort_keys)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as e:
        key = ",".join(cfg.sort_by)
        if key not in _sort_unsortable_warned:
            log.warning(
                "Sort field(s) %s have no usable sort kernel (%s); skipping sort for affected flushes",
                cfg.sort_by,
                e,
            )
            _sort_unsortable_warned.add(key)
        metrics.sort_skipped_total.labels(reason="unsortable_type").inc(len(table))
        return table
    return table.take(indices)


def _sortable_key_projection(table: pa.Table, sort_by: tuple[str, ...]) -> pa.Table:
    """`table` with every extension-typed sort key replaced by its storage.

    Only the key columns are touched and only for the `sort_indices` call —
    the indices it returns are positional, so the caller still takes from the
    untouched table and the batch reaches the sink with its declared types
    intact. Returns the input itself when no key is an extension type.
    """
    columns = None
    for name in sort_by:
        column = table[name]
        if not isinstance(column.type, pa.BaseExtensionType):
            continue
        if columns is None:
            columns = {n: table[n] for n in table.column_names}
        combined = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
        columns[name] = combined.storage
    if columns is None:
        return table
    return pa.table(columns)


def _is_commit_contention(exc: BaseException) -> bool:
    """Detect DuckLake catalog-write contention from the exception text.

    DuckLake's commit loop retries up to ducklake_max_retry_count times
    on PK collisions and serialization conflicts; if that budget is
    exhausted the surfaced exception carries either an explicit
    "maximum retry count" sentinel (DuckLake-emitted) or the underlying
    duplicate-key / serialization-failure text bubbled up from
    Postgres. Anything else (S3 timeout, schema-evolution race,
    OOM, etc.) is classed as plain write_retry.

    Keep the substrings stable — they label a metric that an alert
    may key on. Strings are sourced from:
      - DuckLake: src/storage/ducklake_transaction_state.cpp:1748
        "Exceeded the maximum retry count of ..."
      - Postgres (libpq): "duplicate key value violates unique constraint"
      - Postgres (libpq): "could not serialize access"
      - DuckLake conflict checker (ducklake_transaction_state.cpp
        ConflictCheck): "Transaction conflict - attempting to ..." — a
        hard-abort (can_retry=false) after a concurrent snapshot's change
        token, e.g. a drop-partitions catalog write ending files under an
        in-flight insert. Absorbed by the retry below on a fresh baseline.
    """
    msg = str(exc)
    return (
        "maximum retry count" in msg
        or "duplicate key value" in msg
        or "could not serialize access" in msg
        or "Transaction conflict" in msg
    )


def _classify_write_error(exc: BaseException, destination: str = "ducklake") -> str:
    """errors_total label for a failed write attempt.

    Hoglake commit conflicts are typed: pyhoglake's CommitConflictError
    carries `retryable=True` on the class (the server's OCC 409 —
    refresh the baseline and retry). Checked duck-typed by module name
    so main.py never imports the hoglake backend for a ducklake-only
    deployment.

    DuckLake contention stays the string-matching classifier
    (_is_commit_contention), GATED BY DESTINATION: those substrings are
    generic Postgres/DuckLake wording, and a hoglake failure is free to
    contain any of them (the hoglake control plane is a Postgres-backed
    service too, so a 500 can carry "duplicate key value" straight
    through). Labeling that `ducklake_commit_contention` would fire a
    DuckLake alert from a pod that has no DuckLake. Everything else is a
    plain write_retry.
    """
    if getattr(exc, "retryable", None) is True and type(exc).__module__.startswith("pyhoglake"):
        return "hoglake_commit_contention"
    if destination == "ducklake" and _is_commit_contention(exc):
        return "ducklake_commit_contention"
    return "write_retry"


def _write_retry_budget(sink) -> tuple[int, float]:
    """(max attempts, base backoff) for this sink.

    The DuckLake defaults are 3 attempts / 1s base — inherited from a
    backend that carries its OWN inner commit-retry loop
    (`ducklake_max_retry_count`, default 100 here), so the outer three
    attempts were never the real budget. Hoglake has no inner loop: the
    pyhoglake client issues one request and raises. A sink may therefore
    publish its own budget via `write_retry_budget()`; sinks that don't
    keep the historical values.
    """
    budget = getattr(sink, "write_retry_budget", None)
    if budget is None:
        return _WRITE_MAX_RETRIES, _WRITE_BASE_DELAY_S
    return budget()


def _retry_delay(sink, attempt: int, base: float) -> float:
    """Exponential backoff, FLOORED by a server-supplied Retry-After and
    spread by jitter.

    Hoglake's commit admission control answers 503 with `Retry-After`;
    the server knows how long the queue actually is and our doubling
    curve does not. A sink may expose the last hint via
    `retry_after_hint()` (seconds, or None).

    The hint is a floor, not a replacement. Hoglake's hint is the
    hardcoded string "1", so letting it REPLACE the curve collapsed the
    whole ladder to one-second steps: eight attempts against a convoyed
    catalog, spent in about eight seconds, then a crash — which adds a
    cold pod to the convoy it was backing off from. Taking the larger of
    the two keeps the exponential shape under sustained backpressure and
    still never returns before the server asked to be asked again.

    Jitter is added upward (never below the floor) so that a fleet of
    pods refused by the same 503 does not wake in lockstep and re-form
    the convoy on every rung. It applies to BOTH destinations, which is a
    behaviour change to the deployed DuckLake path and an intended one:
    DuckLake pods contend for the same Postgres catalog commit lock and
    have the same lockstep problem, and the cost is bounded — its ladder
    becomes 3.75s of backoff at worst instead of 3s, under the same
    ceiling. A second, hoglake-only curve would be two things to keep in
    step for a spread of 25%. See the retry table in README.md.

    Both are clamped to _RETRY_AFTER_MAX_S: the consume loop is single
    threaded, so a backoff is also a poll gap, and a misbehaving or
    hostile header must not park it past the liveness deadline.
    """
    delay = min(base * (2**attempt), _RETRY_AFTER_MAX_S)
    hint = getattr(sink, "retry_after_hint", None)
    if hint is not None:
        seconds = hint()
        if seconds is not None:
            delay = max(delay, min(float(seconds), _RETRY_AFTER_MAX_S))
    return min(delay + random.uniform(0.0, delay * _RETRY_JITTER), _RETRY_AFTER_MAX_S)


def _write_with_retry(sink, consolidated, *, destination: str = "ducklake", write_kwargs=None):
    """Write to the sink with exponential backoff on transient failures.

    Returns the record count the sink actually wrote (0 when it skipped the
    batch whole, e.g. every column was a VARIANT companion collision).

    `write_kwargs` is the per-call escape hatch for backends that need to
    know WHICH batch this is, not just what is in it (the icebox sink
    took its Kafka offsets this way at tag `final-iceberg`).
    DuckLakeSink.write takes the batch alone, so the default is empty.
    The same kwargs go to every attempt — a retry must be recognizable as
    the same flush, not merely a similar one.

    A sink may also declare a failure non-retryable via `is_retryable()`
    (a permanent 422 is not worth three attempts and a backoff; crash the
    pod now and let the operator see it), and may publish its own retry
    budget and Retry-After hint.
    """
    write_kwargs = write_kwargs or {}
    max_attempts, base_delay = _write_retry_budget(sink)
    classify_retryable = getattr(sink, "is_retryable", None)
    for attempt in range(max_attempts):
        try:
            return sink.write(consolidated, **write_kwargs)
        except Exception as exc:
            error_type = _classify_write_error(exc, destination)
            metrics.errors_total.labels(type=error_type).inc()
            permanent = classify_retryable is not None and not classify_retryable(exc)
            if permanent:
                log.error(
                    "Write failed permanently (attempt %d/%d, type=%s); not retrying",
                    attempt + 1,
                    max_attempts,
                    error_type,
                    exc_info=True,
                )
                raise
            if attempt == max_attempts - 1:
                raise
            delay = _retry_delay(sink, attempt, base_delay)
            log.warning(
                "Write failed (attempt %d/%d, type=%s), retrying in %.1fs",
                attempt + 1,
                max_attempts,
                error_type,
                delay,
                exc_info=True,
            )
            # Invalidate caches so retry re-checks table existence and schema —
            # another pod may have created the table or changed columns.
            sink.reset_caches()
            time.sleep(delay)


def _sink_write_kwargs(cfg, offsets: dict[tuple[str, int], tuple[int, int]]) -> dict:
    """Per-call arguments for backends that need to know WHICH batch this
    is, not only what is in it.

    `offsets` is the consume loop's (topic, partition) -> (first, last)
    offset map for everything in the pending buffer: the exact Kafka
    range this flush is about to publish and then commit. Flattened to a
    sorted tuple of `(topic, partition, first, last)` so it is hashable
    and order-independent, and so it is identical on every retry of the
    same flush — HoglakeSink hashes it into the commit's idempotency key,
    which is what turns a retry after a lost commit response into a
    replay instead of a second publication.

    BOTH ends of the range, not just the high end. A key naming only the
    high offset says "everything up to here", which is not the row set a
    flush publishes: rewind a partition and re-consume, and a flush of
    [0, 41] carries the name of an earlier flush of [30, 41], whose
    receipt then reports it as already published — offsets advance over
    rows that were never written.

    DuckLake takes no per-call identity: its INSERT sits in a transaction
    whose commit outcome the client always learns, so a retry there
    cannot be ambiguous the way a lost HTTP response is. The seam stays
    empty for it — the same shape the icebox sink used at tag
    `final-iceberg`.
    """
    if cfg.destination != "hoglake":
        return {}
    flushed = tuple(sorted((topic, partition, first, last) for (topic, partition), (first, last) in offsets.items()))
    return {"kafka_offsets": flushed}


def _flush(
    sink,
    cfg,
    kafka,
    consolidated,
    pending_bytes,
    pending_records,
    offsets,
    elapsed,
    trigger="time",
):
    """Write to the sink, commit offsets, update metrics."""
    consolidated = _apply_sort(consolidated, cfg)

    t0 = time.monotonic()
    records_written = _write_with_retry(
        sink,
        consolidated,
        destination=cfg.destination,
        write_kwargs=_sink_write_kwargs(cfg, offsets),
    )
    write_duration = time.monotonic() - t0

    # Commit offsets synchronously — at-least-once requires knowing commit succeeded
    tp_offsets = [
        TopicPartition(topic, partition, last + 1)  # +1: committed offset is next-to-fetch
        for (topic, partition), (_first, last) in offsets.items()
    ]
    for attempt in range(_COMMIT_MAX_RETRIES):
        try:
            kafka.commit(offsets=tp_offsets, asynchronous=False)
            break
        except Exception:
            metrics.errors_total.labels(type="offset_commit").inc()
            if attempt == _COMMIT_MAX_RETRIES - 1:
                log.error(
                    "Offset commit failed after %d attempts — duplicates possible on restart",
                    _COMMIT_MAX_RETRIES,
                    exc_info=True,
                )
                raise
            delay = _COMMIT_BASE_DELAY_S * (2**attempt)
            log.warning(
                "Offset commit failed (attempt %d/%d), retrying in %.1fs",
                attempt + 1,
                _COMMIT_MAX_RETRIES,
                delay,
                exc_info=True,
            )
            time.sleep(delay)

    log.info(
        "Flush: %d records, %d bytes, %d columns, write=%.2fs, elapsed=%.1fs",
        len(consolidated),
        pending_bytes,
        len(consolidated.schema),
        write_duration,
        elapsed,
    )

    metrics.flush_duration_seconds.observe(write_duration)
    metrics.flush_size_bytes.observe(pending_bytes)
    metrics.flush_size_records.observe(pending_records)
    # The sink's count, not pending_records: a skipped batch (all columns
    # were companion collisions) is records_skipped, not records_written.
    metrics.records_written_total.inc(records_written)
    metrics.batches_flushed_total.labels(trigger=trigger).inc()
    server.health.record_flush()

    for tp in tp_offsets:
        metrics.last_committed_offset.labels(partition=str(tp.partition)).set(tp.offset)


def _update_lag_metrics(kafka, admin, tp_offsets, auto_offset_reset):
    """Refresh millpond_consumer_lag for every assigned partition.

    Called periodically, not on every flush. Partitions that delivered
    since the last flush use their flushed positions. Idle partitions fall
    back to the librdkafka fetch position, then the committed offset, then
    the auto.offset.reset target. Without the idle refresh, a partition
    with zero deliveries keeps a frozen gauge for the whole pod lifetime
    (PostHog/millpond#133). Watermarks come from one batched AdminClient
    query instead of a per-partition consumer RPC.
    """
    positions = {tp.partition: tp.offset for tp in tp_offsets}
    try:
        assigned = kafka.assignment()
    except Exception:
        assigned = []
    idle = [tp for tp in assigned if tp.partition not in positions]
    if idle:
        try:
            for ptp in kafka.position(idle):
                if ptp.offset >= 0:
                    positions[ptp.partition] = ptp.offset
        except Exception:
            pass  # fall through to committed, then the reset target
        unresolved = [tp for tp in idle if tp.partition not in positions]
        if unresolved:
            try:
                for ctp in kafka.committed(unresolved, timeout=5):
                    if ctp.error is None and ctp.offset >= 0:
                        positions[ctp.partition] = ctp.offset
            except Exception:
                pass  # the reset-target fallback below still applies
    query = list(tp_offsets) + idle
    if not query:
        return
    watermarks = consumer.query_watermarks(admin, query)
    for partition, (low, high) in watermarks.items():
        position = positions.get(partition)
        if position is None:
            # Fresh and never-fetched: librdkafka will start at the reset
            # target on first fetch, so report lag from that position.
            position = high if auto_offset_reset == "latest" else low
        metrics.consumer_lag.labels(partition=str(partition)).set(max(0, high - position))


def main():
    logging_config.setup_stdout()
    try:
        __version__ = version("millpond")
    except PackageNotFoundError:
        __version__ = "0.0.0+unknown"
    log.info("millpond %s starting", __version__)

    # Initialize close-targets to None so the finally block doesn't NameError
    # if a startup step (DuckLakeSink, kafka.create, server.start) raises.
    http = None
    sink = None
    kafka = None
    logger_provider = None
    include_source = None

    cfg = config.load()
    # metrics.init() FIRST so a failure in OTLP setup (DNS lookup at
    # OTLPLogExporter construction, malformed token, etc.) doesn't take
    # out the /metrics endpoint — operators still get the writer's
    # Prometheus axis to triage the failure.
    metrics.init(f"{cfg.topic}-{cfg.table_label}", broker_source=cfg.broker_source)
    # OTLP/HTTP export to PostHog Logs. Returns None when
    # cfg.posthog_project_token is unset (default for local dev). The
    # provider is flushed on the shutdown path so in-flight batches
    # make it out before the process exits.
    logger_provider = logging_config.attach_posthog_otlp(cfg)

    pending: list[pa.Table] = []
    pending_bytes = 0
    pending_records = 0
    # (topic, partition) -> (first, last) offset held in the pending
    # buffer. The high end is what gets committed to Kafka; BOTH ends are
    # what name the flush for an idempotent destination (see
    # _sink_write_kwargs — a name that omits the low end is a name a
    # rewound partition can collide with).
    offsets: dict[tuple[str, int], tuple[int, int]] = {}
    last_flush = time.monotonic()
    last_lag_sample = 0.0  # force immediate first sample
    last_heartbeat = time.monotonic()

    try:
        http = server.start(cfg.http_port)
        server.health.mark_started()
        log.info("Health server started, probes passing")

        # No connection recovery logic — if the destination fails, the pod
        # crashes and K8s restarts it. Reconnection adds complexity for no
        # benefit when the restart path already handles offset replay correctly.
        sink = sink_mod.make_sink(cfg)
        log.info("Sink ready: destination=%s table=%s", cfg.destination, cfg.table_label)
        kafka = consumer.create(cfg)
        lag_admin = consumer.make_admin_client(cfg)
        log.info("Kafka consumer created, partitions assigned")
        backpressure.init(cfg.consume_batch_size)

        # Include-values source: what the filter reads each batch. In
        # authoritative mode start() BLOCKS until the first successful
        # poll (a halt is recoverable from Kafka; running on a stale
        # bootstrap silently drops records). The mode gauge is what lets
        # fleet-level queries (shadow-flip gate, staleness alerts) tell
        # which replicas actually run which source.
        include_source = include_values.build(cfg)
        metrics.include_values_mode.labels(mode=include_source.mode).set(1)
        include_source.start()
        log.info(
            "Include-values source started (mode=%s, url=%s)",
            include_source.mode,
            cfg.include_values_url,
        )

        shutdown = False

        def on_signal(signum, _frame):
            nonlocal shutdown
            log.info("Received signal %s, shutting down", signal.Signals(signum).name)
            shutdown = True

        signal.signal(signal.SIGTERM, on_signal)
        signal.signal(signal.SIGINT, on_signal)

        log.info("millpond ready, entering main loop")

        while not shutdown:
            remaining = cfg.flush_interval_s - (time.monotonic() - last_flush)
            timeout = _consume_timeout(remaining)

            batch_size = backpressure.compute_batch_size(pending_bytes, cfg.flush_size)
            msgs = kafka.consume(num_messages=batch_size, timeout=timeout)
            server.health.record_poll()

            now = time.monotonic()
            if now - last_heartbeat >= _HEARTBEAT_INTERVAL_S:
                log.info(
                    "Heartbeat: pending=%d records (%d bytes), partitions=%d",
                    pending_records,
                    pending_bytes,
                    len(offsets),
                )
                last_heartbeat = now

            if msgs:
                values = []
                for msg in msgs:
                    if msg.error():
                        metrics.errors_total.labels(type="kafka").inc()
                        log.warning("Kafka error: %s", msg.error())
                        continue
                    metrics.records_consumed_total.labels(partition=str(msg.partition())).inc()
                    if msg.value() is not None:
                        values.append(msg.value())
                        key = (msg.topic(), msg.partition())
                        offset = msg.offset()
                        first, last = offsets.get(key, (offset, offset))
                        offsets[key] = (min(first, offset), max(last, offset))

                if values:
                    skipped = 0
                    table = _convert_batch(values)
                    if table is not None:
                        skipped = len(values) - len(table)
                        table = _coerce_columns(table, cfg)
                        table = _apply_filter(table, cfg, include_source.current())
                        table = _apply_drop_filter(table, cfg)
                        if len(table) > 0:
                            pending.append(table)
                            pending_bytes += table.nbytes
                            pending_records += len(table)
                            metrics.pending_bytes.set(pending_bytes)
                    else:
                        skipped = len(values)

                    if skipped > 0:
                        metrics.records_skipped_total.labels(reason="json_parse").inc(skipped)

            # Check flush triggers
            elapsed = time.monotonic() - last_flush
            size_triggered = pending_bytes >= cfg.flush_size
            time_triggered = elapsed >= cfg.flush_interval_s
            should_flush = pending_records > 0 and (size_triggered or time_triggered)

            if should_flush:
                trigger = "size" if size_triggered else "time"
                consolidated = pa.concat_tables(pending, promote_options="default")
                _flush(
                    sink,
                    cfg,
                    kafka,
                    consolidated,
                    pending_bytes,
                    pending_records,
                    offsets,
                    elapsed,
                    trigger,
                )

                # Sample lag metrics periodically, not on every flush
                now = time.monotonic()
                if now - last_lag_sample >= _LAG_SAMPLE_INTERVAL_S:
                    tp_offsets = [
                        TopicPartition(topic, partition, last + 1)
                        for (topic, partition), (_first, last) in offsets.items()
                    ]
                    _update_lag_metrics(kafka, lag_admin, tp_offsets, cfg.auto_offset_reset)
                    last_lag_sample = now

                pending.clear()
                pending_bytes = 0
                pending_records = 0
                offsets.clear()
                metrics.pending_bytes.set(0)
                last_flush = time.monotonic()

    except Exception:
        log.exception("Fatal error in main loop")
        raise
    finally:
        # Stop the include-values poll thread first — it's daemonized so
        # this is cosmetic on crash paths, but a clean shutdown shouldn't
        # leave it racing the final flush.
        if include_source is not None:
            include_source.stop()

        # Final flush only makes sense if both sink and kafka were created;
        # if startup failed earlier, there's no consumed data to flush.
        if pending_records > 0 and sink is not None and kafka is not None:
            try:
                consolidated = pa.concat_tables(pending, promote_options="default")
                elapsed = time.monotonic() - last_flush
                log.info("Final flush: %d records, %d bytes", len(consolidated), pending_bytes)
                _flush(
                    sink,
                    cfg,
                    kafka,
                    consolidated,
                    pending_bytes,
                    pending_records,
                    offsets,
                    elapsed,
                )
            except Exception:
                log.exception("Final flush failed — data safe in Kafka, will replay on restart")

        # Close in reverse-startup order. Each close is guarded so a partial
        # startup (e.g. DuckLakeSink raised) doesn't NameError its way through here.
        if kafka is not None:
            log.info("Closing consumer")
            try:
                kafka.close()
            except Exception:
                log.exception("Kafka consumer close failed")
        if sink is not None:
            try:
                sink.close()
            except Exception:
                log.exception("Sink close failed")
        if http is not None:
            try:
                http.shutdown()
            except Exception:
                log.exception("HTTP server shutdown failed")
        if logger_provider is not None:
            # Flush the OTLP batch processor so in-flight log records
            # make it to PostHog Logs before the process exits.
            try:
                logger_provider.shutdown()
            except Exception:
                log.exception("OTLP logger provider shutdown failed")
        log.info("millpond shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
