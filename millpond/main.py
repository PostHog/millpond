import logging
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
    ducklake,
    include_values,
    logging_config,
    metrics,
    server,
)

log = logging.getLogger(__name__)

_LAG_SAMPLE_INTERVAL_S = 60.0  # how often to query watermark offsets for lag metrics
_HEARTBEAT_INTERVAL_S = 60.0  # periodic log when idle (well under 300s liveness timeout)
# Longest a single consume() may block. record_poll() runs only after consume
# returns, and server.health marks the process dead at max_poll_age_s=300 —
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

# Module-level set tracking which "missing sort fields" patterns we've
# already warned about. Without this, a misconfigured sort against a
# high-volume topic would log once per flush — at production cadence,
# tens of thousands of log lines / hour. The key is the comma-joined
# missing-fields tuple, so distinct misconfigurations still each get
# one warning. Pod restart resets the set (the lifetime tied to the
# process is intentional — operators get a fresh warning on each
# restart, which signals a likely persistent misconfiguration).
_sort_missing_fields_warned: set[str] = set()


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
        # include set). value_counts on the filtered column is one pass
        # over the minority the filter retained.
        for chunk in pc.value_counts(filtered[field]).to_pylist():
            metrics.filter_matched_total.labels(value=str(chunk["values"])).inc(chunk["counts"])
    return filtered


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
    if not (
        pa.types.is_integer(column.type) or pa.types.is_string(column.type) or pa.types.is_large_string(column.type)
    ):
        log.warning(
            "Drop-filter field %r has unsupported type %s; keeping batch unchanged (fail-open)",
            field,
            column.type,
        )
        return table

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
    indices = pc.sort_indices(table, sort_keys=sort_keys)
    return table.take(indices)


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
    """
    msg = str(exc)
    return "maximum retry count" in msg or "duplicate key value" in msg or "could not serialize access" in msg


def _write_with_retry(sink, consolidated):
    """Write to the sink with exponential backoff on transient failures.

    Returns the record count the sink actually wrote (0 when it skipped the
    batch whole, e.g. every column was a VARIANT companion collision).
    """
    for attempt in range(_WRITE_MAX_RETRIES):
        try:
            return sink.write(consolidated)
        except Exception as exc:
            error_type = "ducklake_commit_contention" if _is_commit_contention(exc) else "write_retry"
            metrics.errors_total.labels(type=error_type).inc()
            if attempt == _WRITE_MAX_RETRIES - 1:
                raise
            delay = _WRITE_BASE_DELAY_S * (2**attempt)
            log.warning(
                "Write failed (attempt %d/%d, type=%s), retrying in %.1fs",
                attempt + 1,
                _WRITE_MAX_RETRIES,
                error_type,
                delay,
                exc_info=True,
            )
            # Invalidate caches so retry re-checks table existence and schema —
            # another pod may have created the table or changed columns.
            sink.reset_caches()
            time.sleep(delay)


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
    records_written = _write_with_retry(sink, consolidated)
    write_duration = time.monotonic() - t0

    # Commit offsets synchronously — at-least-once requires knowing commit succeeded
    tp_offsets = [
        TopicPartition(topic, partition, offset + 1)  # +1: committed offset is next-to-fetch
        for (topic, partition), offset in offsets.items()
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


def _update_lag_metrics(kafka, tp_offsets):
    """Sample watermark offsets for lag metrics. Called periodically, not on every flush."""
    for tp in tp_offsets:
        try:
            _lo, hi = kafka.get_watermark_offsets(tp, timeout=5)
            metrics.consumer_lag.labels(partition=str(tp.partition)).set(hi - tp.offset)
        except Exception:
            pass  # best-effort lag tracking


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
    offsets: dict[tuple[str, int], int] = {}  # (topic, partition) -> max offset
    last_flush = time.monotonic()
    last_lag_sample = 0.0  # force immediate first sample
    last_heartbeat = time.monotonic()

    try:
        http = server.start()
        server.health.mark_started()
        log.info("Health server started, probes passing")

        # No connection recovery logic — if the destination fails, the pod
        # crashes and K8s restarts it. Reconnection adds complexity for no
        # benefit when the restart path already handles offset replay correctly.
        sink = ducklake.DuckLakeSink(cfg)
        log.info("Sink ready: table=%s", cfg.table_label)
        kafka = consumer.create(cfg)
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
                        offsets[key] = max(offsets.get(key, -1), msg.offset())

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
                        TopicPartition(topic, partition, offset + 1) for (topic, partition), offset in offsets.items()
                    ]
                    _update_lag_metrics(kafka, tp_offsets)
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
