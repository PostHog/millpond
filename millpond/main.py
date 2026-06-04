import datetime
import logging
import signal
import sys
import time
from importlib.metadata import PackageNotFoundError, version

import pyarrow as pa
import pyarrow.compute as pc
from confluent_kafka import TopicPartition

from millpond import arrow_converter, backpressure, config, consumer, logging_config, metrics, server
from millpond import sink as sink_mod

log = logging.getLogger(__name__)

_LAG_SAMPLE_INTERVAL_S = 60.0  # how often to query watermark offsets for lag metrics
_HEARTBEAT_INTERVAL_S = 60.0  # periodic log when idle (well under 300s liveness timeout)
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


def _apply_filter(table: pa.Table, cfg: config.Config) -> pa.Table:
    """Apply the configured keep-filter: drop records whose value in
    `filter_keep_field` is not in `filter_values`.

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
    reserved at the config layer but not implemented here yet — config
    rejects it explicitly at startup.
    """
    if cfg.filter_keep_field is None or cfg.filter_values is None:
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
        value_array = pa.array(cfg.filter_values).cast(column.type)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as e:
        log.warning(
            "Filter values %r incompatible with column %r type %s (%s); treating batch as field-missing",
            cfg.filter_values,
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
    consolidates the pending buffer but before `sink.write()`. Both
    DuckLake and Iceberg paths see pre-sorted data; sink-side partition
    columns (year/month/day/hour added by IcebergSink, computed by the
    ducklake extension) are not yet present and so are not in scope for
    the sort, which is by design — operators specify sort keys against
    the source schema, not the lake's derived columns.
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


def _write_with_retry(sink, consolidated, *, write_kwargs=None):
    """Write to the sink with exponential backoff on transient failures.

    `write_kwargs` is the icebox path's escape hatch: IceboxSink.write
    needs kafka_offsets passed in per-call, but DuckLakeSink/IcebergSink
    don't. The caller decides which kwargs apply based on cfg.destination.
    """
    write_kwargs = write_kwargs or {}
    for attempt in range(_WRITE_MAX_RETRIES):
        try:
            sink.write(consolidated, **write_kwargs)
            return
        except Exception:
            metrics.errors_total.labels(type="write_retry").inc()
            if attempt == _WRITE_MAX_RETRIES - 1:
                raise
            delay = _WRITE_BASE_DELAY_S * (2**attempt)
            log.warning(
                "Write failed (attempt %d/%d), retrying in %.1fs",
                attempt + 1,
                _WRITE_MAX_RETRIES,
                delay,
                exc_info=True,
            )
            # Invalidate caches so retry re-checks table existence and schema —
            # another pod may have created the table or changed columns.
            sink.reset_caches()
            time.sleep(delay)


def _kafka_offsets_for_icebox(
    offsets: dict[tuple[str, int], int],
    topic: str,
) -> dict[str, int]:
    """Flatten the consumer's (topic, partition) → offset map into the
    {partition_id_str: max_offset} shape the icebox wire format wants.

    The icebox runs per (topic, table), so all offsets MUST be for the
    same topic. A mismatch is a wiring bug (writer subscribed to the
    wrong topic, or to multiple topics) — raise loudly rather than
    silently drop, because the dropped offsets would become permanently
    stuck consumer-group offsets on the icebox side (recovery only
    sees what got registered via POSTs).
    """
    out: dict[str, int] = {}
    for (t, partition), offset in offsets.items():
        if t != topic:
            raise RuntimeError(
                f"_kafka_offsets_for_icebox: unexpected topic {t!r} in offsets; "
                f"icebox is configured for {topic!r}. Check millpond consumer "
                f"subscription — silent drops here would lose offsets."
            )
        out[str(partition)] = offset
    return out


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
    *,
    max_msg_timestamp_ms: int = -1,
):
    """Write to the sink, commit offsets, update metrics.

    For DuckLake / Iceberg sinks, this writes the batch then commits
    Kafka offsets directly. For icebox, the sink POSTs files to the
    icebox service which commits Kafka offsets atomically with its
    Iceberg snapshot — so this function MUST NOT call kafka.commit().

    `max_msg_timestamp_ms` is the MAX Kafka message timestamp in the
    batch (milliseconds since epoch). Required for the icebox path —
    the sink stamps `_inserted_at` + partition columns from this so
    that idempotent replay produces the same S3 path. -1 sentinel
    triggers a fall-back to current time IF the batch has zero Kafka
    timestamps (shouldn't happen for real data).
    """
    consolidated = _apply_sort(consolidated, cfg)
    # Single dispatch decision — keeps the write-shape choice and the
    # kafka.commit decision structurally coupled. Splitting them invites
    # a regression where one branch is updated but the other isn't.
    is_icebox = cfg.destination == "icebox"

    t0 = time.monotonic()
    if is_icebox:
        # Icebox sink path: pass kafka_offsets + inserted_at through;
        # the sink writes parquet to S3 + POSTs to icebox. No local
        # kafka.commit() — the icebox owns offset commits atomically
        # with its Iceberg snapshot.
        if max_msg_timestamp_ms < 0:
            # No Kafka timestamps in this batch — shouldn't happen for
            # production data (Kafka always stamps), but guard against
            # the test path. Fall back to current time; replay would
            # produce a different value, breaking determinism. Log
            # loudly so operators see the divergence.
            log.warning(
                "_flush: max_msg_timestamp_ms unset; falling back to now() "
                "— idempotent replay determinism is COMPROMISED for this batch"
            )
            inserted_at = datetime.datetime.now(datetime.UTC)
        else:
            inserted_at = datetime.datetime.fromtimestamp(max_msg_timestamp_ms / 1000.0, tz=datetime.UTC)
        _write_with_retry(
            sink,
            consolidated,
            write_kwargs={
                "kafka_offsets": _kafka_offsets_for_icebox(offsets, cfg.topic),
                "inserted_at": inserted_at,
            },
        )
    else:
        _write_with_retry(sink, consolidated)
    write_duration = time.monotonic() - t0

    if is_icebox:
        # Icebox commits Kafka offsets atomically with its Iceberg
        # snapshot; skip the local commit. We DO compute tp_offsets for
        # metric emission below so dashboards keep working.
        tp_offsets = [TopicPartition(topic, partition, offset + 1) for (topic, partition), offset in offsets.items()]
    else:
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
    metrics.records_written_total.inc(pending_records)
    metrics.batches_flushed_total.labels(trigger=trigger).inc()
    server.health.record_flush()

    # Always update committed offset metrics (cheap, local). For icebox
    # this reflects the offset the SINK saw, not necessarily what the
    # icebox has committed yet — the icebox advances Kafka offsets on
    # its own cadence. Operators looking for "committed in Iceberg" should
    # use icebox's GET /v1/status, not this metric.
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
    # if a startup step (make_sink, kafka.create, server.start) raises.
    http = None
    sink = None
    kafka = None
    logger_provider = None

    cfg = config.load()
    # OTLP/HTTP export to PostHog Logs. Returns None when
    # cfg.posthog_project_token is unset (default for local dev). The
    # provider is flushed on the shutdown path so in-flight batches
    # make it out before the process exits.
    logger_provider = logging_config.attach_posthog_otlp(cfg)
    metrics.init(f"{cfg.topic}-{cfg.table_label}", broker_source=cfg.broker_source)

    pending: list[pa.Table] = []
    pending_bytes = 0
    pending_records = 0
    offsets: dict[tuple[str, int], int] = {}  # (topic, partition) -> max offset
    # MAX Kafka message timestamp seen in the current batch, in
    # milliseconds. Used by the icebox sink to stamp _inserted_at +
    # partition columns deterministically — same offsets → same MAX
    # → same partition → same S3 path → idempotent replay. -1 sentinel
    # means "no messages yet". Reset alongside offsets on each flush.
    max_msg_timestamp_ms: int = -1
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
        sink = sink_mod.make_sink(cfg)
        log.info("Sink ready: destination=%s", cfg.destination)
        kafka = consumer.create(cfg)
        log.info("Kafka consumer created, partitions assigned")
        backpressure.init(cfg.consume_batch_size)

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
            timeout = max(remaining, 0.1)

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
                        # Track MAX message timestamp for deterministic
                        # partition stamping in the icebox path.
                        # msg.timestamp() returns (type, value_ms);
                        # we want the value regardless of type since
                        # any monotonic-ish timestamp is fine for
                        # partition-bucket purposes.
                        _ts_type, ts_value_ms = msg.timestamp()
                        if ts_value_ms > max_msg_timestamp_ms:
                            max_msg_timestamp_ms = ts_value_ms

                if values:
                    skipped = 0
                    table = _convert_batch(values)
                    if table is not None:
                        skipped = len(values) - len(table)
                        table = _apply_filter(table, cfg)
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
                    max_msg_timestamp_ms=max_msg_timestamp_ms,
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
                max_msg_timestamp_ms = -1
                metrics.pending_bytes.set(0)
                last_flush = time.monotonic()

    except Exception:
        log.exception("Fatal error in main loop")
        raise
    finally:
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
                    max_msg_timestamp_ms=max_msg_timestamp_ms,
                )
            except Exception:
                log.exception("Final flush failed — data safe in Kafka, will replay on restart")

        # Close in reverse-startup order. Each close is guarded so a partial
        # startup (e.g. make_sink raised) doesn't NameError its way through here.
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
