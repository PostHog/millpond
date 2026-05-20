import logging
import signal
import sys
import time
from importlib.metadata import PackageNotFoundError, version

import pyarrow as pa
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


def _convert_batch(values: list[bytes]) -> pa.Table | None:
    """Convert raw message values to Arrow, timing the conversion."""
    t0 = time.monotonic()
    table = arrow_converter.convert(values)
    if table is not None:
        duration = time.monotonic() - t0
        metrics.arrow_conversion_seconds.observe(duration)
    return table


def _write_with_retry(sink, consolidated):
    """Write to the sink with exponential backoff on transient failures."""
    for attempt in range(_WRITE_MAX_RETRIES):
        try:
            sink.write(consolidated)
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


def _flush(sink, cfg, kafka, consolidated, pending_bytes, pending_records, offsets, elapsed, trigger="time"):
    """Write to the sink, commit offsets, update metrics."""
    t0 = time.monotonic()
    _write_with_retry(sink, consolidated)
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
    metrics.records_written_total.inc(pending_records)
    metrics.batches_flushed_total.labels(trigger=trigger).inc()
    server.health.record_flush()

    # Always update committed offset metrics (cheap, local)
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
    logging_config.setup()
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

    cfg = config.load()
    metrics.init(f"{cfg.topic}-{cfg.table_label}", broker_source=cfg.broker_source)

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

                if values:
                    skipped = 0
                    table = _convert_batch(values)
                    if table is not None:
                        skipped = len(values) - len(table)
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
        # Final flush only makes sense if both sink and kafka were created;
        # if startup failed earlier, there's no consumed data to flush.
        if pending_records > 0 and sink is not None and kafka is not None:
            try:
                consolidated = pa.concat_tables(pending, promote_options="default")
                elapsed = time.monotonic() - last_flush
                log.info("Final flush: %d records, %d bytes", len(consolidated), pending_bytes)
                _flush(sink, cfg, kafka, consolidated, pending_bytes, pending_records, offsets, elapsed)
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
        log.info("millpond shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
