import logging
import signal
import sys
import time

import pyarrow as pa
from confluent_kafka import TopicPartition

from millpond import arrow_converter, config, consumer, ducklake, logging_config, metrics, schema, server

log = logging.getLogger(__name__)

_LAG_SAMPLE_INTERVAL_S = 60.0  # how often to query watermark offsets for lag metrics
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


def _write_with_retry(db, table_name, consolidated, schema_mgr, partition_by=None):
    """Write to DuckLake with exponential backoff on transient failures."""
    for attempt in range(_WRITE_MAX_RETRIES):
        try:
            ducklake.write(db, table_name, consolidated, schema_mgr, partition_by)
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
            # Invalidate cached schema so retry re-runs evolve() — another pod
            # may have changed the table schema since our last attempt.
            if schema_mgr is not None:
                schema_mgr.invalidate()
            time.sleep(delay)


def _flush(db, cfg, kafka, consolidated, pending_bytes, pending_records, offsets, elapsed, schema_mgr):
    """Write to DuckLake, commit offsets, update metrics."""
    t0 = time.monotonic()
    _write_with_retry(db, cfg.ducklake_table, consolidated, schema_mgr, cfg.partition_by)
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
    metrics.batches_flushed_total.inc()
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
    log.info("millpond starting")

    cfg = config.load()
    metrics.init(f"{cfg.topic}-{cfg.ducklake_table}")
    http = server.start()

    # No connection recovery logic — if DuckLake/Postgres fails, the pod crashes
    # and K8s restarts it. Reconnection adds complexity for no benefit when the
    # restart path already handles offset replay correctly.
    db = ducklake.connect(cfg)
    schema_mgr = schema.SchemaManager(db, cfg.ducklake_table)
    kafka = consumer.create(cfg)
    server.health.mark_started()

    shutdown = False

    def on_signal(signum, _frame):
        nonlocal shutdown
        log.info("Received signal %s, shutting down", signal.Signals(signum).name)
        shutdown = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    log.info("millpond ready, entering main loop")

    pending: list[pa.Table] = []
    pending_bytes = 0
    pending_records = 0
    offsets: dict[tuple[str, int], int] = {}  # (topic, partition) -> max offset
    last_flush = time.monotonic()
    last_lag_sample = 0.0  # force immediate first sample

    try:
        while not shutdown:
            remaining = cfg.flush_interval_s - (time.monotonic() - last_flush)
            timeout = max(remaining, 0.1)

            msgs = kafka.consume(num_messages=cfg.consume_batch_size, timeout=timeout)
            if msgs:
                server.health.record_poll()
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
            should_flush = pending_records > 0 and (pending_bytes >= cfg.flush_size or elapsed >= cfg.flush_interval_s)

            if should_flush:
                consolidated = pa.concat_tables(pending, promote_options="default")
                _flush(db, cfg, kafka, consolidated, pending_bytes, pending_records, offsets, elapsed, schema_mgr)

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
        if pending_records > 0:
            try:
                consolidated = pa.concat_tables(pending, promote_options="default")
                elapsed = time.monotonic() - last_flush
                log.info("Final flush: %d records, %d bytes", len(consolidated), pending_bytes)
                _flush(db, cfg, kafka, consolidated, pending_bytes, pending_records, offsets, elapsed, schema_mgr)
            except Exception:
                log.exception("Final flush failed — data safe in Kafka, will replay on restart")

        log.info("Closing consumer")
        kafka.close()
        db.close()
        http.shutdown()
        log.info("millpond shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
