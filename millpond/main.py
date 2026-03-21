import logging
import signal
import sys
import time

import pyarrow as pa

from millpond import arrow_converter, config, consumer, logging_config, metrics, server

log = logging.getLogger(__name__)


def main():
    logging_config.setup()
    log.info("millpond starting")

    cfg = config.load()
    http = server.start()

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
                    table = arrow_converter.convert(values)
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
                consolidated = pa.concat_tables(pending)
                log.info(
                    "Flush: %d records, %d bytes, %d columns, %.1fs elapsed",
                    len(consolidated),
                    pending_bytes,
                    len(consolidated.schema),
                    elapsed,
                )

                # TODO: write to DuckLake, commit offsets
                metrics.flush_size_bytes.observe(pending_bytes)
                metrics.flush_size_records.observe(pending_records)
                metrics.flush_duration_seconds.observe(0)  # TODO: time the actual write
                metrics.records_written_total.inc(pending_records)
                metrics.batches_flushed_total.inc()
                server.health.record_flush()

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
        # Graceful shutdown: flush remaining, close consumer
        if pending_records > 0:
            consolidated = pa.concat_tables(pending)
            log.info("Final flush: %d records, %d bytes", len(consolidated), pending_bytes)
            # TODO: write to DuckLake, commit offsets
            metrics.flush_size_bytes.observe(pending_bytes)
            metrics.flush_size_records.observe(pending_records)
            metrics.records_written_total.inc(pending_records)
            metrics.batches_flushed_total.inc()
            server.health.record_flush()

        log.info("Closing consumer")
        kafka.close()
        http.shutdown()
        log.info("millpond shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
