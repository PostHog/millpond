import logging
import signal
import sys
import time

from millpond import config, consumer, logging_config, metrics, server

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

    pending_bytes = 0
    last_flush = time.monotonic()

    try:
        while not shutdown:
            remaining = cfg.flush_interval_s - (time.monotonic() - last_flush)
            timeout = max(remaining, 0.1)

            msgs = kafka.consume(num_messages=cfg.consume_batch_size, timeout=timeout)
            if msgs:
                server.health.record_poll()
                good = 0
                for msg in msgs:
                    if msg.error():
                        metrics.errors_total.labels(type="kafka").inc()
                        log.warning("Kafka error: %s", msg.error())
                        continue
                    good += 1
                    partition = msg.partition()
                    metrics.records_consumed_total.labels(partition=str(partition)).inc()

                if good > 0:
                    # TODO: convert to Arrow, accumulate in pending
                    # For now, just track bytes from raw messages
                    batch_bytes = sum(len(m.value()) for m in msgs if not m.error() and m.value() is not None)
                    pending_bytes += batch_bytes
                    metrics.pending_bytes.set(pending_bytes)

            # Check flush triggers
            elapsed = time.monotonic() - last_flush
            should_flush = pending_bytes > 0 and (pending_bytes >= cfg.flush_size or elapsed >= cfg.flush_interval_s)

            if should_flush:
                # TODO: write to DuckLake, commit offsets
                log.info("Flush triggered: %d bytes, %.1fs elapsed", pending_bytes, elapsed)
                metrics.flush_size_bytes.observe(pending_bytes)
                metrics.batches_flushed_total.inc()
                server.health.record_flush()
                pending_bytes = 0
                metrics.pending_bytes.set(0)
                last_flush = time.monotonic()

    except Exception:
        log.exception("Fatal error in main loop")
        raise
    finally:
        # Graceful shutdown: flush remaining, close consumer
        if pending_bytes > 0:
            log.info("Final flush: %d bytes", pending_bytes)
            # TODO: write to DuckLake, commit offsets
            metrics.flush_size_bytes.observe(pending_bytes)
            metrics.batches_flushed_total.inc()
            server.health.record_flush()

        log.info("Closing consumer")
        kafka.close()
        http.shutdown()
        log.info("millpond shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
