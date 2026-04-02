"""Adaptive batch sizing based on buffer fullness.

Implements proportional backpressure: when the pending buffer approaches
the flush threshold, reduce the consume batch size to smooth throughput.
When the buffer is mostly empty, consume at full speed.

    fullness = pending_bytes / flush_size          # 0.0 to 1.0+
    batch_size = max(MIN, int(MAX * (1.0 - fullness)))

This handles catchup, steady state, and traffic spikes with a single
code path. No state machine, no mode switching.

NOTE: This is a throughput-smoothing mechanism, not OOM prevention.
The actual memory safety knob is librdkafka's `queued.max.messages.kbytes`
(set in consumer.py), which bounds the internal fetch buffer per partition.
Without that, librdkafka pre-fetches up to 64MB per partition regardless
of how slowly we dequeue.
"""

from millpond import metrics

# Minimum batch size — never go below this to avoid per-call overhead domination
MIN_BATCH_SIZE = 10

# The batch size at zero buffer fullness (max throughput)
# Overridden at init from cfg.consume_batch_size
_max_batch_size: int = 1000


def init(max_batch_size: int) -> None:
    """Set the max batch size from config. Called once at startup."""
    global _max_batch_size
    _max_batch_size = max(max_batch_size, MIN_BATCH_SIZE)


def compute_batch_size(pending_bytes: int, flush_size: int) -> int:
    """Compute the adaptive batch size based on buffer fullness.

    Returns a value between MIN_BATCH_SIZE and _max_batch_size.
    """
    if flush_size <= 0:
        return _max_batch_size

    fullness = pending_bytes / flush_size
    batch_size = max(MIN_BATCH_SIZE, int(_max_batch_size * (1.0 - fullness)))

    metrics.buffer_fullness.set(round(fullness, 3))
    metrics.consume_batch_size_current.set(batch_size)

    return batch_size
