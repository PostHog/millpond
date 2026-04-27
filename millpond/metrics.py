from prometheus_client import Counter, Gauge, Histogram


class _AutoCommonLabels:
    """Wrapper that auto-injects common labels (pipeline, broker_source) into .labels() calls."""

    def __init__(self, metric, pipeline: str, broker_source: str):
        self._metric = metric
        self._pipeline = pipeline
        self._broker_source = broker_source

    def labels(self, **kwargs):
        return self._metric.labels(pipeline=self._pipeline, broker_source=self._broker_source, **kwargs)


# --- Raw metric definitions (with pipeline as first label) ---

_records_consumed_total = Counter(
    "millpond_records_consumed_total",
    "Records polled from Kafka",
    ["pipeline", "broker_source", "partition"],
)
_records_written_total = Counter(
    "millpond_records_written_total",
    "Records written to DuckLake",
    ["pipeline", "broker_source"],
)
_batches_flushed_total = Counter(
    "millpond_batches_flushed_total",
    "Flush cycles completed",
    ["pipeline", "broker_source", "trigger"],
)
_records_skipped_total = Counter(
    "millpond_records_skipped_total",
    "Records skipped",
    ["pipeline", "broker_source", "reason"],
)
_errors_total = Counter(
    "millpond_errors_total",
    "Errors by type",
    ["pipeline", "broker_source", "type"],
)

_flush_duration_seconds = Histogram(
    "millpond_flush_duration_seconds",
    "Time per DuckLake write",
    ["pipeline", "broker_source"],
)
_arrow_conversion_seconds = Histogram(
    "millpond_arrow_conversion_seconds",
    "Time to convert JSON to Arrow table",
    ["pipeline", "broker_source"],
)
_flush_size_bytes = Histogram(
    "millpond_flush_size_bytes",
    "Arrow bytes per flush",
    ["pipeline", "broker_source"],
    buckets=[1e6, 5e6, 10e6, 25e6, 50e6, 100e6, 250e6, 500e6, 1e9],
)
_flush_size_records = Histogram(
    "millpond_flush_size_records",
    "Records per flush",
    ["pipeline", "broker_source"],
    buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000],
)

_pending_bytes = Gauge(
    "millpond_pending_bytes",
    "Current pending Arrow bytes awaiting flush",
    ["pipeline", "broker_source"],
)
_buffer_fullness = Gauge(
    "millpond_buffer_fullness",
    "Ratio of pending bytes to flush size (0.0 = empty, 1.0 = flush threshold)",
    ["pipeline", "broker_source"],
)
_consume_batch_size_current = Gauge(
    "millpond_consume_batch_size_current",
    "Current adaptive consume batch size",
    ["pipeline", "broker_source"],
)
_consumer_lag = Gauge(
    "millpond_consumer_lag",
    "Highwater mark minus committed offset",
    ["pipeline", "broker_source", "partition"],
)
_last_committed_offset = Gauge(
    "millpond_last_committed_offset",
    "Last committed offset",
    ["pipeline", "broker_source", "partition"],
)

_schema_columns_added_total = Counter(
    "millpond_schema_columns_added_total",
    "Columns added via schema evolution",
    ["pipeline", "broker_source"],
)
_schema_columns_widened_total = Counter(
    "millpond_schema_columns_widened_total",
    "Columns widened via schema evolution",
    ["pipeline", "broker_source"],
)

# librdkafka internal stats (via statistics.interval.ms callback)
_rdkafka_replyq = Gauge(
    "millpond_rdkafka_replyq",
    "Number of ops waiting for broker response",
    ["pipeline", "broker_source"],
)
_rdkafka_msg_cnt = Gauge(
    "millpond_rdkafka_msg_cnt",
    "Messages in internal producer/consumer queues",
    ["pipeline", "broker_source"],
)
_rdkafka_msg_size = Gauge(
    "millpond_rdkafka_msg_size",
    "Bytes in internal producer/consumer queues",
    ["pipeline", "broker_source"],
)
_rdkafka_broker_rtt_avg = Gauge(
    "millpond_rdkafka_broker_rtt_avg_seconds",
    "Broker round-trip time average",
    ["pipeline", "broker_source", "broker"],
)
_rdkafka_broker_rtt_p99 = Gauge(
    "millpond_rdkafka_broker_rtt_p99_seconds",
    "Broker round-trip time p99",
    ["pipeline", "broker_source", "broker"],
)

# --- Public names (replaced by init() with pipeline-bound instances) ---

records_consumed_total = _records_consumed_total
records_written_total = _records_written_total
batches_flushed_total = _batches_flushed_total
records_skipped_total = _records_skipped_total
errors_total = _errors_total
flush_duration_seconds = _flush_duration_seconds
arrow_conversion_seconds = _arrow_conversion_seconds
flush_size_bytes = _flush_size_bytes
flush_size_records = _flush_size_records
pending_bytes = _pending_bytes
buffer_fullness = _buffer_fullness
consume_batch_size_current = _consume_batch_size_current
consumer_lag = _consumer_lag
last_committed_offset = _last_committed_offset
schema_columns_added_total = _schema_columns_added_total
schema_columns_widened_total = _schema_columns_widened_total
rdkafka_replyq = _rdkafka_replyq
rdkafka_msg_cnt = _rdkafka_msg_cnt
rdkafka_msg_size = _rdkafka_msg_size
rdkafka_broker_rtt_avg = _rdkafka_broker_rtt_avg
rdkafka_broker_rtt_p99 = _rdkafka_broker_rtt_p99


def init(pipeline: str, broker_source: str = ""):
    """Bind all metrics to pipeline and broker_source labels. Must be called once at startup."""
    global records_consumed_total, records_written_total, batches_flushed_total
    global records_skipped_total, errors_total
    global flush_duration_seconds, arrow_conversion_seconds
    global flush_size_bytes, flush_size_records
    global pending_bytes, buffer_fullness, consume_batch_size_current, consumer_lag, last_committed_offset
    global schema_columns_added_total, schema_columns_widened_total
    global rdkafka_replyq, rdkafka_msg_cnt, rdkafka_msg_size
    global rdkafka_broker_rtt_avg, rdkafka_broker_rtt_p99

    bs = broker_source

    # Metrics with additional labels — wrap so .labels() auto-injects common labels
    records_consumed_total = _AutoCommonLabels(_records_consumed_total, pipeline, bs)
    batches_flushed_total = _AutoCommonLabels(_batches_flushed_total, pipeline, bs)
    records_skipped_total = _AutoCommonLabels(_records_skipped_total, pipeline, bs)
    errors_total = _AutoCommonLabels(_errors_total, pipeline, bs)
    consumer_lag = _AutoCommonLabels(_consumer_lag, pipeline, bs)
    last_committed_offset = _AutoCommonLabels(_last_committed_offset, pipeline, bs)
    rdkafka_broker_rtt_avg = _AutoCommonLabels(_rdkafka_broker_rtt_avg, pipeline, bs)
    rdkafka_broker_rtt_p99 = _AutoCommonLabels(_rdkafka_broker_rtt_p99, pipeline, bs)

    # Metrics with no other labels — pre-label to get direct .inc()/.set()/.observe()
    records_written_total = _records_written_total.labels(pipeline=pipeline, broker_source=bs)
    flush_duration_seconds = _flush_duration_seconds.labels(pipeline=pipeline, broker_source=bs)
    arrow_conversion_seconds = _arrow_conversion_seconds.labels(pipeline=pipeline, broker_source=bs)
    flush_size_bytes = _flush_size_bytes.labels(pipeline=pipeline, broker_source=bs)
    flush_size_records = _flush_size_records.labels(pipeline=pipeline, broker_source=bs)
    pending_bytes = _pending_bytes.labels(pipeline=pipeline, broker_source=bs)
    buffer_fullness = _buffer_fullness.labels(pipeline=pipeline, broker_source=bs)
    consume_batch_size_current = _consume_batch_size_current.labels(pipeline=pipeline, broker_source=bs)
    schema_columns_added_total = _schema_columns_added_total.labels(pipeline=pipeline, broker_source=bs)
    schema_columns_widened_total = _schema_columns_widened_total.labels(pipeline=pipeline, broker_source=bs)
    rdkafka_replyq = _rdkafka_replyq.labels(pipeline=pipeline, broker_source=bs)
    rdkafka_msg_cnt = _rdkafka_msg_cnt.labels(pipeline=pipeline, broker_source=bs)
    rdkafka_msg_size = _rdkafka_msg_size.labels(pipeline=pipeline, broker_source=bs)
