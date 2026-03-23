from prometheus_client import Counter, Gauge, Histogram


class _AutoPipelineLabels:
    """Wrapper that auto-injects the pipeline label into .labels() calls."""

    def __init__(self, metric, pipeline: str):
        self._metric = metric
        self._pipeline = pipeline

    def labels(self, **kwargs):
        return self._metric.labels(pipeline=self._pipeline, **kwargs)


# --- Raw metric definitions (with pipeline as first label) ---

_records_consumed_total = Counter(
    "millpond_records_consumed_total",
    "Records polled from Kafka",
    ["pipeline", "partition"],
)
_records_written_total = Counter(
    "millpond_records_written_total",
    "Records written to DuckLake",
    ["pipeline"],
)
_batches_flushed_total = Counter(
    "millpond_batches_flushed_total",
    "Flush cycles completed",
    ["pipeline"],
)
_records_skipped_total = Counter(
    "millpond_records_skipped_total",
    "Records skipped",
    ["pipeline", "reason"],
)
_errors_total = Counter(
    "millpond_errors_total",
    "Errors by type",
    ["pipeline", "type"],
)

_flush_duration_seconds = Histogram(
    "millpond_flush_duration_seconds",
    "Time per DuckLake write",
    ["pipeline"],
)
_arrow_conversion_seconds = Histogram(
    "millpond_arrow_conversion_seconds",
    "Time to convert JSON to Arrow table",
    ["pipeline"],
)
_flush_size_bytes = Histogram(
    "millpond_flush_size_bytes",
    "Arrow bytes per flush",
    ["pipeline"],
    buckets=[1e6, 5e6, 10e6, 25e6, 50e6, 100e6, 250e6, 500e6, 1e9],
)
_flush_size_records = Histogram(
    "millpond_flush_size_records",
    "Records per flush",
    ["pipeline"],
    buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000],
)

_pending_bytes = Gauge(
    "millpond_pending_bytes",
    "Current pending Arrow bytes awaiting flush",
    ["pipeline"],
)
_consumer_lag = Gauge(
    "millpond_consumer_lag",
    "Highwater mark minus committed offset",
    ["pipeline", "partition"],
)
_last_committed_offset = Gauge(
    "millpond_last_committed_offset",
    "Last committed offset",
    ["pipeline", "partition"],
)

_schema_columns_added_total = Counter(
    "millpond_schema_columns_added_total",
    "Columns added via schema evolution",
    ["pipeline"],
)
_schema_columns_widened_total = Counter(
    "millpond_schema_columns_widened_total",
    "Columns widened via schema evolution",
    ["pipeline"],
)

# librdkafka internal stats (via statistics.interval.ms callback)
_rdkafka_replyq = Gauge(
    "millpond_rdkafka_replyq",
    "Number of ops waiting for broker response",
    ["pipeline"],
)
_rdkafka_msg_cnt = Gauge(
    "millpond_rdkafka_msg_cnt",
    "Messages in internal producer/consumer queues",
    ["pipeline"],
)
_rdkafka_msg_size = Gauge(
    "millpond_rdkafka_msg_size",
    "Bytes in internal producer/consumer queues",
    ["pipeline"],
)
_rdkafka_broker_rtt_avg = Gauge(
    "millpond_rdkafka_broker_rtt_avg_seconds",
    "Broker round-trip time average",
    ["pipeline", "broker"],
)
_rdkafka_broker_rtt_p99 = Gauge(
    "millpond_rdkafka_broker_rtt_p99_seconds",
    "Broker round-trip time p99",
    ["pipeline", "broker"],
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
consumer_lag = _consumer_lag
last_committed_offset = _last_committed_offset
schema_columns_added_total = _schema_columns_added_total
schema_columns_widened_total = _schema_columns_widened_total
rdkafka_replyq = _rdkafka_replyq
rdkafka_msg_cnt = _rdkafka_msg_cnt
rdkafka_msg_size = _rdkafka_msg_size
rdkafka_broker_rtt_avg = _rdkafka_broker_rtt_avg
rdkafka_broker_rtt_p99 = _rdkafka_broker_rtt_p99


def init(pipeline: str):
    """Bind all metrics to a pipeline label. Must be called once at startup."""
    global records_consumed_total, records_written_total, batches_flushed_total
    global records_skipped_total, errors_total
    global flush_duration_seconds, arrow_conversion_seconds
    global flush_size_bytes, flush_size_records
    global pending_bytes, consumer_lag, last_committed_offset
    global schema_columns_added_total, schema_columns_widened_total
    global rdkafka_replyq, rdkafka_msg_cnt, rdkafka_msg_size
    global rdkafka_broker_rtt_avg, rdkafka_broker_rtt_p99

    # Metrics with additional labels — wrap so .labels() auto-injects pipeline
    records_consumed_total = _AutoPipelineLabels(_records_consumed_total, pipeline)
    records_skipped_total = _AutoPipelineLabels(_records_skipped_total, pipeline)
    errors_total = _AutoPipelineLabels(_errors_total, pipeline)
    consumer_lag = _AutoPipelineLabels(_consumer_lag, pipeline)
    last_committed_offset = _AutoPipelineLabels(_last_committed_offset, pipeline)
    rdkafka_broker_rtt_avg = _AutoPipelineLabels(_rdkafka_broker_rtt_avg, pipeline)
    rdkafka_broker_rtt_p99 = _AutoPipelineLabels(_rdkafka_broker_rtt_p99, pipeline)

    # Metrics with no other labels — pre-label to get direct .inc()/.set()/.observe()
    records_written_total = _records_written_total.labels(pipeline=pipeline)
    batches_flushed_total = _batches_flushed_total.labels(pipeline=pipeline)
    flush_duration_seconds = _flush_duration_seconds.labels(pipeline=pipeline)
    arrow_conversion_seconds = _arrow_conversion_seconds.labels(pipeline=pipeline)
    flush_size_bytes = _flush_size_bytes.labels(pipeline=pipeline)
    flush_size_records = _flush_size_records.labels(pipeline=pipeline)
    pending_bytes = _pending_bytes.labels(pipeline=pipeline)
    schema_columns_added_total = _schema_columns_added_total.labels(pipeline=pipeline)
    schema_columns_widened_total = _schema_columns_widened_total.labels(pipeline=pipeline)
    rdkafka_replyq = _rdkafka_replyq.labels(pipeline=pipeline)
    rdkafka_msg_cnt = _rdkafka_msg_cnt.labels(pipeline=pipeline)
    rdkafka_msg_size = _rdkafka_msg_size.labels(pipeline=pipeline)
