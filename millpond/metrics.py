from prometheus_client import Counter, Gauge, Histogram

# Counters
records_consumed_total = Counter(
    "millpond_records_consumed_total",
    "Records polled from Kafka",
    ["partition"],
)
records_written_total = Counter(
    "millpond_records_written_total",
    "Records written to DuckLake",
)
batches_flushed_total = Counter(
    "millpond_batches_flushed_total",
    "Flush cycles completed",
)
records_skipped_total = Counter(
    "millpond_records_skipped_total",
    "Records skipped",
    ["reason"],
)
errors_total = Counter(
    "millpond_errors_total",
    "Errors by type",
    ["type"],
)

# Histograms
flush_duration_seconds = Histogram(
    "millpond_flush_duration_seconds",
    "Time per DuckLake write",
)
arrow_conversion_seconds = Histogram(
    "millpond_arrow_conversion_seconds",
    "Time to convert JSON to Arrow table",
)
flush_size_bytes = Histogram(
    "millpond_flush_size_bytes",
    "Arrow bytes per flush",
    buckets=[1e6, 5e6, 10e6, 25e6, 50e6, 100e6, 250e6, 500e6, 1e9],
)
flush_size_records = Histogram(
    "millpond_flush_size_records",
    "Records per flush",
    buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000],
)

# Gauges
pending_bytes = Gauge(
    "millpond_pending_bytes",
    "Current pending Arrow bytes awaiting flush",
)
consumer_lag = Gauge(
    "millpond_consumer_lag",
    "Highwater mark minus committed offset",
    ["partition"],
)
last_committed_offset = Gauge(
    "millpond_last_committed_offset",
    "Last committed offset",
    ["partition"],
)

# librdkafka internal stats (via statistics.interval.ms callback)
rdkafka_replyq = Gauge(
    "millpond_rdkafka_replyq",
    "Number of ops waiting for broker response",
)
rdkafka_msg_cnt = Gauge(
    "millpond_rdkafka_msg_cnt",
    "Messages in internal producer/consumer queues",
)
rdkafka_msg_size = Gauge(
    "millpond_rdkafka_msg_size",
    "Bytes in internal producer/consumer queues",
)
rdkafka_broker_rtt_avg = Gauge(
    "millpond_rdkafka_broker_rtt_avg_seconds",
    "Broker round-trip time average",
    ["broker"],
)
rdkafka_broker_rtt_p99 = Gauge(
    "millpond_rdkafka_broker_rtt_p99_seconds",
    "Broker round-trip time p99",
    ["broker"],
)
