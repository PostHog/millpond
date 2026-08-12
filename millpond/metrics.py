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
_filter_matched_total = Counter(
    "millpond_filter_matched_total",
    "Records kept by the include filter, by matched filter value (team id). "
    "Cardinality is bounded by the include set (static pins + CP discovery).",
    ["pipeline", "broker_source", "value"],
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

# --- Include-values source (see include_values.py for semantics) ---

_include_values_size = Gauge(
    "millpond_include_values_size",
    "Current number of values in the keep-filter include set",
    ["pipeline", "broker_source"],
)
_include_values_last_success_timestamp_seconds = Gauge(
    "millpond_include_values_last_success_timestamp_seconds",
    "Unix time of the last successful include-values poll (alert on age)",
    ["pipeline", "broker_source"],
)
_include_values_pending_removals = Gauge(
    "millpond_include_values_pending_removals",
    "Values in the removal-confirmation countdown (absent but not yet removed)",
    ["pipeline", "broker_source"],
)
_include_values_poll_failures_total = Counter(
    "millpond_include_values_poll_failures_total",
    "Failed include-values polls (the set is kept as-is on failure)",
    ["pipeline", "broker_source"],
)
_include_values_refused_total = Counter(
    "millpond_include_values_refused_total",
    "Successful polls refused by a safety guard (reason: empty|bulk_removal|type_flip)",
    ["pipeline", "broker_source", "reason"],
)
_include_values_mode = Gauge(
    "millpond_include_values_mode",
    "1 for the include-values mode this replica actually runs (static|shadow|authoritative)",
    ["pipeline", "broker_source", "mode"],
)
_include_values_changes_total = Counter(
    "millpond_include_values_changes_total",
    "Applied include-set changes by action (add|remove)",
    ["pipeline", "broker_source", "action"],
)
_include_values_shadow_only_static = Gauge(
    "millpond_include_values_shadow_only_static",
    "Shadow mode: values in the static set but not the polled set",
    ["pipeline", "broker_source"],
)
_include_values_shadow_only_remote = Gauge(
    "millpond_include_values_shadow_only_remote",
    "Shadow mode: values in the polled set but not the static set",
    ["pipeline", "broker_source"],
)
_include_values_pinned = Gauge(
    "millpond_include_values_pinned",
    "Statically-pinned values (permanent floor the endpoint cannot remove)",
    ["pipeline", "broker_source"],
)
_include_values_pinned_only = Gauge(
    "millpond_include_values_pinned_only",
    "Pinned values the endpoint does not currently serve (kept via the pin floor)",
    ["pipeline", "broker_source"],
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
# Counts records that should have been sorted but weren't, broken down by
# why. Distinct from `records_skipped_total` because the records still
# land in the sink — only the sort was skipped, so the operational
# signal is "sort coverage is degraded," not "data is missing."
_sort_skipped_total = Counter(
    "millpond_sort_skipped_total",
    "Records in a flush whose sort step was skipped",
    ["pipeline", "broker_source", "reason"],
)
# Columns pinned to a target type before the sink (see
# arrow_converter.coerce_typed_columns), labelled by target_type. Counts
# column-coercions, not rows. A flatline can mean coercion isn't configured
# (MILLPOND_TYPED_COLUMNS unset), or that the configured columns are absent from
# the batch or already arrive correctly typed — so read it alongside the config
# and errors_total{type="column_coercion"} rather than on its own.
_columns_coerced_total = Counter(
    "millpond_columns_coerced_total",
    "Columns pinned to a target type before write",
    ["pipeline", "broker_source", "target_type"],
)
# Counts payload columns stripped because they collide with a sink-managed
# VARIANT dual-write companion name. Counts column-drop events per flush,
# not records — the records themselves still land (minus the field), so
# this is deliberately not a `records_skipped_total` reason.
_variant_companion_columns_dropped_total = Counter(
    "millpond_variant_companion_columns_dropped_total",
    "Payload columns dropped for colliding with a VARIANT dual-write companion",
    ["pipeline", "broker_source"],
)
# Flushes that fell back to string-only because a value reached the VARIANT
# column that DuckDB could not shred, despite the per-row guard in
# ducklake._variant_projection. Counts completed fallbacks (incremented after
# the string-only write succeeds), not attempts. Expected to stay at zero:
# nonzero means the guard pattern missed a value shape, so the whole batch
# lost its companions and a partly-written Parquet file was abandoned. Read
# alongside errors_total{type="variant_write"}.
_variant_write_fallback_total = Counter(
    "millpond_variant_write_fallback_total",
    "Flushes written string-only after the VARIANT projection failed",
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
filter_matched_total = _filter_matched_total
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
sort_skipped_total = _sort_skipped_total
columns_coerced_total = _columns_coerced_total
variant_companion_columns_dropped_total = _variant_companion_columns_dropped_total
variant_write_fallback_total = _variant_write_fallback_total
include_values_size = _include_values_size
include_values_last_success_timestamp_seconds = _include_values_last_success_timestamp_seconds
include_values_pending_removals = _include_values_pending_removals
include_values_poll_failures_total = _include_values_poll_failures_total
include_values_refused_total = _include_values_refused_total
include_values_mode = _include_values_mode
include_values_changes_total = _include_values_changes_total
include_values_shadow_only_static = _include_values_shadow_only_static
include_values_shadow_only_remote = _include_values_shadow_only_remote
include_values_pinned = _include_values_pinned
include_values_pinned_only = _include_values_pinned_only
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
    global schema_columns_added_total, schema_columns_widened_total, sort_skipped_total
    global columns_coerced_total, variant_companion_columns_dropped_total
    global variant_write_fallback_total
    global include_values_size, include_values_last_success_timestamp_seconds
    global include_values_pending_removals, include_values_poll_failures_total
    global include_values_refused_total, include_values_changes_total, include_values_mode
    global filter_matched_total
    global include_values_shadow_only_static, include_values_shadow_only_remote
    global include_values_pinned, include_values_pinned_only
    global rdkafka_replyq, rdkafka_msg_cnt, rdkafka_msg_size
    global rdkafka_broker_rtt_avg, rdkafka_broker_rtt_p99

    bs = broker_source

    # Metrics with additional labels — wrap so .labels() auto-injects common labels
    records_consumed_total = _AutoCommonLabels(_records_consumed_total, pipeline, bs)
    batches_flushed_total = _AutoCommonLabels(_batches_flushed_total, pipeline, bs)
    records_skipped_total = _AutoCommonLabels(_records_skipped_total, pipeline, bs)
    filter_matched_total = _AutoCommonLabels(_filter_matched_total, pipeline, bs)
    sort_skipped_total = _AutoCommonLabels(_sort_skipped_total, pipeline, bs)
    columns_coerced_total = _AutoCommonLabels(_columns_coerced_total, pipeline, bs)
    include_values_changes_total = _AutoCommonLabels(_include_values_changes_total, pipeline, bs)
    include_values_refused_total = _AutoCommonLabels(_include_values_refused_total, pipeline, bs)
    include_values_mode = _AutoCommonLabels(_include_values_mode, pipeline, bs)
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
    include_values_size = _include_values_size.labels(pipeline=pipeline, broker_source=bs)
    include_values_last_success_timestamp_seconds = _include_values_last_success_timestamp_seconds.labels(
        pipeline=pipeline, broker_source=bs
    )
    include_values_pending_removals = _include_values_pending_removals.labels(pipeline=pipeline, broker_source=bs)
    include_values_poll_failures_total = _include_values_poll_failures_total.labels(pipeline=pipeline, broker_source=bs)
    include_values_shadow_only_static = _include_values_shadow_only_static.labels(pipeline=pipeline, broker_source=bs)
    include_values_shadow_only_remote = _include_values_shadow_only_remote.labels(pipeline=pipeline, broker_source=bs)
    include_values_pinned = _include_values_pinned.labels(pipeline=pipeline, broker_source=bs)
    include_values_pinned_only = _include_values_pinned_only.labels(pipeline=pipeline, broker_source=bs)
    pending_bytes = _pending_bytes.labels(pipeline=pipeline, broker_source=bs)
    buffer_fullness = _buffer_fullness.labels(pipeline=pipeline, broker_source=bs)
    consume_batch_size_current = _consume_batch_size_current.labels(pipeline=pipeline, broker_source=bs)
    schema_columns_added_total = _schema_columns_added_total.labels(pipeline=pipeline, broker_source=bs)
    schema_columns_widened_total = _schema_columns_widened_total.labels(pipeline=pipeline, broker_source=bs)
    variant_companion_columns_dropped_total = _variant_companion_columns_dropped_total.labels(
        pipeline=pipeline, broker_source=bs
    )
    variant_write_fallback_total = _variant_write_fallback_total.labels(pipeline=pipeline, broker_source=bs)
    rdkafka_replyq = _rdkafka_replyq.labels(pipeline=pipeline, broker_source=bs)
    rdkafka_msg_cnt = _rdkafka_msg_cnt.labels(pipeline=pipeline, broker_source=bs)
    rdkafka_msg_size = _rdkafka_msg_size.labels(pipeline=pipeline, broker_source=bs)
