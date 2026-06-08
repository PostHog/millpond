"""Prometheus metrics for the icebox polling daemon.

Metric naming follows the prometheus convention: lowercased units in
the suffix, `_total` for counters. See
docs/icebox-self-healing-recovery.md "Operational / Metrics".
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# Live state gauges — queried from icebox_files; refreshed every tick via
# daemon.refresh_state_gauges so /metrics scrapes see fresh values.
# ---------------------------------------------------------------------------

ICEBOX_FILES_COUNT = Gauge(
    "icebox_files_count",
    "Rows in icebox_files, partitioned by result. The 'pending' label is "
    "the daemon's backlog signal; 'failed' is the audit queue size; "
    "'committed' trends with throughput.",
    labelnames=("result",),
)

ICEBOX_FILES_OLDEST_PENDING_SECONDS = Gauge(
    "icebox_files_oldest_pending_seconds",
    "Age of the oldest pending row in icebox_files, in seconds. -1 when "
    "the backlog is empty.",
)

ICEBOX_FILES_BYTES = Gauge(
    "icebox_files_bytes",
    "SUM(file_size) over icebox_files, partitioned by result. Operational "
    "signal for 'how much data is stuck' (label='pending'|'failed').",
    labelnames=("result",),
)


# ---------------------------------------------------------------------------
# Per-tick observability
# ---------------------------------------------------------------------------

# Outcome labels — kept in sync with daemon.OUTCOME_*.
#   - success: rows committed to Iceberg, marked committed, offsets advanced.
#   - vacuous: no eligible pending rows.
#   - transport_failure: requests/timeout error talking to Lakekeeper;
#     rows revert to pending via tx rollback.
#   - batch_failure: non-transport error (validation, internal); rows
#     marked failed, offsets advanced past them.
_TICK_OUTCOMES = ("success", "vacuous", "transport_failure", "batch_failure")

ICEBOX_TICK_DURATION_SECONDS = Histogram(
    "icebox_tick_duration_seconds",
    "End-to-end daemon-tick duration, including the SELECT FOR UPDATE, "
    "Iceberg RPC, PG UPDATEs, Kafka commit, and heartbeat. Labeled by "
    "outcome.",
    labelnames=("outcome",),
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

ICEBOX_ICEBERG_COMMIT_DURATION_SECONDS = Histogram(
    "icebox_iceberg_commit_duration_seconds",
    "Time spent inside commit_data_files (the Lakekeeper RPC + local "
    "manifest write). Lets us see Lakekeeper p99 directly without "
    "needing Lakekeeper-side metrics. Captured even on failure.",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

ICEBOX_KAFKA_COMMIT_DURATION_SECONDS = Histogram(
    "icebox_kafka_commit_duration_seconds",
    "Time spent committing Kafka offsets via AdminClient.",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

ICEBOX_BATCH_SIZE = Histogram(
    "icebox_batch_size",
    "Rows committed per non-vacuous tick. Buckets centered on the "
    "BATCH_SIZE knob; consistently hitting the top bucket means we're "
    "under-sized.",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000),
)


# ---------------------------------------------------------------------------
# Throughput counters
# ---------------------------------------------------------------------------

ICEBOX_FILES_COMMITTED_TOTAL = Counter(
    "icebox_files_committed_total",
    "Files (rows) that reached result='committed' across all ticks.",
)

ICEBOX_FILES_FAILED_TOTAL = Counter(
    "icebox_files_failed_total",
    "Files (rows) that reached result='failed' across all ticks. Rate "
    "alert independent of the gauge (which only re-pages on threshold "
    "crossings).",
)

ICEBOX_RECORDS_COMMITTED_TOTAL = Counter(
    "icebox_records_committed_total",
    "SUM(record_count) over rows that reached result='committed'. "
    "Reconciliation signal against expected ingest rate.",
)


# ---------------------------------------------------------------------------
# Liveness / progress
# ---------------------------------------------------------------------------

ICEBOX_LAST_SUCCESS_AT = Gauge(
    "icebox_last_success_at",
    "Unix timestamp of the most recent successful (non-vacuous) tick. "
    "Alert: now() - last_success_at > N AND files_count{result='pending'} > 0 "
    "= 'we have work and aren't doing it.'",
)

ICEBOX_TICKS_TOTAL = Counter(
    "icebox_ticks_total",
    "Daemon ticks executed, labeled by outcome (same labelset as the "
    "duration histogram). Combined with last_success_at gives a clear "
    "'alive but not progressing' signal.",
    labelnames=("outcome",),
)


# ---------------------------------------------------------------------------
# Failure mode counters
# ---------------------------------------------------------------------------

ICEBOX_LAKEKEEPER_FAILURES_TOTAL = Counter(
    "icebox_lakekeeper_failures_total",
    "Transport-level Lakekeeper failures (requests/HTTP errors). Rate "
    "alert > 5/min for 5m = Lakekeeper degraded.",
)

ICEBOX_BATCH_FAILURES_TOTAL = Counter(
    "icebox_batch_failures_total",
    "Non-transport Iceberg commit failures — schema mismatch, malformed "
    "data, PyIceberg internal, etc. Rate alert > 1/min for 5m = "
    "something's actively rejecting batches; page.",
)

ICEBOX_PG_UNREACHABLE_TOTAL = Counter(
    "icebox_pg_unreachable_total",
    "Times the daemon couldn't reach PG (pool checkout or query failure). "
    "Distinguishes PG vs. Lakekeeper outages on Grafana.",
)

ICEBOX_ICEBERG_TIMEOUT_TOTAL = Counter(
    "icebox_iceberg_timeout_total",
    "Times the with_timeout wrapper fired on commit_data_files (separate "
    "from other transport failures). Bumping this is a Lakekeeper "
    "wedge signal — restart pod, investigate.",
)


# ---------------------------------------------------------------------------
# Iceberg table state — read from the snapshot.summary returned by every
# successful iceberg-commit. Free signal (no extra Lakekeeper round-trip,
# no background thread), updated once per successful tick.
#
# RESERVED: these gauges are intentionally UNLABELED. Each icebox process
# serves exactly one (Iceberg namespace, table) pair (see icebox/README.md
# "One icebox per (Iceberg namespace, table)") — single-table invariant.
# The per-(table) axis comes from the icebox.iceberg.* OTLP resource attrs +
# the Prometheus job dimension. If a future change ever runs multiple
# tables per process, adding labels here is a breaking metric-series
# change; plan the migration explicitly.
# ---------------------------------------------------------------------------

ICEBERG_TABLE_DATA_FILES = Gauge(
    "icebox_iceberg_table_data_files",
    "Total data files in the icebox's target Iceberg table after the "
    "last successful tick. Source: snapshot.summary['total-data-files'].",
)

ICEBERG_TABLE_RECORDS = Gauge(
    "icebox_iceberg_table_records",
    "Total records in the icebox's target Iceberg table after the last "
    "successful tick. Source: snapshot.summary['total-records'].",
)

ICEBERG_TABLE_FILES_SIZE_BYTES = Gauge(
    "icebox_iceberg_table_files_size_bytes",
    "Total bytes across all data files in the icebox's target Iceberg "
    "table after the last successful tick. Source: "
    "snapshot.summary['total-files-size'].",
)

# Per-tick deltas — operational signals distinct from cumulative state:
# - added_data_files climbing = compaction debt growing
# - added_records / added_files_size = effective per-tick ingest rate
ICEBERG_TABLE_ADDED_DATA_FILES = Gauge(
    "icebox_iceberg_table_added_data_files",
    "Data files added by the last successful tick. Source: "
    "snapshot.summary['added-data-files'].",
)

ICEBERG_TABLE_ADDED_RECORDS = Gauge(
    "icebox_iceberg_table_added_records",
    "Records added by the last successful tick. Source: "
    "snapshot.summary['added-records'].",
)

ICEBERG_TABLE_ADDED_FILES_SIZE_BYTES = Gauge(
    "icebox_iceberg_table_added_files_size_bytes",
    "Bytes added by the last successful tick. Source: "
    "snapshot.summary['added-files-size'].",
)


def initialize_outcome_counters() -> None:
    """Force-instantiate per-outcome counters/histograms so a fresh
    install exports a value (0) for every label, not just the ones
    we've actually observed. Otherwise Grafana queries against unseen
    labels return no data, which is harder to distinguish from
    'everything is fine' than from 'metric not implemented'.

    Safe to call repeatedly — prometheus_client's labels() is idempotent.
    """
    for outcome in _TICK_OUTCOMES:
        ICEBOX_TICK_DURATION_SECONDS.labels(outcome=outcome)
        ICEBOX_TICKS_TOTAL.labels(outcome=outcome)
    for result in ("pending", "committed", "failed"):
        ICEBOX_FILES_COUNT.labels(result=result)
        ICEBOX_FILES_BYTES.labels(result=result)
