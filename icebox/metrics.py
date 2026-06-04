"""Prometheus metrics for the icebox.

Metric naming follows the prometheus convention: lowercased units in the
suffix, `_total` for counters. The shape mirrors what mw-prod-us
Grafana dashboards will consume.

Gauges that reflect PG-derived live state (pending_files,
heartbeat_age, consecutive_failures) are updated inside the ``/metrics``
handler before ``generate_latest`` runs — see ``icebox/api.py``. That
keeps the gauge values fresh on every scrape without a background
thread.

Counters and the cycle-duration histogram are updated from the
committer (``icebox/committer.py``) and from the POST middleware
(``icebox/api.py``).
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# Live gauges — set per /metrics scrape inside the API handler
# ---------------------------------------------------------------------------

PENDING_FILES = Gauge(
    "icebox_pending_files",
    "Number of files staged in PG but not yet claimed by a cycle.",
)

OLDEST_PENDING_AGE_SECONDS = Gauge(
    "icebox_oldest_pending_age_seconds",
    "Age of the oldest unclaimed staged file, in seconds. -1 when no "
    "files are pending (the underlying PG MIN(staged_at) returns NULL).",
)

CONSECUTIVE_FAILURES = Gauge(
    "icebox_consecutive_failures",
    "Number of consecutive committer cycle failures since the last success. "
    "Crosses the degraded threshold at the value set by "
    "ICEBOX_COMMITTER_DEGRADED_FAILURE_THRESHOLD.",
)

COMMITTER_HEARTBEAT_AGE_SECONDS = Gauge(
    "icebox_committer_heartbeat_age_seconds",
    "Seconds since the committer thread last wrote a heartbeat. -1 when "
    "no heartbeat has been written yet (fresh boot). Crosses the "
    "stale-multiple × cadence threshold to trigger 503 on /v1/files.",
)


# ---------------------------------------------------------------------------
# Cycle outcomes — counter + histogram
# ---------------------------------------------------------------------------

# `result` labels:
#   - success                         — cycle committed an Iceberg snapshot
#   - skipped_no_files                — vacuous cycle (no work to do)
#   - skipped_schema_mismatch         — fingerprint check rejected the claim
#   - failed_iceberg_commit           — iceberg commit raised
#   - failed_kafka_commit             — iceberg committed but kafka commit raised
#   - failed_other                    — any other unhandled exception in run_cycle
CYCLES_TOTAL = Counter(
    "icebox_cycles_total",
    "Committer cycles executed, partitioned by outcome.",
    labelnames=("result",),
)

CYCLE_DURATION_SECONDS = Histogram(
    "icebox_cycle_duration_seconds",
    "End-to-end run_cycle duration in seconds, partitioned by outcome.",
    labelnames=("result",),
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

FILES_COMMITTED_TOTAL = Counter(
    "icebox_files_committed_total",
    "Total individual data files committed to Iceberg across all cycles.",
)


# ---------------------------------------------------------------------------
# API perimeter counter — set by middleware
# ---------------------------------------------------------------------------

POST_TOTAL = Counter(
    "icebox_post_total",
    "POST /v1/files responses, partitioned by HTTP status code.",
    labelnames=("status",),
)


# ---------------------------------------------------------------------------
# Schema-fingerprint cache
# ---------------------------------------------------------------------------

SCHEMA_FINGERPRINT_CACHE_MISSES_TOTAL = Counter(
    "icebox_schema_fingerprint_cache_misses_total",
    "Times the writer-claimed schema fingerprint didn't match the cached "
    "value, forcing a catalog refresh. Partitioned by reason:\n"
    "  - cache_stale_after_alter: the catalog refresh matched the writer "
    "    (the cache was just behind reality — normal post-ALTER race).\n"
    "  - fingerprint_mismatch: the refresh STILL doesn't match — the "
    "    writer is presenting a fingerprint the catalog doesn't know. "
    "    Alertable; the cache_stale_after_alter rate is not.",
    labelnames=("reason",),
)
