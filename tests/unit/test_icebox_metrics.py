"""Unit tests for icebox/metrics.py — verify the Prometheus exposition
layer reflects cycle outcomes and request-perimeter counts.

These tests poke the metric objects directly rather than scraping
``/metrics``; the Docker integration test exercises the scrape path
end-to-end against the real image.
"""
from __future__ import annotations

import pytest
from prometheus_client import REGISTRY

from icebox import metrics


def _sample_value(metric_name: str, labels: dict[str, str] | None = None) -> float | None:
    """Look up a single metric's current value out of the global
    REGISTRY. Returns None if the metric / labels are absent."""
    for collector in REGISTRY.collect():
        for sample in collector.samples:
            if sample.name == metric_name:
                if labels is None or sample.labels == labels:
                    return sample.value
    return None


def test_initialize_outcome_counters_emits_zero_for_every_label():
    """Fresh installs must export 0 for every (outcome, result) label,
    not just the labels we've actually observed. Otherwise Grafana
    queries against unseen labels return no data, which is hard to
    distinguish from 'metric not implemented'."""
    metrics.initialize_outcome_counters()
    # Tick outcomes
    for outcome in ("success", "vacuous", "transport_failure", "batch_failure"):
        assert _sample_value("icebox_ticks_total", {"outcome": outcome}) is not None
    # Result states
    for result in ("pending", "committed", "failed"):
        assert _sample_value("icebox_files_count", {"result": result}) is not None
        assert _sample_value("icebox_files_bytes", {"result": result}) is not None


def test_icebox_files_count_gauge_labeled_by_result():
    metrics.ICEBOX_FILES_COUNT.labels(result="pending").set(50)
    metrics.ICEBOX_FILES_COUNT.labels(result="committed").set(1000)
    metrics.ICEBOX_FILES_COUNT.labels(result="failed").set(3)
    assert _sample_value("icebox_files_count", {"result": "pending"}) == 50.0
    assert _sample_value("icebox_files_count", {"result": "committed"}) == 1000.0
    assert _sample_value("icebox_files_count", {"result": "failed"}) == 3.0


def test_icebox_files_oldest_pending_seconds_supports_minus_one_sentinel():
    metrics.ICEBOX_FILES_OLDEST_PENDING_SECONDS.set(-1.0)
    assert _sample_value("icebox_files_oldest_pending_seconds") == -1.0


def test_tick_duration_histogram_observes_into_buckets():
    metrics.ICEBOX_TICK_DURATION_SECONDS.labels(outcome="success").observe(0.05)
    metrics.ICEBOX_TICK_DURATION_SECONDS.labels(outcome="success").observe(2.0)
    # Spot-check a few critical bucket boundaries documented in the
    # histogram definition.
    found = set()
    for collector in REGISTRY.collect():
        for sample in collector.samples:
            if sample.name == "icebox_tick_duration_seconds_bucket":
                if sample.labels.get("outcome") == "success":
                    found.add(sample.labels.get("le", ""))
    assert "0.05" in found
    assert "5.0" in found
    assert "60.0" in found


def test_iceberg_commit_duration_histogram_unlabeled():
    """Iceberg commit timing isn't labeled by outcome — we observe it
    on every commit attempt regardless of success/failure, so we can
    see Lakekeeper p99 cleanly."""
    metrics.ICEBOX_ICEBERG_COMMIT_DURATION_SECONDS.observe(1.2)
    found_le = set()
    for collector in REGISTRY.collect():
        for sample in collector.samples:
            if sample.name == "icebox_iceberg_commit_duration_seconds_bucket":
                found_le.add(sample.labels.get("le", ""))
    assert "1.0" in found_le
    assert "10.0" in found_le


def test_failure_mode_counters_distinct():
    """Each failure mode has its own counter so Grafana can break out
    Lakekeeper outages vs PG outages vs Iceberg timeouts vs genuine
    batch rejections without label collisions."""
    metrics.ICEBOX_LAKEKEEPER_FAILURES_TOTAL.inc()
    metrics.ICEBOX_BATCH_FAILURES_TOTAL.inc(2)
    metrics.ICEBOX_PG_UNREACHABLE_TOTAL.inc(3)
    metrics.ICEBOX_ICEBERG_TIMEOUT_TOTAL.inc()
    assert _sample_value("icebox_lakekeeper_failures_total") is not None
    assert _sample_value("icebox_batch_failures_total") is not None
    assert _sample_value("icebox_pg_unreachable_total") is not None
    assert _sample_value("icebox_iceberg_timeout_total") is not None


def test_last_success_at_set_to_current_time():
    import time as _t
    before = _t.time()
    metrics.ICEBOX_LAST_SUCCESS_AT.set_to_current_time()
    after = _t.time()
    val = _sample_value("icebox_last_success_at")
    assert val is not None
    assert before <= val <= after
