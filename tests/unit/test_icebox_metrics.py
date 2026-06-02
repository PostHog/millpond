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


def test_pending_files_gauge_set_visible_via_registry():
    metrics.PENDING_FILES.set(7)
    assert _sample_value("icebox_pending_files") == 7.0


def test_oldest_pending_age_seconds_supports_negative_sentinel():
    # Convention: -1 when there are no pending files (read_status
    # returns NULL from MIN(staged_at)).
    metrics.OLDEST_PENDING_AGE_SECONDS.set(-1.0)
    assert _sample_value("icebox_oldest_pending_age_seconds") == -1.0


def test_consecutive_failures_gauge_reflects_set_value():
    metrics.CONSECUTIVE_FAILURES.set(3)
    assert _sample_value("icebox_consecutive_failures") == 3.0


def test_committer_heartbeat_age_seconds_gauge_reflects_set_value():
    metrics.COMMITTER_HEARTBEAT_AGE_SECONDS.set(0.5)
    assert _sample_value("icebox_committer_heartbeat_age_seconds") == 0.5


def test_cycles_total_counter_increments_per_outcome():
    # Snapshot baseline so the assertion is robust against parallel
    # tests in this module that also poke CYCLES_TOTAL.
    before = _sample_value("icebox_cycles_total", {"result": "success"}) or 0.0
    metrics.CYCLES_TOTAL.labels(result="success").inc()
    after = _sample_value("icebox_cycles_total", {"result": "success"})
    assert after == before + 1.0


def test_cycle_duration_histogram_observes_into_buckets():
    # _count tracks observation count; _sum tracks total observed seconds.
    count_before = (
        _sample_value("icebox_cycle_duration_seconds_count", {"result": "success"}) or 0.0
    )
    sum_before = (
        _sample_value("icebox_cycle_duration_seconds_sum", {"result": "success"}) or 0.0
    )
    metrics.CYCLE_DURATION_SECONDS.labels(result="success").observe(1.5)
    count_after = _sample_value("icebox_cycle_duration_seconds_count", {"result": "success"})
    sum_after = _sample_value("icebox_cycle_duration_seconds_sum", {"result": "success"})
    assert count_after == count_before + 1.0
    assert sum_after == pytest.approx((sum_before or 0.0) + 1.5)


def test_files_committed_total_increments_by_amount():
    before = _sample_value("icebox_files_committed_total") or 0.0
    metrics.FILES_COMMITTED_TOTAL.inc(5)
    assert _sample_value("icebox_files_committed_total") == before + 5.0


@pytest.mark.parametrize("status", ["201", "400", "429", "503"])
def test_post_total_labelled_by_status_code(status: str):
    before = _sample_value("icebox_post_total", {"status": status}) or 0.0
    metrics.POST_TOTAL.labels(status=status).inc()
    after = _sample_value("icebox_post_total", {"status": status})
    assert after == before + 1.0


def test_run_cycle_records_metrics_and_resets_cycle_id_var():
    """End-to-end through run_cycle: the wrapper must increment
    ``cycles_total`` and observe into ``cycle_duration_seconds`` on
    every return path, and the ``cycle_id_var`` ContextVar must be
    reset to None on exit (so the next cycle's logs don't inherit a
    stale id)."""
    # Defer imports so the metric module test file stays independent
    # of committer module side effects on import order.
    from icebox import committer as cm
    from icebox.structured_logging import cycle_id_var
    from tests.unit.test_icebox_committer import _cfg, _deps  # type: ignore

    # Snapshot success counter before invoking run_cycle.
    before = _sample_value("icebox_cycles_total", {"result": "success"}) or 0.0

    deps, pool = _deps()
    result = cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    assert result.success is True

    after = _sample_value("icebox_cycles_total", {"result": "success"})
    assert after == before + 1.0, (
        f"run_cycle did not increment icebox_cycles_total{{result='success'}}: "
        f"before={before}, after={after}"
    )

    # Duration must have been observed at least once.
    count = _sample_value("icebox_cycle_duration_seconds_count", {"result": "success"})
    assert count is not None and count >= 1.0

    # ContextVar must be back to None after run_cycle returns.
    assert cycle_id_var.get() is None


def test_run_cycle_no_files_records_skipped_no_files_label():
    """Vacuous cycles must increment cycles_total{result="skipped_no_files"}."""
    from icebox import committer as cm
    from tests.unit.test_icebox_committer import _cfg, _deps  # type: ignore

    before = _sample_value("icebox_cycles_total", {"result": "skipped_no_files"}) or 0.0

    deps, pool = _deps(claimed_ids=[])
    pool.cursor.fetchall.side_effect = [[]]
    cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)

    after = _sample_value("icebox_cycles_total", {"result": "skipped_no_files"})
    assert after == before + 1.0


def test_cycle_duration_histogram_buckets_cover_expected_range():
    """The bucket boundaries must cover sub-second cycles (typical
    healthy path) through 60s (timeout / pathological). A bucket layout
    that's all >=1s would lose visibility on the healthy distribution.
    """
    # Probe by observing values at the edges and confirming the
    # corresponding _bucket sample exists.
    metrics.CYCLE_DURATION_SECONDS.labels(result="success").observe(0.1)
    metrics.CYCLE_DURATION_SECONDS.labels(result="success").observe(30.0)

    found_le_values: set[str] = set()
    for collector in REGISTRY.collect():
        for sample in collector.samples:
            if sample.name == "icebox_cycle_duration_seconds_bucket":
                if sample.labels.get("result") == "success":
                    found_le_values.add(sample.labels.get("le", ""))
    # Spot-check critical boundaries: sub-second visibility (0.1) and
    # the 30s/60s upper region.
    assert "0.1" in found_le_values
    assert "30.0" in found_le_values
    assert "60.0" in found_le_values
