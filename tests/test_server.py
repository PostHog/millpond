import time

from millpond.server import _HealthState


class TestHealthState:
    def test_not_started_is_unhealthy(self):
        h = _HealthState()
        assert not h.is_healthy()

    def test_started_is_healthy(self):
        h = _HealthState()
        h.mark_started()
        assert h.is_healthy()

    def test_stale_poll_is_unhealthy(self):
        h = _HealthState(max_poll_age_s=0.01)
        h.mark_started()
        time.sleep(0.02)
        assert not h.is_healthy()

    def test_poll_refreshes(self):
        h = _HealthState(max_poll_age_s=0.05)
        h.mark_started()
        time.sleep(0.03)
        h.record_poll()
        assert h.is_healthy()

    def test_stale_flush_is_unhealthy(self):
        h = _HealthState(max_flush_age_s=0.01)
        h.mark_started()
        time.sleep(0.02)
        assert not h.is_healthy()

    def test_flush_refreshes(self):
        h = _HealthState(max_flush_age_s=0.05)
        h.mark_started()
        time.sleep(0.03)
        h.record_flush()
        assert h.is_healthy()
