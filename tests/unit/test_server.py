import time

from millpond.server import _HealthState


class TestLiveness:
    def test_not_started(self):
        h = _HealthState()
        assert not h.is_alive()

    def test_started(self):
        h = _HealthState()
        h.mark_started()
        assert h.is_alive()

    def test_stale_poll(self):
        h = _HealthState(max_poll_age_s=0.01)
        h.mark_started()
        time.sleep(0.02)
        assert not h.is_alive()

    def test_poll_refreshes(self):
        h = _HealthState(max_poll_age_s=0.05)
        h.mark_started()
        time.sleep(0.03)
        h.record_poll()
        assert h.is_alive()

    def test_liveness_ignores_flush(self):
        """Liveness doesn't care about flush recency."""
        h = _HealthState(max_flush_age_s=0.01)
        h.mark_started()
        h.record_flush()
        time.sleep(0.02)
        h.record_poll()
        assert h.is_alive()


class TestReadiness:
    def test_not_started(self):
        h = _HealthState()
        assert not h.is_ready()

    def test_started_no_flush(self):
        h = _HealthState()
        h.mark_started()
        assert h.is_ready()

    def test_idle_topic_stays_ready(self):
        """Pod with no messages (no flush ever) stays ready as long as polling."""
        h = _HealthState(max_flush_age_s=0.01)
        h.mark_started()
        time.sleep(0.02)
        h.record_poll()
        assert h.is_ready()

    def test_stale_flush_after_first_flush(self):
        """Once a flush has occurred, stale flush marks not ready."""
        h = _HealthState(max_flush_age_s=0.01)
        h.mark_started()
        h.record_flush()
        time.sleep(0.02)
        h.record_poll()
        assert not h.is_ready()

    def test_flush_refreshes(self):
        h = _HealthState(max_flush_age_s=0.05)
        h.mark_started()
        h.record_flush()
        time.sleep(0.03)
        h.record_flush()
        assert h.is_ready()


class TestHealthBackcompat:
    def test_is_healthy_matches_is_ready(self):
        h = _HealthState()
        h.mark_started()
        assert h.is_healthy() == h.is_ready()
