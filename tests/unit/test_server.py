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


class TestReadiness:
    """Readiness = liveness = started + actively polling.

    Flush state is irrelevant — a consumer with no incoming data is still
    ready as long as it's in its consume-write loop.
    """

    def test_not_started(self):
        h = _HealthState()
        assert not h.is_ready()

    def test_started(self):
        h = _HealthState()
        h.mark_started()
        assert h.is_ready()

    def test_stale_poll_not_ready(self):
        h = _HealthState(max_poll_age_s=0.01)
        h.mark_started()
        time.sleep(0.02)
        assert not h.is_ready()

    def test_ready_without_any_flush(self):
        """Idle topic — no messages, no flush ever."""
        h = _HealthState()
        h.mark_started()
        h.record_poll()
        assert h.is_ready()

    def test_ready_despite_past_flush(self):
        """Consumer was active, data stopped. Still ready as long as polling."""
        h = _HealthState()
        h.mark_started()
        h.record_flush()
        h.record_poll()
        assert h.is_ready()


class TestHealthBackcompat:
    def test_is_healthy_matches_is_ready(self):
        h = _HealthState()
        h.mark_started()
        assert h.is_healthy() == h.is_ready()


class TestStatusBody:
    def test_before_start(self):
        h = _HealthState()
        body = h.status_body()
        assert "poll=never" in body
        assert "flush=never" in body

    def test_after_start(self):
        h = _HealthState()
        h.mark_started()
        body = h.status_body()
        assert "poll=" in body
        assert "s ago" in body
        assert "flush=never" in body

    def test_after_flush(self):
        h = _HealthState()
        h.mark_started()
        h.record_flush()
        body = h.status_body()
        assert "flush=" in body
        assert "flush=never" not in body
