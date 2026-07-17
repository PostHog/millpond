"""Include-values source semantics.

The safety contract under test (see include_values.py):
- additions apply on first sight; removals need M consecutive successful
  ACCEPTED polls absent — failed polls AND refused polls advance nothing;
- a poll failure keeps the last-known set;
- refusal guards: empty result vs a non-empty set, empty initial seed,
  bulk removal (> half of a multi-value set at once), and int↔str type
  flips (which would break the filter's cast and drop whole batches);
- shadow mode reports diffs while `current()` serves only the static set,
  and the prober's set is not reachable through the public surface.

The metrics module is mocked (same approach as test_main's filter tests)
so counter/gauge calls are visible without a Prometheus registry.
"""

from unittest.mock import MagicMock, patch

import pytest

from millpond.include_values import (
    HttpIncludeValues,
    ShadowIncludeValues,
    StaticIncludeValues,
    _normalize,
    build,
)


class TestNormalize:
    def test_all_ints_sorted_deduped(self):
        assert _normalize([3, 1, 2, 3]) == (1, 2, 3)

    def test_strings_sorted_deduped(self):
        assert _normalize(["b", "a", "b"]) == ("a", "b")

    def test_mixed_becomes_strings(self):
        assert _normalize([1, "a"]) == ("1", "a")

    def test_bools_are_not_ints(self):
        # JSON `true` must not silently become 1 and match team_id=1.
        assert _normalize([True, 2]) == ("2", "True")

    def test_nested_rejected(self):
        with pytest.raises(ValueError):
            _normalize([[1, 2]])
        with pytest.raises(ValueError):
            _normalize([{"team": 1}])

    def test_empty(self):
        assert _normalize([]) == ()


class TestStaticIncludeValues:
    def test_passthrough(self):
        src = StaticIncludeValues((1, 2))
        assert src.current() == (1, 2)
        assert src.mode == "static"
        src.start()
        src.stop()
        assert src.current() == (1, 2)

    def test_none_passthrough(self):
        assert StaticIncludeValues(None).current() is None


def _http(**kwargs):
    defaults = dict(
        url="http://example.invalid/values",
        poll_interval_s=60.0,
        removal_confirm_polls=3,
        bootstrap=(1, 2, 3),
    )
    defaults.update(kwargs)
    return HttpIncludeValues(**defaults)


@pytest.fixture(autouse=True)
def _mock_metrics():
    with patch("millpond.include_values.metrics", MagicMock()) as m:
        yield m


def _refusal_reasons(mock_metrics):
    return [
        c.kwargs.get("reason") or (c.args[0] if c.args else None)
        for c in mock_metrics.include_values_refused_total.labels.call_args_list
    ]


class TestPollSemantics:
    def _poll(self, src, remote):
        with patch.object(src, "_fetch", return_value=remote):
            src._poll_once()

    def test_addition_applies_immediately(self):
        src = _http()
        self._poll(src, [1, 2, 3, 4])
        assert src.current() == (1, 2, 3, 4)

    def test_removal_needs_consecutive_absent_polls(self):
        src = _http(removal_confirm_polls=3)
        self._poll(src, [1, 2])  # 3 absent (1/3)
        assert src.current() == (1, 2, 3)
        self._poll(src, [1, 2])  # 3 absent (2/3)
        assert src.current() == (1, 2, 3)
        self._poll(src, [1, 2])  # 3 absent (3/3) -> removed
        assert src.current() == (1, 2)

    def test_reappearance_resets_the_countdown(self):
        src = _http(removal_confirm_polls=3)
        self._poll(src, [1, 2])  # 3 absent (1/3)
        self._poll(src, [1, 2])  # 3 absent (2/3)
        self._poll(src, [1, 2, 3])  # 3 back -> counter reset
        self._poll(src, [1, 2])  # 3 absent (1/3 again)
        self._poll(src, [1, 2])  # (2/3)
        assert src.current() == (1, 2, 3)

    def test_failed_poll_keeps_set_and_freezes_countdown(self):
        src = _http(removal_confirm_polls=2)
        self._poll(src, [1, 2])  # 3 absent (1/2)
        with patch.object(src, "_fetch", side_effect=OSError("boom")):
            with pytest.raises(OSError):
                src._poll_once()
        assert src.current() == (1, 2, 3)
        assert src._absent_polls == {3: 1}
        # The absence countdown resumes where it left off, not from zero.
        self._poll(src, [1, 2])  # (2/2) -> removed
        assert src.current() == (1, 2)

    def test_empty_result_refused_when_set_nonempty(self, _mock_metrics):
        src = _http(removal_confirm_polls=1)
        self._poll(src, [])
        assert src.current() == (1, 2, 3)
        assert "empty" in _refusal_reasons(_mock_metrics)
        self._poll(src, [])
        assert src.current() == (1, 2, 3)

    def test_refused_polls_do_not_precharge_removal(self):
        # REGRESSION (review finding): refused-empty polls must not advance
        # the absence counters. Otherwise N refused polls followed by one
        # junk value confirm-removes every real value instantly.
        src = _http(removal_confirm_polls=5)
        for _ in range(5):
            self._poll(src, [])
        assert src._absent_polls == {}
        self._poll(src, [999])
        # 999 added; real values have only ONE accepted absent poll — far
        # from confirmation. Nothing removed.
        assert src.current() == (1, 2, 3, 999)
        assert all(n == 1 for n in src._absent_polls.values())

    def test_bulk_removal_refused(self, _mock_metrics):
        # Confirming removal of > half of a multi-value set in one poll is
        # refused (endpoint replacing the world != routine churn).
        src = _http(bootstrap=(1, 2, 3, 4), removal_confirm_polls=1)
        self._poll(src, [9])
        # The WHOLE poll is refused — additions too. A world-replacement
        # response is an endpoint bug or a migration; either goes through
        # static config, not an unattended poll.
        assert src.current() == (1, 2, 3, 4)
        assert "bulk_removal" in _refusal_reasons(_mock_metrics)
        # And nothing was committed to the countdown.
        assert src._absent_polls == {}

    def test_removing_one_of_two_is_allowed(self):
        src = _http(bootstrap=(1, 2), removal_confirm_polls=1)
        self._poll(src, [1])
        assert src.current() == (1,)

    def test_type_flip_refused(self, _mock_metrics):
        # REGRESSION (review finding): a junk element flips _normalize to
        # strings; accepting that set would make _apply_filter's cast fail
        # against an int column and drop ENTIRE batches. Refuse instead.
        src = _http(bootstrap=(2, 50689))
        self._poll(src, [2, 50689, "beta-team"])
        assert src.current() == (2, 50689)
        assert "type_flip" in _refusal_reasons(_mock_metrics)
        # And the countdown was not advanced by the refused poll.
        assert src._absent_polls == {}

    def test_type_flip_refused_both_directions(self):
        src = _http(bootstrap=("us-east-1", "eu-central-1"))
        self._poll(src, [1, 2])
        assert src.current() == ("us-east-1", "eu-central-1")  # bootstrap verbatim

    def test_empty_seed_refused_without_bootstrap(self, _mock_metrics):
        # REGRESSION (review finding): an empty remote is never an
        # acceptable FIRST state — accepting () arms the filter with an
        # empty include set and drops every record.
        src = _http(bootstrap=None)
        self._poll(src, [])
        assert src.current() is None
        assert "empty" in _refusal_reasons(_mock_metrics)
        assert not src._synced.is_set()

    def test_no_bootstrap_first_nonempty_poll_seeds(self):
        src = _http(bootstrap=None)
        self._poll(src, [7, 8])
        assert src.current() == (7, 8)
        assert src._synced.is_set()

    def test_int_homogeneity_survives_updates(self):
        src = _http()
        self._poll(src, [1, 2, 3, 9])
        assert all(isinstance(v, int) for v in src.current())

    def test_shadow_reference_diff_gauges(self, _mock_metrics):
        src = _http(shadow_reference=(1, 2, 3))
        self._poll(src, [2, 3, 4])
        _mock_metrics.include_values_shadow_only_static.set.assert_called_with(1)  # {1}
        _mock_metrics.include_values_shadow_only_remote.set.assert_called_with(1)  # {4}

    def test_change_metrics_and_size_gauge(self, _mock_metrics):
        src = _http(bootstrap=(1, 2), removal_confirm_polls=1)
        self._poll(src, [1, 4])  # +4, -2 (1 of 2 allowed)
        actions = str(_mock_metrics.include_values_changes_total.labels.call_args_list)
        assert "add" in actions and "remove" in actions
        _mock_metrics.include_values_size.set.assert_called_with(2)

    def test_pending_removals_gauge_counts_countdown_entries(self, _mock_metrics):
        src = _http(removal_confirm_polls=3)
        self._poll(src, [1, 2])
        _mock_metrics.include_values_pending_removals.set.assert_called_with(1)


class TestStartStop:
    def test_start_blocks_and_raises_without_sync_even_with_bootstrap(self):
        # REGRESSION (review finding): restart amnesia. Proceeding on the
        # static bootstrap after a sync timeout performs de-facto removals
        # (values added since the static list was last touched vanish with
        # zero polls of evidence). Authoritative mode must halt instead.
        src = _http(bootstrap=(1,), poll_interval_s=0.05, startup_timeout_s=0.2)
        with patch.object(src, "_fetch", side_effect=OSError("down")):
            with pytest.raises(RuntimeError, match="no successful poll"):
                src.start()

    def test_start_syncs_from_endpoint(self):
        src = _http(bootstrap=(1,), poll_interval_s=0.05, startup_timeout_s=2.0)
        with patch.object(src, "_fetch", return_value=[1, 2]):
            src.start()
            assert src.current() == (1, 2)
            src.stop()


class TestBuild:
    def _cfg(self, **overrides):
        cfg = MagicMock()
        cfg.filter_values = (1, 2)
        cfg.include_values_url = None
        cfg.include_values_mode = "shadow"
        cfg.include_values_poll_interval_s = 60.0
        cfg.include_values_removal_polls = 5
        cfg.include_values_request_timeout_s = 10.0
        cfg.include_values_startup_timeout_s = 60.0
        cfg.include_values_auth_header_name = None
        cfg.include_values_auth_token = None
        for k, v in overrides.items():
            setattr(cfg, k, v)
        return cfg

    def test_no_url_is_static_only(self):
        src = build(self._cfg())
        assert isinstance(src, StaticIncludeValues)
        assert src.mode == "static"
        assert src.current() == (1, 2)

    def test_shadow_serves_static_and_hides_the_prober(self):
        src = build(self._cfg(include_values_url="http://x/v"))
        assert isinstance(src, ShadowIncludeValues)
        assert src.mode == "shadow"
        assert src.current() == (1, 2)
        # The prober is private; its polled set must not be part of the
        # public surface a future change could mistakenly read.
        assert not hasattr(src, "prober")
        assert src._prober._shadow_reference == (1, 2)

    def test_authoritative_is_http_with_bootstrap(self):
        src = build(self._cfg(include_values_url="http://x/v", include_values_mode="authoritative"))
        assert isinstance(src, HttpIncludeValues)
        assert src.mode == "authoritative"
        assert src.current() == (1, 2)  # bootstrap until first sync
        assert src._shadow_reference is None

    def test_auth_header_threaded(self):
        src = build(
            self._cfg(
                include_values_url="http://x/v",
                include_values_mode="authoritative",
                include_values_auth_header_name="X-Internal-Secret",
                include_values_auth_token="tok",
            )
        )
        assert src._auth_header == ("X-Internal-Secret", "tok")

    def test_shadow_start_does_not_block_on_sync(self):
        src = build(self._cfg(include_values_url="http://x/v"))
        with patch.object(src._prober, "_fetch", side_effect=OSError("down")):
            src.start()  # must return immediately despite the dead endpoint
            assert src.current() == (1, 2)
            src.stop()
