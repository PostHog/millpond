"""Tests for icebox.timeout.with_timeout.

PyIceberg has no native timeout on commit_data_files; with_timeout is
the daemon's only defence against a wedged Lakekeeper holding row
locks indefinitely. The tests pin:

  - happy path returns the call's value
  - exceptions propagate verbatim
  - over-budget call raises TimeoutError
  - sub-second budgets work
  - the thread is `daemon=True` so it doesn't keep the process alive
    after a timeout
"""
from __future__ import annotations

import threading
import time

import pytest

from icebox.timeout import with_timeout


def test_returns_callable_result():
    assert with_timeout(1.0, lambda: 42) == 42


def test_returns_none_value():
    """`None` is a valid return value. Earlier drafts used a sentinel
    truthiness check that would have silently broken this."""
    assert with_timeout(1.0, lambda: None) is None


def test_propagates_exception():
    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        with_timeout(1.0, lambda: (_ for _ in ()).throw(Boom("nope")))


def test_propagates_base_exception():
    """KeyboardInterrupt etc. should also propagate — we catch and
    re-raise via `BaseException`."""

    def raise_keyboard():
        raise KeyboardInterrupt("simulated")

    with pytest.raises(KeyboardInterrupt):
        with_timeout(1.0, raise_keyboard)


def test_raises_timeout_when_call_exceeds_budget():
    def slow():
        time.sleep(1.0)
        return "should not reach"

    start = time.monotonic()
    with pytest.raises(TimeoutError) as ei:
        with_timeout(0.1, slow)
    elapsed = time.monotonic() - start
    # Allow generous slack on CI runners; the contract is "raised
    # within roughly the budget", not "exactly N ms".
    assert elapsed < 0.5
    assert "0.1" in str(ei.value)


def test_thread_is_daemon_so_timeout_doesnt_block_process_exit():
    """Hold a reference to the runner thread by patching threading.Thread
    just to inspect the daemon flag — this would otherwise be a
    runtime-only invariant."""
    seen: list[threading.Thread] = []
    real_thread_cls = threading.Thread

    class CapturingThread(real_thread_cls):  # type: ignore[misc, valid-type]
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            seen.append(self)

    threading.Thread = CapturingThread  # type: ignore[misc, assignment]
    try:
        with_timeout(1.0, lambda: 1)
    finally:
        threading.Thread = real_thread_cls  # type: ignore[misc, assignment]

    assert len(seen) == 1
    assert seen[0].daemon is True


def test_subsecond_budget():
    """The daemon configures 5s in production, but tests want millisecond
    timeouts to stay fast. Verify sub-second budgets are honored."""
    with pytest.raises(TimeoutError):
        with_timeout(0.01, lambda: time.sleep(0.5))


def test_fast_call_returns_within_budget():
    """A call that finishes well under the budget shouldn't be
    artificially delayed by the wrapper."""
    start = time.monotonic()
    assert with_timeout(5.0, lambda: 7) == 7
    assert time.monotonic() - start < 0.5
