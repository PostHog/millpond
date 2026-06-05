"""Thread-based timeout wrapper for sync callables.

The polling daemon's tick calls PyIceberg's ``commit_data_files``,
which does not expose a timeout. A wedged Lakekeeper would otherwise
pin the daemon's PG row locks (held across the RPC) indefinitely.
``with_timeout`` runs the callable on a daemon thread and joins with
a wall-clock budget; if the budget expires the call appears to time
out to the caller.

Caveat: Python can't safely kill a thread. If ``with_timeout`` fires,
the original call keeps running in the background. The OS reclaims it
when the process exits — which k8s does whenever the heartbeat goes
stale. This is acceptable for our use case (singleton daemon, k8s-managed
restarts) but the wrapper is NOT a general-purpose cancellation
primitive.
"""
from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def with_timeout(seconds: float, fn: Callable[[], T]) -> T:
    """Run ``fn`` on a daemon thread; raise ``TimeoutError`` if it
    doesn't return in ``seconds``.

    Args:
        seconds: wall-clock budget. Sub-second values are allowed.
        fn: zero-argument callable. Bind args at the call site with a
            ``lambda`` or ``functools.partial``.

    Returns:
        Whatever ``fn`` returned.

    Raises:
        TimeoutError: if the join exceeds ``seconds``. The thread keeps
            running in the background; the OS reclaims it on process
            exit.
        Whatever ``fn`` raised: re-raised in the caller's frame so
            error handling looks the same as a direct call would.
    """
    result: list[T] = []
    exc: list[BaseException] = []

    def runner() -> None:
        try:
            result.append(fn())
        except BaseException as e:  # noqa: BLE001 — we re-raise verbatim
            exc.append(e)

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    t.join(seconds)
    if t.is_alive():
        raise TimeoutError(f"call did not return in {seconds}s")
    if exc:
        raise exc[0]
    # If the thread finished without populating either list, that's a
    # bug in `runner` — should be unreachable.
    if not result:
        raise RuntimeError(
            "with_timeout: thread completed without producing a result or "
            "exception; this is a bug"
        )
    return result[0]
