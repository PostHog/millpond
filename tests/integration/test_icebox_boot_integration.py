"""Integration tests for the icebox boot sequence (B1 follow-up).

Verifies that the heartbeat seed in main.main() actually runs between
apply_migrations and the daemon thread start. A regression that drops
lines 154-159 of main.py would leave status.last_committer_heartbeat
NULL until the first tick, racing the kubelet probe → CrashLoopBackOff
on a slow-start Lakekeeper. Without these tests, the probe-handler
tests would still pass (they manually NULL the heartbeat to exercise
the 503 path), but the boot-sequence regression would not.
"""
from __future__ import annotations

import inspect

import pytest

from icebox import main as icebox_main
from icebox import postgres_sync as ps

pytestmark = pytest.mark.integration


def test_main_main_seeds_heartbeat_after_migrations_before_daemon_start():
    """Source-inspection check: main.main() must call update_heartbeat
    AFTER apply_migrations AND BEFORE the daemon thread starts. This is
    the cheapest way to catch a regression that drops the seed step;
    behavioural verification would require running the full main() with
    Kafka + catalog mocks, which is overkill for a 3-line invariant."""
    src = inspect.getsource(icebox_main.main)
    # Order-preserving substring search: each anchor must appear in
    # source order. apply_migrations runs first, then the heartbeat
    # seed (which calls ps.update_heartbeat), then daemon_thread.start().
    anchors = [
        "apply_migrations",
        "ps.update_heartbeat",
        "daemon_thread.start()",
    ]
    positions = []
    cursor = 0
    for anchor in anchors:
        idx = src.find(anchor, cursor)
        assert idx != -1, (
            f"main.main() is missing required anchor {anchor!r}. "
            f"The boot sequence must apply migrations, seed the "
            f"heartbeat, then start the daemon thread — in that order."
        )
        positions.append(idx)
        cursor = idx + len(anchor)
    assert positions == sorted(positions), (
        "main.main() anchors are present but in the wrong order. "
        "Expected: apply_migrations → ps.update_heartbeat (seed) → "
        f"daemon_thread.start(). Got positions {positions}."
    )


def test_seeded_heartbeat_makes_status_row_non_null(cfg, pool):
    """End-to-end against real PG: after the boot prelude operations
    (apply_migrations + update_heartbeat), the status row's
    last_committer_heartbeat column is non-NULL. The probe handler
    reads this column; non-NULL means /healthz returns 200 on a
    freshly booted pod, BEFORE the first tick runs."""
    # The cfg fixture already applied migrations. Clear the heartbeat
    # to simulate the post-migration / pre-seed state.
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE status SET last_committer_heartbeat = NULL WHERE id = 1"
            )
            conn.commit()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_committer_heartbeat FROM status WHERE id = 1"
            )
            assert cur.fetchone()[0] is None  # pre-seed: NULL

    # Run the boot-prelude seed step (same call main.py makes).
    with pool.connection() as conn:
        with conn.transaction():
            ps.update_heartbeat(conn)

    # Post-seed: non-NULL.
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_committer_heartbeat FROM status WHERE id = 1"
            )
            assert cur.fetchone()[0] is not None
