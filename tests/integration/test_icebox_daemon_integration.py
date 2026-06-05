"""Integration tests for the icebox polling daemon against a real PG.

What this file covers (mocks Lakekeeper at the ``commit_data_files``
boundary; PG is real via testcontainers):

  - tick happy path: pending rows → committed, snapshot_id stamped,
    Kafka offsets committed.
  - age filter: rows younger than the filter are NOT picked up.
  - SKIP LOCKED: two daemons running concurrent ticks claim disjoint
    rows; no row is committed twice.
  - transport failure: requests-style exception → rows stay pending,
    no Kafka commit, heartbeat still stamped.
  - batch failure: any other exception → rows marked 'failed', Kafka
    offsets advanced, audit-friendly.
  - heartbeat stamped on every tick exit path (the icebox-stays-up
    invariant).
  - crash mid-tick: simulated by raising in mark_committed; tx rolls
    back, rows revert to pending, next tick re-commits.
  - tick records the iceberg_snapshot_id under the same row id.
"""
from __future__ import annotations

import threading
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
import requests.exceptions

from icebox import daemon as dm
from icebox import iceberg as ib

from .conftest import (
    heartbeat_age_seconds,
    insert_pending_row,
    select_result,
)

pytestmark = pytest.mark.integration


def _deps_with_commit_returning(snapshot_id: int = 12345):
    """DaemonDeps where commit_data_files returns a fixed snapshot id
    and the Kafka commit is captured for assertions."""
    deps = dm.DaemonDeps(
        load_table=lambda: MagicMock(),
        build_data_file=MagicMock(side_effect=lambda **kw: MagicMock()),
        commit_data_files=MagicMock(
            side_effect=lambda table, data_files, **kw: ib.CommitResult(
                snapshot_id=snapshot_id, summary=None,
            ),
        ),
        kafka_admin=MagicMock(),
        kafka_commit_offsets=MagicMock(),
    )
    return deps


def _run_one_tick(cfg, pool, deps, table=None) -> dm.TickResult:
    """Run a single tick inside a fresh transaction, the way the loop
    does. Returns the TickResult for assertions."""
    if table is None:
        table = MagicMock()
    with pool.connection() as conn:
        with conn.transaction():
            return dm.daemon_tick(conn, cfg=cfg, table=table, deps=deps)


# ---------------------------------------------------------------------------
# Happy path + age filter
# ---------------------------------------------------------------------------


def test_tick_commits_pending_rows_returns_rows_for_offset_commit(cfg, pool):
    """Happy path: insert pending rows older than the age filter, run
    one tick, all rows reach result='committed' with the snapshot id
    stamped. The tick surfaces the rows whose Kafka offsets the loop
    will advance AFTER the tx commits — the tick itself does NOT
    call Kafka anymore (B2 / PE BLOCKER)."""
    ids = [
        insert_pending_row(pool, file_path="s3://b/a.parquet", offset=100),
        insert_pending_row(pool, file_path="s3://b/b.parquet", offset=200),
    ]
    deps = _deps_with_commit_returning(snapshot_id=999)

    result = _run_one_tick(cfg, pool, deps)

    assert result.outcome == dm.OUTCOME_SUCCESS
    assert result.file_count == 2
    assert result.snapshot_id == 999

    for rid in ids:
        state, snap = select_result(pool, rid)
        assert state == "committed"
        assert snap == 999

    # Kafka commit moved to daemon_loop (post-tx). The tick must NOT
    # invoke it directly; instead it returns the rows for the loop.
    deps.kafka_commit_offsets.assert_not_called()
    assert sorted(r.id for r in result.rows_to_commit_offsets) == sorted(ids)


def test_vacuous_tick_when_only_young_rows_pending(cfg, pool):
    """Age filter holds rows younger than `age_filter_seconds` back.
    With the fixture's filter at 0.1s, a row inserted at `now()` is
    too young and the tick is vacuous (heartbeat only)."""
    # inserted_at = now() bypasses the conftest helper's default
    # 'now() - 1s' so the row is too young.
    insert_pending_row(
        pool, file_path="s3://b/young.parquet",
        inserted_at=datetime.now(UTC),
    )
    deps = _deps_with_commit_returning()

    result = _run_one_tick(cfg, pool, deps)

    assert result.outcome == dm.OUTCOME_VACUOUS
    deps.commit_data_files.assert_not_called()


# ---------------------------------------------------------------------------
# SKIP LOCKED — two concurrent daemons disjoint
# ---------------------------------------------------------------------------


def test_two_concurrent_ticks_disjoint_via_skip_locked(cfg, pool):
    """Two ticks running concurrently against the same schema must take
    disjoint row slices via SKIP LOCKED. The test forces the overlap
    by gating thread A's `commit_data_files` on an event released
    after thread B has reached and completed its SELECT FOR UPDATE
    SKIP LOCKED. Without SKIP LOCKED, B would block on A's row locks;
    with it, B claims a disjoint slice.

    Both threads use the SAME batch_size so each grabs at most that
    many rows. With 20 pending rows and batch_size=8, A claims 8, B
    claims 8, 4 stay pending."""
    from dataclasses import replace

    for i in range(20):
        insert_pending_row(pool, file_path=f"s3://b/{i:02d}.parquet", offset=100 + i)

    cfg_small = replace(cfg, committer_max_pending_files=8)

    # Event-gated commit_data_files for A: it signals "I'm holding row
    # locks now" (by having reached this point, the SELECT FOR UPDATE
    # has already executed and A's tx holds locks on its 8 rows) and
    # then waits for the main thread to release it. This is the
    # cross-thread signal we use to sequence B's tick.
    a_holding_locks = threading.Event()
    release_a = threading.Event()

    def _commit_a(table, data_files, **kw):
        # At this point A has done SELECT FOR UPDATE SKIP LOCKED and
        # mark_committed has NOT yet run, so A holds row locks on its
        # claimed rows.
        a_holding_locks.set()
        ok = release_a.wait(timeout=10.0)
        if not ok:
            raise RuntimeError("release_a never signalled — test bug")
        return ib.CommitResult(snapshot_id=111, summary=None)

    deps_a = dm.DaemonDeps(
        load_table=lambda: MagicMock(),
        build_data_file=MagicMock(side_effect=lambda **kw: MagicMock()),
        commit_data_files=MagicMock(side_effect=_commit_a),
        kafka_admin=MagicMock(),
        kafka_commit_offsets=MagicMock(),
    )
    deps_b = _deps_with_commit_returning(snapshot_id=222)

    results: dict[str, dm.TickResult] = {}

    def _tick(label: str, deps):
        results[label] = _run_one_tick(cfg_small, pool, deps)

    ta = threading.Thread(target=_tick, args=("a", deps_a), name="tick-a")
    ta.start()

    # Wait until A reports it's holding locks (it's now in _commit_a's
    # wait loop after the SELECT FOR UPDATE has acquired the locks).
    if not a_holding_locks.wait(timeout=10.0):
        release_a.set()
        ta.join(timeout=5.0)
        raise AssertionError("thread A never reached the commit_data_files hook")

    # Now B runs while A's locks are held. SKIP LOCKED is what lets B
    # progress past A's claimed rows.
    tb = threading.Thread(target=_tick, args=("b", deps_b), name="tick-b")
    tb.start()
    tb.join(timeout=15.0)
    assert not tb.is_alive(), "thread B blocked on A's locks (SKIP LOCKED broken?)"

    # B finished its tick (claimed 8 disjoint rows, committed,
    # released its locks). NOW release A.
    release_a.set()
    ta.join(timeout=15.0)
    assert not ta.is_alive(), "thread A never finished"

    assert results["a"].outcome == dm.OUTCOME_SUCCESS
    assert results["b"].outcome == dm.OUTCOME_SUCCESS
    assert results["a"].file_count == 8
    assert results["b"].file_count == 8

    # 16 of 20 rows committed, split by snapshot id; 4 stay pending.
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT iceberg_snapshot_id, COUNT(*) FROM icebox_files "
                "WHERE result='committed' GROUP BY iceberg_snapshot_id "
                "ORDER BY iceberg_snapshot_id"
            )
            counts = dict(cur.fetchall())
            cur.execute(
                "SELECT COUNT(*) FROM icebox_files WHERE result='pending'"
            )
            pending = cur.fetchone()[0]
    assert counts == {111: 8, 222: 8}
    assert pending == 4


# ---------------------------------------------------------------------------
# Transport failure — rows stay pending, no Kafka commit
# ---------------------------------------------------------------------------


def test_transport_failure_keeps_rows_pending_and_heartbeats(cfg, pool):
    """A requests-style transport exception means Lakekeeper is
    unreachable. The daemon must NOT mark rows failed (or committed)
    and must NOT advance Kafka offsets — but it MUST heartbeat. Rows
    stay pending for the next tick to retry."""
    ids = [
        insert_pending_row(pool, file_path="s3://b/x.parquet", offset=100),
        insert_pending_row(pool, file_path="s3://b/y.parquet", offset=200),
    ]
    deps = _deps_with_commit_returning()
    deps.commit_data_files.side_effect = requests.exceptions.ConnectionError(
        "Lakekeeper unreachable"
    )

    result = _run_one_tick(cfg, pool, deps)

    assert result.outcome == dm.OUTCOME_TRANSPORT_FAILURE
    for rid in ids:
        state, snap = select_result(pool, rid)
        assert state == "pending"
        assert snap is None
    deps.kafka_commit_offsets.assert_not_called()
    age = heartbeat_age_seconds(pool)
    assert age is not None
    assert age < 5.0  # stamped very recently


# ---------------------------------------------------------------------------
# Batch failure — rows marked failed, Kafka offsets advanced
# ---------------------------------------------------------------------------


def test_batch_failure_marks_failed_advances_kafka_offsets(cfg, pool):
    """A non-transport exception (validation, internal error) means
    the daemon can't recover by retrying. Mark rows failed, advance
    Kafka offsets past the batch so the writer doesn't replay forever,
    and audit them later via `result='failed'`."""
    ids = [
        insert_pending_row(pool, file_path="s3://b/bad1.parquet", offset=300),
        insert_pending_row(pool, file_path="s3://b/bad2.parquet", offset=400),
    ]
    deps = _deps_with_commit_returning()
    deps.commit_data_files.side_effect = ValueError("malformed data file")

    result = _run_one_tick(cfg, pool, deps)

    assert result.outcome == dm.OUTCOME_BATCH_FAILURE
    for rid in ids:
        state, snap = select_result(pool, rid)
        assert state == "failed"
        assert snap is None

    # Kafka commit moved to daemon_loop (post-tx). The tick surfaces
    # the rows for the loop's offset advance — that's the "make
    # progress" tradeoff for poisoned data, deferred to the loop.
    deps.kafka_commit_offsets.assert_not_called()
    assert sorted(r.id for r in result.rows_to_commit_offsets) == sorted(ids)


# ---------------------------------------------------------------------------
# Crash mid-tick — tx rolls back, rows revert to pending
# ---------------------------------------------------------------------------


def test_crash_mid_tick_rolls_back_rows_stay_pending(cfg, pool):
    """Simulate the daemon dying between Iceberg commit and PG UPDATE
    by making `ps.mark_committed` raise. The outer `with conn.transaction():`
    rolls back; rows revert to pending; next tick re-commits.

    Note: in production this scenario produces a silent duplicate
    manifest entry in Iceberg (verified in tests/unit/test_icebox_iceberg
    + /tmp/dup_repro.py). Downstream UUID dedup tolerates it. This test
    verifies the PG side of the recovery: rows are NOT lost.
    """
    from icebox import postgres_sync as ps

    ids = [insert_pending_row(pool, file_path="s3://b/recover.parquet", offset=500)]
    deps = _deps_with_commit_returning(snapshot_id=42)

    # First tick: poison mark_committed so the tx rolls back AFTER the
    # mocked Iceberg commit succeeds. Mimics the
    # "crash after Iceberg, before PG" window.
    original_mark = ps.mark_committed
    try:
        ps.mark_committed = MagicMock(side_effect=RuntimeError("simulated crash"))
        with pytest.raises(RuntimeError, match="simulated crash"):
            with pool.connection() as conn:
                with conn.transaction():
                    dm.daemon_tick(conn, cfg=cfg, table=MagicMock(), deps=deps)
    finally:
        ps.mark_committed = original_mark

    # Row is back to pending — the tx rollback undid the (would-be) UPDATE.
    state, snap = select_result(pool, ids[0])
    assert state == "pending"
    assert snap is None

    # Re-tick with the real mark_committed restored; row commits.
    result = _run_one_tick(cfg, pool, deps)
    assert result.outcome == dm.OUTCOME_SUCCESS
    state, snap = select_result(pool, ids[0])
    assert state == "committed"
    assert snap == 42


# ---------------------------------------------------------------------------
# Heartbeat — stamped on every tick exit path
# ---------------------------------------------------------------------------


def test_heartbeat_stamped_on_vacuous_tick(cfg, pool):
    """No pending rows, but heartbeat must still fire — "icebox stays
    up no matter what". /healthz checks this timestamp."""
    deps = _deps_with_commit_returning()
    # No rows inserted.
    result = _run_one_tick(cfg, pool, deps)
    assert result.outcome == dm.OUTCOME_VACUOUS
    age = heartbeat_age_seconds(pool)
    assert age is not None
    assert age < 5.0


def test_heartbeat_stamped_on_success(cfg, pool):
    insert_pending_row(pool, file_path="s3://b/heartbeat.parquet")
    deps = _deps_with_commit_returning()
    _run_one_tick(cfg, pool, deps)
    age = heartbeat_age_seconds(pool)
    assert age is not None
    assert age < 5.0


def test_heartbeat_stamped_on_batch_failure(cfg, pool):
    insert_pending_row(pool, file_path="s3://b/heartbeat-fail.parquet")
    deps = _deps_with_commit_returning()
    deps.commit_data_files.side_effect = ValueError("bad")
    _run_one_tick(cfg, pool, deps)
    age = heartbeat_age_seconds(pool)
    assert age is not None
    assert age < 5.0
