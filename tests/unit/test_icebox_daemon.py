"""Tests for icebox.daemon — the v6 polling-daemon tick + loop.

Pinned behaviors:

  - Heartbeat fires on EVERY tick-exit path. "Icebox stays up no
    matter what" — surfacing progress is the metrics' job, not the
    process state's.
  - Transport exceptions (requests/timeout/OCC/state-unknown) leave
    rows in `pending` (no UPDATE), do NOT advance Kafka offsets, and
    return OUTCOME_TRANSPORT_FAILURE.
  - Non-transport exceptions mark rows `failed`, ADVANCE Kafka
    offsets past the batch (to unblock the writer), and return
    OUTCOME_BATCH_FAILURE.
  - build_data_file raising counts as batch failure (the row's
    metadata is wrong for the table's current state).
  - Kafka offset commit failures are swallowed with a WARN log; the
    next tick's cumulative-offset semantic covers the gap.
  - Vacuous tick (no eligible rows) heartbeats and returns
    OUTCOME_VACUOUS.

The tests use mocked PG cursors and a mocked DaemonDeps. Real PG +
real Iceberg are out of scope here (integration-test territory).
"""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
import requests.exceptions
from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException

from icebox import daemon as dm
from icebox import iceberg as ib
from icebox import metrics
from icebox import postgres_sync as ps
from icebox.schema import IceboxPendingFileRow


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


def _make_cfg(**overrides):
    """Build a Config-shaped MagicMock that the daemon code accepts."""
    cfg = MagicMock()
    cfg.committer_max_pending_files = 100
    cfg.committer_cadence_seconds = 60
    cfg.age_filter_seconds = 60.0
    cfg.iceberg_timeout_s = 5.0
    cfg.kafka_group_id = "millpond-icebox-events"
    cfg.kafka_topic = "clickhouse_events_json"
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _make_row(id_: int, *, partition: str = "0", offset: int = 100,
              record_count: int = 10, file_size: int = 1024,
              file_path: str | None = None) -> IceboxPendingFileRow:
    return IceboxPendingFileRow(
        id=id_,
        file_path=file_path or f"s3://bucket/{id_}.parquet",
        writer_ordinal=0,
        kafka_offsets={partition: offset},
        partition_values={"day": 19000},
        record_count=record_count,
        file_size=file_size,
        parquet_stats={},
        inserted_at=datetime.now(UTC),
        result="pending",
    )


def _make_deps(*, snapshot_id: int = 12345, commit_raises: BaseException | None = None):
    """Build a DaemonDeps with mocks. `commit_raises`: if set,
    deps.commit_data_files raises this instead of returning."""
    table = MagicMock()

    def _commit(table, data_files, **kw):
        if commit_raises is not None:
            raise commit_raises
        return ib.CommitResult(snapshot_id=snapshot_id, summary=None)

    deps = dm.DaemonDeps(
        load_table=lambda: table,
        build_data_file=MagicMock(side_effect=lambda **kw: MagicMock()),
        commit_data_files=MagicMock(side_effect=_commit),
        kafka_admin=MagicMock(),
        kafka_commit_offsets=MagicMock(),
    )
    return deps, table


def _mock_conn_returning(rows: list[IceboxPendingFileRow]):
    """Build a psycopg.Connection mock whose cursor returns `rows` from
    claim_pending_batch and lets all UPDATE/heartbeat calls succeed."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = lambda self: cursor
    conn.cursor.return_value.__exit__ = lambda self, *a: None
    # claim_pending_batch's fetchall — produces tuples in the schema
    # order CLAIM_PENDING_BATCH_SQL specifies.
    cursor.fetchall.return_value = [
        (
            r.id, r.file_path, r.writer_ordinal, r.kafka_offsets,
            r.partition_values, r.record_count, r.file_size,
            r.parquet_stats, r.inserted_at, r.result, r.result_at,
            r.iceberg_snapshot_id,
        )
        for r in rows
    ]
    return conn, cursor


# ---------------------------------------------------------------------------
# Tick outcome paths
# ---------------------------------------------------------------------------


def test_vacuous_tick_heartbeats_and_returns():
    """No eligible rows: heartbeat, no UPDATEs, no Kafka, OUTCOME_VACUOUS."""
    cfg = _make_cfg()
    deps, table = _make_deps()
    conn, cur = _mock_conn_returning([])

    result = dm.daemon_tick(conn, cfg=cfg, table=table, deps=deps)

    assert result.outcome == dm.OUTCOME_VACUOUS
    assert result.file_count == 0
    # Heartbeat ran (the UPDATE on status table).
    executed_sql = [c[0][0] for c in cur.execute.call_args_list]
    assert ps.UPDATE_HEARTBEAT_SQL in executed_sql
    # No mark_committed / mark_failed.
    assert ps.MARK_COMMITTED_SQL not in executed_sql
    assert ps.MARK_FAILED_SQL not in executed_sql
    # No Kafka offsets committed on vacuous.
    deps.kafka_commit_offsets.assert_not_called()


def test_success_tick_marks_committed_returns_rows_to_commit_offsets():
    """The tick itself no longer commits Kafka offsets — that moved to
    daemon_loop so the offset advance happens AFTER the PG tx commits.
    The tick returns the rows whose offsets should be advanced; the
    loop walks them post-tx."""
    cfg = _make_cfg()
    deps, table = _make_deps(snapshot_id=999)
    rows = [_make_row(1, offset=100), _make_row(2, offset=200)]
    conn, cur = _mock_conn_returning(rows)

    result = dm.daemon_tick(conn, cfg=cfg, table=table, deps=deps)

    assert result.outcome == dm.OUTCOME_SUCCESS
    assert result.file_count == 2
    assert result.snapshot_id == 999
    # The tick MUST NOT touch the Kafka AdminClient — the loop does
    # that after the tx commits. (Q1 / B2 from the PE review.)
    deps.kafka_commit_offsets.assert_not_called()
    # Rows surface in the result for the loop's post-tx Kafka commit.
    assert sorted(r.id for r in result.rows_to_commit_offsets) == [1, 2]

    executed_sql = [c[0][0] for c in cur.execute.call_args_list]
    assert ps.MARK_COMMITTED_SQL in executed_sql
    assert ps.UPDATE_HEARTBEAT_SQL in executed_sql
    mark_call = [
        c for c in cur.execute.call_args_list
        if c[0][0] == ps.MARK_COMMITTED_SQL
    ][0]
    assert mark_call[0][1]["snapshot_id"] == 999
    assert sorted(mark_call[0][1]["ids"]) == [1, 2]


@pytest.mark.parametrize("exc", [
    requests.exceptions.Timeout("read timeout"),
    requests.exceptions.ConnectionError("conn refused"),
    requests.exceptions.HTTPError("500"),
    requests.exceptions.RequestException("generic"),
    CommitFailedException("OCC conflict"),
    CommitStateUnknownException("unknown"),
])
def test_transport_failure_keeps_rows_pending_no_kafka_commit(exc):
    """Transport-class errors: no UPDATE on the rows (they stay pending
    when the tx commits), no Kafka offsets advanced, heartbeat fires,
    return OUTCOME_TRANSPORT_FAILURE."""
    cfg = _make_cfg()
    deps, table = _make_deps(commit_raises=exc)
    rows = [_make_row(1)]
    conn, cur = _mock_conn_returning(rows)

    result = dm.daemon_tick(conn, cfg=cfg, table=table, deps=deps)

    assert result.outcome == dm.OUTCOME_TRANSPORT_FAILURE
    executed_sql = [c[0][0] for c in cur.execute.call_args_list]
    assert ps.MARK_COMMITTED_SQL not in executed_sql
    assert ps.MARK_FAILED_SQL not in executed_sql
    assert ps.UPDATE_HEARTBEAT_SQL in executed_sql
    deps.kafka_commit_offsets.assert_not_called()


def test_timeout_error_classified_as_transport():
    """`with_timeout` raises TimeoutError. The daemon must treat it as
    transport (so rows stay pending) and bump the iceberg_timeout
    counter."""
    cfg = _make_cfg(iceberg_timeout_s=0.05)
    deps, table = _make_deps()

    def slow_commit(table, data_files, **kw):
        import time as _t
        _t.sleep(0.3)
        return ib.CommitResult(snapshot_id=1, summary=None)

    deps.commit_data_files.side_effect = slow_commit
    rows = [_make_row(1)]
    conn, cur = _mock_conn_returning(rows)

    before = metrics.ICEBOX_ICEBERG_TIMEOUT_TOTAL._value.get()
    result = dm.daemon_tick(conn, cfg=cfg, table=table, deps=deps)
    after = metrics.ICEBOX_ICEBERG_TIMEOUT_TOTAL._value.get()

    assert result.outcome == dm.OUTCOME_TRANSPORT_FAILURE
    assert after == before + 1
    executed_sql = [c[0][0] for c in cur.execute.call_args_list]
    assert ps.MARK_COMMITTED_SQL not in executed_sql
    assert ps.MARK_FAILED_SQL not in executed_sql


def test_batch_failure_marks_failed_advances_offsets_and_heartbeats():
    """A non-transport exception → mark rows failed, ADVANCE Kafka
    offsets past them, heartbeat. We can't fix bad data; we choose
    'make progress' over 'preserve every event.'"""
    cfg = _make_cfg()
    deps, table = _make_deps(commit_raises=ValueError("malformed data file"))
    rows = [_make_row(1, offset=300), _make_row(2, offset=400)]
    conn, cur = _mock_conn_returning(rows)

    result = dm.daemon_tick(conn, cfg=cfg, table=table, deps=deps)

    assert result.outcome == dm.OUTCOME_BATCH_FAILURE
    assert result.file_count == 2
    executed_sql = [c[0][0] for c in cur.execute.call_args_list]
    assert ps.MARK_FAILED_SQL in executed_sql
    assert ps.MARK_COMMITTED_SQL not in executed_sql
    assert ps.UPDATE_HEARTBEAT_SQL in executed_sql
    # Kafka commit moved to daemon_loop (post-tx). The tick surfaces
    # the rows whose offsets the loop should advance.
    deps.kafka_commit_offsets.assert_not_called()
    assert sorted(r.id for r in result.rows_to_commit_offsets) == [1, 2]


def test_build_data_file_error_treated_as_batch_failure():
    """A failure constructing the DataFile (e.g., KeyError on a missing
    partition column) is non-transient: the row's metadata is wrong
    for the table's current state, no number of retries will fix it.
    Same handling as a non-transport Iceberg failure."""
    cfg = _make_cfg()
    deps, table = _make_deps()
    deps.build_data_file = MagicMock(side_effect=KeyError("missing partition col"))
    rows = [_make_row(1, offset=500)]
    conn, cur = _mock_conn_returning(rows)

    result = dm.daemon_tick(conn, cfg=cfg, table=table, deps=deps)

    assert result.outcome == dm.OUTCOME_BATCH_FAILURE
    executed_sql = [c[0][0] for c in cur.execute.call_args_list]
    assert ps.MARK_FAILED_SQL in executed_sql
    # The Iceberg call must NOT have been attempted — we failed
    # before even building the DataFiles.
    deps.commit_data_files.assert_not_called()


def test_try_commit_kafka_offsets_swallows_exceptions():
    """`_try_commit_kafka_offsets` (called from daemon_loop AFTER the
    PG tx commits) must NOT re-raise a Kafka AdminClient failure. The
    cumulative-offset semantic covers any gap on the next tick; re-
    raising would crash the loop and lose the heartbeat-stamping
    contract for subsequent ticks."""
    cfg = _make_cfg()
    deps, _ = _make_deps()
    deps.kafka_commit_offsets = MagicMock(side_effect=RuntimeError("broker down"))
    rows = [_make_row(1), _make_row(2)]
    # Should NOT raise.
    dm._try_commit_kafka_offsets(deps, cfg, rows, context="success")
    deps.kafka_commit_offsets.assert_called_once()


# Kept for symmetry: the cycle-era separate-paths test stayed because
# the new pattern still needs to verify the success path doesn't
# accidentally re-invoke the Kafka call from inside the tick. (Below.)
def test_kafka_commit_not_invoked_from_tick_on_success(deprecated_for_loop=None):
    """A Kafka AdminClient hiccup must NOT undo the PG commit. The
    tick body never calls Kafka directly anymore (the loop does, post-
    tx); this is a contract-preservation test in case someone re-adds
    the inline call."""
    cfg = _make_cfg()
    deps, table = _make_deps(snapshot_id=42)
    deps.kafka_commit_offsets = MagicMock(side_effect=RuntimeError("broker down"))
    rows = [_make_row(1)]
    conn, cur = _mock_conn_returning(rows)

    result = dm.daemon_tick(conn, cfg=cfg, table=table, deps=deps)

    assert result.outcome == dm.OUTCOME_SUCCESS
    assert result.snapshot_id == 42
    # Rows were marked committed; Kafka was NOT invoked from the tick.
    executed_sql = [c[0][0] for c in cur.execute.call_args_list]
    assert ps.MARK_COMMITTED_SQL in executed_sql
    deps.kafka_commit_offsets.assert_not_called()


def test_kafka_commit_not_invoked_from_tick_on_batch_failure():
    """Same on the batch-failure branch — the tick must not invoke
    Kafka. The loop does it post-tx using TickResult.rows_to_commit_offsets."""
    cfg = _make_cfg()
    deps, table = _make_deps(commit_raises=ValueError("bad data"))
    deps.kafka_commit_offsets = MagicMock(side_effect=RuntimeError("broker down"))
    rows = [_make_row(1)]
    conn, cur = _mock_conn_returning(rows)

    result = dm.daemon_tick(conn, cfg=cfg, table=table, deps=deps)

    assert result.outcome == dm.OUTCOME_BATCH_FAILURE
    executed_sql = [c[0][0] for c in cur.execute.call_args_list]
    assert ps.UPDATE_HEARTBEAT_SQL in executed_sql
    deps.kafka_commit_offsets.assert_not_called()


# ---------------------------------------------------------------------------
# Per-row batch partitioning (PE-M1 fix)
# ---------------------------------------------------------------------------


def test_build_data_file_partial_failure_only_marks_bad_rows():
    """One bad row in a 3-row batch must NOT poison the other two.
    The good rows are committed; the bad row is marked failed; both
    sets land in the SAME PG tx so the kafka commit (post-tx, in the
    loop) advances offsets past all three."""
    cfg = _make_cfg()
    deps, table = _make_deps(snapshot_id=777)

    # build_data_file raises on row id=2 only.
    def _build(table, file_path, **kw):
        if file_path == "s3://bucket/2.parquet":
            raise KeyError("partition_values missing 'day'")
        return MagicMock()
    deps.build_data_file = MagicMock(side_effect=_build)

    rows = [_make_row(1, offset=100), _make_row(2, offset=200), _make_row(3, offset=300)]
    conn, cur = _mock_conn_returning(rows)

    result = dm.daemon_tick(conn, cfg=cfg, table=table, deps=deps)

    assert result.outcome == dm.OUTCOME_SUCCESS
    assert result.file_count == 2  # 1 + 3 succeeded; 2 failed
    # Iceberg got only the good rows (2 DataFiles).
    assert deps.commit_data_files.call_count == 1
    submitted = deps.commit_data_files.call_args.kwargs["data_files"]
    assert len(submitted) == 2

    executed_sql = [c[0][0] for c in cur.execute.call_args_list]
    assert ps.MARK_COMMITTED_SQL in executed_sql
    assert ps.MARK_FAILED_SQL in executed_sql

    mark_committed = [
        c[0][1] for c in cur.execute.call_args_list
        if c[0][0] == ps.MARK_COMMITTED_SQL
    ][0]
    assert sorted(mark_committed["ids"]) == [1, 3]

    mark_failed = [
        c[0][1] for c in cur.execute.call_args_list
        if c[0][0] == ps.MARK_FAILED_SQL
    ][0]
    assert mark_failed["ids"] == [2]

    # rows_to_commit_offsets covers ALL three rows so the writer's
    # Kafka offsets advance past the bad row too.
    assert sorted(r.id for r in result.rows_to_commit_offsets) == [1, 2, 3]


def test_build_data_file_all_rows_fail_falls_back_to_batch_failure():
    """If EVERY row fails build_data_file, fall through to the regular
    batch-failure path: no Iceberg call attempted, all rows marked
    failed, all offsets advanced."""
    cfg = _make_cfg()
    deps, table = _make_deps()
    deps.build_data_file = MagicMock(side_effect=KeyError("missing partition col"))
    rows = [_make_row(1, offset=400), _make_row(2, offset=500)]
    conn, cur = _mock_conn_returning(rows)

    result = dm.daemon_tick(conn, cfg=cfg, table=table, deps=deps)

    assert result.outcome == dm.OUTCOME_BATCH_FAILURE
    assert result.file_count == 2
    deps.commit_data_files.assert_not_called()
    executed_sql = [c[0][0] for c in cur.execute.call_args_list]
    assert ps.MARK_FAILED_SQL in executed_sql
    assert sorted(r.id for r in result.rows_to_commit_offsets) == [1, 2]


# ---------------------------------------------------------------------------
# Kafka offset commit logic
# ---------------------------------------------------------------------------


def test_try_commit_kafka_offsets_noops_on_empty_rows():
    cfg = _make_cfg()
    deps, _ = _make_deps()
    dm._try_commit_kafka_offsets(deps, cfg, rows=[], context="x")
    deps.kafka_commit_offsets.assert_not_called()


def test_try_commit_kafka_offsets_passes_merged_max():
    """Two rows on the same partition collapse to max offset."""
    cfg = _make_cfg()
    deps, _ = _make_deps()
    rows = [
        _make_row(1, partition="0", offset=50),
        _make_row(2, partition="0", offset=200),
        _make_row(3, partition="1", offset=10),
    ]
    dm._try_commit_kafka_offsets(deps, cfg, rows=rows, context="success")
    deps.kafka_commit_offsets.assert_called_once()
    kw = deps.kafka_commit_offsets.call_args.kwargs
    assert kw["max_offsets"] == {0: 200, 1: 10}


# ---------------------------------------------------------------------------
# refresh_state_gauges
# ---------------------------------------------------------------------------


def test_refresh_state_gauges_populates_counts_and_bytes():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = lambda self: cursor
    conn.cursor.return_value.__exit__ = lambda self, *a: None
    # First fetchall: result, count, sum
    # Second fetchone: oldest pending seconds
    cursor.fetchall.return_value = [
        ("pending", 50, 5_000_000),
        ("committed", 1_000, 100_000_000),
        ("failed", 3, 30_000),
    ]
    cursor.fetchone.return_value = (123.4,)

    dm.refresh_state_gauges(conn)

    assert metrics.ICEBOX_FILES_COUNT.labels(result="pending")._value.get() == 50
    assert metrics.ICEBOX_FILES_COUNT.labels(result="committed")._value.get() == 1_000
    assert metrics.ICEBOX_FILES_COUNT.labels(result="failed")._value.get() == 3
    assert metrics.ICEBOX_FILES_BYTES.labels(result="pending")._value.get() == 5_000_000
    assert metrics.ICEBOX_FILES_OLDEST_PENDING_SECONDS._value.get() == pytest.approx(123.4)


def test_refresh_state_gauges_resets_to_zero_when_label_absent():
    """A result that drops to zero in PG (e.g. an operator deletes all
    `failed` rows) should read as zero in the gauge — not retain its
    last seen non-zero value."""
    metrics.ICEBOX_FILES_COUNT.labels(result="failed").set(99)

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = lambda self: cursor
    conn.cursor.return_value.__exit__ = lambda self, *a: None
    cursor.fetchall.return_value = [("pending", 5, 500)]
    cursor.fetchone.return_value = (10.0,)

    dm.refresh_state_gauges(conn)

    assert metrics.ICEBOX_FILES_COUNT.labels(result="failed")._value.get() == 0
    assert metrics.ICEBOX_FILES_COUNT.labels(result="committed")._value.get() == 0
    assert metrics.ICEBOX_FILES_COUNT.labels(result="pending")._value.get() == 5


def test_refresh_state_gauges_handles_empty_pending():
    """No pending rows → MIN(inserted_at) returns NULL → gauge set to -1."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = lambda self: cursor
    conn.cursor.return_value.__exit__ = lambda self, *a: None
    cursor.fetchall.return_value = [("committed", 100, 0)]
    cursor.fetchone.return_value = (None,)

    dm.refresh_state_gauges(conn)

    assert metrics.ICEBOX_FILES_OLDEST_PENDING_SECONDS._value.get() == -1


# ---------------------------------------------------------------------------
# Loop behavior
# ---------------------------------------------------------------------------


def test_loop_pg_error_increments_counter_and_keeps_going():
    """A PG error inside the tick must NOT crash the loop — bump
    icebox_pg_unreachable_total, sleep, retry. The daemon stays up
    no matter what."""
    import threading as _t

    import psycopg

    cfg = _make_cfg(committer_cadence_seconds=0.05)
    deps, _ = _make_deps()
    pg_pool = MagicMock()
    # First .connection() raises a PG error; second returns a context
    # manager whose entry returns a working conn for the second tick
    # (we set stop_event right after).
    pg_pool.connection.return_value.__enter__.side_effect = psycopg.OperationalError("boom")
    pg_pool.connection.return_value.__exit__ = lambda self, *a: None

    stop = _t.Event()
    # Run the loop for a brief window in a worker thread; set stop_event
    # after we've seen at least one tick.
    before = metrics.ICEBOX_PG_UNREACHABLE_TOTAL._value.get()
    runner = _t.Thread(
        target=dm.daemon_loop,
        kwargs={"cfg": cfg, "pg_pool": pg_pool, "deps": deps, "stop_event": stop},
        daemon=True,
    )
    runner.start()
    # Give the loop a moment to fire at least once.
    import time as _time
    _time.sleep(0.5)
    stop.set()
    runner.join(timeout=2.0)
    after = metrics.ICEBOX_PG_UNREACHABLE_TOTAL._value.get()

    assert after >= before + 1, "PG error must bump icebox_pg_unreachable_total"


def test_loop_runs_tick_and_refresh_state_gauges():
    """One sanity-check pass: the loop opens a pg_pool conn per
    iteration, runs daemon_tick + refresh_state_gauges, and respects
    stop_event."""
    import threading as _t

    cfg = _make_cfg(committer_cadence_seconds=0.01)
    deps = MagicMock()
    deps.load_table.return_value = MagicMock()

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = lambda self: cursor
    conn.cursor.return_value.__exit__ = lambda self, *a: None
    cursor.fetchall.return_value = []  # claim_pending_batch → empty
    cursor.fetchone.return_value = (None,)  # oldest pending NULL
    pg_pool = MagicMock()
    pg_pool.connection.return_value.__enter__ = lambda self: conn
    pg_pool.connection.return_value.__exit__ = lambda self, *a: None

    stop = _t.Event()
    with patch.object(dm, "daemon_tick", wraps=dm.daemon_tick) as tick_spy:
        with patch.object(dm, "refresh_state_gauges", wraps=dm.refresh_state_gauges) as gauges_spy:
            runner = _t.Thread(
                target=dm.daemon_loop,
                kwargs={"cfg": cfg, "pg_pool": pg_pool, "deps": deps, "stop_event": stop},
                daemon=True,
            )
            runner.start()
            import time as _time
            _time.sleep(0.3)
            stop.set()
            runner.join(timeout=2.0)

    assert tick_spy.call_count >= 1
    assert gauges_spy.call_count >= 1


def test_loop_initializes_outcome_counters():
    """On startup the loop must force-instantiate the per-outcome
    counters/histograms so a fresh install exports a value (0) for
    every label, not just observed ones. Otherwise Grafana queries
    return no data for never-fired outcomes."""
    import threading as _t

    cfg = _make_cfg(committer_cadence_seconds=10.0)
    deps = MagicMock()
    deps.load_table.return_value = MagicMock()
    pg_pool = MagicMock()
    pg_pool.connection.side_effect = RuntimeError("no PG; just check the init step")

    stop = _t.Event()
    stop.set()  # exit immediately after init

    with patch.object(metrics, "initialize_outcome_counters") as init_spy:
        dm.daemon_loop(cfg=cfg, pg_pool=pg_pool, deps=deps, stop_event=stop)

    init_spy.assert_called_once()


# ---------------------------------------------------------------------------
# Sanity: transient exceptions tuple is exactly what we claim
# ---------------------------------------------------------------------------


def test_transient_exception_tuple_covers_requests_and_pyiceberg():
    """If PyIceberg adds new transient exception types, or if the
    requests library moves things around, this guard fires. Update
    deliberately."""
    expected = {
        TimeoutError,
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
        requests.exceptions.RequestException,
        CommitFailedException,
        CommitStateUnknownException,
    }
    assert set(dm._TRANSIENT_EXCEPTIONS) == expected


# ---------------------------------------------------------------------------
# NoSuchTableError → bootstrap_table recovery
# ---------------------------------------------------------------------------


def test_loop_calls_bootstrap_table_on_no_such_table_with_pending_row():
    """daemon_loop's NoSuchTableError catch-block must:
      1. peek_oldest_pending_file_path on its own pool conn
      2. invoke deps.bootstrap_table(file_path) when a row exists
      3. NOT crash (next tick will load_table successfully)"""
    import threading as _t
    import time as _time

    from pyiceberg.exceptions import NoSuchTableError

    cfg = _make_cfg(committer_cadence_seconds=0.05)
    deps, _ = _make_deps()
    deps.load_table = MagicMock(side_effect=NoSuchTableError("table not found"))
    deps.bootstrap_table = MagicMock(return_value=MagicMock())

    # PG pool: peek_oldest_pending_file_path issues SELECT → fetchone
    # returns the staged parquet path. Same conn handles the tick's
    # load_table failure (which throws before any cursor is touched).
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = ("s3://b/seed.parquet",)
    conn.cursor.return_value.__enter__ = lambda self: cursor
    conn.cursor.return_value.__exit__ = lambda self, *a: None
    pg_pool = MagicMock()
    pg_pool.connection.return_value.__enter__ = lambda self: conn
    pg_pool.connection.return_value.__exit__ = lambda self, *a: None

    stop = _t.Event()
    runner = _t.Thread(
        target=dm.daemon_loop,
        kwargs={"cfg": cfg, "pg_pool": pg_pool, "deps": deps, "stop_event": stop},
        daemon=True,
    )
    runner.start()
    _time.sleep(0.3)
    stop.set()
    runner.join(timeout=2.0)

    deps.bootstrap_table.assert_called_with("s3://b/seed.parquet")
    assert deps.bootstrap_table.call_count >= 1


def test_loop_no_such_table_with_no_pending_rows_skips_bootstrap():
    """No pending rows yet — we can't infer a schema, so don't call
    bootstrap_table. The next tick will retry; eventually a writer
    flushes and the bootstrap fires."""
    import threading as _t
    import time as _time

    from pyiceberg.exceptions import NoSuchTableError

    cfg = _make_cfg(committer_cadence_seconds=0.05)
    deps, _ = _make_deps()
    deps.load_table = MagicMock(side_effect=NoSuchTableError("table not found"))
    deps.bootstrap_table = MagicMock()

    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = None  # peek → no pending rows
    conn.cursor.return_value.__enter__ = lambda self: cursor
    conn.cursor.return_value.__exit__ = lambda self, *a: None
    pg_pool = MagicMock()
    pg_pool.connection.return_value.__enter__ = lambda self: conn
    pg_pool.connection.return_value.__exit__ = lambda self, *a: None

    stop = _t.Event()
    runner = _t.Thread(
        target=dm.daemon_loop,
        kwargs={"cfg": cfg, "pg_pool": pg_pool, "deps": deps, "stop_event": stop},
        daemon=True,
    )
    runner.start()
    _time.sleep(0.3)
    stop.set()
    runner.join(timeout=2.0)

    deps.bootstrap_table.assert_not_called()


def test_loop_no_such_table_without_bootstrap_dep_logs_and_continues(caplog):
    """If deps.bootstrap_table is None (mis-wired main.py), the loop
    must log a WARNING and NOT crash. The daemon's one job is to stay
    up."""
    import logging as _logging
    import threading as _t
    import time as _time

    from pyiceberg.exceptions import NoSuchTableError

    cfg = _make_cfg(committer_cadence_seconds=0.05)
    deps, _ = _make_deps()
    deps.load_table = MagicMock(side_effect=NoSuchTableError("table not found"))
    deps.bootstrap_table = None  # mis-wired

    pg_pool = MagicMock()
    pg_pool.connection.return_value.__enter__ = lambda self: MagicMock()
    pg_pool.connection.return_value.__exit__ = lambda self, *a: None

    stop = _t.Event()
    runner = _t.Thread(
        target=dm.daemon_loop,
        kwargs={"cfg": cfg, "pg_pool": pg_pool, "deps": deps, "stop_event": stop},
        daemon=True,
    )
    with caplog.at_level(_logging.WARNING, logger="icebox.daemon"):
        runner.start()
        _time.sleep(0.3)
        stop.set()
        runner.join(timeout=2.0)

    assert any(
        "bootstrap_table is None" in rec.message
        for rec in caplog.records
    ), "expected a WARNING when deps.bootstrap_table is missing"
