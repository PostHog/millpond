"""Tests for icebox.committer — the cycle state machine.

The committer is the load-bearing module: it owns the "Iceberg snapshot
landed ≡ Kafka offsets advanced" invariant. These tests verify the
state-machine transitions at every step + the recovery path's three
branches.

We mock PG via a tiny FakePool that records every transaction, and
mock the iceberg/kafka deps via callable injection in CommitterDeps.
"""
from __future__ import annotations

import inspect
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import UUID, uuid4

from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

from icebox import committer as cm
from icebox.config import Config
from icebox.schema import CommitCycleRow
from shared.fingerprint import schema_fingerprint

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _cfg() -> Config:
    """Minimal config — committer doesn't touch PG host etc., only the
    cadence/budget/group_id/topic fields."""
    return Config(
        pg_host="x", pg_port=5432, pg_database="x", pg_username="x", pg_password="x",
        pg_sslmode="disable", pg_schema="icebox",
        asyncpg_pool_min=1, asyncpg_pool_max=2,
        psycopg_pool_min=1, psycopg_pool_max=1,
        iceberg_catalog_uri="x", iceberg_warehouse="x", iceberg_namespace="kafka", iceberg_table="events",
        kafka_bootstrap_servers="x", kafka_topic="events",
        kafka_group_id="grp", kafka_extra_config_json="{}",
        committer_cadence_seconds=60,
        committer_max_pending_files=1000,
        committer_degraded_failure_threshold=2,
        committer_heartbeat_stale_multiple=3.0,
        api_host="0.0.0.0", api_port=8000, log_level="INFO",
    )


def _schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="event", field_type=StringType(), required=True),
        NestedField(field_id=2, name="year", field_type=IntegerType(), required=True),
    )


class FakePool:
    """A psycopg-shaped pool that models DISTINCT connections for
    `getconn()` (the advisory-lock conn) vs `connection()` (cycle work).

    Why distinct: the production semantic is "the lock is held on a
    specific connection's session for the lifetime of the committer
    thread." If a regression hands the lock conn out to a cycle's
    `connection()` block (or vice versa), the lock could be released
    mid-cycle when the with-block exits. A single-connection fake
    can't catch this.

    Tracks:
      - `getconn_calls`: count of getconn invocations.
      - `putconn_calls`: list of conns returned (for ordering assertions).
      - `lock_conn` is the conn returned by getconn(); always the same
        object so the test can verify it's NEVER returned via the
        `connection()` path.
      - `cycle_conn` is the conn yielded by `with pool.connection() as
        conn:` — DIFFERENT from lock_conn.

    Records all SQL via:
      - `lock_cursor.execute`: SQL run via the lock_conn (e.g.,
        `pg_try_advisory_lock`, `pg_advisory_unlock`)
      - `cycle_cursor.execute`: SQL run via cycle/heartbeat conns
        (e.g., `update_heartbeat`, `claim_files`, `mark_*`)
      - `cursor.execute` (legacy): an aggregated view that combines
        both — kept for backward-compat with existing assertion code
        that just checks "did this SQL appear anywhere."
    """

    def __init__(self):
        @contextmanager
        def _tx_ctx():
            yield

        # Build the two distinct mock connections
        self.lock_conn = MagicMock()
        self.cycle_conn = MagicMock()
        self.lock_cursor = MagicMock()
        self.cycle_cursor = MagicMock()

        # Default fetchone for the lock cursor: True (acquire succeeds)
        self.lock_cursor.fetchone.return_value = (True,)
        # Default fetchall for the cycle cursor: empty (no rows)
        self.cycle_cursor.fetchall.return_value = []
        self.cycle_cursor.fetchone.return_value = None

        # Wire each connection's cursor() to return its dedicated cursor
        def _make_cursor_side_effect(cursor):
            @contextmanager
            def _cursor_ctx():
                yield cursor
            return _cursor_ctx

        self.lock_conn.cursor.side_effect = _make_cursor_side_effect(self.lock_cursor)
        self.cycle_conn.cursor.side_effect = _make_cursor_side_effect(self.cycle_cursor)
        self.lock_conn.transaction.side_effect = _tx_ctx
        self.cycle_conn.transaction.side_effect = _tx_ctx

        # Aggregated views — most existing tests assert against this
        # without caring which connection ran the SQL. Provide via a
        # property so they always see the combined log.
        # We use a single shared mock with side_effect that just
        # forwards to whichever side ran.
        self.conn = self.cycle_conn  # back-compat: most tests use .conn for cycle work
        self.cursor = self._AggregatingCursor(self.lock_cursor, self.cycle_cursor)

        self.getconn_calls = 0
        self.putconn_calls: list = []

    class _AggregatingCursor:
        """A view over both real cursors that reports execute calls
        across BOTH in chronological order. Lets tests assert "this
        SQL appeared SOMEWHERE in the flow" without having to know
        which connection ran it."""
        def __init__(self, lock_cursor, cycle_cursor):
            self._lock = lock_cursor
            self._cycle = cycle_cursor

        @property
        def execute(self):
            # Synthetic execute that exposes call_args_list as the
            # union of both, ordered by call sequence.
            agg = MagicMock()
            # Combine and sort by the mock's call sequence number,
            # which is what mock_calls records. Since we can't easily
            # synthesize a global ordering across two separate mocks,
            # we just concatenate (lock SQLs first, cycle SQLs second).
            # In practice, the production flow runs the lock SQL FIRST
            # (at startup, on the lock_conn) and cycle SQL LATER (per
            # cycle, on pool-managed conns), so concatenation reflects
            # the actual order.
            agg.call_args_list = list(self._lock.execute.call_args_list) + list(
                self._cycle.execute.call_args_list
            )
            return agg

        @property
        def fetchall(self):
            return self._cycle.fetchall

        @property
        def fetchone(self):
            return self._cycle.fetchone

    @contextmanager
    def connection(self):
        """Cycle-work conn — DISTINCT from lock_conn."""
        yield self.cycle_conn

    def getconn(self):
        """The advisory-lock dedicated conn."""
        self.getconn_calls += 1
        return self.lock_conn

    def putconn(self, conn):
        """Track which conn is being returned. A regression that
        passes the wrong conn surfaces in the recorded sequence."""
        self.putconn_calls.append(conn)


def _file_row(
    *,
    file_id: int,
    fp: str,
    kafka_offsets: dict[str, int] | None = None,
    partition_values: dict[str, int] | None = None,
) -> tuple:
    """Mimic the row shape SELECTed by files_for_cycle."""
    now = datetime.now(UTC)
    return (
        file_id,
        f"s3://b/file-{file_id}.parquet",
        0,  # writer_ordinal
        kafka_offsets if kafka_offsets is not None else {"0": 100},
        partition_values if partition_values is not None else {"year": 2026, "month": 6, "day": 1, "hour": 14},
        1000,  # record_count
        4096,  # file_size
        "v1",  # schema_version
        fp,  # schema_fingerprint
        {"column_sizes": {"1": 100}, "value_counts": {"1": 1000},
         "null_value_counts": {"1": 0}, "lower_bounds": {"1": 1},
         "upper_bounds": {"1": 1000}},  # parquet_stats
        None,  # cycle_id
        now,  # staged_at
        None,  # committed_at
        None,  # iceberg_snapshot_id
    )


def _deps(
    *,
    table_schema: Schema | None = None,
    claimed_ids: list[int] | None = None,
    files: list[tuple] | None = None,
    iceberg_commit_raises: Exception | None = None,
    kafka_commit_raises: Exception | None = None,
    iceberg_snapshot_id: int = 12345,
    snapshot_log_lookup_result: int | None = None,
):
    """Build a CommitterDeps + a FakePool wired together for tests."""
    table = MagicMock()
    table.schema.return_value = table_schema or _schema()

    pool = FakePool()
    claimed = claimed_ids or [1, 2, 3]
    file_rows = files if files is not None else [
        _file_row(file_id=i, fp=schema_fingerprint(table.schema())) for i in claimed
    ]

    # claim_files calls cur.fetchall() once
    # files_for_cycle calls cur.fetchall() once
    pool.cursor.fetchall.side_effect = [
        [(i,) for i in claimed],  # claim_files
        file_rows,  # files_for_cycle
    ]

    deps = cm.CommitterDeps(
        load_table=lambda: table,
        commit_data_files=MagicMock(
            return_value=(iceberg_snapshot_id, None),
            side_effect=iceberg_commit_raises,
        ),
        find_snapshot_for_cycle=MagicMock(return_value=snapshot_log_lookup_result),
        build_data_file=MagicMock(return_value=MagicMock()),
        kafka_admin=MagicMock(),
        kafka_commit_offsets=MagicMock(side_effect=kafka_commit_raises),
    )
    return deps, pool


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_run_cycle_happy_path():
    deps, pool = _deps()
    result = cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    assert result.success is True
    assert result.skipped_reason is None
    assert result.iceberg_snapshot_id == 12345
    assert result.file_count == 3
    assert result.kafka_offsets_committed  # non-empty
    deps.commit_data_files.assert_called_once()
    deps.kafka_commit_offsets.assert_called_once()


def test_run_cycle_sets_iceberg_table_gauges_from_snapshot_summary():
    """When commit_data_files returns a summary with the spec keys,
    the committer must surface them on the icebox.* table-state
    Gauges. Operators want a zero-thread, zero-Lakekeeper-poll signal
    of table growth + compaction state."""
    from icebox.metrics import (
        ICEBERG_TABLE_DATA_FILES,
        ICEBERG_TABLE_FILES_SIZE_BYTES,
        ICEBERG_TABLE_RECORDS,
    )

    deps, pool = _deps(iceberg_snapshot_id=12345)
    # commit_data_files now returns (snapshot_id, summary). Override the
    # _deps default so the test injects realistic Iceberg-spec values.
    deps.commit_data_files = MagicMock(
        return_value=(
            12345,
            {
                "total-data-files": "42",
                "total-records": "1000000",
                "total-files-size": "987654321",
                "operation": "append",
            },
        )
    )

    result = cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    assert result.success is True
    assert ICEBERG_TABLE_DATA_FILES._value.get() == 42.0
    assert ICEBERG_TABLE_RECORDS._value.get() == 1000000.0
    assert ICEBERG_TABLE_FILES_SIZE_BYTES._value.get() == 987654321.0


def test_run_cycle_handles_none_summary_without_touching_gauges():
    """If commit_data_files returns summary=None (PyIceberg API drift
    safety net), the committer must not raise and the gauges keep
    their previous value (which is still the truth — table state
    hasn't changed)."""
    from icebox.metrics import ICEBERG_TABLE_DATA_FILES

    # Pre-set the gauge so we can detect any clobber to 0.
    ICEBERG_TABLE_DATA_FILES.set(999)

    deps, pool = _deps(iceberg_snapshot_id=12345)
    deps.commit_data_files = MagicMock(return_value=(12345, None))

    result = cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    assert result.success is True
    # Gauge unchanged — last successful value preserved.
    assert ICEBERG_TABLE_DATA_FILES._value.get() == 999.0


def test_run_cycle_no_files_marks_vacuous_success():
    """No claimed files → mark heartbeat + return without touching
    Iceberg or Kafka. This must NOT consume a degraded-failures slot."""
    deps, pool = _deps(claimed_ids=[])
    pool.cursor.fetchall.side_effect = [
        [],  # claim_files: empty
    ]
    result = cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    assert result.success is True
    assert result.skipped_reason == "no_files"
    deps.commit_data_files.assert_not_called()
    deps.kafka_commit_offsets.assert_not_called()


# ---------------------------------------------------------------------------
# Failure paths
# ---------------------------------------------------------------------------


def test_run_cycle_iceberg_failure_releases_claims():
    """Iceberg commit raises → we release the file claims so the next
    cycle can re-batch them. We MUST NOT advance Kafka offsets."""
    deps, pool = _deps(iceberg_commit_raises=RuntimeError("catalog 503"))
    result = cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    assert result.success is False
    assert "iceberg-commit failed" in (result.error or "")
    deps.kafka_commit_offsets.assert_not_called()
    # release_cycle_claim was executed (one of the cursor.execute calls)
    executed_sqls = [c[0][0] for c in pool.cursor.execute.call_args_list]
    from icebox.postgres_sync import RECORD_FAILURE_SQL, RELEASE_CYCLE_CLAIM_SQL
    assert RELEASE_CYCLE_CLAIM_SQL in executed_sqls
    assert RECORD_FAILURE_SQL in executed_sqls


def test_run_cycle_kafka_failure_leaves_cycle_stuck():
    """Iceberg committed, Kafka failed. We MUST NOT release claims —
    the data already landed in Iceberg. Recovery on next iteration
    completes the cycle."""
    deps, pool = _deps(kafka_commit_raises=RuntimeError("kafka 503"))
    result = cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    assert result.success is False
    assert result.iceberg_snapshot_id == 12345  # iceberg DID commit
    assert "kafka-commit failed" in (result.error or "")
    executed_sqls = [c[0][0] for c in pool.cursor.execute.call_args_list]
    from icebox.postgres_sync import (
        MARK_ICEBERG_COMMITTED_SQL,
        RECORD_FAILURE_SQL,
        RELEASE_CYCLE_CLAIM_SQL,
    )
    assert RELEASE_CYCLE_CLAIM_SQL not in executed_sqls
    assert RECORD_FAILURE_SQL in executed_sqls
    assert MARK_ICEBERG_COMMITTED_SQL in executed_sqls


def test_run_cycle_schema_fingerprint_mismatch_releases_claims():
    """Writer's parquet has a different schema than the table — refuse
    the cycle, release claims. v1 doesn't auto-evolve."""
    deps, pool = _deps()
    # Mutate one row's fingerprint to a wrong value
    pool.cursor.fetchall.side_effect = [
        [(1,), (2,), (3,)],  # claim_files
        [_file_row(file_id=i, fp="wrong-fingerprint") for i in (1, 2, 3)],
    ]
    result = cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    assert result.success is False
    assert result.skipped_reason == "schema_mismatch"
    assert "Schema fingerprint mismatch" in (result.error or "")
    deps.commit_data_files.assert_not_called()


# ---------------------------------------------------------------------------
# Recovery — three branches
# ---------------------------------------------------------------------------


def _recovery_cycle_row(
    cycle_id: UUID,
    *,
    iceberg_snapshot_id: int | None,
    kafka_committed_at: datetime | None,
) -> CommitCycleRow:
    return CommitCycleRow(
        cycle_id=cycle_id,
        started_at=datetime.now(UTC),
        iceberg_snapshot_id=iceberg_snapshot_id,
        kafka_committed_at=kafka_committed_at,
        completed_at=None,
    )


def test_recover_branch_a_no_snapshot_releases_claims():
    """A) iceberg_snapshot_id null AND Lakekeeper's snapshot_log doesn't
    have our cycle → committer crashed before Iceberg landed. Release
    file claims so they re-enter the unclaimed pool."""
    cid = uuid4()
    deps, pool = _deps(snapshot_log_lookup_result=None)
    pool.cursor.fetchall.side_effect = []  # no files lookup in this branch
    result = cm._recover_one(
        _recovery_cycle_row(cid, iceberg_snapshot_id=None, kafka_committed_at=None),
        cfg=_cfg(), pg_pool=pool, deps=deps,
    )
    assert result.success is True
    assert result.skipped_reason == "released_no_iceberg_commit"
    executed_sqls = [c[0][0] for c in pool.cursor.execute.call_args_list]
    from icebox.postgres_sync import RELEASE_CYCLE_CLAIM_SQL
    assert RELEASE_CYCLE_CLAIM_SQL in executed_sqls


def test_recover_branch_a_snapshot_found_backfills_and_continues():
    """A') iceberg_snapshot_id null but Lakekeeper DOES have our cycle's
    snapshot — committer crashed after Iceberg commit but before PG
    update. Backfill snapshot_id and proceed to Kafka step."""
    cid = uuid4()
    deps, pool = _deps(snapshot_log_lookup_result=999)
    # files_for_cycle is called once in the kafka step
    pool.cursor.fetchall.side_effect = [
        [_file_row(file_id=1, fp="anything")],
    ]
    result = cm._recover_one(
        _recovery_cycle_row(cid, iceberg_snapshot_id=None, kafka_committed_at=None),
        cfg=_cfg(), pg_pool=pool, deps=deps,
    )
    assert result.success is True
    assert result.iceberg_snapshot_id == 999
    deps.kafka_commit_offsets.assert_called_once()
    executed_sqls = [c[0][0] for c in pool.cursor.execute.call_args_list]
    from icebox.postgres_sync import (
        COMPLETE_CYCLE_SQL,
        MARK_ICEBERG_COMMITTED_SQL,
        MARK_KAFKA_COMMITTED_SQL,
    )
    assert MARK_ICEBERG_COMMITTED_SQL in executed_sqls
    assert MARK_KAFKA_COMMITTED_SQL in executed_sqls
    assert COMPLETE_CYCLE_SQL in executed_sqls


def test_recover_branch_b_kafka_retry_completes_cycle():
    """B) iceberg_snapshot_id set, kafka_committed_at null — Iceberg
    landed, Kafka didn't. Retry Kafka and finalize."""
    cid = uuid4()
    deps, pool = _deps()
    pool.cursor.fetchall.side_effect = [
        [_file_row(file_id=1, fp="anything")],
    ]
    result = cm._recover_one(
        _recovery_cycle_row(cid, iceberg_snapshot_id=777, kafka_committed_at=None),
        cfg=_cfg(), pg_pool=pool, deps=deps,
    )
    assert result.success is True
    assert result.iceberg_snapshot_id == 777
    deps.kafka_commit_offsets.assert_called_once()


def test_recover_branch_c_only_finalize():
    """C) iceberg AND kafka committed, just need to mark cycle complete +
    record success."""
    cid = uuid4()
    deps, pool = _deps()
    result = cm._recover_one(
        _recovery_cycle_row(
            cid, iceberg_snapshot_id=555, kafka_committed_at=datetime.now(UTC),
        ),
        cfg=_cfg(), pg_pool=pool, deps=deps,
    )
    assert result.success is True
    deps.kafka_commit_offsets.assert_not_called()
    executed_sqls = [c[0][0] for c in pool.cursor.execute.call_args_list]
    from icebox.postgres_sync import COMPLETE_CYCLE_SQL
    assert COMPLETE_CYCLE_SQL in executed_sqls


def test_recover_branch_b_kafka_failure_leaves_stuck():
    """If Kafka commit fails again during recovery, leave the cycle
    stuck so the next recovery attempt can pick it up."""
    cid = uuid4()
    deps, pool = _deps(kafka_commit_raises=RuntimeError("still down"))
    pool.cursor.fetchall.side_effect = [
        [_file_row(file_id=1, fp="anything")],
    ]
    result = cm._recover_one(
        _recovery_cycle_row(cid, iceberg_snapshot_id=777, kafka_committed_at=None),
        cfg=_cfg(), pg_pool=pool, deps=deps,
    )
    assert result.success is False
    assert "kafka-commit failed during recovery" in (result.error or "")
    executed_sqls = [c[0][0] for c in pool.cursor.execute.call_args_list]
    from icebox.postgres_sync import COMPLETE_CYCLE_SQL
    assert COMPLETE_CYCLE_SQL not in executed_sqls


# ---------------------------------------------------------------------------
# Loop control
# ---------------------------------------------------------------------------


def test_committer_loop_exits_when_stop_event_set():
    """The loop must respond to stop_event for graceful shutdown."""
    import threading
    deps, pool = _deps(claimed_ids=[])
    # Make cursor fetchall always return empty (no files) so each cycle
    # short-circuits
    pool.cursor.fetchall.return_value = []
    pool.cursor.fetchall.side_effect = None

    stop = threading.Event()

    # incomplete_cycles also reads via fetchall, returns []
    # Set the event after a tiny delay so the loop runs zero or one
    # cycles, then exits.
    cfg = _cfg()
    # Override cadence to short so wait() doesn't stall the test
    cfg = Config(**{**cfg.__dict__, "committer_cadence_seconds": 1})
    stop.set()  # set BEFORE call so loop exits after one iteration
    cm.committer_loop(cfg=cfg, pg_pool=pool, deps=deps, stop_event=stop)
    # No assertion beyond "did not hang"


def test_committer_loop_continues_after_recovery_exception(monkeypatch):
    """If startup recovery raises, the steady-state loop still runs."""
    import threading
    deps, pool = _deps(claimed_ids=[])
    pool.cursor.fetchall.return_value = []
    pool.cursor.fetchall.side_effect = None

    # Patch recover_in_flight_cycles to raise
    monkeypatch.setattr(
        cm, "recover_in_flight_cycles",
        MagicMock(side_effect=RuntimeError("recovery boom")),
    )

    stop = threading.Event()
    stop.set()
    cfg = _cfg()
    cfg = Config(**{**cfg.__dict__, "committer_cadence_seconds": 1})
    # Should not raise
    cm.committer_loop(cfg=cfg, pg_pool=pool, deps=deps, stop_event=stop)


# ---------------------------------------------------------------------------
# Review-driven: state-machine SQL ordering invariants
# ---------------------------------------------------------------------------


def test_run_cycle_happy_path_state_machine_order():
    """The state-machine ordering is the load-bearing invariant. If
    MARK_KAFKA_COMMITTED runs BEFORE the Kafka API call, a crash between
    them leaves a stuck cycle that recovery can't distinguish from the
    'kafka commit landed' case. If MARK_ICEBERG_COMMITTED runs AFTER
    the Kafka call, a crash between Iceberg and PG leaves no recovery
    signal."""
    from icebox.postgres_sync import (
        COMPLETE_CYCLE_SQL,
        MARK_ICEBERG_COMMITTED_SQL,
        MARK_KAFKA_COMMITTED_SQL,
    )
    deps, pool = _deps()
    cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    idx_ice = executed.index(MARK_ICEBERG_COMMITTED_SQL)
    idx_kfk = executed.index(MARK_KAFKA_COMMITTED_SQL)
    idx_done = executed.index(COMPLETE_CYCLE_SQL)
    # MARK_ICEBERG persists snapshot_id BEFORE Kafka — guarantees
    # recovery can find the snapshot if Kafka commit doesn't land
    assert idx_ice < idx_kfk, "MARK_ICEBERG_COMMITTED must run before Kafka SQL"
    # MARK_KAFKA must run BEFORE COMPLETE_CYCLE — finalization is the
    # last step of the saga
    assert idx_kfk < idx_done, "MARK_KAFKA_COMMITTED must run before COMPLETE_CYCLE"


def test_run_cycle_iceberg_failure_release_before_record_failure():
    """Defensive ordering: release the file claims FIRST so they can be
    re-batched, THEN bump consecutive_failures. If a second committer
    pod existed (it shouldn't, but defensively), reversing this would
    open a window where the failure counter triggered 503 backpressure
    on writers BEFORE the files were releasable."""
    from icebox.postgres_sync import (
        RECORD_FAILURE_SQL,
        RELEASE_CYCLE_CLAIM_SQL,
    )
    deps, pool = _deps(iceberg_commit_raises=RuntimeError("catalog 503"))
    cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    idx_rel = executed.index(RELEASE_CYCLE_CLAIM_SQL)
    idx_fail = executed.index(RECORD_FAILURE_SQL)
    assert idx_rel < idx_fail


# ---------------------------------------------------------------------------
# Review-driven: heartbeat invariants
# ---------------------------------------------------------------------------


def test_run_cycle_pumps_heartbeat_on_happy_path():
    """If the committer succeeds, last_committer_heartbeat must advance.
    Otherwise the API's stale-heartbeat backpressure could fire on a
    healthy committer."""
    from icebox.postgres_sync import UPDATE_HEARTBEAT_SQL
    deps, pool = _deps()
    cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    assert UPDATE_HEARTBEAT_SQL in executed


def test_run_cycle_pumps_heartbeat_on_vacuous_cycle():
    """No files queued ⇒ committer still pumps heartbeat. Otherwise a
    quiet Kafka topic causes the API to flip to 503 after one cadence
    × stale_multiple even though the committer is healthy."""
    from icebox.postgres_sync import UPDATE_HEARTBEAT_SQL
    deps, pool = _deps(claimed_ids=[])
    pool.cursor.fetchall.side_effect = [[]]  # claim returns empty
    cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    assert UPDATE_HEARTBEAT_SQL in executed


def test_run_cycle_pumps_heartbeat_on_iceberg_failure():
    """Iceberg commit failed ⇒ still pump the heartbeat (the committer
    is alive, the catalog is the problem). Otherwise the API also
    starts 503-ing legitimate POSTs."""
    from icebox.postgres_sync import UPDATE_HEARTBEAT_SQL
    deps, pool = _deps(iceberg_commit_raises=RuntimeError("catalog 503"))
    cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    assert UPDATE_HEARTBEAT_SQL in executed


def test_run_cycle_pumps_heartbeat_on_kafka_failure():
    """Kafka commit failed ⇒ Iceberg already landed (cycle stuck) but
    heartbeat must pump. Recovery will pick the cycle up."""
    from icebox.postgres_sync import UPDATE_HEARTBEAT_SQL
    deps, pool = _deps(kafka_commit_raises=RuntimeError("kafka 503"))
    cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    assert UPDATE_HEARTBEAT_SQL in executed


def test_run_cycle_pumps_heartbeat_on_schema_fingerprint_mismatch():
    """Schema mismatch is a writer-side bug (writer deployed before
    icebox). Committer's still alive — heartbeat must advance."""
    from icebox.postgres_sync import UPDATE_HEARTBEAT_SQL
    deps, pool = _deps()
    pool.cursor.fetchall.side_effect = [
        [(1,), (2,), (3,)],
        [_file_row(file_id=i, fp="wrong-fingerprint") for i in (1, 2, 3)],
    ]
    cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    assert UPDATE_HEARTBEAT_SQL in executed


def test_recover_one_pumps_heartbeat_on_completion():
    """Recovery completing a stuck cycle ⇒ heartbeat advance. Without
    this, a long recovery (many stuck cycles) leaves API 503-ing the
    whole time. PE review #14."""
    from icebox.postgres_sync import UPDATE_HEARTBEAT_SQL
    cid = uuid4()
    deps, pool = _deps()
    pool.cursor.fetchall.side_effect = [
        [_file_row(file_id=1, fp="anything")],
    ]
    cm._recover_one(
        _recovery_cycle_row(cid, iceberg_snapshot_id=777, kafka_committed_at=None),
        cfg=_cfg(), pg_pool=pool, deps=deps,
    )
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    assert UPDATE_HEARTBEAT_SQL in executed


def test_committer_loop_stamps_heartbeat_before_recovery(monkeypatch):
    """The first thing committer_loop does AFTER lock acquisition — before
    recovery, before any cycle — is stamp the heartbeat. Without this,
    dead-committer accept-writes window extends past cadence ×
    stale_multiple. PE-review #15.

    Recovery is allowed to run for real (FakePool returns empty
    incomplete_cycles, so recovery is a natural no-op). The
    try_acquire_committer_lock monkeypatch is the coordination shim
    that trips stop_event right after a successful acquire; the
    underlying lock-acquire behavior is exercised via FakePool."""
    import threading

    from icebox.postgres_sync import UPDATE_HEARTBEAT_SQL

    deps, pool = _deps(claimed_ids=[])

    stop = threading.Event()

    # Coordinate test timing: trip stop_event from inside the lock
    # acquisition so the loop runs through lock → heartbeat → recovery
    # (naturally empty) → check stop (set) → exit. This monkeypatch is
    # a sync primitive, NOT a substitution of the lock behavior — the
    # cursor.execute still runs through FakePool.
    original_try = cm.ps.try_acquire_committer_lock
    def lock_then_stop(conn, **kwargs):
        result = original_try(conn, **kwargs)
        stop.set()
        return result
    monkeypatch.setattr(cm.ps, "try_acquire_committer_lock", lock_then_stop)

    cfg = _cfg()
    cfg = Config(**{**cfg.__dict__, "committer_cadence_seconds": 1})
    cm.committer_loop(cfg=cfg, pg_pool=pool, deps=deps, stop_event=stop)

    # Heartbeat MUST appear in the SQL log
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    assert UPDATE_HEARTBEAT_SQL in executed


def test_committer_loop_exits_if_stop_set_during_lock_acquisition():
    """PE #10 acquire path: if stop_event fires while the lock is held
    by another committer, the loop exits cleanly without ever stamping
    a heartbeat or running a cycle. Used during graceful shutdown of a
    still-waiting pod."""
    import threading

    deps, pool = _deps(claimed_ids=[])
    # Make the lock acquisition always fail (lock held elsewhere)
    pool.cursor.fetchone.return_value = (False,)

    stop = threading.Event()
    stop.set()
    cfg = _cfg()
    cfg = Config(**{**cfg.__dict__, "committer_cadence_seconds": 1})
    # Must not hang
    cm.committer_loop(cfg=cfg, pg_pool=pool, deps=deps, stop_event=stop)


def test_committer_loop_releases_advisory_lock_on_exit(monkeypatch):
    """Graceful shutdown calls pg_advisory_unlock so a fast pod restart
    doesn't have to wait for TCP timeout on the dying connection.
    Recovery runs through real code (FakePool yields empty incomplete
    cycles); the lock monkeypatch is purely a stop coordinator."""
    import threading

    from icebox.postgres_sync import UNLOCK_ADVISORY_LOCK_SQL

    deps, pool = _deps(claimed_ids=[])

    stop = threading.Event()

    original = cm.ps.try_acquire_committer_lock
    def lock_then_arm_stop(conn, **kwargs):
        result = original(conn, **kwargs)
        stop.set()
        return result
    monkeypatch.setattr(cm.ps, "try_acquire_committer_lock", lock_then_arm_stop)

    cfg = _cfg()
    cfg = Config(**{**cfg.__dict__, "committer_cadence_seconds": 1})
    cm.committer_loop(cfg=cfg, pg_pool=pool, deps=deps, stop_event=stop)

    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    assert UNLOCK_ADVISORY_LOCK_SQL in executed


# ---------------------------------------------------------------------------
# Review-driven: multi-file Kafka offset merge
# ---------------------------------------------------------------------------


def test_run_cycle_merges_offsets_across_multiple_files_max_per_partition():
    """A single cycle batches multiple files; if 3 files all wrote to
    partition 0 with offsets 100, 500, 200 respectively, the Kafka
    commit MUST be at offset 501 (max + 1), not 200, 100, or anything
    else. Tests the wiring between files_for_cycle row reads and the
    kafka.commit_offsets call."""
    deps, pool = _deps()
    pool.cursor.fetchall.side_effect = [
        [(1,), (2,), (3,)],
        [
            _file_row(file_id=1, fp=schema_fingerprint(_schema()), kafka_offsets={"0": 100}),
            _file_row(file_id=2, fp=schema_fingerprint(_schema()), kafka_offsets={"0": 500}),
            _file_row(file_id=3, fp=schema_fingerprint(_schema()), kafka_offsets={"0": 200}),
        ],
    ]
    cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    # commit_offsets was called once with max_offsets={0: 500}
    kafka_call = deps.kafka_commit_offsets.call_args
    assert kafka_call.kwargs["max_offsets"] == {0: 500}


def test_run_cycle_merges_disjoint_partition_offsets():
    """Different writers may produce files for different partitions.
    The cycle's Kafka commit must cover all partitions seen."""
    deps, pool = _deps()
    pool.cursor.fetchall.side_effect = [
        [(1,), (2,), (3,)],
        [
            _file_row(file_id=1, fp=schema_fingerprint(_schema()), kafka_offsets={"0": 100}),
            _file_row(file_id=2, fp=schema_fingerprint(_schema()), kafka_offsets={"5": 50}),
            _file_row(file_id=3, fp=schema_fingerprint(_schema()), kafka_offsets={"10": 1000}),
        ],
    ]
    cm.run_cycle(cfg=_cfg(), pg_pool=pool, deps=deps)
    kafka_call = deps.kafka_commit_offsets.call_args
    assert kafka_call.kwargs["max_offsets"] == {0: 100, 5: 50, 10: 1000}


# ---------------------------------------------------------------------------
# Review-driven: PE #23 minimum inter-cycle sleep
# ---------------------------------------------------------------------------


def test_min_inter_cycle_sleep_caps_failure_rate():
    """PE-review #23 + QE-review T3: the floor exists to bound the
    attempt rate during sustained Lakekeeper failure. Tied to the
    design intent: ≤ 15 attempts/min at the baseline 60s cadence.

    Replacing a bare `== 5.0` pin with this property-based check
    communicates WHY: if a future engineer tightens to 1s (60/min),
    the test fails with a message explaining the prod incident cost;
    if they relax to 30s (2/min), it still passes."""
    assert cm.MIN_INTER_CYCLE_SLEEP_SECONDS > 0, (
        "MIN_INTER_CYCLE_SLEEP_SECONDS must be positive — a zero floor "
        "allows the loop to thrash Lakekeeper at thread-scheduling speed"
    )
    baseline_cadence_s = 60.0
    max_attempts_per_min = 60.0 / cm.MIN_INTER_CYCLE_SLEEP_SECONDS
    assert max_attempts_per_min <= 15, (
        f"MIN_INTER_CYCLE_SLEEP_SECONDS={cm.MIN_INTER_CYCLE_SLEEP_SECONDS} "
        f"allows {max_attempts_per_min}/min failure-cycle attempts at the "
        f"{baseline_cadence_s}s baseline cadence. The cost of thrashing "
        f"Lakekeeper during a sustained 5xx incident scales with conn "
        f"churn × manifest writes that may partially commit before "
        f"failing. Bumping this floor down requires re-evaluating prod "
        f"incident rate; see PE-review #23 in the PR history."
    )


# ---------------------------------------------------------------------------
# Review-driven: incomplete_cycles bounded
# ---------------------------------------------------------------------------


def test_incomplete_cycles_has_limit_default():
    """PE #2: a stuck-cycle backlog could grow unbounded. The query
    enforces a default LIMIT and the helper exposes it as a kwarg."""
    from icebox import postgres_sync as psy
    sig = inspect.signature(psy.incomplete_cycles)
    assert "limit" in sig.parameters
    assert psy.INCOMPLETE_CYCLES_LIMIT_DEFAULT == 100


# ---------------------------------------------------------------------------
# Review-driven: stale-table-handle invariant in recovery
# ---------------------------------------------------------------------------


def test_recover_in_flight_cycles_loads_table_fresh_per_cycle():
    """QE re-review: `_recover_one` must call deps.load_table() once per
    cycle, not cache it across cycles. A future refactor that hoists
    load_table() out into recover_in_flight_cycles would silently break
    the staleness guarantee — the second cycle's snapshot_log lookup
    would not see snapshots committed between the first and second
    cycle's processing.

    Pin: 3 in-flight cycles → 3 load_table calls.

    The fixture cycles below all have iceberg_snapshot_id set and
    kafka_committed_at set (branch C in _recover_one): branch C only
    runs finalize, no files_for_cycle query — simplest setup that
    invokes load_table per cycle without other plumbing."""
    cid_a, cid_b, cid_c = uuid4(), uuid4(), uuid4()
    table = MagicMock()
    table.schema.return_value = _schema()

    load_calls: list[int] = []

    def counting_load():
        load_calls.append(len(load_calls))
        return table

    pool = FakePool()
    # incomplete_cycles SQL returns 3 rows — all in branch C state
    # (iceberg_snapshot_id set, kafka_committed_at set, completed_at
    # null): each _recover_one call only runs the finalize transaction.
    now = datetime.now(UTC)
    pool.cursor.fetchall.side_effect = [
        # incomplete_cycles result
        [
            (cid_a, now, 100, now, None),
            (cid_b, now, 200, now, None),
            (cid_c, now, 300, now, None),
        ],
    ]

    deps = cm.CommitterDeps(
        load_table=counting_load,
        commit_data_files=MagicMock(return_value=(999, None)),
        find_snapshot_for_cycle=MagicMock(return_value=None),
        build_data_file=MagicMock(return_value=MagicMock()),
        kafka_admin=MagicMock(),
        kafka_commit_offsets=MagicMock(),
    )
    cm.recover_in_flight_cycles(cfg=_cfg(), pg_pool=pool, deps=deps)
    assert len(load_calls) == 3, (
        f"expected fresh load_table per cycle (3 total); got {len(load_calls)}. "
        f"A regression that caches table across cycles silently breaks "
        f"snapshot_log freshness."
    )


def test_recover_branch_a_pumps_heartbeat():
    """QE re-review: branch A (released-no-iceberg-commit) was the only
    recovery exit path that didn't update the heartbeat. Long recovery
    walks of released cycles would otherwise leave the API 503'ing the
    whole time. Verify the update is in the same transaction as the
    release."""
    from icebox.postgres_sync import (
        RELEASE_CYCLE_CLAIM_SQL,
        UPDATE_HEARTBEAT_SQL,
    )
    cid = uuid4()
    deps, pool = _deps(snapshot_log_lookup_result=None)
    pool.cursor.fetchall.side_effect = []
    cm._recover_one(
        _recovery_cycle_row(cid, iceberg_snapshot_id=None, kafka_committed_at=None),
        cfg=_cfg(), pg_pool=pool, deps=deps,
    )
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    assert RELEASE_CYCLE_CLAIM_SQL in executed
    assert UPDATE_HEARTBEAT_SQL in executed


def test_recover_branch_a_deletes_cycle_row():
    """PE re-review #16: zombie cycle rows accumulate against
    incomplete_cycles' LIMIT=100 if released cycles aren't cleaned up.
    Branch A must DELETE the cycle row (the cycle didn't happen from
    Iceberg's perspective — no snapshot exists for it)."""
    from icebox.postgres_sync import (
        DELETE_CYCLE_ROW_SQL,
        RELEASE_CYCLE_CLAIM_SQL,
    )
    cid = uuid4()
    deps, pool = _deps(snapshot_log_lookup_result=None)
    pool.cursor.fetchall.side_effect = []
    cm._recover_one(
        _recovery_cycle_row(cid, iceberg_snapshot_id=None, kafka_committed_at=None),
        cfg=_cfg(), pg_pool=pool, deps=deps,
    )
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    assert RELEASE_CYCLE_CLAIM_SQL in executed
    assert DELETE_CYCLE_ROW_SQL in executed
    # Ordering: release the file claims BEFORE deleting the cycle row,
    # otherwise the FK from files.cycle_id → commit_cycles.cycle_id
    # would block the delete.
    idx_rel = executed.index(RELEASE_CYCLE_CLAIM_SQL)
    idx_del = executed.index(DELETE_CYCLE_ROW_SQL)
    assert idx_rel < idx_del


def test_committer_loop_lock_conn_distinct_from_cycle_conns(monkeypatch):
    """PE/QE review #8: the advisory lock is session-scoped on a
    SPECIFIC connection that's held for the lifetime of the thread.
    If that conn is ever returned to the pool during cycle work, the
    session ends and the lock evaporates — defeating the singleton
    guarantee. Production code holds `lock_conn` outside the pool's
    context managers; this test pins that property.

    Uses the new FakePool that returns distinct mocks for getconn()
    vs connection() — a regression that handed the lock_conn to a
    cycle's `pool.connection()` would surface in the recorded SQL
    ending up on the wrong cursor."""
    import threading

    from icebox.postgres_sync import TRY_ADVISORY_LOCK_SQL, UPDATE_HEARTBEAT_SQL

    deps, pool = _deps(claimed_ids=[])

    stop = threading.Event()
    original_try = cm.ps.try_acquire_committer_lock
    def lock_then_stop(conn, **kwargs):
        result = original_try(conn, **kwargs)
        stop.set()
        return result
    monkeypatch.setattr(cm.ps, "try_acquire_committer_lock", lock_then_stop)

    cfg = _cfg()
    cfg = Config(**{**cfg.__dict__, "committer_cadence_seconds": 1})
    cm.committer_loop(cfg=cfg, pg_pool=pool, deps=deps, stop_event=stop)

    # The advisory-lock SQL must have run on the lock_cursor (via lock_conn),
    # NOT the cycle_cursor (via cycle_conn).
    lock_executed = [c[0][0] for c in pool.lock_cursor.execute.call_args_list]
    cycle_executed = [c[0][0] for c in pool.cycle_cursor.execute.call_args_list]
    assert TRY_ADVISORY_LOCK_SQL in lock_executed, (
        "pg_try_advisory_lock must run on the dedicated lock connection"
    )
    assert TRY_ADVISORY_LOCK_SQL not in cycle_executed, (
        "pg_try_advisory_lock leaked onto a cycle-work connection — "
        "session-scoped lock would be released when the with-block exits"
    )

    # Heartbeat goes through pool.connection() (cycle_conn), NOT the lock_conn
    assert UPDATE_HEARTBEAT_SQL in cycle_executed, (
        "update_heartbeat should run on a cycle-work connection (via pool.connection())"
    )

    # And the lock_conn was returned to the pool on shutdown.
    assert pool.lock_conn in pool.putconn_calls


def test_committer_loop_putconn_returns_lock_conn_on_acquisition_failure(monkeypatch):
    """PE review #7: if try_acquire_committer_lock raises, the
    candidate conn must NOT leak (try/finally around the acquire).
    Pool budget at psycopg_pool_max=2 means one leaked conn halves
    the budget — followed by another would brick the pool."""
    import threading

    deps, pool = _deps(claimed_ids=[])
    raise_count = {"n": 0}

    def lock_raiser(conn, **kwargs):
        raise_count["n"] += 1
        raise RuntimeError("transient PG error")

    monkeypatch.setattr(cm.ps, "try_acquire_committer_lock", lock_raiser)

    stop = threading.Event()
    # Stop the loop after a short delay so it exits cleanly without
    # the lock ever being acquired.
    def stop_after_short_delay():
        import time
        time.sleep(0.05)
        stop.set()
    threading.Thread(target=stop_after_short_delay, daemon=True).start()

    cfg = _cfg()
    cfg = Config(**{**cfg.__dict__, "committer_cadence_seconds": 1})
    cm.committer_loop(cfg=cfg, pg_pool=pool, deps=deps, stop_event=stop)

    # Lock acquisition raised; the conn MUST have been returned to the
    # pool (no leak). Putconn count = getconn count (every checkout
    # got a corresponding return).
    assert raise_count["n"] >= 1, "expected at least one acquire attempt"
    assert pool.lock_conn in pool.putconn_calls, (
        "lock_conn leaked when try_acquire_committer_lock raised"
    )


def test_committer_loop_recovers_from_getconn_exception(monkeypatch):
    """PE review #6: if pg_pool.getconn() raises during the
    acquisition loop, the committer must log + retry, NOT crash the
    thread silently."""
    import threading

    deps, pool = _deps(claimed_ids=[])

    # First getconn raises, second succeeds. Substitute the FakePool
    # method directly — this isn't a monkeypatch of production code;
    # it's a test-double behavior override.
    original_getconn = pool.getconn
    call_count = {"n": 0}

    def flaky_getconn():
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("transient PG connect error")
        return original_getconn()

    pool.getconn = flaky_getconn

    stop = threading.Event()
    original_try = cm.ps.try_acquire_committer_lock
    def lock_then_stop(conn, **kwargs):
        result = original_try(conn, **kwargs)
        stop.set()
        return result
    monkeypatch.setattr(cm.ps, "try_acquire_committer_lock", lock_then_stop)

    cfg = _cfg()
    cfg = Config(**{**cfg.__dict__, "committer_cadence_seconds": 1})
    # Must not raise
    cm.committer_loop(cfg=cfg, pg_pool=pool, deps=deps, stop_event=stop)
    assert call_count["n"] >= 2, "expected getconn to be retried after the raise"


def test_committer_advisory_lock_uses_schema_derived_lock_id(monkeypatch):
    """The lock id at the call site MUST be derived from cfg.pg_schema
    via committer_advisory_lock_id. A regression that hardcodes a
    constant — or uses a stale schema name — silently breaks the
    per-deployment lock isolation we rely on for multi-icebox-on-one-PG."""
    import threading

    from icebox.postgres_sync import committer_advisory_lock_id

    cfg = _cfg()
    deps, pool = _deps(claimed_ids=[])

    stop = threading.Event()
    original_try = cm.ps.try_acquire_committer_lock

    captured_key = {"value": None}
    def lock_capturing(conn, **kwargs):
        captured_key["value"] = kwargs.get("lock_id")
        stop.set()
        return original_try(conn, **kwargs)
    monkeypatch.setattr(cm.ps, "try_acquire_committer_lock", lock_capturing)

    cm.committer_loop(cfg=cfg, pg_pool=pool, deps=deps, stop_event=stop)
    assert captured_key["value"] == committer_advisory_lock_id(cfg.pg_schema)


def test_committer_loop_heartbeat_runs_before_recovery(monkeypatch):
    """QE review T2: the heartbeat MUST be stamped before recovery
    runs (otherwise a long recovery sees a None heartbeat → API treats
    as not-stale → writes accumulate against a maybe-dead committer).

    Asserts ORDERING via the SQL trace recorded on the cycle_cursor.
    Real heartbeat + real recovery both run (recovery is a natural
    no-op because FakePool returns empty incomplete_cycles), and the
    cycle_cursor records both queries — so UPDATE_HEARTBEAT_SQL must
    appear before INCOMPLETE_CYCLES_SQL in execute.call_args_list.

    This approach pins the production code's behavior end-to-end,
    not just the order of two mocked method calls."""
    import threading

    from icebox.postgres_sync import INCOMPLETE_CYCLES_SQL, UPDATE_HEARTBEAT_SQL

    deps, pool = _deps(claimed_ids=[])

    stop = threading.Event()
    original_try = cm.ps.try_acquire_committer_lock
    def lock_then_stop(conn, **kwargs):
        result = original_try(conn, **kwargs)
        stop.set()
        return result
    monkeypatch.setattr(cm.ps, "try_acquire_committer_lock", lock_then_stop)

    cfg = _cfg()
    cfg = Config(**{**cfg.__dict__, "committer_cadence_seconds": 1})
    cm.committer_loop(cfg=cfg, pg_pool=pool, deps=deps, stop_event=stop)

    # Both queries land on the cycle_cursor (heartbeat via
    # pool.connection(), incomplete_cycles via pool.connection() inside
    # recover_in_flight_cycles). The lock SQLs are on the lock_cursor
    # so they don't interfere with this assertion.
    executed = [c[0][0] for c in pool.cycle_cursor.execute.call_args_list]
    assert UPDATE_HEARTBEAT_SQL in executed
    assert INCOMPLETE_CYCLES_SQL in executed
    idx_hb = executed.index(UPDATE_HEARTBEAT_SQL)
    idx_rec = executed.index(INCOMPLETE_CYCLES_SQL)
    assert idx_hb < idx_rec, (
        f"update_heartbeat must run BEFORE incomplete_cycles (recovery); "
        f"SQL order on cycle_cursor was {executed}"
    )


def test_recover_kafka_failure_pumps_heartbeat():
    """PE re-review: the kafka-retry-failure branch returns before
    reaching the finalize transaction. Without an explicit heartbeat
    update, a series of kafka-stuck cycles in recovery would silently
    leave the API 503'ing. PE #14 finish-fix."""
    from icebox.postgres_sync import UPDATE_HEARTBEAT_SQL
    cid = uuid4()
    deps, pool = _deps(kafka_commit_raises=RuntimeError("still down"))
    pool.cursor.fetchall.side_effect = [
        [_file_row(file_id=1, fp="anything")],
    ]
    result = cm._recover_one(
        _recovery_cycle_row(cid, iceberg_snapshot_id=777, kafka_committed_at=None),
        cfg=_cfg(), pg_pool=pool, deps=deps,
    )
    assert result.success is False
    executed = [c[0][0] for c in pool.cursor.execute.call_args_list]
    assert UPDATE_HEARTBEAT_SQL in executed


def test_recover_in_flight_cycles_logs_error_on_limit_hit(caplog):
    """PE re-review bonus: hitting the incomplete_cycles LIMIT means
    more stuck cycles than the recovery scan can see. Page ops via
    log.error so the absence isn't silent."""
    import logging

    from icebox import postgres_sync as psy

    # Build a fake pool returning exactly LIMIT rows for incomplete_cycles
    pool = FakePool()
    limit = psy.INCOMPLETE_CYCLES_LIMIT_DEFAULT
    now = datetime.now(UTC)
    pool.cursor.fetchall.side_effect = [
        # Each tuple is (cycle_id, started_at, iceberg_snapshot_id,
        # kafka_committed_at, completed_at). All branch C (will be
        # finalized in the loop, but we're only checking the warning).
        [(uuid4(), now, 1, now, None) for _ in range(limit)],
    ]
    # Make each cycle's recovery essentially a no-op: branch C path
    # finalizes via complete_cycle/record_success which we don't care
    # to assert here.
    deps = cm.CommitterDeps(
        load_table=lambda: MagicMock(),
        commit_data_files=MagicMock(return_value=(1, None)),
        find_snapshot_for_cycle=MagicMock(return_value=1),
        build_data_file=MagicMock(return_value=MagicMock()),
        kafka_admin=MagicMock(),
        kafka_commit_offsets=MagicMock(),
    )
    with caplog.at_level(logging.ERROR, logger="icebox.committer"):
        cm.recover_in_flight_cycles(cfg=_cfg(), pg_pool=pool, deps=deps)
    error_records = [
        rec for rec in caplog.records
        if rec.levelno == logging.ERROR and rec.name == "icebox.committer"
    ]
    assert error_records, (
        "expected an ERROR-level log on the icebox.committer logger when "
        "incomplete_cycles hits the safety LIMIT"
    )
    # Defensive content check — the message should mention the limit by
    # name. If a future refactor renames the constant, this is the
    # signal to update the log message too.
    assert any(
        "INCOMPLETE_CYCLES_LIMIT" in rec.message for rec in error_records
    )
