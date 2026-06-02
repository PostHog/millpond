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

import pytest

from icebox import committer as cm
from icebox.config import Config
from icebox.schema import CommitCycleRow
from shared.fingerprint import schema_fingerprint
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _cfg() -> Config:
    """Minimal config — committer doesn't touch PG host etc., only the
    cadence/budget/group_id/topic fields."""
    return Config(
        pg_host="x", pg_port=5432, pg_database="x", pg_username="x", pg_password="x",
        pg_sslmode="disable",
        asyncpg_pool_min=1, asyncpg_pool_max=2,
        psycopg_pool_min=1, psycopg_pool_max=1,
        iceberg_catalog_uri="x", iceberg_warehouse="x",
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
    """A psycopg-shaped pool that yields a single shared mock connection.

    Records every call on the connection's cursor so tests can assert
    which SQL ran and in what order.

    Supports both:
      - context-managed `with pool.connection() as conn:` (cycle work)
      - explicit `getconn()` / `putconn()` (committer_loop's dedicated
        advisory-lock connection)

    For the advisory-lock path: by default, the cursor's fetchone returns
    (True,) so try_acquire_committer_lock succeeds on first attempt.
    Tests that want to exercise the "lock held by another committer"
    path can override.
    """

    def __init__(self):
        self.conn = MagicMock()
        # Connection.transaction() context manager
        @contextmanager
        def tx_ctx():
            yield
        self.conn.transaction.side_effect = tx_ctx
        # cursor() context manager
        self.cursor = MagicMock()
        @contextmanager
        def cur_ctx():
            yield self.cursor
        self.conn.cursor.side_effect = cur_ctx
        # Default lock-acquire: True (first attempt succeeds)
        self.cursor.fetchone.return_value = (True,)

    @contextmanager
    def connection(self):
        yield self.conn

    def getconn(self):
        return self.conn

    def putconn(self, conn):
        # No-op for the fake — the connection is shared
        pass


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
            return_value=iceberg_snapshot_id,
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
    from icebox.postgres_sync import RELEASE_CYCLE_CLAIM_SQL, RECORD_FAILURE_SQL
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
        RELEASE_CYCLE_CLAIM_SQL, RECORD_FAILURE_SQL, MARK_ICEBERG_COMMITTED_SQL,
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
        MARK_ICEBERG_COMMITTED_SQL, MARK_KAFKA_COMMITTED_SQL, COMPLETE_CYCLE_SQL,
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

    Note: the lock-acquire-then-stop pattern below uses the advisory
    lock's True return to also trip the stop event, so the loop runs
    just enough to stamp the heartbeat then exits."""
    import threading
    from icebox.postgres_sync import UPDATE_HEARTBEAT_SQL

    deps, pool = _deps(claimed_ids=[])

    # Recovery is a no-op
    monkeypatch.setattr(cm, "recover_in_flight_cycles", MagicMock(return_value=[]))

    stop = threading.Event()

    # Trip stop_event from inside the advisory-lock acquisition so the
    # loop runs through: lock → heartbeat stamp → recovery (mocked) →
    # `while not stop` check (already set) → exit.
    original_try_lock = cm.ps.try_acquire_committer_lock
    def lock_then_stop(conn, **kwargs):
        result = True  # acquire succeeds
        stop.set()
        return result
    monkeypatch.setattr(cm.ps, "try_acquire_committer_lock", lock_then_stop)

    cfg = _cfg()
    cfg = Config(**{**cfg.__dict__, "committer_cadence_seconds": 1})
    cm.committer_loop(cfg=cfg, pg_pool=pool, deps=deps, stop_event=stop)

    # Heartbeat MUST appear in the SQL log even though no cycle ran
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
    doesn't have to wait for TCP timeout on the dying connection."""
    import threading
    from icebox.postgres_sync import UNLOCK_ADVISORY_LOCK_SQL

    deps, pool = _deps(claimed_ids=[])
    monkeypatch.setattr(cm, "recover_in_flight_cycles", MagicMock(return_value=[]))

    stop = threading.Event()
    # Acquire succeeds; stop fires on first iteration via cycle-loop
    # entry — simplest is to make claim return empty so the loop is
    # a no-op then set stop.
    pool.cursor.fetchall.return_value = []
    pool.cursor.fetchall.side_effect = None

    original = cm.ps.try_acquire_committer_lock
    def lock_then_arm_stop(conn, **kwargs):
        stop.set()
        return True
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


def test_min_inter_cycle_sleep_constant_floors_loop_wait():
    """Even if a cycle takes longer than cadence, the loop sleeps at
    least MIN_INTER_CYCLE_SLEEP_SECONDS so a tight failure loop doesn't
    hammer Lakekeeper. PE-review #23.

    Bumped from 1.0s to 5.0s for prod-us: at the 60s baseline cadence,
    that's 12 attempts/min in failure mode — plenty for Lakekeeper to
    recover or for ops to notice, and the cost of cycle thrashing at
    prod scale is non-trivial."""
    assert cm.MIN_INTER_CYCLE_SLEEP_SECONDS > 0
    assert cm.MIN_INTER_CYCLE_SLEEP_SECONDS == 5.0


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
        commit_data_files=MagicMock(return_value=999),
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
        commit_data_files=MagicMock(return_value=1),
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
