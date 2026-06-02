"""Tests for icebox.postgres_sync — SQL string structure + call shapes.

Real PG integration is exercised by a testcontainers-based test
elsewhere (out-of-scope here). Unit tests here verify:
  - SQL strings reference the right tables/columns/clauses (the same
    structural-assertion pattern as test_icebox_schema.py).
  - The Python helpers issue the expected cursor calls with the
    expected parameters.
"""
from __future__ import annotations

import re
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from icebox import postgres_sync as ps


# ---------------------------------------------------------------------------
# SQL structural assertions — catches "renamed column, forgot to update SQL"
# ---------------------------------------------------------------------------


def test_claim_files_sql_uses_skip_locked():
    """Without SKIP LOCKED, two recovering committers would block each
    other rather than skipping locked rows. The skip-locked pattern is
    load-bearing."""
    assert "for update skip locked" in ps.CLAIM_FILES_SQL.lower()


def test_claim_files_sql_only_claims_unclaimed_uncommitted_files():
    """The WHERE filter must exclude already-claimed AND already-committed
    rows. Either omission would re-claim files into a new cycle."""
    sql = ps.CLAIM_FILES_SQL.lower()
    assert "cycle_id is null" in sql
    assert "committed_at is null" in sql


def test_claim_files_sql_orders_by_staged_at():
    """FIFO: oldest unclaimed file goes first, so commit latency stays
    bounded even under heavy steady-state load."""
    assert re.search(r"order\s+by\s+staged_at", ps.CLAIM_FILES_SQL.lower())


def test_claim_files_sql_returns_id():
    """Caller needs the id list for per-cycle bookkeeping."""
    # CTE-form UPDATE uses `RETURNING f.id` (table-qualified) because
    # the UPDATE references `icebox.files f` with an alias.
    assert "returning f.id" in ps.CLAIM_FILES_SQL.lower()


def test_claim_files_sql_uses_cte_form():
    """PE review: FOR UPDATE SKIP LOCKED inside a scalar subquery in
    an UPDATE doesn't actually pass the lock semantics through to the
    outer UPDATE in all PG planner paths. CTE form is the canonical
    fix and what we need for multi-committer correctness."""
    sql = ps.CLAIM_FILES_SQL.lower()
    assert "with candidates as" in sql
    assert "update icebox.files f" in sql
    assert "from candidates c" in sql


def test_insert_cycle_sql_only_writes_cycle_id():
    """started_at takes the PG default now(); the other state columns
    stay null until the cycle progresses."""
    sql = ps.INSERT_CYCLE_SQL.lower()
    assert "insert into icebox.commit_cycles (cycle_id)" in sql


def test_mark_iceberg_committed_sql_updates_only_snapshot_id():
    sql = ps.MARK_ICEBERG_COMMITTED_SQL.lower()
    assert "set iceberg_snapshot_id" in sql
    assert "where cycle_id" in sql


def test_mark_kafka_committed_sql_stamps_now():
    """Timestamp matters — recovery scans key on this field's null-ness."""
    sql = ps.MARK_KAFKA_COMMITTED_SQL.lower()
    assert "kafka_committed_at = now()" in sql


def test_complete_cycle_sql_stamps_completed_at():
    assert "completed_at = now()" in ps.COMPLETE_CYCLE_SQL.lower()


def test_mark_files_committed_sql_sets_both_committed_and_snapshot():
    """Both committed_at AND iceberg_snapshot_id need to flow into files
    so the operator can answer 'which snapshot does this file belong to?'."""
    sql = ps.MARK_FILES_COMMITTED_SQL.lower()
    assert "committed_at = now()" in sql
    assert "iceberg_snapshot_id" in sql


def test_incomplete_cycles_sql_filters_on_completed_at_null():
    """The partial index commit_cycles_incomplete_idx covers this filter."""
    assert "completed_at is null" in ps.INCOMPLETE_CYCLES_SQL.lower()


def test_incomplete_cycles_sql_selects_state_machine_columns():
    """The Python helper builds CommitCycleRow from these — every state-
    machine column must be in the SELECT list."""
    sql = ps.INCOMPLETE_CYCLES_SQL.lower()
    for col in ("cycle_id", "started_at", "iceberg_snapshot_id",
                "kafka_committed_at", "completed_at"):
        assert col in sql, f"INCOMPLETE_CYCLES_SQL missing column {col}"


def test_update_heartbeat_sql_targets_singleton_row():
    """The status table has CHECK(id=1); the heartbeat update must
    target id=1, not run unfiltered (which would surprise readers)."""
    assert "where id = 1" in ps.UPDATE_HEARTBEAT_SQL.lower()


def test_record_failure_increments_consecutive_failures():
    assert "consecutive_failures + 1" in ps.RECORD_FAILURE_SQL.lower()


def test_record_success_resets_counter_and_stamps_timestamps():
    sql = ps.RECORD_SUCCESS_SQL.lower()
    assert "consecutive_failures = 0" in sql
    assert "last_success_at = now()" in sql
    assert "last_cycle_at = now()" in sql


def test_release_cycle_claim_only_releases_uncommitted_files():
    """Defensive: never release a committed file (its data is already
    in Iceberg)."""
    sql = ps.RELEASE_CYCLE_CLAIM_SQL.lower()
    assert "committed_at is null" in sql


def test_delete_cycle_row_targets_by_cycle_id():
    """PE re-review #16: zombie cycle rows in the released-no-iceberg
    branch accumulate against the LIMIT=100. DELETE removes them
    cleanly. Verify the SQL is parameterized on cycle_id (not unfiltered)."""
    sql = ps.DELETE_CYCLE_ROW_SQL.lower()
    assert "delete from icebox.commit_cycles" in sql
    assert "where cycle_id = %(cycle_id)s" in sql


# ---------------------------------------------------------------------------
# Helper call-shape verification
# ---------------------------------------------------------------------------


def _mock_conn_with_cursor():
    """A psycopg.Connection mock whose cursor() context manager yields
    a cursor we can introspect."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = lambda self: cursor
    conn.cursor.return_value.__exit__ = lambda self, *a: None
    return conn, cursor


def test_claim_files_passes_cycle_id_and_max_files():
    conn, cur = _mock_conn_with_cursor()
    cycle = uuid4()
    cur.fetchall.return_value = [(1,), (2,), (3,)]
    result = ps.claim_files(conn, cycle_id=cycle, max_files=100)
    cur.execute.assert_called_once_with(
        ps.CLAIM_FILES_SQL, {"cycle_id": cycle, "max_files": 100}
    )
    assert result == [1, 2, 3]


def test_claim_files_returns_empty_on_no_rows():
    conn, cur = _mock_conn_with_cursor()
    cur.fetchall.return_value = []
    assert ps.claim_files(conn, cycle_id=uuid4(), max_files=10) == []


def test_insert_cycle_executes_with_cycle_id():
    conn, cur = _mock_conn_with_cursor()
    cycle = uuid4()
    ps.insert_cycle(conn, cycle_id=cycle)
    cur.execute.assert_called_once_with(ps.INSERT_CYCLE_SQL, {"cycle_id": cycle})


def test_mark_iceberg_committed_passes_snapshot_id():
    conn, cur = _mock_conn_with_cursor()
    cycle = uuid4()
    ps.mark_iceberg_committed(conn, cycle_id=cycle, snapshot_id=42)
    cur.execute.assert_called_once_with(
        ps.MARK_ICEBERG_COMMITTED_SQL,
        {"cycle_id": cycle, "snapshot_id": 42},
    )


def test_mark_kafka_committed_passes_only_cycle_id():
    conn, cur = _mock_conn_with_cursor()
    cycle = uuid4()
    ps.mark_kafka_committed(conn, cycle_id=cycle)
    cur.execute.assert_called_once_with(
        ps.MARK_KAFKA_COMMITTED_SQL, {"cycle_id": cycle}
    )


def test_complete_cycle_runs_two_updates():
    """First updates files, then the cycle row — both target the same
    cycle_id in the same transaction."""
    conn, cur = _mock_conn_with_cursor()
    cycle = uuid4()
    ps.complete_cycle(conn, cycle_id=cycle, snapshot_id=42)
    calls = cur.execute.call_args_list
    assert len(calls) == 2
    assert calls[0][0][0] == ps.MARK_FILES_COMMITTED_SQL
    assert calls[1][0][0] == ps.COMPLETE_CYCLE_SQL


def test_incomplete_cycles_builds_pydantic_rows():
    from datetime import UTC, datetime

    conn, cur = _mock_conn_with_cursor()
    cid = uuid4()
    cur.fetchall.return_value = [
        (cid, datetime.now(UTC), None, None, None),
    ]
    rows = ps.incomplete_cycles(conn)
    assert len(rows) == 1
    assert rows[0].cycle_id == cid
    assert rows[0].iceberg_snapshot_id is None


def test_update_heartbeat_executes_without_args():
    conn, cur = _mock_conn_with_cursor()
    ps.update_heartbeat(conn)
    cur.execute.assert_called_once_with(ps.UPDATE_HEARTBEAT_SQL)


def test_record_failure_and_success_no_args():
    conn, cur = _mock_conn_with_cursor()
    ps.record_failure(conn)
    ps.record_success(conn)
    assert cur.execute.call_args_list[0][0][0] == ps.RECORD_FAILURE_SQL
    assert cur.execute.call_args_list[1][0][0] == ps.RECORD_SUCCESS_SQL


def test_release_cycle_claim_passes_cycle_id():
    conn, cur = _mock_conn_with_cursor()
    cycle = uuid4()
    ps.release_cycle_claim(conn, cycle_id=cycle)
    cur.execute.assert_called_once_with(
        ps.RELEASE_CYCLE_CLAIM_SQL, {"cycle_id": cycle}
    )


def test_delete_cycle_row_passes_cycle_id():
    conn, cur = _mock_conn_with_cursor()
    cycle = uuid4()
    ps.delete_cycle_row(conn, cycle_id=cycle)
    cur.execute.assert_called_once_with(
        ps.DELETE_CYCLE_ROW_SQL, {"cycle_id": cycle}
    )


# ---------------------------------------------------------------------------
# Advisory lock — singleton-committer guarantee
# ---------------------------------------------------------------------------


def test_committer_advisory_lock_id_is_stable_constant():
    """The lock id is part of the deployment contract — any change
    means an old pod holding the lock would NOT block a new pod, which
    breaks the single-committer guarantee."""
    assert isinstance(ps.COMMITTER_ADVISORY_LOCK_ID, int)
    # 64-bit value; rolling it forces a deliberate review
    assert ps.COMMITTER_ADVISORY_LOCK_ID == 0x4F6E1C3E_5B7A8D90


def test_try_acquire_advisory_lock_returns_true_when_pg_returns_true():
    conn, cur = _mock_conn_with_cursor()
    cur.fetchone.return_value = (True,)
    assert ps.try_acquire_committer_lock(conn) is True
    cur.execute.assert_called_once_with(
        ps.TRY_ADVISORY_LOCK_SQL,
        {"key": ps.COMMITTER_ADVISORY_LOCK_ID},
    )


def test_try_acquire_advisory_lock_returns_false_when_pg_returns_false():
    """Another committer is holding the lock."""
    conn, cur = _mock_conn_with_cursor()
    cur.fetchone.return_value = (False,)
    assert ps.try_acquire_committer_lock(conn) is False


def test_try_acquire_advisory_lock_returns_false_on_empty_result():
    """Defensive: PG should always return a row from pg_try_advisory_lock
    but if it didn't we'd rather treat it as 'not acquired' than crash."""
    conn, cur = _mock_conn_with_cursor()
    cur.fetchone.return_value = None
    assert ps.try_acquire_committer_lock(conn) is False


def test_try_acquire_advisory_lock_accepts_explicit_key():
    """The lock id is overridable for tests; production passes the
    default constant."""
    conn, cur = _mock_conn_with_cursor()
    cur.fetchone.return_value = (True,)
    ps.try_acquire_committer_lock(conn, lock_id=42)
    cur.execute.assert_called_once_with(
        ps.TRY_ADVISORY_LOCK_SQL, {"key": 42}
    )


def test_release_committer_lock_calls_pg_advisory_unlock():
    conn, cur = _mock_conn_with_cursor()
    ps.release_committer_lock(conn)
    cur.execute.assert_called_once_with(
        ps.UNLOCK_ADVISORY_LOCK_SQL,
        {"key": ps.COMMITTER_ADVISORY_LOCK_ID},
    )


# ---------------------------------------------------------------------------
# apply_migrations — runs ALL_DDL statements one at a time
# ---------------------------------------------------------------------------


def test_apply_migrations_executes_each_ddl_statement():
    from icebox.schema import ALL_DDL

    conn, cur = _mock_conn_with_cursor()
    ps.apply_migrations(conn)
    assert cur.execute.call_count == len(ALL_DDL)
    # Verify each call's SQL matches the DDL in order
    for i, call in enumerate(cur.execute.call_args_list):
        assert call[0][0] == ALL_DDL[i]
    conn.commit.assert_called_once()


def test_apply_migrations_attributes_failure_to_statement():
    """A DDL syntax error should produce an error message naming WHICH
    statement broke."""
    import psycopg

    conn, cur = _mock_conn_with_cursor()
    cur.execute.side_effect = psycopg.Error("syntax error")
    with pytest.raises(RuntimeError, match=r"DDL statement 1/\d+"):
        ps.apply_migrations(conn)
