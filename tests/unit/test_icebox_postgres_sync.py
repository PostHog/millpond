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
    # the UPDATE references `files f` with an alias.
    assert "returning f.id" in ps.CLAIM_FILES_SQL.lower()


def test_claim_files_sql_uses_cte_form():
    """PE review: FOR UPDATE SKIP LOCKED inside a scalar subquery in
    an UPDATE doesn't actually pass the lock semantics through to the
    outer UPDATE in all PG planner paths. CTE form is the canonical
    fix and what we need for multi-committer correctness."""
    sql = ps.CLAIM_FILES_SQL.lower()
    assert "with candidates as" in sql
    assert "update files f" in sql
    assert "from candidates c" in sql


def test_insert_cycle_sql_only_writes_cycle_id():
    """started_at takes the PG default now(); the other state columns
    stay null until the cycle progresses."""
    sql = ps.INSERT_CYCLE_SQL.lower()
    assert "insert into commit_cycles (cycle_id)" in sql


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


def test_no_state_machine_sql_references_table_name():
    """Permanent per-schema-design invariant: no SQL in the committer's
    state machine filters by `table_name`. Per-table routing happens at
    the deployment layer (one icebox per Iceberg table, isolated by PG
    schema). If a future PR adds `WHERE table_name = ...` to any of
    these queries, that's a sign someone's trying to fold multiple
    tables back into one icebox — which the per-schema design
    deliberately avoids."""
    state_sqls = (
        ps.CLAIM_FILES_SQL,
        ps.INSERT_CYCLE_SQL,
        ps.MARK_ICEBERG_COMMITTED_SQL,
        ps.MARK_KAFKA_COMMITTED_SQL,
        ps.COMPLETE_CYCLE_SQL,
        ps.MARK_FILES_COMMITTED_SQL,
        ps.INCOMPLETE_CYCLES_SQL,
        ps.UPDATE_HEARTBEAT_SQL,
        ps.RECORD_FAILURE_SQL,
        ps.RECORD_SUCCESS_SQL,
        ps.FILES_FOR_CYCLE_SQL,
        ps.RELEASE_CYCLE_CLAIM_SQL,
        ps.DELETE_CYCLE_ROW_SQL,
    )
    for sql in state_sqls:
        assert "table_name" not in sql.lower(), (
            f"state-machine SQL contains 'table_name' — per-schema "
            f"design expresses per-table routing as deployment topology:\n{sql}"
        )


def test_delete_cycle_row_targets_by_cycle_id():
    """PE re-review #16: zombie cycle rows in the released-no-iceberg
    branch accumulate against the LIMIT=100. DELETE removes them
    cleanly. Verify the SQL is parameterized on cycle_id (not unfiltered)."""
    sql = ps.DELETE_CYCLE_ROW_SQL.lower()
    assert "delete from commit_cycles" in sql
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


class TestEnsureDatabaseExists:
    """Tactical DB-create-if-missing bootstrap. Mocked at the
    psycopg.connect boundary; the integration test exercises the real
    PG path."""

    def _cfg(self):
        from icebox.config import Config
        return Config(
            pg_host="x", pg_port=5432, pg_database="icebox",
            pg_username="u", pg_password="p", pg_sslmode="disable", pg_schema="icebox",
            asyncpg_pool_min=1, asyncpg_pool_max=2,
            psycopg_pool_min=1, psycopg_pool_max=2,
            iceberg_catalog_uri="x", iceberg_warehouse="x", iceberg_namespace="kafka", iceberg_table="events",
            kafka_bootstrap_servers="x", kafka_topic="events",
            kafka_group_id="grp", kafka_extra_config_json="{}",
            committer_cadence_seconds=60,
            committer_max_pending_files=1000,
            committer_degraded_failure_threshold=2,
            committer_heartbeat_stale_multiple=3.0,
            api_host="0.0.0.0", api_port=8000, log_level="INFO",
        )

    def _fake_connect_to_postgres(self, monkeypatch, *, exists: bool, create_raises=None):
        """Set up psycopg.connect to return a mock connection whose
        cursor returns `exists`-controlled results for the pg_database
        check, and optionally raises on the CREATE DATABASE statement."""
        from unittest.mock import MagicMock
        import psycopg

        cursor = MagicMock()
        cursor.__enter__ = lambda self: cursor
        cursor.__exit__ = lambda self, *a: None
        # The check query: cursor.fetchone() returns (1,) if exists else None.
        cursor.fetchone.return_value = (1,) if exists else None
        if create_raises is not None:
            cursor.execute.side_effect = (
                lambda stmt, *a, **kw: None if "pg_database" in str(stmt) else _raise(create_raises)
            )

        target_db = []

        def fake_connect(conninfo, **kwargs):
            for fragment in conninfo.split():
                if fragment.startswith("dbname="):
                    target_db.append(fragment.split("=", 1)[1])
            conn = MagicMock()
            conn.cursor.return_value = cursor
            conn.__enter__ = lambda self: conn
            conn.__exit__ = lambda self, *a: None
            return conn

        monkeypatch.setattr(psycopg, "connect", fake_connect)
        return cursor, target_db

    def test_returns_silently_when_database_already_exists(self, monkeypatch):
        """Steady-state path: the database exists; the function does the
        check query and returns without running CREATE DATABASE."""
        cfg = self._cfg()
        cursor, target_db = self._fake_connect_to_postgres(monkeypatch, exists=True)
        ps.ensure_database_exists(cfg)
        # Connected ONLY to the postgres system DB (one round-trip,
        # autocommit). No connect to the target DB itself.
        assert target_db == ["postgres"]
        # Check query ran; CREATE DATABASE did NOT.
        executed_sqls = [str(c.args[0]) for c in cursor.execute.call_args_list]
        assert any("pg_database" in s for s in executed_sqls)
        assert not any("CREATE DATABASE" in s for s in executed_sqls)

    def test_creates_database_when_missing(self, monkeypatch):
        """Bootstrap path: pg_database check returns no row; function
        issues CREATE DATABASE against the same `postgres` connection."""
        cfg = self._cfg()
        cursor, target_db = self._fake_connect_to_postgres(monkeypatch, exists=False)
        ps.ensure_database_exists(cfg)
        assert target_db == ["postgres"]
        executed_sqls = [str(c.args[0]) for c in cursor.execute.call_args_list]
        assert any("pg_database" in s for s in executed_sqls)
        assert any("CREATE DATABASE" in s for s in executed_sqls), (
            f"expected CREATE DATABASE in executed SQL, got: {executed_sqls}"
        )
        # The identifier is included in the Composed SQL
        assert any("icebox" in s for s in executed_sqls)

    def test_raises_when_postgres_system_db_unreachable(self, monkeypatch):
        """A connection refused (network failure, wrong host) shouldn't
        be papered over — it's an ops signal, not a bootstrap state."""
        import psycopg

        cfg = self._cfg()

        def fake_connect(conninfo, **kwargs):
            raise psycopg.OperationalError("connection refused")

        monkeypatch.setattr(psycopg, "connect", fake_connect)
        with pytest.raises(psycopg.OperationalError, match="connection refused"):
            ps.ensure_database_exists(cfg)

    def test_tolerates_concurrent_creation_race(self, monkeypatch):
        """Two icebox replicas boot simultaneously. Both pg_database
        checks return no row. Both try CREATE DATABASE. One wins; the
        other gets DuplicateDatabase. Treat as success — the DB now
        exists either way."""
        import psycopg.errors

        cfg = self._cfg()
        cursor, _ = self._fake_connect_to_postgres(
            monkeypatch,
            exists=False,
            create_raises=psycopg.errors.DuplicateDatabase("already exists"),
        )
        # MUST NOT raise — race is benign.
        ps.ensure_database_exists(cfg)

    def test_propagates_insufficient_privilege_on_create_database(self, monkeypatch):
        """If the icebox PG user lacks CREATEDB privilege, the
        bootstrap can't paper over it — the operator needs an
        actionable error pointing at the GRANT they need. PE-review
        #5: wrap the raw InsufficientPrivilege with a RuntimeError
        that names the required GRANT."""
        import psycopg.errors

        cfg = self._cfg()
        self._fake_connect_to_postgres(
            monkeypatch,
            exists=False,
            create_raises=psycopg.errors.InsufficientPrivilege(
                "permission denied to create database"
            ),
        )
        with pytest.raises(RuntimeError, match="CREATEDB"):
            ps.ensure_database_exists(cfg)


class TestEnsureSchemaExists:
    """Per-schema bootstrap. Mocked at the psycopg.connect boundary;
    real-PG path covered by tests/integration/test_icebox_e2e.py."""

    def _cfg(self, schema="icebox_events"):
        from icebox.config import Config
        return Config(
            pg_host="x", pg_port=5432, pg_database="icebox",
            pg_username="u", pg_password="p", pg_sslmode="disable",
            pg_schema=schema,
            asyncpg_pool_min=1, asyncpg_pool_max=2,
            psycopg_pool_min=1, psycopg_pool_max=2,
            iceberg_catalog_uri="x", iceberg_warehouse="x", iceberg_namespace="kafka", iceberg_table="events",
            kafka_bootstrap_servers="x", kafka_topic="events",
            kafka_group_id="grp", kafka_extra_config_json="{}",
            committer_cadence_seconds=60,
            committer_max_pending_files=1000,
            committer_degraded_failure_threshold=2,
            committer_heartbeat_stale_multiple=3.0,
            api_host="0.0.0.0", api_port=8000, log_level="INFO",
        )

    def test_propagates_insufficient_privilege_on_create_schema(self, monkeypatch):
        """If the icebox PG user lacks CREATE-on-database privilege,
        the bootstrap can't paper over it — operator needs the clear
        signal to run `GRANT CREATE ON DATABASE <db> TO <user>`.
        Wrap the InsufficientPrivilege with a RuntimeError that names
        the required GRANT (PE-review #5)."""
        from unittest.mock import MagicMock

        import psycopg
        import psycopg.errors

        cfg = self._cfg()
        cursor = MagicMock()
        cursor.__enter__ = lambda self: cursor
        cursor.__exit__ = lambda self, *a: None
        # First execute: the SELECT short-circuit check. Schema doesn't
        # exist → fetchone returns None → proceed to CREATE.
        # Second execute: the CREATE SCHEMA, which raises.
        cursor.fetchone.return_value = None

        def fake_execute(stmt, *a, **kw):
            if "information_schema" in str(stmt).lower():
                return None  # SELECT succeeds with no row
            raise psycopg.errors.InsufficientPrivilege(
                "permission denied for database"
            )

        cursor.execute.side_effect = fake_execute

        def fake_connect(conninfo, **kwargs):
            conn = MagicMock()
            conn.cursor.return_value = cursor
            conn.__enter__ = lambda self: conn
            conn.__exit__ = lambda self, *a: None
            return conn

        monkeypatch.setattr(psycopg, "connect", fake_connect)
        with pytest.raises(RuntimeError, match="GRANT CREATE ON DATABASE"):
            ps.ensure_schema_exists(cfg)


def _raise(exc):
    """Helper for side_effect that raises a specific exception."""
    raise exc


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


def test_committer_advisory_lock_id_is_derived_from_schema():
    """The lock id is part of the deployment contract — same schema
    always derives the same lock id, different schemas derive different
    lock ids. This is what lets events and person iceboxes share a PG
    instance without lock conflicts."""
    a = ps.committer_advisory_lock_id("icebox")
    b = ps.committer_advisory_lock_id("icebox")
    assert isinstance(a, int)
    # Stable: same input → same output
    assert a == b
    # 64-bit signed range
    assert -(2**63) <= a < 2**63
    # Different schemas → different ids
    assert ps.committer_advisory_lock_id("icebox_events") != ps.committer_advisory_lock_id("icebox_person")
    assert ps.committer_advisory_lock_id("icebox") != ps.committer_advisory_lock_id("icebox_events")


def test_committer_advisory_lock_id_known_values_for_deployed_schemas():
    """Pin EXACT lock ids for every schema we plan to deploy. Any
    change to the derivation (algorithm, prefix, version tag, byte
    order, truncation length) breaks this test loudly.

    Bumping these values is a deliberate migration — an old pod
    holding the stale-key lock would NOT block a new pod and the
    singleton-committer invariant collapses for the duration. Don't
    casually."""
    expected = {
        # Default and the 6 PostHog deployment schemas
        "icebox": 828423287862594287,
        "icebox_events": -5369055521494626710,
        "icebox_person": -5406596138296386448,
        "icebox_person_distinct_id": 307705151288516432,
        "icebox_groups": 6963762061167329373,
        "icebox_heatmap_events": 7276092344871654942,
        "icebox_ai_events": 353718527436049174,
    }
    actual = {s: ps.committer_advisory_lock_id(s) for s in expected}
    assert actual == expected, (
        f"committer_advisory_lock_id derivation drifted; got:\n{actual}\n"
        f"expected:\n{expected}"
    )


def test_committer_advisory_lock_id_never_zero_for_deployed_schemas():
    """`pg_try_advisory_lock(0)` works in PG but `pg_locks` diagnostic
    queries some operators rely on filter on `objid != 0`. Verify
    none of our deployed schemas hash to 0."""
    for schema in (
        "icebox",
        "icebox_events", "icebox_person", "icebox_person_distinct_id",
        "icebox_groups", "icebox_heatmap_events", "icebox_ai_events",
    ):
        assert ps.committer_advisory_lock_id(schema) != 0, (
            f"schema {schema!r} hashes to lock id 0; pick a different name"
        )


def test_try_acquire_advisory_lock_returns_true_when_pg_returns_true():
    conn, cur = _mock_conn_with_cursor()
    cur.fetchone.return_value = (True,)
    lock_id = ps.committer_advisory_lock_id("icebox")
    assert ps.try_acquire_committer_lock(conn, lock_id=lock_id) is True
    cur.execute.assert_called_once_with(
        ps.TRY_ADVISORY_LOCK_SQL,
        {"key": lock_id},
    )


def test_try_acquire_advisory_lock_returns_false_when_pg_returns_false():
    """Another committer is holding the lock."""
    conn, cur = _mock_conn_with_cursor()
    cur.fetchone.return_value = (False,)
    assert ps.try_acquire_committer_lock(conn, lock_id=42) is False


def test_try_acquire_advisory_lock_raises_on_null_row():
    """PE-review #6: an empty result or NULL value from
    pg_try_advisory_lock is a TRANSPORT ERROR, not a 'lock held'
    signal. The previous behavior coerced it to False, which would
    make six pods spin on a phantom 'lock held' while PG was actually
    degraded. RuntimeError so the caller's exception-handler arm
    catches it as a transient failure rather than mistaking it for a
    permanent state."""
    conn, cur = _mock_conn_with_cursor()
    cur.fetchone.return_value = None
    with pytest.raises(RuntimeError, match="NULL"):
        ps.try_acquire_committer_lock(conn, lock_id=42)


def test_try_acquire_advisory_lock_raises_on_null_value_in_row():
    """Same defense for the row-exists-but-value-is-NULL case."""
    conn, cur = _mock_conn_with_cursor()
    cur.fetchone.return_value = (None,)
    with pytest.raises(RuntimeError, match="NULL"):
        ps.try_acquire_committer_lock(conn, lock_id=42)


def test_try_acquire_advisory_lock_passes_through_explicit_key():
    """Caller-controlled lock id; production derives via
    committer_advisory_lock_id(cfg.pg_schema), tests pass arbitrary."""
    conn, cur = _mock_conn_with_cursor()
    cur.fetchone.return_value = (True,)
    ps.try_acquire_committer_lock(conn, lock_id=42)
    cur.execute.assert_called_once_with(
        ps.TRY_ADVISORY_LOCK_SQL, {"key": 42}
    )


def test_release_committer_lock_calls_pg_advisory_unlock():
    conn, cur = _mock_conn_with_cursor()
    ps.release_committer_lock(conn, lock_id=42)
    cur.execute.assert_called_once_with(
        ps.UNLOCK_ADVISORY_LOCK_SQL,
        {"key": 42},
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
