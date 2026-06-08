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


def test_update_heartbeat_sql_targets_singleton_row():
    """The status table has CHECK(id=1); the heartbeat update must
    target id=1, not run unfiltered (which would surprise readers)."""
    assert "where id = 1" in ps.UPDATE_HEARTBEAT_SQL.lower()


def test_claim_pending_batch_sql_filters_on_pending_and_age():
    """The hot SELECT is bounded by O(pending) via the partial index AND
    by the age filter that prevents committing batches younger than the
    interval (batching efficiency)."""
    sql = ps.CLAIM_PENDING_BATCH_SQL.lower()
    assert "result = 'pending'" in sql
    assert "inserted_at < now() - make_interval(secs => %(age_seconds)s)" in sql


def test_claim_pending_batch_sql_orders_by_inserted_at_with_limit():
    """FIFO. Oldest pending row gets committed first; LIMIT bounds blast
    radius per tick."""
    sql = ps.CLAIM_PENDING_BATCH_SQL.lower()
    assert re.search(r"order\s+by\s+inserted_at", sql)
    assert "limit %(batch_size)s" in sql


def test_mark_committed_sql_sets_result_and_snapshot_id():
    """Both columns transition atomically. result_at stamps the moment
    the daemon learned of success."""
    sql = ps.MARK_COMMITTED_SQL.lower()
    assert "set result='committed'" in sql
    assert "result_at=now()" in sql
    assert "iceberg_snapshot_id=%(snapshot_id)s" in sql
    assert "where id = any(%(ids)s)" in sql


def test_mark_failed_sql_does_not_touch_snapshot_id():
    """A failed batch never produced an Iceberg snapshot; leaving
    iceberg_snapshot_id NULL is part of the audit signal."""
    sql = ps.MARK_FAILED_SQL.lower()
    assert "set result='failed'" in sql
    assert "result_at=now()" in sql
    assert "iceberg_snapshot_id" not in sql
    assert "where id = any(%(ids)s)" in sql


def test_no_polling_daemon_sql_references_cycle_id():
    """v6 deletes the cycle abstraction. None of the new helpers' SQL
    should reference cycle_id — that's the lingering-cycle-code smell
    we explicitly guard against during the rollout window where both
    code paths coexist."""
    new_sqls = (
        ps.CLAIM_PENDING_BATCH_SQL,
        ps.MARK_COMMITTED_SQL,
        ps.MARK_FAILED_SQL,
    )
    for sql in new_sqls:
        assert "cycle_id" not in sql.lower(), (
            f"polling-daemon SQL must not reference cycle_id; "
            f"cycle abstraction is being deleted:\n{sql}"
        )


def test_claim_pending_batch_passes_batch_size_and_age():
    from datetime import UTC, datetime

    conn, cur = _mock_conn_with_cursor()
    cur.fetchall.return_value = [
        (
            1,                                # id
            "s3://bucket/a.parquet",          # file_path
            0,                                # writer_ordinal
            {"0": 100},                       # kafka_offsets
            {"day": 19000},                   # partition_values
            42,                               # record_count
            1234,                             # file_size
            {"col": {}},                      # parquet_stats
            datetime.now(UTC),                # inserted_at
            "pending",                        # result
            None,                             # result_at
            None,                             # iceberg_snapshot_id
        ),
    ]
    rows = ps.claim_pending_batch(conn, batch_size=100, age_seconds=60.0)
    cur.execute.assert_called_once_with(
        ps.CLAIM_PENDING_BATCH_SQL,
        {"batch_size": 100, "age_seconds": 60.0},
    )
    assert len(rows) == 1
    assert rows[0].id == 1
    assert rows[0].file_path == "s3://bucket/a.parquet"
    assert rows[0].result == "pending"


def test_claim_pending_batch_returns_empty_on_no_rows():
    conn, cur = _mock_conn_with_cursor()
    cur.fetchall.return_value = []
    rows = ps.claim_pending_batch(conn, batch_size=100, age_seconds=60.0)
    assert rows == []


def test_mark_committed_passes_ids_and_snapshot_id():
    conn, cur = _mock_conn_with_cursor()
    ps.mark_committed(conn, ids=[1, 2, 3], snapshot_id=999)
    cur.execute.assert_called_once_with(
        ps.MARK_COMMITTED_SQL,
        {"snapshot_id": 999, "ids": [1, 2, 3]},
    )


def test_mark_committed_noop_on_empty_ids():
    """Avoid pointless round-trips when callers hand us an empty list
    (e.g. a vacuous tick that somehow got this far)."""
    conn, cur = _mock_conn_with_cursor()
    ps.mark_committed(conn, ids=[], snapshot_id=1)
    cur.execute.assert_not_called()


def test_mark_failed_passes_ids():
    conn, cur = _mock_conn_with_cursor()
    ps.mark_failed(conn, ids=[10, 20])
    cur.execute.assert_called_once_with(
        ps.MARK_FAILED_SQL, {"ids": [10, 20]}
    )


def test_mark_failed_noop_on_empty_ids():
    conn, cur = _mock_conn_with_cursor()
    ps.mark_failed(conn, ids=[])
    cur.execute.assert_not_called()


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


def test_update_heartbeat_executes_without_args():
    conn, cur = _mock_conn_with_cursor()
    ps.update_heartbeat(conn)
    cur.execute.assert_called_once_with(ps.UPDATE_HEARTBEAT_SQL)


def test_peek_oldest_pending_file_path_returns_path_when_row_exists():
    conn, cur = _mock_conn_with_cursor()
    cur.fetchone.return_value = ("s3://b/oldest.parquet",)
    assert ps.peek_oldest_pending_file_path(conn) == "s3://b/oldest.parquet"
    cur.execute.assert_called_once_with(ps.PEEK_OLDEST_PENDING_FILE_PATH_SQL)


def test_peek_oldest_pending_file_path_returns_none_when_empty():
    """No pending rows = the caller (daemon's bootstrap path) skips
    table creation and waits for the next writer flush."""
    conn, cur = _mock_conn_with_cursor()
    cur.fetchone.return_value = None
    assert ps.peek_oldest_pending_file_path(conn) is None


def test_peek_oldest_pending_file_path_sql_filters_pending_and_orders():
    """FIFO + result='pending' so we don't seed table bootstrap off a
    row that's already been committed or failed."""
    sql = ps.PEEK_OLDEST_PENDING_FILE_PATH_SQL.lower()
    assert "result='pending'" in sql
    assert re.search(r"order\s+by\s+inserted_at", sql)
    assert "limit 1" in sql


class TestEnsureDatabaseExists:
    """Tactical DB-create-if-missing bootstrap. Mocked at the
    psycopg.connect boundary; the integration test exercises the real
    PG path."""

    def _cfg(self):
        from icebox.config import Config
        return Config(
            pg_host="x", pg_port=5432, pg_database="icebox",
            pg_username="u", pg_password="p", pg_sslmode="disable", pg_schema="icebox",
            psycopg_pool_min=1, psycopg_pool_max=2,
            iceberg_catalog_uri="x", iceberg_warehouse="x", iceberg_namespace="kafka", iceberg_table="events",
            kafka_bootstrap_servers="x", kafka_topic="events",
            kafka_group_id="grp", kafka_extra_config_json="{}",
            committer_cadence_seconds=60,
            committer_max_pending_files=1000,
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
            psycopg_pool_min=1, psycopg_pool_max=2,
            iceberg_catalog_uri="x", iceberg_warehouse="x", iceberg_namespace="kafka", iceberg_table="events",
            kafka_bootstrap_servers="x", kafka_topic="events",
            kafka_group_id="grp", kafka_extra_config_json="{}",
            committer_cadence_seconds=60,
            committer_max_pending_files=1000,
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
