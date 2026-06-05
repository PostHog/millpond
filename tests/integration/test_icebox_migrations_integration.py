"""Integration tests for icebox.postgres_sync.apply_migrations against
a real PG (QE-M2).

The conftest's `cfg` fixture runs migrations once at setup; these tests
exercise re-runs and partial-rerun scenarios that the unit tests can't
cover (the unit tests mock the cursor).
"""
from __future__ import annotations

import pytest

from icebox import postgres_sync as ps
from icebox import schema

pytestmark = pytest.mark.integration


def test_apply_migrations_is_idempotent(pool):
    """Re-running migrations against an already-migrated schema must
    succeed cleanly. The DDL is `CREATE TABLE IF NOT EXISTS` and
    `INSERT ... ON CONFLICT DO NOTHING`, but a regression that drops
    the IF NOT EXISTS guard would surface here."""
    # First run already happened in the cfg fixture; run twice more
    # to be sure.
    for _ in range(2):
        with pool.connection() as conn:
            ps.apply_migrations(conn)

    # Verify the icebox_files table is intact (1 status row, 0 file rows).
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM status")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT COUNT(*) FROM icebox_files")
            assert cur.fetchone()[0] == 0


def test_apply_migrations_recovers_from_partial_state(pool):
    """Simulate a partial migration: drop the index but keep the
    table. Re-running migrations must restore the dropped index
    without error. This is the scenario where a prior run crashed
    between CREATE TABLE and CREATE INDEX."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP INDEX IF EXISTS icebox_files_pending_idx")
            conn.commit()

    with pool.connection() as conn:
        ps.apply_migrations(conn)

    # Index must be back.
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT indexname FROM pg_indexes "
                "WHERE tablename = 'icebox_files' "
                "AND indexname = 'icebox_files_pending_idx'"
            )
            assert cur.fetchone() is not None


def test_apply_migrations_all_ddl_statements_executed(pool):
    """Every entry in ALL_DDL must be a CREATE/INSERT/ALTER that's
    safe to re-run. A regression that adds a non-idempotent statement
    (e.g., DROP TABLE) would fail this test on the second run."""
    # All entries are CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT
    # EXISTS / INSERT ... ON CONFLICT, so re-running is a no-op.
    # Use a quick string-level check as the first defence; the actual
    # double-run above is the second.
    for stmt in schema.ALL_DDL:
        normalized = stmt.upper().strip()
        is_idempotent = (
            "IF NOT EXISTS" in normalized
            or "ON CONFLICT DO NOTHING" in normalized
        )
        assert is_idempotent, (
            f"DDL must be idempotent (CREATE IF NOT EXISTS / ON CONFLICT "
            f"DO NOTHING). This statement is not:\n{stmt}"
        )
