"""Tests for icebox.postgres_async — SQL structure + pure helpers.

The full asyncpg path requires a real PG (testcontainers-based) and is
exercised elsewhere. Here we cover:
  - SQL string structure invariants (catches column-rename drift).
  - Pure decision functions (heartbeat staleness, backpressure thresholds).
"""
from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

from icebox import postgres_async as pa

# ---------------------------------------------------------------------------
# SQL structural assertions
# ---------------------------------------------------------------------------


def test_insert_file_sql_uses_on_conflict_do_nothing():
    """Idempotent replay: same file_path submitted twice MUST collide on
    the UNIQUE constraint and produce 409 — not error 500 from a real
    UniqueViolation exception leaking up."""
    sql = pa.INSERT_FILE_SQL.lower()
    assert "on conflict (file_path) do nothing" in sql


def test_insert_file_sql_returns_id_and_staged_at():
    """The RegisteredFile response body needs both fields; RETURNING
    avoids a second round-trip on the happy path."""
    assert "returning id, staged_at" in pa.INSERT_FILE_SQL.lower()


def test_insert_file_sql_writes_all_columns():
    """If we miss a column here, the writer's POST body silently drops it."""
    sql = pa.INSERT_FILE_SQL.lower()
    for col in (
        "file_path",
        "writer_ordinal",
        "kafka_offsets",
        "partition_values",
        "record_count",
        "file_size",
        "schema_version",
        "schema_fingerprint",
        "parquet_stats",
    ):
        assert col in sql, f"INSERT_FILE_SQL missing column {col}"


def test_insert_file_sql_casts_jsonb_columns():
    """asyncpg sends strings; the cast makes PG store as jsonb."""
    sql = pa.INSERT_FILE_SQL.lower()
    # kafka_offsets, partition_values, parquet_stats are jsonb
    assert sql.count("::jsonb") == 3


def test_lookup_existing_sql_targets_file_path_unique():
    """The 409 path looks up by the same UNIQUE column it inserted on."""
    assert "where file_path = $1" in pa.LOOKUP_EXISTING_SQL.lower()


def test_status_query_returns_hot_path_fields():
    """The hot-path status query runs on every POST. It must return the
    fields the backpressure decisions depend on, but NOT
    last_committed_iceberg_snapshot (which requires an unindexed scan
    of commit_cycles that grows with cycle history)."""
    sql = pa.STATUS_QUERY_SQL.lower()
    for col in (
        "pending_files",
        "oldest_pending_age_seconds",
        "last_success_at",
        "last_cycle_at",
        "last_committer_heartbeat",
        "consecutive_failures",
    ):
        assert col in sql, f"STATUS_QUERY_SQL missing column {col}"


def test_hot_path_status_query_does_not_scan_commit_cycles():
    """PE re-review: heartbeat-first POST handler runs STATUS_QUERY_SQL
    on every request. A subquery against commit_cycles (which grows
    one row per cadence forever) would degrade to a sequential scan and
    multiply per-POST cost over time."""
    sql = pa.STATUS_QUERY_SQL.lower()
    assert "commit_cycles" not in sql, (
        "Hot-path STATUS_QUERY_SQL must NOT touch commit_cycles; use "
        "LAST_COMMITTED_SNAPSHOT_SQL via read_status_full for the "
        "/v1/status endpoint instead"
    )


def test_last_committed_snapshot_sql_scoped_to_commit_cycles():
    """Augmented status (GET /v1/status only) has its own query for
    last_committed_iceberg_snapshot. Kept separate so that adding an
    index on commit_cycles.iceberg_snapshot_id targets ONLY this
    query, not the hot path."""
    sql = pa.LAST_COMMITTED_SNAPSHOT_SQL.lower()
    assert "max(iceberg_snapshot_id)" in sql
    assert "from commit_cycles" in sql


def test_status_query_pending_filter_matches_partial_index():
    """The partial index files_unclaimed_idx covers
    (committed_at IS NULL AND cycle_id IS NULL); the pending-count
    subquery must use the same predicate to leverage that index."""
    sql = pa.STATUS_QUERY_SQL.lower()
    # Both pending_files and oldest_pending_age_seconds compute over the
    # same predicate
    occurrences = re.findall(
        r"committed_at is null and cycle_id is null", sql
    )
    assert len(occurrences) >= 2


def test_status_query_oldest_pending_uses_extract_epoch():
    """oldest_pending_age_seconds is seconds — EXTRACT(EPOCH FROM ...)
    gives fractional seconds, which is what the StatusResponse expects."""
    assert "extract(epoch from" in pa.STATUS_QUERY_SQL.lower()


def test_status_query_targets_singleton_status_row():
    """One status row, id=1 (enforced by CHECK(id=1) in DDL)."""
    assert "where s.id = 1" in pa.STATUS_QUERY_SQL.lower()


# ---------------------------------------------------------------------------
# is_heartbeat_stale — pure decision function
# ---------------------------------------------------------------------------


def test_heartbeat_none_is_not_stale():
    """Fresh install: status row exists but the committer hasn't
    written its first heartbeat. POSTs must NOT 503 in this case —
    that would block first-boot."""
    now = datetime.now(UTC)
    assert pa.is_heartbeat_stale(
        None, now=now, cadence_seconds=60, stale_multiple=3.0
    ) is False


def test_heartbeat_within_threshold_is_not_stale():
    """3 × 60 = 180s threshold. A heartbeat 60s old is fresh."""
    now = datetime.now(UTC)
    last = now - timedelta(seconds=60)
    assert pa.is_heartbeat_stale(
        last, now=now, cadence_seconds=60, stale_multiple=3.0
    ) is False


def test_heartbeat_past_threshold_is_stale():
    """Heartbeat older than 3 × cadence ⇒ committer probably dead."""
    now = datetime.now(UTC)
    last = now - timedelta(seconds=200)  # > 180s threshold
    assert pa.is_heartbeat_stale(
        last, now=now, cadence_seconds=60, stale_multiple=3.0
    ) is True


def test_heartbeat_exactly_at_threshold_is_not_stale():
    """Boundary: == 180s old, NOT > 180s. Use strict > so the boundary
    case doesn't flap."""
    now = datetime.now(UTC)
    last = now - timedelta(seconds=180)
    assert pa.is_heartbeat_stale(
        last, now=now, cadence_seconds=60, stale_multiple=3.0
    ) is False


def test_heartbeat_with_float_multiple_2_5():
    """2.5 × 60 = 150s threshold. Verify float multiples work."""
    now = datetime.now(UTC)
    fresh = now - timedelta(seconds=149)
    stale = now - timedelta(seconds=151)
    assert pa.is_heartbeat_stale(
        fresh, now=now, cadence_seconds=60, stale_multiple=2.5
    ) is False
    assert pa.is_heartbeat_stale(
        stale, now=now, cadence_seconds=60, stale_multiple=2.5
    ) is True


# ---------------------------------------------------------------------------
# Queue-depth + degraded thresholds
# ---------------------------------------------------------------------------


def test_queue_depth_below_max_does_not_reject():
    assert pa.should_reject_for_queue_depth(500, max_pending=1000) is False


def test_queue_depth_at_max_rejects():
    """Boundary: pending == max ⇒ reject. Same boundary used in plan."""
    assert pa.should_reject_for_queue_depth(1000, max_pending=1000) is True


def test_queue_depth_above_max_rejects():
    assert pa.should_reject_for_queue_depth(1500, max_pending=1000) is True


def test_degraded_below_threshold_does_not_reject():
    assert pa.should_reject_for_degraded(1, degraded_threshold=2) is False


def test_degraded_at_threshold_rejects():
    """One more failure and we're over — start 503-ing now so the writer
    backs off before we drift further."""
    assert pa.should_reject_for_degraded(2, degraded_threshold=2) is True


def test_degraded_above_threshold_rejects():
    assert pa.should_reject_for_degraded(5, degraded_threshold=2) is True
