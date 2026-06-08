"""Integration tests for the writer-side IceboxClient (psycopg INSERT).

Real PG via testcontainers. No Lakekeeper / Kafka involvement — these
tests pin the wire shape between the writer and ``icebox_files``:

  - INSERT succeeds → returns (body, 201) with the new row id.
  - ON CONFLICT (file_path) DO NOTHING → returns (body, 409) with the
    existing row id (idempotent writer replay).
  - jsonb columns round-trip through json.dumps (psycopg won't
    auto-encode dicts in our version).
  - The INSERTed row is visible to the daemon's claim_pending_batch.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from icebox import postgres_sync as ps
from millpond.icebox_sink import IceboxClient
from shared.models import ParquetStats, RegisterFileRequest

pytestmark = pytest.mark.integration


def _make_client(cfg) -> IceboxClient:
    client = IceboxClient(
        host=cfg.pg_host,
        port=cfg.pg_port,
        database=cfg.pg_database,
        username=cfg.pg_username,
        password=cfg.pg_password,
        schema=cfg.pg_schema,
        sslmode=cfg.pg_sslmode,
    )
    client._pool.open(wait=True)
    return client


def _make_request(file_path: str = "s3://b/file.parquet", *, offset: int = 100) -> RegisterFileRequest:
    return RegisterFileRequest(
        file_path=file_path,
        writer_ordinal=0,
        kafka_offsets={"0": offset},
        partition_values={"year": 2026, "month": 6, "day": 5},
        record_count=10,
        file_size=1024,
        schema_fingerprint="a" * 64,
        parquet_stats=ParquetStats(
            column_sizes={"1": 100},
            value_counts={"1": 10},
            null_value_counts={"1": 0},
            lower_bounds={"1": 1},
            upper_bounds={"1": 100},
        ),
    )


def test_register_file_201_inserts_row(cfg, pool):
    """A fresh file_path → INSERT → 201 with the new row id and
    inserted_at timestamp in the body."""
    client = _make_client(cfg)
    try:
        body, status = client.register_file(_make_request("s3://b/new.parquet"))
    finally:
        client.close()

    assert status == 201
    assert isinstance(body["row_id"], int)
    assert body["row_id"] > 0
    # queued_at parses as an ISO timestamp.
    parsed = datetime.fromisoformat(body["queued_at"])
    assert parsed is not None

    # Row is visible in PG with result='pending'.
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_path, result, kafka_offsets, partition_values "
                "FROM icebox_files WHERE id = %s",
                (body["row_id"],),
            )
            row = cur.fetchone()
    assert row is not None
    assert row[0] == "s3://b/new.parquet"
    assert row[1] == "pending"
    # jsonb fields round-trip.
    assert row[2] == {"0": 100}
    assert row[3] == {"year": 2026, "month": 6, "day": 5}


def test_register_file_409_on_replay_returns_existing_row(cfg, pool):
    """Re-INSERT with the same file_path hits ON CONFLICT DO NOTHING,
    falls back to a SELECT, returns 409 with the SAME row id as the
    first INSERT. This is the writer's idempotent-replay contract."""
    client = _make_client(cfg)
    try:
        body1, status1 = client.register_file(_make_request("s3://b/replay.parquet"))
        assert status1 == 201

        # Replay — same file_path.
        body2, status2 = client.register_file(_make_request("s3://b/replay.parquet"))
        assert status2 == 409
        assert body2["row_id"] == body1["row_id"]
    finally:
        client.close()


def test_register_file_distinct_paths_get_distinct_rows(cfg, pool):
    """Different file_paths produce different rows (no accidental
    collapse via the ON CONFLICT clause)."""
    client = _make_client(cfg)
    try:
        body_a, _ = client.register_file(_make_request("s3://b/a.parquet"))
        body_b, _ = client.register_file(_make_request("s3://b/b.parquet"))
    finally:
        client.close()
    assert body_a["row_id"] != body_b["row_id"]


def test_register_file_is_visible_to_daemon_select(cfg, pool):
    """The writer's INSERT must be visible to the daemon's
    `claim_pending_batch` (with an age filter that the test row
    clears). This is the end-to-end handoff that the polling design
    depends on."""
    import time as _t

    client = _make_client(cfg)
    try:
        body, _ = client.register_file(_make_request("s3://b/handoff.parquet"))
    finally:
        client.close()

    # Sleep past the age filter (cfg sets 0.1s for tests).
    _t.sleep(0.2)

    with pool.connection() as conn:
        rows = ps.claim_pending_batch(
            conn,
            batch_size=cfg.committer_max_pending_files,
            age_seconds=cfg.age_filter_seconds,
        )

    assert any(r.id == body["row_id"] for r in rows), (
        f"writer-INSERTed row id={body['row_id']} not returned by "
        f"daemon claim_pending_batch (got ids {[r.id for r in rows]!r})"
    )
