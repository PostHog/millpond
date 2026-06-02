"""Tests for the icebox FastAPI app.

The pool is mocked at the asyncpg-pool level — FastAPI's TestClient
hits handlers that call `pa.read_status(pool)` and `pa.insert_file(pool, ...)`.
We patch those two module-level functions instead of mocking asyncpg
internals directly (cleaner test surface).
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from icebox.api import create_app
from icebox.config import Config
from shared.models import (
    PROTOCOL_VERSION,
    RegisteredFile,
    StatusResponse,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _cfg(
    *,
    max_pending: int = 1000,
    degraded_threshold: int = 2,
    cadence: int = 60,
    stale_multiple: float = 3.0,
) -> Config:
    return Config(
        pg_host="x", pg_port=5432, pg_database="x", pg_username="x", pg_password="x",
        pg_sslmode="disable",
        asyncpg_pool_min=1, asyncpg_pool_max=2,
        psycopg_pool_min=1, psycopg_pool_max=1,
        iceberg_catalog_uri="x", iceberg_warehouse="x",
        kafka_bootstrap_servers="x", kafka_topic="events",
        kafka_group_id="grp", kafka_extra_config_json="{}",
        committer_cadence_seconds=cadence,
        committer_max_pending_files=max_pending,
        committer_degraded_failure_threshold=degraded_threshold,
        committer_heartbeat_stale_multiple=stale_multiple,
        api_host="0.0.0.0", api_port=8000, log_level="INFO",
    )


def _valid_register_body() -> dict:
    return {
        "protocol_version": PROTOCOL_VERSION,
        "file_path": "s3://bucket/data/year=2026/month=06/day=01/hour=14/writer-0-abc123.parquet",
        "writer_ordinal": 0,
        "kafka_offsets": {"20": 12345},
        "partition_values": {"year": 2026, "month": 6, "day": 1, "hour": 14},
        "record_count": 1000,
        "file_size": 4096,
        "schema_fingerprint": "deadbeef" * 8,
        "schema_version": "v1",
        "parquet_stats": {
            "column_sizes": {"1": 1024},
            "value_counts": {"1": 1000},
            "null_value_counts": {"1": 0},
            "lower_bounds": {"1": 1},
            "upper_bounds": {"1": 1000},
        },
    }


def _client(
    *,
    cfg: Config | None = None,
    status: StatusResponse | None = None,
    insert_return: tuple[RegisteredFile, bool] | None = None,
    clock: datetime | None = None,
    monkeypatch=None,
):
    """Build a TestClient with mocked PG access. Returns (client, mocks)."""
    import icebox.api as api_mod
    cfg = cfg or _cfg()
    pool = MagicMock()
    clock = clock or datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    app = create_app(cfg=cfg, pool=pool, clock=lambda: clock)

    # Patch the module-level functions the handlers call
    if status is None:
        status = StatusResponse(
            pending_files=0,
            oldest_pending_age_seconds=None,
            last_success_at=clock - timedelta(seconds=5),
            last_cycle_at=clock - timedelta(seconds=5),
            last_committer_heartbeat=clock - timedelta(seconds=5),
            consecutive_failures=0,
            last_committed_iceberg_snapshot=None,
        )
    read_status_mock = AsyncMock(return_value=status)
    monkeypatch.setattr(api_mod.pa, "read_status", read_status_mock)
    # The /v1/status endpoint uses read_status_full; route it to the
    # same mock for simplicity.
    monkeypatch.setattr(api_mod.pa, "read_status_full", read_status_mock)

    if insert_return is None:
        insert_return = (
            RegisteredFile(row_id=42, queued_at=clock),
            True,
        )
    insert_file_mock = AsyncMock(return_value=insert_return)
    monkeypatch.setattr(api_mod.pa, "insert_file", insert_file_mock)

    client = TestClient(app)
    return client, read_status_mock, insert_file_mock


# ---------------------------------------------------------------------------
# POST /v1/files happy path
# ---------------------------------------------------------------------------


def test_post_v1_files_returns_201_on_new(monkeypatch):
    client, _, insert_mock = _client(monkeypatch=monkeypatch)
    resp = client.post("/v1/files", json=_valid_register_body())
    assert resp.status_code == 201
    body = resp.json()
    assert body["row_id"] == 42
    assert "queued_at" in body
    insert_mock.assert_called_once()


def test_post_v1_files_returns_409_on_existing(monkeypatch):
    """Idempotent replay: same file_path POSTed again gets the SAME body
    shape with status 409."""
    client, _, _ = _client(
        monkeypatch=monkeypatch,
        insert_return=(
            RegisteredFile(row_id=7, queued_at=datetime.now(UTC)),
            False,
        ),
    )
    resp = client.post("/v1/files", json=_valid_register_body())
    assert resp.status_code == 409
    assert resp.json()["row_id"] == 7


# ---------------------------------------------------------------------------
# Backpressure responses
# ---------------------------------------------------------------------------


def test_post_v1_files_returns_429_when_queue_full(monkeypatch):
    """Pending count >= max → 429 with queue_depth and Retry-After header."""
    status = StatusResponse(
        pending_files=1500,
        last_committer_heartbeat=datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC),
        consecutive_failures=0,
    )
    client, _, insert_mock = _client(
        monkeypatch=monkeypatch,
        cfg=_cfg(max_pending=1000),
        status=status,
    )
    resp = client.post("/v1/files", json=_valid_register_body())
    assert resp.status_code == 429
    body = resp.json()
    assert body["reason"] == "queue full"
    assert body["queue_depth"] == 1500
    assert "Retry-After" in resp.headers
    insert_mock.assert_not_called()  # Don't persist when rejecting


def test_post_v1_files_returns_503_when_degraded(monkeypatch):
    """consecutive_failures >= threshold → 503."""
    status = StatusResponse(
        pending_files=0,
        last_committer_heartbeat=datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC),
        consecutive_failures=3,
    )
    client, _, insert_mock = _client(
        monkeypatch=monkeypatch,
        cfg=_cfg(degraded_threshold=2),
        status=status,
    )
    resp = client.post("/v1/files", json=_valid_register_body())
    assert resp.status_code == 503
    assert resp.json()["reason"] == "committer degraded"
    insert_mock.assert_not_called()


def test_post_v1_files_returns_503_when_heartbeat_stale(monkeypatch):
    """Heartbeat older than stale_multiple × cadence → 503."""
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    # 60s cadence × 3.0 = 180s threshold; 300s old is well past
    status = StatusResponse(
        pending_files=0,
        last_committer_heartbeat=now - timedelta(seconds=300),
        consecutive_failures=0,
    )
    client, _, insert_mock = _client(
        monkeypatch=monkeypatch,
        cfg=_cfg(cadence=60, stale_multiple=3.0),
        status=status,
        clock=now,
    )
    resp = client.post("/v1/files", json=_valid_register_body())
    assert resp.status_code == 503
    assert resp.json()["reason"] == "committer heartbeat stale"
    insert_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Protocol version + validation
# ---------------------------------------------------------------------------


def test_post_v1_files_returns_400_on_protocol_mismatch(monkeypatch):
    """Writer ran a different image version — fail loud rather than
    silently dropping fields."""
    body = _valid_register_body()
    body["protocol_version"] = 99
    client, _, insert_mock = _client(monkeypatch=monkeypatch)
    resp = client.post("/v1/files", json=body)
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["error"] == "protocol_version_mismatch"
    assert detail["writer_version"] == 99
    assert detail["icebox_version"] == PROTOCOL_VERSION
    insert_mock.assert_not_called()


def test_post_v1_files_returns_422_on_invalid_body(monkeypatch):
    """Pydantic validation error → FastAPI returns 422."""
    body = _valid_register_body()
    del body["schema_fingerprint"]
    client, _, _ = _client(monkeypatch=monkeypatch)
    resp = client.post("/v1/files", json=body)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /v1/status
# ---------------------------------------------------------------------------


def test_get_v1_status_returns_status_response(monkeypatch):
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    status = StatusResponse(
        pending_files=42,
        oldest_pending_age_seconds=12.5,
        last_success_at=now,
        last_cycle_at=now,
        last_committer_heartbeat=now,
        consecutive_failures=0,
        last_committed_iceberg_snapshot=999,
    )
    client, _, _ = _client(monkeypatch=monkeypatch, status=status, clock=now)
    resp = client.get("/v1/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["pending_files"] == 42
    assert body["oldest_pending_age_seconds"] == 12.5
    assert body["last_committed_iceberg_snapshot"] == 999


# ---------------------------------------------------------------------------
# GET /readyz
# ---------------------------------------------------------------------------


def test_readyz_returns_200_when_healthy(monkeypatch):
    client, _, _ = _client(monkeypatch=monkeypatch)
    resp = client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json()["ready"] is True


def test_readyz_returns_503_when_heartbeat_stale(monkeypatch):
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    status = StatusResponse(
        pending_files=0,
        last_committer_heartbeat=now - timedelta(seconds=600),
        consecutive_failures=0,
    )
    client, _, _ = _client(monkeypatch=monkeypatch, status=status, clock=now)
    resp = client.get("/readyz")
    assert resp.status_code == 503
    assert resp.json()["reason"] == "committer_heartbeat_stale"


def test_readyz_returns_503_when_pg_unreachable(monkeypatch):
    """Postgres down → readyz fails, k8s removes the pod from the
    backend pool. The committer thread may still keep retrying."""
    import icebox.api as api_mod
    cfg = _cfg()
    pool = MagicMock()
    app = create_app(cfg=cfg, pool=pool, clock=lambda: datetime.now(UTC))
    monkeypatch.setattr(
        api_mod.pa, "read_status", AsyncMock(side_effect=RuntimeError("PG down")),
    )
    client = TestClient(app)
    resp = client.get("/readyz")
    assert resp.status_code == 503
    assert resp.json()["reason"] == "postgres_unreachable"


def test_readyz_does_not_fail_on_first_boot_with_no_heartbeat(monkeypatch):
    """Fresh install: status row exists but no heartbeat written yet.
    readyz must NOT fail on this — otherwise the pod is unhealthy at
    boot and k8s restarts it before the committer ever runs its first
    cycle."""
    status = StatusResponse(
        pending_files=0,
        last_committer_heartbeat=None,
        consecutive_failures=0,
    )
    client, _, _ = _client(monkeypatch=monkeypatch, status=status)
    resp = client.get("/readyz")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Review-driven: priority order of backpressure checks
# ---------------------------------------------------------------------------


def test_post_v1_files_priority_heartbeat_over_degraded(monkeypatch):
    """PE #8: heartbeat = LIVENESS, degraded/queue = capacity. A
    stale-heartbeat + degraded icebox should return 503 with reason
    'heartbeat stale', not 'degraded' — because heartbeat fires sooner
    and is more diagnostic for ops."""
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    status = StatusResponse(
        pending_files=0,
        last_committer_heartbeat=now - timedelta(seconds=999),  # stale
        consecutive_failures=99,  # also degraded
    )
    client, _, _ = _client(
        monkeypatch=monkeypatch, status=status, clock=now,
    )
    resp = client.post("/v1/files", json=_valid_register_body())
    assert resp.status_code == 503
    assert resp.json()["reason"] == "committer heartbeat stale"


def test_post_v1_files_priority_degraded_over_queue(monkeypatch):
    """Degraded check fires before queue depth — capacity is irrelevant
    when the committer is broken."""
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    status = StatusResponse(
        pending_files=99999,  # also over the queue cap
        last_committer_heartbeat=now,
        consecutive_failures=10,  # degraded
    )
    client, _, _ = _client(
        monkeypatch=monkeypatch,
        cfg=_cfg(max_pending=1000, degraded_threshold=2),
        status=status,
        clock=now,
    )
    resp = client.post("/v1/files", json=_valid_register_body())
    assert resp.status_code == 503
    assert resp.json()["reason"] == "committer degraded"


def test_post_v1_files_reads_status_exactly_once(monkeypatch):
    """Read-status amplification protection: each POST should hit the
    DB exactly once for the status snapshot. A regression that adds a
    second read doubles PG load under high writer throughput."""
    client, read_status_mock, _ = _client(monkeypatch=monkeypatch)
    client.post("/v1/files", json=_valid_register_body())
    assert read_status_mock.call_count == 1


def test_readyz_uses_injected_clock(monkeypatch):
    """Refactor safety: if someone replaces `request.app.state.clock()`
    with `datetime.now(UTC)` directly, this test catches it because the
    test's injected clock is +100 years in the future."""
    future = datetime(2126, 6, 1, 12, 0, 0, tzinfo=UTC)  # year 2126
    # Heartbeat is "fresh" by wall-clock, but with the injected future
    # clock it's 100 years stale → readyz returns 503
    status = StatusResponse(
        pending_files=0,
        last_committer_heartbeat=datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC),
        consecutive_failures=0,
    )
    client, _, _ = _client(monkeypatch=monkeypatch, status=status, clock=future)
    resp = client.get("/readyz")
    assert resp.status_code == 503
