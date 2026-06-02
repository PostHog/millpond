"""FastAPI app for the icebox REST API.

Endpoints:
  - POST /v1/files       — writer registers a parquet file
  - GET  /v1/status      — observability snapshot
  - GET  /readyz         — liveness / readiness probe (cluster + k8s use)

Backpressure logic lives here (the API decides 429 / 503 before
inserting into PG). Insertion + status reads live in postgres_async.

Dependency injection: the app's state holds the asyncpg pool, config,
and a function returning current UTC time (injected so tests can pin
'now' without monkeypatching). Handlers grab them via the `request.app`
attribute.

The app does NOT own the committer thread — that's the entrypoint's
job (main.py). The API just reads/writes PG and trusts the committer
to do its work.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime

import asyncpg
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from icebox import postgres_async as pa
from icebox.config import Config
from shared.models import (
    PROTOCOL_VERSION,
    BackpressureResponse,
    RegisteredFile,
    RegisterFileRequest,
    StatusResponse,
)

log = logging.getLogger(__name__)


def create_app(
    *,
    cfg: Config,
    pool: asyncpg.Pool,
    clock: Callable[[], datetime] | None = None,
) -> FastAPI:
    """Build the FastAPI app with config + pool + clock wired in.

    Args:
        cfg: loaded Config.
        pool: asyncpg pool (already opened).
        clock: optional callable returning current UTC time. Tests pin
            this for deterministic heartbeat-staleness checks.
    """
    app = FastAPI(title="icebox", version="1")
    app.state.cfg = cfg
    app.state.pool = pool
    app.state.clock = clock or (lambda: datetime.now(UTC))
    _register_routes(app)
    return app


def _register_routes(app: FastAPI) -> None:
    @app.post("/v1/files")
    async def register_file(req: RegisterFileRequest, request: Request):
        return await _handle_register_file(req, request)

    @app.get("/v1/status", response_model=StatusResponse)
    async def get_status(request: Request) -> StatusResponse:
        # Human-facing endpoint: pay the extra round-trip to populate
        # last_committed_iceberg_snapshot. The hot-path POST handler
        # uses the cheaper read_status which omits that field.
        return await pa.read_status_full(request.app.state.pool)

    @app.get("/readyz")
    async def readyz(request: Request) -> Response:
        return await _handle_readyz(request)


async def _handle_register_file(
    req: RegisterFileRequest,
    request: Request,
) -> Response:
    """Backpressure-first POST handler.

    Order of checks:
      1. Protocol-version mismatch → 400 (deploy skew).
      2. Committer degraded (consecutive failures >= threshold) → 503.
      3. Queue full (pending_files >= max_pending) → 429.
      4. Heartbeat stale → 503.
      5. INSERT (happy path). 409 returned if file_path already exists.

    Step 1 is cheap and pure; steps 2-4 share a single status query.
    """
    cfg: Config = request.app.state.cfg
    pool: asyncpg.Pool = request.app.state.pool
    clock = request.app.state.clock

    if req.protocol_version != PROTOCOL_VERSION:
        # Don't go further — different protocols may have different
        # body shapes and validation rules below could be wrong.
        raise HTTPException(
            status_code=400,
            detail={
                "error": "protocol_version_mismatch",
                "writer_version": req.protocol_version,
                "icebox_version": PROTOCOL_VERSION,
            },
        )

    # Backpressure checks, in priority order:
    #   1. heartbeat — the LIVENESS signal. Without a fresh heartbeat
    #      the committer might be silently dead and our other counters
    #      (consecutive_failures, pending_files) are stale.
    #   2. degraded (consecutive_failures) — failures are happening but
    #      committer IS alive.
    #   3. queue depth — capacity signal under healthy operation.
    status = await pa.read_status(pool)
    if pa.is_heartbeat_stale(
        status.last_committer_heartbeat,
        now=clock(),
        cadence_seconds=cfg.committer_cadence_seconds,
        stale_multiple=cfg.committer_heartbeat_stale_multiple,
    ):
        return JSONResponse(
            status_code=503,
            content=BackpressureResponse(
                reason="committer heartbeat stale",
                retry_after_s=cfg.committer_cadence_seconds * 2,
                consecutive_failures=status.consecutive_failures,
            ).model_dump(),
            headers={"Retry-After": str(cfg.committer_cadence_seconds * 2)},
        )

    if pa.should_reject_for_degraded(
        status.consecutive_failures,
        degraded_threshold=cfg.committer_degraded_failure_threshold,
    ):
        return JSONResponse(
            status_code=503,
            content=BackpressureResponse(
                reason="committer degraded",
                retry_after_s=cfg.committer_cadence_seconds * 2,
                consecutive_failures=status.consecutive_failures,
            ).model_dump(),
            headers={"Retry-After": str(cfg.committer_cadence_seconds * 2)},
        )

    if pa.should_reject_for_queue_depth(
        status.pending_files,
        max_pending=cfg.committer_max_pending_files,
    ):
        return JSONResponse(
            status_code=429,
            content=BackpressureResponse(
                reason="queue full",
                retry_after_s=cfg.committer_cadence_seconds,
                queue_depth=status.pending_files,
            ).model_dump(),
            headers={"Retry-After": str(cfg.committer_cadence_seconds)},
        )

    registered, was_new = await pa.insert_file(pool, req)
    status_code = 201 if was_new else 409
    return JSONResponse(
        status_code=status_code,
        content=registered.model_dump(mode="json"),
    )


async def _handle_readyz(request: Request) -> Response:
    """Readyz semantics (per ICEBOX-PLAN.md "GET /readyz decoupled from
    downstream"): pass iff Postgres is reachable AND the committer
    heartbeat is not stale. Downstream (Lakekeeper, Kafka) outages do
    NOT fail readyz — the icebox can keep accepting POSTs and stage
    files for future commit cycles.
    """
    cfg: Config = request.app.state.cfg
    pool: asyncpg.Pool = request.app.state.pool
    clock = request.app.state.clock

    try:
        status = await pa.read_status(pool)
    except Exception as e:
        log.exception("readyz: PG unreachable")
        return JSONResponse(
            status_code=503,
            content={"ready": False, "reason": "postgres_unreachable", "error": str(e)},
        )

    if pa.is_heartbeat_stale(
        status.last_committer_heartbeat,
        now=clock(),
        cadence_seconds=cfg.committer_cadence_seconds,
        stale_multiple=cfg.committer_heartbeat_stale_multiple,
    ):
        return JSONResponse(
            status_code=503,
            content={
                "ready": False,
                "reason": "committer_heartbeat_stale",
                "last_heartbeat": status.last_committer_heartbeat.isoformat()
                if status.last_committer_heartbeat else None,
            },
        )

    return JSONResponse(
        status_code=200,
        content={
            "ready": True,
            "pending_files": status.pending_files,
        },
    )
