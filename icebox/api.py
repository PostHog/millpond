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
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from icebox import metrics
from icebox import postgres_async as pa
from icebox.config import Config
from icebox.schema_cache import SchemaFingerprintCache
from shared.models import (
    PROTOCOL_VERSION,
    BackpressureResponse,
    RegisterFileRequest,
    StatusResponse,
)

log = logging.getLogger(__name__)


def create_app(
    *,
    cfg: Config,
    pool: asyncpg.Pool,
    clock: Callable[[], datetime] | None = None,
    schema_fingerprint_cache: SchemaFingerprintCache | None = None,
) -> FastAPI:
    """Build the FastAPI app with config + pool + clock wired in.

    Args:
        cfg: loaded Config.
        pool: asyncpg pool (already opened).
        clock: optional callable returning current UTC time. Tests pin
            this for deterministic heartbeat-staleness checks.
        schema_fingerprint_cache: optional cache for the Iceberg table's
            current schema fingerprint. When set, POST /v1/files
            rejects mismatches with 400 at the API perimeter instead
            of stalling them in the committer. When None (test
            harnesses that don't wire a catalog), the perimeter check
            is skipped — the committer's check still applies.
    """
    app = FastAPI(title="icebox", version="1")
    app.state.cfg = cfg
    app.state.pool = pool
    app.state.clock = clock or (lambda: datetime.now(UTC))
    app.state.schema_fingerprint_cache = schema_fingerprint_cache
    _register_routes(app)
    _register_metrics_middleware(app)
    return app


def _register_metrics_middleware(app: FastAPI) -> None:
    """Count POST /v1/files responses by status code.

    Kept as a middleware (instead of scattering ``.inc()`` calls
    across the handler's many return paths) so a refactor that adds
    a new response code is automatically counted.
    """

    @app.middleware("http")
    async def _count_post_v1_files(request: Request, call_next):  # type: ignore[no-untyped-def]
        response = await call_next(request)
        if request.method == "POST" and request.url.path == "/v1/files":
            metrics.POST_TOTAL.labels(status=str(response.status_code)).inc()
        return response


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

    @app.get("/metrics")
    async def metrics_endpoint(request: Request) -> Response:
        # Refresh PG-derived live gauges before emitting. Doing this
        # synchronously in the scrape handler keeps the values fresh
        # (at most one scrape stale) without needing a background
        # thread, and matches what Prometheus dashboards expect.
        pool: asyncpg.Pool = request.app.state.pool
        clock = request.app.state.clock
        try:
            status = await pa.read_status(pool)
        except Exception:
            log.exception("/metrics: read_status failed; live gauges stale this scrape")
        else:
            metrics.PENDING_FILES.set(status.pending_files)
            metrics.OLDEST_PENDING_AGE_SECONDS.set(
                status.oldest_pending_age_seconds
                if status.oldest_pending_age_seconds is not None
                else -1.0
            )
            metrics.CONSECUTIVE_FAILURES.set(status.consecutive_failures)
            if status.last_committer_heartbeat is not None:
                age_s = (clock() - status.last_committer_heartbeat).total_seconds()
                metrics.COMMITTER_HEARTBEAT_AGE_SECONDS.set(age_s)
            else:
                metrics.COMMITTER_HEARTBEAT_AGE_SECONDS.set(-1.0)
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/healthz")
    async def healthz() -> Response:
        # Liveness check: the API process is responsive. Cheaper than
        # /readyz which does a PG round-trip. K8s liveness probes hit
        # this; readiness probes hit /readyz. Shape mirrors millpond's
        # /healthz so the same Dockerfile HEALTHCHECK works for both
        # binaries.
        return JSONResponse(status_code=200, content={"alive": True})


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

    # Validation-only check: per-schema topology means each icebox
    # serves exactly one (namespace, table). Writers SHOULD declare
    # which table they think they're targeting; we 400 on mismatch.
    # Catches a misconfigured writer POSTing to the wrong icebox URL —
    # without this check the file would land in the wrong Iceberg
    # table and UNIQUE(file_path) wouldn't catch it (paths are still
    # globally unique).
    if (
        req.expected_iceberg_namespace is not None
        and req.expected_iceberg_namespace != cfg.iceberg_namespace
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "iceberg_namespace_mismatch",
                "writer_expected": req.expected_iceberg_namespace,
                "icebox_serves": cfg.iceberg_namespace,
                "hint": "writer is POSTing to the wrong icebox URL",
            },
        )
    if (
        req.expected_iceberg_table is not None
        and req.expected_iceberg_table != cfg.iceberg_table
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "iceberg_table_mismatch",
                "writer_expected": req.expected_iceberg_table,
                "icebox_serves": cfg.iceberg_table,
                "hint": "writer is POSTing to the wrong icebox URL",
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

    # Schema-fingerprint check at the API perimeter. The committer's
    # own fingerprint check is preserved as defense-in-depth, but
    # catching mismatches synchronously here lets writers see the
    # rejection immediately (rather than stalling in the next cycle's
    # skipped_reason="schema_mismatch") and avoids burning a cycle's
    # failure-counter slot on a bad writer.
    #
    # Ordered AFTER the backpressure checks so we don't pay the async
    # catalog round-trip cost on POSTs we'd reject for liveness or
    # capacity reasons anyway — and so backpressure 503/429 take
    # precedence over 400 (system-can't-handle takes priority over
    # bad-body).
    #
    # FAIL-OPEN if the catalog is unreachable — the existing /readyz
    # contract says downstream outages don't block POSTs, and the
    # committer's check still catches real mismatches even when the
    # perimeter is degraded.
    fp_cache: SchemaFingerprintCache | None = (
        request.app.state.schema_fingerprint_cache
    )
    if fp_cache is not None:
        try:
            fp_ok = await fp_cache.validate(req.schema_fingerprint)
        except Exception:
            log.exception(
                "register_file: fingerprint cache refresh failed; "
                "falling through to committer-side check"
            )
        else:
            if not fp_ok:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "error": "schema_fingerprint_mismatch",
                        "writer_fingerprint": req.schema_fingerprint,
                        "hint": (
                            "writer's view of the Iceberg table schema is "
                            "stale — refresh from the catalog and retry"
                        ),
                    },
                )

    registered, was_new = await pa.insert_file(pool, req)
    status_code = 201 if was_new else 409
    return JSONResponse(
        status_code=status_code,
        content=registered.model_dump(mode="json"),
    )


async def _handle_readyz(request: Request) -> Response:
    """Readyz semantics: pass iff Postgres is reachable AND the
    committer heartbeat is not stale. Downstream (Lakekeeper, Kafka)
    outages do NOT fail readyz — the icebox can keep accepting POSTs
    and stage files for future commit cycles. Decoupling from
    downstream is intentional; see ``icebox/README.md`` for the
    rationale.
    """
    cfg: Config = request.app.state.cfg
    pool: asyncpg.Pool = request.app.state.pool
    clock = request.app.state.clock

    try:
        status = await pa.read_status(pool)
    except Exception as e:
        # Log the exception detail server-side ONLY. Bind the exception
        # so its repr appears in the log MESSAGE (not just the trailing
        # traceback) — operators scanning ERROR-level logs see the
        # cause at a glance. `log.exception` also attaches the full
        # traceback via sys.exc_info() under the same record.
        #
        # The response body intentionally omits exception text / stack
        # info — /readyz is reachable from outside the pod (K8s,
        # dashboards) and an unredacted exception string could leak
        # PG connection details, internal hostnames, etc.
        log.exception("readyz: PG unreachable: %r", e)
        return JSONResponse(
            status_code=503,
            content={"ready": False, "reason": "postgres_unreachable"},
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
