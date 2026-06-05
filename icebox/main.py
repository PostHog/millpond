"""Icebox entrypoint — wires config + pool + daemon thread + probe HTTP server.

Run as: `icebox` (the console script registered in pyproject.toml).

Boot sequence:
  1. Load config from env.
  2. Configure logging.
  3. Open psycopg pool, run migrations idempotently.
  4. Build Kafka AdminClient.
  5. Spawn daemon thread.
  6. Start a minimal HTTP server on cfg.api_host:api_port serving
     /healthz (k8s liveness) and /metrics (Prometheus).

Shutdown:
  - SIGTERM → stop_event set → daemon thread exits → server exits.
"""
from __future__ import annotations

import logging
import signal
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pyiceberg.catalog import load_catalog

from icebox import config as icebox_config
from icebox import daemon as dm
from icebox import kafka as ikafka
from icebox import postgres_sync as ps
from icebox.structured_logging import setup_logging

log = logging.getLogger(__name__)

# Sentinel log line emitted at the end of main() iff the drain
# completed and the pool closed cleanly. Bound as a module constant so
# integration tests can import it.
SHUTDOWN_COMPLETE_MARKER = "icebox: shutdown complete"


def _read_heartbeat(pg_pool) -> datetime | None:
    """Read last_committer_heartbeat from the status table. Returns
    None on no row or on PG error (callers treat the latter as
    'unhealthy' for probe purposes)."""
    try:
        with pg_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_committer_heartbeat FROM status WHERE id = 1"
                )
                row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        log.warning("icebox: /healthz could not read heartbeat", exc_info=True)
        return None


def _make_probe_handler(*, cfg, pg_pool):
    """Build the BaseHTTPRequestHandler subclass for /healthz + /metrics.

    /healthz reads the daemon heartbeat from PG; stale (> N × cadence)
    or missing → 503 → k8s liveness restarts the pod.
    /metrics calls prometheus_client.generate_latest.
    """
    stale_after_s = float(cfg.committer_cadence_seconds) * float(
        cfg.committer_heartbeat_stale_multiple
    )

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):  # noqa: A002 — match stdlib API
            # Suppress the default per-request stderr line; k8s probes
            # and Prometheus scrape hits would otherwise dominate the
            # log volume.
            return

        def do_GET(self):  # noqa: N802 — stdlib API
            if self.path == "/healthz":
                hb = _read_heartbeat(pg_pool)
                if hb is None:
                    self._reply(503, "no heartbeat\n")
                    return
                age = (datetime.now(UTC) - hb).total_seconds()
                if age > stale_after_s:
                    self._reply(
                        503,
                        f"heartbeat stale ({age:.1f}s > {stale_after_s:.1f}s)\n",
                    )
                    return
                self._reply(200, f"ok heartbeat_age={age:.1f}s\n")
                return
            if self.path == "/metrics":
                payload = generate_latest()
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            self._reply(404, "not found\n")

        def _reply(self, status: int, body: str) -> None:
            payload = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return _Handler


def main() -> None:
    """Console-script entrypoint."""
    cfg = icebox_config.load()
    logger_provider = setup_logging(
        level=cfg.log_level,
        fmt=cfg.log_format,
        posthog_token=cfg.posthog_project_token,
        posthog_endpoint=cfg.posthog_logs_endpoint,
        service_name="icebox",
        service_namespace=cfg.service_namespace,
        service_version=cfg.service_version,
        service_instance_id=cfg.service_instance_id,
        iceberg_warehouse=cfg.iceberg_warehouse,
        iceberg_namespace=cfg.iceberg_namespace,
        iceberg_table=cfg.iceberg_table,
        kafka_topic=cfg.kafka_topic,
        kafka_group_id=cfg.kafka_group_id,
    )
    log.info("icebox starting on %s:%d", cfg.api_host, cfg.api_port)

    # ---- DB + schema bootstrap + psycopg pool + migrations ----------
    # Tactical hacks: create the database and schema if they don't
    # exist so a fresh deployment doesn't boot-loop. Proper provisioning
    # belongs in Terraform — these are stopgaps.
    ps.ensure_database_exists(cfg)
    ps.ensure_schema_exists(cfg)
    sync_pool = ps.build_psycopg_pool(cfg)
    sync_pool.open(wait=True)
    with sync_pool.connection() as conn:
        ps.apply_migrations(conn)
    log.info("icebox: PG migrations applied")

    # ---- Kafka AdminClient -------------------------------------------
    admin = ikafka.build_admin_client(
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        extra_config_json=cfg.kafka_extra_config_json,
    )
    log.info("icebox: kafka admin client built")

    # ---- Iceberg catalog ---------------------------------------------
    def _load_table() -> Any:
        # Called once per tick. Loading is cheap (Lakekeeper REST GET)
        # and fresh metadata is required because the catalog can
        # advance underneath us between ticks.
        catalog = load_catalog(
            "icebox",
            **{
                "type": "rest",
                "uri": cfg.iceberg_catalog_uri,
                "warehouse": cfg.iceberg_warehouse,
            },
        )
        return catalog.load_table((cfg.iceberg_namespace, cfg.iceberg_table))

    deps = dm.DaemonDeps(
        load_table=_load_table,
        kafka_admin=admin,
    )

    # ---- Daemon thread -----------------------------------------------
    stop_event = threading.Event()
    daemon_thread = threading.Thread(
        target=dm.daemon_loop,
        kwargs={
            "cfg": cfg,
            "pg_pool": sync_pool,
            "deps": deps,
            "stop_event": stop_event,
        },
        daemon=True,
        name="icebox-daemon",
    )
    daemon_thread.start()
    log.info("icebox: daemon thread started")

    # ---- Probe HTTP server -------------------------------------------
    handler_cls = _make_probe_handler(cfg=cfg, pg_pool=sync_pool)
    server = ThreadingHTTPServer((cfg.api_host, cfg.api_port), handler_cls)
    log.info("icebox: probe server listening on %s:%d", cfg.api_host, cfg.api_port)

    # ---- Graceful shutdown wiring ------------------------------------
    def _on_signal(signum, _frame):
        log.info("icebox: signal %d, requesting shutdown", signum)
        stop_event.set()
        # Wake the HTTP server's serve_forever() loop; it polls
        # _shutdown_request on a short interval so this returns quickly.
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    # Block here until server.shutdown() is called from the signal
    # handler. ThreadingHTTPServer's serve_forever spawns a thread per
    # request, so /healthz and /metrics can run concurrently with each
    # other and with the daemon thread.
    server.serve_forever()

    log.info("icebox: probe server exited, waiting for daemon thread")
    stop_event.set()
    # Drain budget = cadence × 5, capped at MAX_DRAIN_BUDGET_S so a
    # misconfigured high cadence doesn't produce a 50-minute drain that
    # outlives K8s `terminationGracePeriodSeconds`.
    MAX_DRAIN_BUDGET_S = 600  # 10 minutes
    drain_budget_s = min(cfg.committer_cadence_seconds * 5, MAX_DRAIN_BUDGET_S)
    log.info("icebox: SIGTERM drain budget = %.0fs", drain_budget_s)
    daemon_thread.join(timeout=drain_budget_s)
    if daemon_thread.is_alive():
        log.error(
            "icebox: daemon thread did not drain within %.0fs — process "
            "will exit with daemon thread still running mid-tick.",
            drain_budget_s,
        )
    else:
        log.info("icebox: daemon thread drained cleanly")
    sync_pool.close()
    if logger_provider is not None:
        try:
            logger_provider.shutdown()
        except Exception:
            log.exception("icebox: OTel LoggerProvider shutdown failed")
    log.info(SHUTDOWN_COMPLETE_MARKER)


if __name__ == "__main__":
    main()
