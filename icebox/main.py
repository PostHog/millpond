"""Icebox entrypoint — wires config + pools + committer thread + FastAPI.

Run as: `icebox` (the console script registered in pyproject.toml).

Boot sequence:
  1. Load config from env.
  2. Configure logging.
  3. Open psycopg pool, run migrations idempotently.
  4. Build Kafka AdminClient.
  5. Open asyncpg pool.
  6. Spawn committer thread.
  7. Start FastAPI under uvicorn on cfg.api_host:api_port.

Shutdown:
  - SIGTERM → stop_event set → committer thread exits → uvicorn exits.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import threading
from contextlib import asynccontextmanager

import uvicorn
from pyiceberg.catalog import load_catalog

from icebox import committer as cm
from icebox import config as icebox_config
from icebox import kafka as ikafka
from icebox import postgres_async as pa
from icebox import postgres_sync as ps
from icebox.api import create_app

log = logging.getLogger(__name__)


def main() -> None:
    """Console-script entrypoint."""
    cfg = icebox_config.load()
    logging.basicConfig(
        level=cfg.log_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    log.info("icebox starting on %s:%d", cfg.api_host, cfg.api_port)

    # ---- DB + schema bootstrap + psycopg pool + migrations -----------
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
    def _load_table():
        # Caller-of-the-callable runs in the committer thread; loading
        # is cheap (Lakekeeper REST GET) and fresh metadata is required
        # at the start of every cycle.
        catalog = load_catalog(
            "icebox",
            **{
                "type": "rest",
                "uri": cfg.iceberg_catalog_uri,
                "warehouse": cfg.iceberg_warehouse,
            },
        )
        # Each icebox deployment serves exactly one (namespace, table),
        # configured explicitly via ICEBOX_ICEBERG_NAMESPACE and
        # ICEBOX_ICEBERG_TABLE. Previously this was parsed from
        # cfg.kafka_topic with a "<ns>.<table>" or fallback "kafka.<topic>"
        # convention — fragile and hidden. Explicit env vars make the
        # mapping visible in chart values.
        return catalog.load_table((cfg.iceberg_namespace, cfg.iceberg_table))

    deps = cm.CommitterDeps(
        load_table=_load_table,
        kafka_admin=admin,
    )

    # ---- Committer thread --------------------------------------------
    stop_event = threading.Event()
    committer_thread = threading.Thread(
        target=cm.committer_loop,
        kwargs={"cfg": cfg, "pg_pool": sync_pool, "deps": deps, "stop_event": stop_event},
        daemon=True,
        name="icebox-committer",
    )
    committer_thread.start()
    log.info("icebox: committer thread started")

    # ---- FastAPI lifecycle wraps async PG pool -----------------------
    @asynccontextmanager
    async def lifespan(_app):
        pool = await pa.build_asyncpg_pool(cfg)
        _app.state.pool = pool
        try:
            yield
        finally:
            await pool.close()

    # build_app with a placeholder pool — lifespan swaps it in
    app = create_app(cfg=cfg, pool=None)  # type: ignore[arg-type]
    app.router.lifespan_context = lifespan

    # ---- Graceful shutdown wiring ------------------------------------
    def _on_signal(signum, _frame):
        log.info("icebox: signal %d, requesting shutdown", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    # ---- Run uvicorn -------------------------------------------------
    uvicorn.run(
        app,
        host=cfg.api_host,
        port=cfg.api_port,
        log_config=None,  # we configured logging above
    )

    log.info("icebox: uvicorn exited, waiting for committer thread")
    stop_event.set()
    # Drain budget = cadence × 5, capped at MAX_DRAIN_BUDGET_S so a
    # misconfigured high cadence doesn't produce a 50-minute drain that
    # outlives K8s `terminationGracePeriodSeconds`. The cap covers
    # realistic Lakekeeper commit-tail latency without becoming a
    # liveness foot-gun.
    MAX_DRAIN_BUDGET_S = 600  # 10 minutes
    drain_budget_s = min(cfg.committer_cadence_seconds * 5, MAX_DRAIN_BUDGET_S)
    log.info("icebox: SIGTERM drain budget = %.0fs", drain_budget_s)
    committer_thread.join(timeout=drain_budget_s)
    if committer_thread.is_alive():
        log.error(
            "icebox: committer thread did not drain within %.0fs — "
            "process will exit with daemon thread still running mid-cycle. "
            "Recovery on next boot will rationalize via cycle_id.",
            drain_budget_s,
        )
    else:
        log.info("icebox: committer thread drained cleanly")
    sync_pool.close()
    log.info("icebox: shutdown complete")


if __name__ == "__main__":
    main()
