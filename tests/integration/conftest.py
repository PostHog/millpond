"""Session + per-test fixtures for icebox integration tests.

This conftest is scoped to ``tests/integration/`` so the existing tests
in ``test_iceberg_integration.py`` etc. continue using ``compose.yaml``
unchanged (none of them request the fixtures defined here).

Strategy (per ``/tmp/icebox-integration-test-plan.md``):

  - Postgres: a ``testcontainers.postgres.PostgresContainer`` kept warm
    at session scope. The icebox needs real PG to exercise jsonb
    encoding, advisory locks, partial-index plans.
  - Iceberg catalog: PyIceberg ``SqlCatalog`` against a per-test SQLite
    file with a per-test filesystem warehouse. Deferred Lakekeeper to a
    follow-up — see plan doc.
  - Kafka: mocked via ``CommitterDeps.kafka_commit_offsets``. The Kafka
    container adds ~30s of boot time and the offset-commit logic is
    already covered by ``test_icebox_kafka.py``.
"""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import urlparse

import pytest
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool
from pyiceberg.catalog.sql import SqlCatalog

from icebox import committer as cm
from icebox import postgres_sync as ps
from icebox.api import create_app
from icebox.config import Config

# ---------------------------------------------------------------------------
# Session-scoped Postgres container
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_container() -> Iterator[Any]:
    """Bring up a single Postgres for the whole integration session.

    Boot cost (~3s) is amortized across every icebox integration test.
    Per-test isolation is handled by the ``migrated_pg`` fixture which
    drops + recreates the ``icebox`` schema between tests.
    """
    # Imported lazily so collection of unit tests doesn't pay the
    # testcontainers import cost.
    from testcontainers.postgres import PostgresContainer

    # postgres:16-alpine matches the major version Lakekeeper ships with
    # (Lakekeeper helm chart's PG dependency); alpine for boot speed.
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture(scope="session")
def pg_conn_kwargs(pg_container) -> dict[str, Any]:
    """Connection kwargs extracted from the container's exposed port.

    Returns dict suitable for both ``asyncpg.create_pool`` and the
    psycopg ``ConnectionPool``. Driver is stripped from the URL — the
    testcontainers default is ``postgresql+psycopg2://`` which neither
    of our drivers wants.
    """
    url = urlparse(pg_container.get_connection_url())
    return {
        "host": url.hostname,
        "port": url.port,
        "database": url.path.lstrip("/"),
        "user": url.username,
        "password": url.password,
    }


# ---------------------------------------------------------------------------
# Per-test isolation: drop + recreate the icebox schema
# ---------------------------------------------------------------------------


@pytest.fixture
def migrated_pg(pg_conn_kwargs) -> Iterator[ConnectionPool]:
    """Open a psycopg pool, drop any existing icebox schema, re-migrate,
    yield the pool. Closes on teardown.

    The drop-and-recreate gives us cleaner per-test state than TRUNCATE
    (no need to enumerate tables) and is cheap on alpine PG.
    """
    cfg = _cfg_from_pg(pg_conn_kwargs)
    pool = ps.build_psycopg_pool(cfg)
    pool.open(wait=True)

    # Per-test isolation: blow away anything from a previous test.
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS icebox CASCADE")
        conn.commit()

    with pool.connection() as conn:
        ps.apply_migrations(conn)

    try:
        yield pool
    finally:
        pool.close()


# ---------------------------------------------------------------------------
# Per-test Iceberg catalog (SqlCatalog + filesystem warehouse)
# ---------------------------------------------------------------------------


@pytest.fixture
def sql_catalog(tmp_path) -> SqlCatalog:
    """Per-test PyIceberg SqlCatalog backed by SQLite + filesystem warehouse.

    Same backend the icebox unit tests use; lets us exercise the real
    `commit_data_files` / `_append_snapshot_producer` / snapshot-summary
    plumbing without standing up Lakekeeper. The catalog-agnostic concerns
    (cycle_id in summary, schema fingerprint, recovery branches) come
    through identically.
    """
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return SqlCatalog(
        "icebox-integration",
        **{
            "uri": f"sqlite:///{tmp_path}/cat.db",
            "warehouse": f"file://{warehouse}",
        },
    )


# ---------------------------------------------------------------------------
# Config + FastAPI client
# ---------------------------------------------------------------------------


def _cfg_from_pg(
    pg_conn_kwargs: dict[str, Any],
    *,
    cadence: int = 60,
    max_pending: int = 1000,
    degraded_threshold: int = 2,
    stale_multiple: float = 3.0,
) -> Config:
    """Build a Config that points at the session Postgres.

    Iceberg/Kafka fields are placeholders — the integration tests
    don't read them through the Config; they invoke the committer with
    a CommitterDeps that wires the SqlCatalog directly.
    """
    return Config(
        pg_host=pg_conn_kwargs["host"],
        pg_port=pg_conn_kwargs["port"],
        pg_database=pg_conn_kwargs["database"],
        pg_username=pg_conn_kwargs["user"],
        pg_password=pg_conn_kwargs["password"],
        pg_sslmode="disable",
        asyncpg_pool_min=1,
        asyncpg_pool_max=4,
        psycopg_pool_min=1,
        psycopg_pool_max=2,
        iceberg_catalog_uri="unused-integration",
        iceberg_warehouse="unused-integration",
        kafka_bootstrap_servers="unused:9092",
        kafka_topic="kafka.events",
        kafka_group_id="icebox-test",
        kafka_extra_config_json="{}",
        committer_cadence_seconds=cadence,
        committer_max_pending_files=max_pending,
        committer_degraded_failure_threshold=degraded_threshold,
        committer_heartbeat_stale_multiple=stale_multiple,
        api_host="127.0.0.1",
        api_port=0,
        log_level="INFO",
    )


@pytest.fixture
def icebox_config(pg_conn_kwargs) -> Config:
    """Per-test Config wired against the session PG."""
    return _cfg_from_pg(pg_conn_kwargs)


@pytest.fixture
def app_client(
    icebox_config: Config,
    migrated_pg: ConnectionPool,
) -> Iterator[TestClient]:
    """FastAPI TestClient wired to the migrated PG.

    The asyncpg pool is owned by the FastAPI app's lifespan (mirrors
    ``icebox.main``'s production wiring). TestClient's context manager
    drives the lifespan: enters → opens the pool; exits → closes it.

    Depends on ``migrated_pg`` (not directly used) so the icebox tables
    exist before the API serves any traffic.

    Clock is left at real wall-clock — tests that need to pin it (for
    heartbeat-stale scenarios) override by building their own TestClient.
    """
    from contextlib import asynccontextmanager

    from icebox import postgres_async as pa

    @asynccontextmanager
    async def lifespan(app):
        pool = await pa.build_asyncpg_pool(icebox_config)
        app.state.pool = pool
        try:
            yield
        finally:
            await pool.close()

    app = create_app(cfg=icebox_config, pool=None)  # type: ignore[arg-type]
    app.router.lifespan_context = lifespan
    with TestClient(app) as client:
        yield client


# ---------------------------------------------------------------------------
# Committer deps — kafka mocked, iceberg wired to the SqlCatalog
# ---------------------------------------------------------------------------


def make_committer_deps(
    *,
    sql_catalog: SqlCatalog,
    namespace: str,
    table: str,
) -> cm.CommitterDeps:
    """Build CommitterDeps that load the named table from sql_catalog
    and use mock Kafka offset-commit.

    Exported as a helper rather than a fixture so tests that want to
    customize one of the deps (e.g., simulate Kafka failure) can still
    use it as a baseline.
    """
    return cm.CommitterDeps(
        load_table=lambda: sql_catalog.load_table((namespace, table)),
        kafka_admin=MagicMock(),
        kafka_commit_offsets=MagicMock(),
    )
