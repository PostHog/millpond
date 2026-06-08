"""Shared fixtures for icebox-daemon integration tests.

Spins up a Postgres container once per test session via testcontainers
and yields per-test isolated psycopg pools that have already been
migrated via ``ps.apply_migrations`` against a unique schema.

The icebox-side container compose (MinIO + iceberg-rest) lives in
``compose.yaml`` and is used only by tests that talk to a real
catalog. The daemon tests below mock Lakekeeper at the
``commit_data_files`` boundary, so they don't need the iceberg-rest
stack — Postgres alone is enough.
"""
from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime

import psycopg
import pytest
from psycopg_pool import ConnectionPool
from testcontainers.postgres import PostgresContainer

from icebox import postgres_sync as ps
from icebox.config import Config


# ---------------------------------------------------------------------------
# Session-scoped Postgres container
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    """One Postgres container per test session. testcontainers pulls a
    fresh image if needed and tears it down at session exit."""
    with PostgresContainer("postgres:16-alpine") as pg:
        # Wait for PG to accept connections — the container's
        # `get_connection_url()` is built before the daemon is ready
        # to accept new connections in some testcontainers versions.
        deadline = time.monotonic() + 30.0
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            try:
                with psycopg.connect(pg.get_connection_url().replace(
                    "postgresql+psycopg2://", "postgresql://"
                ), connect_timeout=2) as conn:
                    conn.execute("SELECT 1")
                    break
            except Exception as e:  # noqa: BLE001
                last_err = e
                time.sleep(0.5)
        else:
            raise RuntimeError(
                f"Postgres in container did not accept connections within 30s; "
                f"last error: {last_err!r}"
            )
        yield pg


# ---------------------------------------------------------------------------
# Per-test isolated schema + migrated psycopg pool
# ---------------------------------------------------------------------------


def _base_cfg(pg: PostgresContainer, *, schema: str) -> Config:
    """Build a Config that points at the container, with the daemon
    knobs sized for fast tests (cadence 1s, batch 50)."""
    return Config(
        pg_host=pg.get_container_host_ip(),
        pg_port=int(pg.get_exposed_port(5432)),
        pg_database="test",
        pg_username="test",
        pg_password="test",
        pg_sslmode="disable",
        pg_schema=schema,
        psycopg_pool_min=1,
        psycopg_pool_max=4,
        iceberg_catalog_uri="http://stub",
        iceberg_warehouse="ingest",
        iceberg_namespace="kafka",
        iceberg_table="events",
        kafka_bootstrap_servers="stub:9092",
        kafka_topic="clickhouse_events_json",
        kafka_group_id="millpond-icebox-clickhouse_events_json-events",
        kafka_extra_config_json="{}",
        committer_cadence_seconds=1,
        committer_max_pending_files=50,
        committer_heartbeat_stale_multiple=3.0,
        api_host="0.0.0.0",
        api_port=8000,
        log_level="INFO",
        # Sub-second age filter so tests don't wait on wall-clock.
        age_filter_seconds=0.1,
        iceberg_timeout_s=5.0,
    )


@pytest.fixture
def cfg(pg_container: PostgresContainer) -> Iterator[Config]:
    """Per-test Config pointing at a freshly created schema.

    Yields the Config; the schema and its tables are dropped at exit.
    Per-test isolation matters because the daemon's SELECT FOR UPDATE
    SKIP LOCKED touches the same icebox_files table, and two tests
    leaking rows would cross-contaminate.
    """
    schema = f"icebox_test_{uuid.uuid4().hex[:8]}"
    cfg = _base_cfg(pg_container, schema=schema)
    # CREATE SCHEMA + apply DDL via the production helpers so the test
    # exercises the same migration runner the deployed icebox uses.
    ps.ensure_schema_exists(cfg)
    pool = ps.build_psycopg_pool(cfg)
    pool.open(wait=True)
    try:
        with pool.connection() as conn:
            ps.apply_migrations(conn)
    finally:
        pool.close()
    try:
        yield cfg
    finally:
        # Drop the schema (CASCADE so the tables go too). Use a fresh
        # connection because the per-test pool is already closed.
        admin_conn = psycopg.connect(
            host=cfg.pg_host,
            port=cfg.pg_port,
            dbname=cfg.pg_database,
            user=cfg.pg_username,
            password=cfg.pg_password,
            sslmode=cfg.pg_sslmode,
            autocommit=True,
        )
        try:
            with admin_conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            admin_conn.close()


@pytest.fixture
def pool(cfg: Config) -> Iterator[ConnectionPool]:
    """Per-test psycopg pool against the migrated schema in ``cfg``."""
    pool = ps.build_psycopg_pool(cfg)
    pool.open(wait=True)
    try:
        yield pool
    finally:
        pool.close()


# ---------------------------------------------------------------------------
# Helpers for inserting pending rows directly (bypassing the writer
# protocol for daemon-side tests). The writer-side tests use
# IceboxClient.register_file.
# ---------------------------------------------------------------------------


def insert_pending_row(
    pool: ConnectionPool,
    *,
    file_path: str,
    partition: int = 0,
    offset: int = 100,
    record_count: int = 10,
    file_size: int = 1024,
    inserted_at: datetime | None = None,
) -> int:
    """Insert one row into icebox_files in 'pending' state and return
    its id. The daemon's claim_pending_batch will pick it up when its
    inserted_at clears the age filter."""
    insert_sql = """
        INSERT INTO icebox_files (
            file_path, writer_ordinal, kafka_offsets, partition_values,
            record_count, file_size, parquet_stats, inserted_at, result
        ) VALUES (
            %(file_path)s, 0, %(kafka_offsets)s::jsonb,
            '{"day": 19000}'::jsonb, %(record_count)s, %(file_size)s,
            '{}'::jsonb, COALESCE(%(inserted_at)s, now() - interval '1 second'),
            'pending'
        )
        RETURNING id
    """
    import json as _json
    params = {
        "file_path": file_path,
        "kafka_offsets": _json.dumps({str(partition): offset}),
        "record_count": record_count,
        "file_size": file_size,
        "inserted_at": inserted_at,
    }
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(insert_sql, params)
            row = cur.fetchone()
    return row[0]


def select_result(pool: ConnectionPool, row_id: int) -> tuple[str, int | None]:
    """Read (result, iceberg_snapshot_id) for one row. Used after the
    daemon ticks to verify the tick wrote what we expect."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT result, iceberg_snapshot_id FROM icebox_files WHERE id = %s",
                (row_id,),
            )
            row = cur.fetchone()
    if row is None:
        raise AssertionError(f"row id={row_id} disappeared")
    return row[0], row[1]


def heartbeat_age_seconds(pool: ConnectionPool) -> float | None:
    """Read the heartbeat age. Returns None if the row hasn't been
    written yet."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXTRACT(EPOCH FROM (now() - last_committer_heartbeat)) "
                "FROM status WHERE id = 1"
            )
            row = cur.fetchone()
    return row[0] if row and row[0] is not None else None
