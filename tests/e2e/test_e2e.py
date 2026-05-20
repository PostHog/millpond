"""E2E test for the full docker-compose stack.

Brings up Kafka, Postgres, MinIO, producer, and Millpond via testcontainers,
then verifies records flow through to DuckLake.

Usage:
    just test-e2e
    uv run python -m pytest tests/e2e/test_e2e.py -v -s
"""

import time
from pathlib import Path

import duckdb
import pytest
from testcontainers.compose import DockerCompose

COMPOSE_DIR = str(Path(__file__).resolve().parents[2])


@pytest.fixture(scope="module")
def compose():
    """Bring up the full stack, yield, then tear down."""
    with DockerCompose(COMPOSE_DIR, compose_file_name="docker-compose.yaml", build=True) as c:
        c.wait_for("http://localhost:9000/minio/health/live")
        yield c


@pytest.fixture(scope="module")
def conn(compose):
    """DuckDB connection to the local DuckLake instance."""
    c = duckdb.connect()
    # INSTALL is idempotent — required on fresh DuckDB user dirs (e.g. CI
    # runners). Local dev machines usually have these from prior LOADs so
    # this used to seem to work without INSTALL; CI exposed the gap.
    c.execute("INSTALL httpfs")
    c.execute("INSTALL ducklake")
    c.execute("INSTALL postgres")
    c.execute("LOAD httpfs")
    c.execute("LOAD ducklake")
    c.execute("LOAD postgres")
    c.execute("""
        CREATE SECRET (
            TYPE s3,
            KEY_ID 'minioadmin',
            SECRET 'minioadmin',
            ENDPOINT 'localhost:9000',
            USE_SSL false,
            URL_STYLE 'path'
        )
    """)
    c.execute("""
        ATTACH 'ducklake:postgres:host=localhost port=5433 dbname=ducklake user=ducklake password=ducklake'
        AS lake (DATA_PATH 's3://ducklake/data')
    """)
    yield c
    c.close()


def _get_count(conn: duckdb.DuckDBPyConnection) -> int:
    try:
        return conn.execute("SELECT count(*) FROM lake.main.events").fetchone()[0]
    except duckdb.CatalogException:
        return 0


def _get_columns(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = 'lake' AND table_schema = 'main' AND table_name = 'events'"
    ).fetchall()
    return {row[0] for row in rows}


def _wait_for_records(conn: duckdb.DuckDBPyConnection, timeout: int = 60) -> int:
    """Poll until at least one record appears in DuckLake."""
    deadline = time.monotonic() + timeout
    while True:
        count = _get_count(conn)
        if count > 0:
            return count
        if time.monotonic() > deadline:
            pytest.fail(f"No records appeared in DuckLake within {timeout}s")
        time.sleep(2)


def _wait_for_count_increase(conn: duckdb.DuckDBPyConnection, baseline: int, timeout: int = 30) -> int:
    """Poll until count exceeds baseline."""
    deadline = time.monotonic() + timeout
    while True:
        count = _get_count(conn)
        if count > baseline:
            return count
        if time.monotonic() > deadline:
            pytest.fail(f"Count did not increase beyond {baseline} within {timeout}s")
        time.sleep(2)


@pytest.fixture(scope="module")
def initial_count(conn):
    """Wait for records once, shared across all tests."""
    return _wait_for_records(conn, timeout=90)


@pytest.mark.e2e
class TestE2E:
    def test_records_ingested(self, initial_count):
        """Records should appear in DuckLake after the stack starts."""
        assert initial_count > 0

    def test_ingestion_ongoing(self, conn, initial_count):
        """Record count should increase over time."""
        count2 = _wait_for_count_increase(conn, initial_count)
        assert count2 > initial_count

    def test_expected_columns(self, conn, initial_count):
        """Table should have the expected columns from the producer schema."""
        columns = _get_columns(conn)
        expected = {"uuid", "event", "team_id", "distinct_id", "timestamp", "_inserted_at"}
        missing = expected - columns
        assert not missing, f"Missing columns: {missing}"

    def test_data_integrity(self, conn, initial_count):
        """Core fields should not be null."""
        sample = conn.execute(
            "SELECT uuid, event, team_id FROM lake.main.events LIMIT 10"
        ).fetchall()
        assert len(sample) > 0
        for uuid, event, team_id in sample:
            assert uuid is not None
            assert event is not None
            assert team_id is not None
