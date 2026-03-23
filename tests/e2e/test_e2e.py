"""E2E test for the full docker-compose stack.

Brings up Kafka, Postgres, MinIO, producer, and Millpond via testcontainers,
then verifies records flow through to DuckLake.

Usage:
    just test-e2e
    uv run python -m pytest test/e2e_test.py -v -s
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
    with DockerCompose(COMPOSE_DIR, build=True) as c:
        # Wait for Postgres — once it's up, Millpond can start connecting.
        # The actual data flow is verified by polling in the tests.
        c.wait_for("http://localhost:9000/minio/health/live")
        yield c


@pytest.fixture(scope="module")
def conn(compose):
    """DuckDB connection to the local DuckLake instance."""
    c = duckdb.connect()
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


@pytest.mark.e2e
class TestE2E:
    def test_records_ingested(self, conn):
        """Records should appear in DuckLake after the stack starts."""
        count = _wait_for_records(conn)
        assert count > 0

    def test_ingestion_ongoing(self, conn):
        """Record count should increase over time."""
        count1 = _wait_for_records(conn)
        time.sleep(10)
        count2 = _get_count(conn)
        assert count2 > count1, f"Count did not increase: {count1} -> {count2}"

    def test_expected_columns(self, conn):
        """Table should have the expected columns from the producer schema."""
        _wait_for_records(conn)
        columns = _get_columns(conn)
        expected = {"uuid", "event", "team_id", "distinct_id", "timestamp", "_inserted_at"}
        missing = expected - columns
        assert not missing, f"Missing columns: {missing}"

    def test_data_integrity(self, conn):
        """Core fields should not be null."""
        _wait_for_records(conn)
        sample = conn.execute(
            "SELECT uuid, event, team_id FROM lake.main.events LIMIT 10"
        ).fetchall()
        assert len(sample) > 0
        for uuid, event, team_id in sample:
            assert uuid is not None
            assert event is not None
            assert team_id is not None
