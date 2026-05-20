"""E2E test for the full docker-compose stack with the Iceberg destination.

Brings up Kafka, MinIO, iceberg-rest, producer, and Millpond (configured with
MILLPOND_DESTINATION=iceberg) via testcontainers, then verifies records flow
through to the Iceberg table via the REST catalog.

Mirrors tests/e2e/test_e2e.py, swapping the DuckLake reader for pyiceberg.

Usage:
    just test-e2e-iceberg
    uv run python -m pytest tests/e2e/test_e2e_iceberg.py -v -s
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from testcontainers.compose import DockerCompose

from millpond import iceberg as millpond_iceberg

COMPOSE_DIR = str(Path(__file__).resolve().parents[2])
COMPOSE_FILE = "docker-compose.iceberg.yaml"

NAMESPACE = "millpond"
TABLE_NAME = "events"


@pytest.fixture(scope="module")
def compose():
    """Bring up the full Iceberg stack, yield, then tear down."""
    with DockerCompose(COMPOSE_DIR, compose_file_name=COMPOSE_FILE, build=True) as c:
        rest_host, rest_port = c.get_service_host_and_port("iceberg-rest", 8181)
        minio_host, minio_port = c.get_service_host_and_port("minio", 9000)
        catalog_uri = f"http://{rest_host}:{rest_port}"
        minio_endpoint = f"http://{minio_host}:{minio_port}"
        yield catalog_uri, minio_endpoint


@pytest.fixture(scope="module")
def catalog(compose):
    """PyIceberg REST catalog client pointed at the compose stack."""
    catalog_uri, minio_endpoint = compose
    cat = millpond_iceberg.connect(
        catalog_uri=catalog_uri,
        warehouse="s3://warehouse/",
        s3_access_key_id="minioadmin",
        s3_secret_access_key="minioadmin",
        s3_region="us-east-1",
        s3_endpoint=minio_endpoint,
    )
    yield cat


def _get_count(catalog) -> int:
    """Return row count, or 0 if the table doesn't exist yet."""
    from pyiceberg.exceptions import NoSuchTableError

    try:
        table = catalog.load_table((NAMESPACE, TABLE_NAME))
        return table.scan().to_arrow().num_rows
    except NoSuchTableError:
        return 0


def _get_columns(catalog) -> set[str]:
    from pyiceberg.exceptions import NoSuchTableError

    try:
        table = catalog.load_table((NAMESPACE, TABLE_NAME))
        return {f.name for f in table.schema().fields}
    except NoSuchTableError:
        return set()


def _wait_for_records(catalog, timeout: int = 120) -> int:
    deadline = time.monotonic() + timeout
    while True:
        count = _get_count(catalog)
        if count > 0:
            return count
        if time.monotonic() > deadline:
            pytest.fail(f"No records appeared in Iceberg table within {timeout}s")
        time.sleep(2)


def _wait_for_count_increase(catalog, baseline: int, timeout: int = 60) -> int:
    deadline = time.monotonic() + timeout
    while True:
        count = _get_count(catalog)
        if count > baseline:
            return count
        if time.monotonic() > deadline:
            pytest.fail(f"Count did not increase beyond {baseline} within {timeout}s")
        time.sleep(2)


@pytest.fixture(scope="module")
def initial_count(catalog):
    """Wait for records once, shared across all tests."""
    return _wait_for_records(catalog, timeout=120)


@pytest.mark.e2e
class TestE2EIceberg:
    def test_records_ingested(self, initial_count):
        """Records should appear in the Iceberg table after the stack starts."""
        assert initial_count > 0

    def test_ingestion_ongoing(self, catalog, initial_count):
        """Record count should increase over time as the producer keeps writing."""
        count2 = _wait_for_count_increase(catalog, initial_count)
        assert count2 > initial_count

    def test_expected_columns(self, catalog, initial_count):
        """Table should have the producer columns plus Iceberg's `_inserted_at`
        and the four partition columns."""
        columns = _get_columns(catalog)
        expected = {
            # Producer top-level fields
            "uuid", "event", "team_id", "distinct_id", "timestamp",
            # IcebergSink-injected metadata
            "_inserted_at", "year", "month", "day", "hour",
        }
        missing = expected - columns
        assert not missing, f"Missing columns: {missing}"

    def test_data_integrity(self, catalog, initial_count):
        """Core fields should not be null in a sample."""
        table = catalog.load_table((NAMESPACE, TABLE_NAME))
        sample = table.scan(limit=10).to_arrow()
        assert sample.num_rows > 0
        for col in ("uuid", "event", "team_id"):
            assert sample.column(col).null_count == 0, f"unexpected nulls in {col}"

    def test_partition_layout_is_hive_style(self, catalog, initial_count):
        """Data file paths should include year=/month=/day=/hour= segments."""
        table = catalog.load_table((NAMESPACE, TABLE_NAME))
        files = list(table.scan().plan_files())
        assert files, "expected at least one data file after ingestion"
        file_path = files[0].file.file_path
        for segment in ("year=", "month=", "day=", "hour="):
            assert segment in file_path, f"{segment} missing from {file_path}"
