"""Integration test for the Iceberg write path.

Brings up MinIO + an Iceberg REST catalog via docker-compose and drives
``millpond.iceberg.connect()`` / ``write()`` against it from Python.
Verifies the full commit cycle (metadata.json + manifest + parquet land
in MinIO, the catalog atomically updates) without standing up Kafka or
the millpond container.

Skipped from the default unit run (``pytest -m "not integration and not e2e"``).
"""

from __future__ import annotations

import socket
import time
import urllib.request
from pathlib import Path

import pyarrow as pa
import pytest
from testcontainers.compose import DockerCompose

from millpond import iceberg
from millpond.config import Config
from millpond.iceberg import IcebergSink


def _wait_for_http(url: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if 200 <= resp.status < 300:
                    return
        except Exception as e:
            last_err = e
        time.sleep(1.0)
    raise RuntimeError(f"endpoint {url} never came up within {timeout}s; last error: {last_err!r}")


def _wait_for_tcp(host: str, port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except Exception as e:
            last_err = e
        time.sleep(0.5)
    raise RuntimeError(f"{host}:{port} never accepted a connection within {timeout}s; last error: {last_err!r}")


@pytest.fixture(scope="module")
def stack():
    """Bring up MinIO + iceberg-rest; yield (catalog_uri, minio_endpoint)."""
    compose_dir = Path(__file__).parent
    with DockerCompose(str(compose_dir), compose_file_name="compose.yaml", pull=True) as compose:
        rest_host, rest_port = compose.get_service_host_and_port("iceberg-rest", 8181)
        minio_host, minio_port = compose.get_service_host_and_port("minio", 9000)
        catalog_uri = f"http://{rest_host}:{rest_port}"
        minio_endpoint = f"http://{minio_host}:{minio_port}"
        # iceberg-rest takes a few seconds to come up after the port binds.
        _wait_for_tcp(rest_host, int(rest_port))
        _wait_for_http(f"{catalog_uri}/v1/config")
        _wait_for_tcp(minio_host, int(minio_port))
        yield catalog_uri, minio_endpoint


@pytest.fixture
def cache() -> dict:
    """Per-test caller-owned ensure cache (formerly module-level)."""
    return {}


def _connect(catalog_uri: str, minio_endpoint: str):
    return iceberg.connect(
        catalog_uri=catalog_uri,
        warehouse="s3://warehouse/",
        s3_access_key_id="minioadmin",
        s3_secret_access_key="minioadmin",
        s3_region="us-east-1",
        s3_endpoint=minio_endpoint,
    )


def _make_iceberg_cfg(catalog_uri: str, minio_endpoint: str, table_name: str) -> Config:
    """Build a Config that targets the integration-test catalog/MinIO.

    Mirrors the env-var contract enforced by ``config.load()`` so the
    Sink-protocol path is exercised end-to-end. DuckLake fields stay None.
    """
    return Config(
        bootstrap_servers="unused:9092",
        topic="unused",
        group_id="unused",
        replica_count=1,
        ordinal=0,
        destination="iceberg",
        ducklake_table=None,
        ducklake_data_path=None,
        ducklake_connection=None,
        rds_host=None,
        rds_port=None,
        rds_database=None,
        rds_username=None,
        rds_password=None,
        partition_by=None,
        iceberg_catalog_uri=catalog_uri,
        iceberg_warehouse="s3://warehouse/",
        iceberg_namespace="millpond",
        iceberg_table=table_name,
        iceberg_table_location=None,
        iceberg_catalog_token=None,
        s3_access_key_id="minioadmin",
        s3_secret_access_key="minioadmin",
        s3_region="us-east-1",
        s3_endpoint=minio_endpoint,
        flush_size=1,
        flush_interval_ms=1,
        fetch_min_bytes=1,
        fetch_max_wait_ms=1,
        consume_batch_size=1,
        stats_interval_ms=1,
        broker_source="",
        filter_keep_field=None,
        filter_drop_field=None,
        filter_values=None,
        sort_by=None,
        kafka_config_overrides=(),
    )


@pytest.mark.integration
class TestIcebergIntegration:
    def test_write_round_trips_through_real_catalog_and_s3(self, stack, cache):
        catalog_uri, minio_endpoint = stack
        catalog = _connect(catalog_uri, minio_endpoint)

        batch = pa.table({"event": ["click", "view", "scroll"], "team_id": [1, 2, 3]})
        # Use a unique table name so module-scoped fixture reuse across
        # tests doesn't see each other's rows (we don't drop between tests).
        table_name = "events_roundtrip"
        iceberg.write(catalog, "millpond", table_name, batch, cache)

        # Re-read via the catalog; full path: metadata.json -> manifest -> parquet on MinIO.
        table = catalog.load_table(("millpond", table_name))
        rows = table.scan().to_arrow()
        assert rows.num_rows == 3
        assert sorted(rows.column("event").to_pylist()) == ["click", "scroll", "view"]
        assert set(rows.column_names) >= {
            "event",
            "team_id",
            "_inserted_at",
            "year",
            "month",
            "day",
            "hour",
        }
        # Partition column should have a sensible year — verifies the
        # PyArrow compute + Iceberg roundtrip preserved the int type.
        year = rows.column("year").to_pylist()[0]
        assert 2020 <= year <= 2099

    def test_multiple_writes_accumulate(self, stack, cache):
        catalog_uri, minio_endpoint = stack
        catalog = _connect(catalog_uri, minio_endpoint)

        batch = pa.table({"event": ["click"], "team_id": [1]})
        table_name = "events_accumulate"
        iceberg.write(catalog, "millpond", table_name, batch, cache)
        iceberg.write(catalog, "millpond", table_name, batch, cache)
        iceberg.write(catalog, "millpond", table_name, batch, cache)

        rows = catalog.load_table(("millpond", table_name)).scan().to_arrow()
        assert rows.num_rows == 3

    def test_partition_layout_is_hive_style(self, stack, cache):
        """File paths on S3 must include year=/month=/day=/hour= segments."""
        catalog_uri, minio_endpoint = stack
        catalog = _connect(catalog_uri, minio_endpoint)

        batch = pa.table({"event": ["x"], "team_id": [1]})
        table_name = "events_partitions"
        iceberg.write(catalog, "millpond", table_name, batch, cache)

        # The catalog records each data file's path; check the layout.
        table = catalog.load_table(("millpond", table_name))
        files = list(table.scan().plan_files())
        assert files, "expected at least one data file after write()"
        file_path = files[0].file.file_path
        # Path should look like: s3://warehouse/.../year=YYYY/month=MM/day=DD/hour=HH/<uuid>.parquet
        for segment in ("year=", "month=", "day=", "hour="):
            assert segment in file_path, f"{segment} missing from data file path: {file_path}"


@pytest.mark.integration
class TestIcebergSinkIntegration:
    """Exercise the `IcebergSink` protocol object — the entry point main.py uses.

    `TestIcebergIntegration` above covers the library functions; this class
    covers the Sink wrapper: construction from a Config, `write()` round-trip,
    schema evolution across writes, and `reset_caches()` after a forced
    invalidation. Same compose stack, different surface.
    """

    def test_sink_constructs_and_round_trips(self, stack):
        catalog_uri, minio_endpoint = stack
        cfg = _make_iceberg_cfg(catalog_uri, minio_endpoint, "sink_round_trip")
        sink = IcebergSink(cfg)

        batch = pa.table({"event": ["click", "view"], "team_id": [10, 20]})
        sink.write(batch)

        # Re-read via the same catalog to confirm the commit landed.
        catalog = _connect(catalog_uri, minio_endpoint)
        rows = catalog.load_table(("millpond", "sink_round_trip")).scan().to_arrow()
        assert rows.num_rows == 2
        assert sorted(rows.column("event").to_pylist()) == ["click", "view"]
        sink.close()

    def test_sink_evolves_schema_across_writes(self, stack):
        """First batch has {event, team_id}; second adds `distinct_id`.

        The Sink's internal SchemaManager should pick up the new column on
        the second write and `update_schema` the table accordingly.
        """
        catalog_uri, minio_endpoint = stack
        cfg = _make_iceberg_cfg(catalog_uri, minio_endpoint, "sink_evolve")
        sink = IcebergSink(cfg)

        sink.write(pa.table({"event": ["a"], "team_id": [1]}))
        sink.write(pa.table({"event": ["b"], "team_id": [2], "distinct_id": ["d-2"]}))

        catalog = _connect(catalog_uri, minio_endpoint)
        rows = catalog.load_table(("millpond", "sink_evolve")).scan().to_arrow()
        assert rows.num_rows == 2
        assert "distinct_id" in rows.column_names
        # First row's distinct_id was unset at write time → null after evolution.
        d = dict(zip(rows.column("event").to_pylist(), rows.column("distinct_id").to_pylist(), strict=True))
        assert d["a"] is None
        assert d["b"] == "d-2"
        sink.close()

    def test_sink_reset_caches_recovers_after_invalidation(self, stack):
        """`reset_caches()` is what main.py calls after a write failure.

        Verifies post-reset writes continue to succeed against the same table
        (cache rebuilt from the catalog).
        """
        catalog_uri, minio_endpoint = stack
        cfg = _make_iceberg_cfg(catalog_uri, minio_endpoint, "sink_reset")
        sink = IcebergSink(cfg)

        sink.write(pa.table({"event": ["pre"], "team_id": [1]}))
        sink.reset_caches()
        sink.write(pa.table({"event": ["post"], "team_id": [2]}))

        catalog = _connect(catalog_uri, minio_endpoint)
        rows = catalog.load_table(("millpond", "sink_reset")).scan().to_arrow()
        assert rows.num_rows == 2
        assert sorted(rows.column("event").to_pylist()) == ["post", "pre"]
        sink.close()
