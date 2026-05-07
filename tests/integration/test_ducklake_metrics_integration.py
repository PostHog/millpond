"""Integration test for tools/ducklake_metrics.py.

Wires the daemon's components together against an in-memory DuckDB
acting as a stub DuckLake catalog: builds the gauges, starts the HTTP
server on an ephemeral port, runs the scheduler in a background
thread, and scrapes /metrics + the health endpoints over real HTTP.

Validates the end-to-end exposition shape (metric names, presence,
status codes) without requiring Postgres, S3, or k8s. Per-query SQL
correctness is covered in unit tests; this test exists for everything
*around* the query: HTTP server, scheduler thread, gauge registration,
ready/healthy handler paths.
"""

from __future__ import annotations

import socket
import threading
import time
import urllib.request

import duckdb
import ducklake_metrics as dm
import pytest
from prometheus_client import CollectorRegistry


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _http_get(url: str, timeout: float = 5.0) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        # /-/ready returns 503 before scheduler is up; that's expected, not a test failure.
        return e.code, (e.read() or b"").decode()


def _stub_full_catalog(c: duckdb.DuckDBPyConnection) -> None:
    """Create the metadata schema and all five tables the built-ins query."""
    c.execute("CREATE SCHEMA __ducklake_metadata_lake")
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_data_file ("
        "data_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, "
        "path VARCHAR, file_size_bytes BIGINT, partition_id BIGINT)"
    )
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_file_partition_value ("
        "data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)"
    )
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_snapshot ("
        "snapshot_id BIGINT, snapshot_time VARCHAR, schema_version BIGINT)"
    )
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion (path TEXT)"
    )
    # Seed: enough rows that every metric has a non-zero value.
    c.execute(
        "INSERT INTO __ducklake_metadata_lake.ducklake_data_file VALUES "
        "(1, 1, 0, NULL, 'a', 500000, 100),"
        "(2, 1, 0, NULL, 'b', 50000000, 100),"
        "(3, 1, 0, NULL, 'c', 200000000, 200)"
    )
    c.execute(
        "INSERT INTO __ducklake_metadata_lake.ducklake_file_partition_value VALUES "
        "(1, 1, 0, '2026-05-01'),"
        "(2, 1, 0, '2026-05-01'),"
        "(3, 1, 0, '2026-05-02')"
    )
    c.execute(
        "INSERT INTO __ducklake_metadata_lake.ducklake_snapshot VALUES "
        "(0, '2026-05-01 12:00:00+00', 0)"
    )
    c.execute(
        "INSERT INTO __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion "
        "VALUES ('s3://b/x'), ('s3://b/x'), ('s3://b/y')"
    )


@pytest.mark.integration
class TestDaemonHTTP:
    """Drive the daemon's HTTP + scheduler surface without mocking either."""

    def test_metrics_and_health_endpoints(self):
        registry = CollectorRegistry()
        conn = duckdb.connect()
        _stub_full_catalog(conn)

        queries = dm.load_queries(None, set())
        gauges = dm._build_query_gauges(queries, registry=registry)
        self_metrics = dm._build_self_metrics(registry=registry)

        port = _free_port()
        # Bind to 127.0.0.1 in tests: HTTPServer.server_bind calls
        # socket.getfqdn(host) which on some macOS configs takes 5s for the
        # default "" (any-address) — and there's no functional reason for
        # the test to bind public interfaces.
        srv = dm._start_http(port, registry=registry, host="127.0.0.1")
        try:
            # Before the scheduler runs, /-/ready must report 503: the
            # daemon is alive but not yet serving up-to-date metrics.
            status, _ = _http_get(f"http://127.0.0.1:{port}/-/ready")
            assert status == 503

            # Healthy is independent of scheduler state — it's a liveness
            # signal only ("the process answers HTTP").
            status, body = _http_get(f"http://127.0.0.1:{port}/-/healthy")
            assert status == 200
            assert body.strip() == "ok"

            # Run the scheduler in a thread. Initial heap fires every
            # query at startup, so the first tick completes almost
            # immediately — no need to wait out the 1-minute interval.
            stop = threading.Event()
            self_metrics.up.set(1)
            srv.ready = True  # type: ignore[attr-defined]
            t = threading.Thread(
                target=dm._scheduler_loop,
                args=(conn, queries, gauges, self_metrics, stop),
                name="scheduler",
                daemon=True,
            )
            t.start()
            try:
                # Wait for every query to have logged a successful run.
                # last_success_timestamp is the cleanest signal because it
                # only flips on success — pure "scheduler did work" check.
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    samples = {
                        m.labels.get("query")
                        for fam in registry.collect()
                        if fam.name == "ducklake_metrics_query_last_success_timestamp"
                        for m in fam.samples
                    }
                    if all(q.name in samples for q in queries):
                        break
                    time.sleep(0.05)
                else:
                    pytest.fail(
                        "scheduler did not complete a successful run for every query within 5s; "
                        f"saw last_success for: {samples}"
                    )

                # /-/ready now flipped to 200.
                status, _ = _http_get(f"http://127.0.0.1:{port}/-/ready")
                assert status == 200

                # /metrics now returns the full exposition. Validate
                # presence (not values — those are unit-tested) so we
                # know the names match what dashboards will scrape.
                status, body = _http_get(f"http://127.0.0.1:{port}/metrics")
                assert status == 200
                expected = [
                    # b4
                    "ducklake_pending_deletes_total",
                    "ducklake_pending_deletes_unique_paths",
                    "ducklake_pending_deletes_dup_rows",
                    # b2
                    "ducklake_files_per_band_count",
                    "ducklake_files_per_band_bytes",
                    # b3
                    "ducklake_compaction_candidates_count",
                    # b5
                    "ducklake_snapshots_count",
                    "ducklake_snapshots_oldest_seconds_ago",
                    "ducklake_snapshots_newest_seconds_ago",
                    # b6
                    "ducklake_files_per_partition_top20_count",
                    # self-metrics
                    "ducklake_metrics_up",
                    "ducklake_metrics_query_duration_seconds",
                    "ducklake_metrics_query_last_success_timestamp",
                ]
                for name in expected:
                    assert name in body, f"{name!r} missing from /metrics output"
                assert "ducklake_metrics_up 1.0" in body

                # 404 path — bare sanity that we didn't open random URLs.
                status, _ = _http_get(f"http://127.0.0.1:{port}/anything-else")
                assert status == 404
            finally:
                stop.set()
                t.join(timeout=2.0)
                assert not t.is_alive(), "scheduler thread did not exit on stop"
        finally:
            srv.shutdown()
            srv.server_close()
            conn.close()
