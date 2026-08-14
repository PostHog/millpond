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
    # Bind loopback only — we just need an unused port number, not actual
    # all-interfaces exposure. Avoids the security-audit "bind to 0.0.0.0"
    # finding, and matches the host the test connects back to.
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _http_get(url: str, timeout: float = 5.0) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        # /-/ready returns 503 before scheduler is up; that's expected, not a test failure.
        return e.code, (e.read() or b"").decode()


TENANT = "test"


def _stub_full_catalog(c: duckdb.DuckDBPyConnection) -> None:
    """Create the metadata schema and every table the built-in queries touch."""
    c.execute("CREATE SCHEMA __ducklake_metadata_lake")
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_data_file ("
        "data_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, "
        "path VARCHAR, file_size_bytes BIGINT, partition_id BIGINT, record_count BIGINT)"
    )
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_delete_file ("
        "delete_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, "
        "path VARCHAR, file_size_bytes BIGINT)"
    )
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_file_partition_value ("
        "data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)"
    )
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_snapshot ("
        "snapshot_id BIGINT, snapshot_time VARCHAR, schema_version BIGINT)"
    )
    c.execute("CREATE TABLE __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion (path TEXT)")
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_metadata ("
        "key VARCHAR, value VARCHAR, scope VARCHAR, scope_id BIGINT)"
    )
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_table ("
        "table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)"
    )
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_inlined_data_tables ("
        "table_id BIGINT, schema_version BIGINT, table_name VARCHAR)"
    )
    c.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_file_column_stats ("
        "data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, "
        "value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, "
        "contains_nan BOOLEAN, extra_stats VARCHAR)"
    )
    # Seed: enough rows that every metric has a non-zero value.
    c.execute(
        "INSERT INTO __ducklake_metadata_lake.ducklake_data_file VALUES "
        "(1, 1, 0, NULL, 'a', 500000, 100, 1000),"
        "(2, 1, 0, NULL, 'b', 50000000, 100, 50000),"
        "(3, 1, 0, NULL, 'c', 200000000, 200, 200000)"
    )
    c.execute("INSERT INTO __ducklake_metadata_lake.ducklake_delete_file VALUES (1, 1, 0, NULL, 'd1', 1024)")
    c.execute(
        "INSERT INTO __ducklake_metadata_lake.ducklake_file_partition_value VALUES "
        "(1, 1, 0, '2026-05-01'),"
        "(2, 1, 0, '2026-05-01'),"
        "(3, 1, 0, '2026-05-02')"
    )
    c.execute("INSERT INTO __ducklake_metadata_lake.ducklake_snapshot VALUES (0, '2026-05-01 12:00:00+00', 0)")
    c.execute(
        "INSERT INTO __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion "
        "VALUES ('s3://b/x'), ('s3://b/x'), ('s3://b/y')"
    )
    c.execute(
        "INSERT INTO __ducklake_metadata_lake.ducklake_metadata VALUES "
        "('version', '0.4', NULL, NULL),"
        "('auto_compact', 'true', NULL, NULL),"
        "('data_inlining_row_limit', '0', NULL, NULL)"
    )
    c.execute(
        "INSERT INTO __ducklake_metadata_lake.ducklake_table VALUES "
        "(1, 0, NULL),"  # live
        "(2, 0, 1)"  # dropped
    )
    c.execute(
        "INSERT INTO __ducklake_metadata_lake.ducklake_inlined_data_tables VALUES "
        "(1, 1, 'ducklake_inlined_data_1_1'),"  # parent live → reachable
        "(99, 1, 'ducklake_inlined_data_99_1')"  # no parent ducklake_table row → unreachable
    )
    c.execute(
        "INSERT INTO __ducklake_metadata_lake.ducklake_file_column_stats VALUES "
        "(1, 1, 0, 1024, 1000, 10, 'a', 'z', false, NULL),"
        "(1, 1, 1, 2048, 1000, 0, '0', '9', false, NULL)"
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
        # Liveness holder threaded through `_start_http` AND `_scheduler_loop`
        # so /-/healthy reflects actual scheduler progress (matching main()'s
        # wiring). Without this the body would stay "starting" forever and
        # the post-tick "ok" assertion below couldn't fire.
        liveness = dm._Liveness(timeout=300.0)

        port = _free_port()
        # Bind to 127.0.0.1 in tests: HTTPServer.server_bind calls
        # socket.getfqdn(host) which on some macOS configs takes 5s for the
        # default "" (any-address) — and there's no functional reason for
        # the test to bind public interfaces.
        srv = dm._start_http(port, registry=registry, host="127.0.0.1", liveness=liveness)
        try:
            # Before the scheduler runs, /-/ready must report 503: the
            # daemon is alive but not yet serving up-to-date metrics.
            status, _ = _http_get(f"http://127.0.0.1:{port}/-/ready")
            assert status == 503

            # Healthy is permissive pre-scheduler-start so initial connect
            # backoff doesn't trip the probe. The body carries the structured
            # reason so an operator hitting the endpoint manually can
            # immediately see WHICH state the daemon is in.
            status, body = _http_get(f"http://127.0.0.1:{port}/-/healthy")
            assert status == 200
            assert body.strip() == "starting"

            # Run the scheduler in a thread. Initial heap fires every
            # query at startup, so the first tick completes almost
            # immediately — no need to wait out the 1-minute interval.
            stop = threading.Event()
            self_metrics.up.labels(TENANT).set(1)
            srv.ready = True  # type: ignore[attr-defined]
            t = threading.Thread(
                target=dm._scheduler_loop,
                args=(conn, queries, gauges, self_metrics, stop, TENANT, liveness),
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

                # /-/healthy body transitions from "starting" → "ok" once
                # the scheduler has ticked. We're well inside the 300s
                # timeout, so the tick alone is enough to flip the body
                # (no in-flight query at this moment).
                status, body = _http_get(f"http://127.0.0.1:{port}/-/healthy")
                assert status == 200
                assert body.strip() == "ok"

                # /metrics now returns the full exposition. Validate
                # presence (not values — those are unit-tested) so we
                # know the names match what dashboards will scrape.
                status, body = _http_get(f"http://127.0.0.1:{port}/metrics")
                assert status == 200
                expected = [
                    "ducklake_pending_deletes_total",
                    "ducklake_pending_deletes_unique_paths",
                    "ducklake_pending_deletes_dup_rows",
                    "ducklake_data_files_files",
                    "ducklake_data_files_bytes",
                    "ducklake_data_files_rows",
                    "ducklake_delete_files_files",
                    "ducklake_delete_files_bytes",
                    "ducklake_files_per_band_count",
                    "ducklake_files_per_band_bytes",
                    "ducklake_snapshots_count",
                    "ducklake_snapshots_oldest_seconds_ago",
                    "ducklake_snapshots_newest_seconds_ago",
                    "ducklake_snapshots_oldest_id",
                    "ducklake_snapshots_newest_id",
                    "ducklake_inlined_data_tables_total",
                    "ducklake_unreachable_inline_tables_total",
                    "ducklake_tables_count",
                    "ducklake_files_per_partition_top20_count",
                    "ducklake_catalog_format_version",
                    "ducklake_config_value",
                    # self-metrics
                    "ducklake_metrics_up",
                    "ducklake_metrics_query_duration_seconds",
                    "ducklake_metrics_query_last_success_timestamp",
                    # `_liveness_failures_total` is created lazily by
                    # prometheus_client (first .labels(...).inc() call
                    # registers the series); under steady-state the probe
                    # never flips so the series is intentionally absent
                    # here. Its presence is exercised in
                    # TestLivenessProbeAtHTTPBoundary.
                ]
                for name in expected:
                    assert name in body, f"{name!r} missing from /metrics output"
                assert f'ducklake_metrics_up{{tenant="{TENANT}"}} 1.0' in body

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


@pytest.mark.integration
class TestLivenessProbeAtHTTPBoundary:
    """End-to-end: a stuck query MUST flip /-/healthy from 200 to 503.

    The whole point of the liveness machinery is unverifiable without
    driving the actual HTTP handler against an actual server with an
    actual _Liveness holder set to the values the failure case produces.
    Pure-function tests in tests/unit/test_ducklake_metrics.py cover the
    decision matrix; this test proves the wiring (handler reads server's
    liveness attr, _liveness_status returns the right reason, status code
    is 503, on_unhealthy callback fires, counter increments, body carries
    the message). If we drop this, a refactor that breaks the wiring
    while leaving the pure function intact would ship green.
    """

    def test_hang_flips_healthy_to_503_and_increments_counter(self):
        registry = CollectorRegistry()
        # 50ms timeout so we don't have to wait for the production default
        # (300s) to elapse; the daemon's wiring is invariant under timeout.
        liveness = dm._Liveness(timeout=0.05)
        self_metrics = dm._build_self_metrics(registry=registry)

        # Bind the on_unhealthy callback the same way main() does so we
        # exercise that path too — without it, the counter would stay at
        # zero even though the handler returned 503.
        def _on_unhealthy(reason_code, _message):
            self_metrics.liveness_failures.labels(TENANT, reason_code).inc()

        port = _free_port()
        srv = dm._start_http(
            port,
            registry=registry,
            host="127.0.0.1",
            liveness=liveness,
            on_unhealthy=_on_unhealthy,
        )
        try:
            # Pre-scheduler-start: handler is unconditionally 200 so initial
            # connect backoff doesn't kill the pod.
            status, body = _http_get(f"http://127.0.0.1:{port}/-/healthy")
            assert status == 200
            assert body.strip() == "starting"

            # Simulate "scheduler ticked once a while ago, then got stuck
            # mid-query": flip scheduler_started, set current_query_start
            # well past the timeout, leave last_tick fresh (the in-flight
            # signal must win over the tick signal — that's the actual
            # production failure mode for a hung DuckDB execute()).
            liveness.scheduler_started = True
            liveness.last_tick = time.monotonic()
            liveness.current_query_start = time.monotonic() - 5.0  # 100x the timeout

            status, body = _http_get(f"http://127.0.0.1:{port}/-/healthy")
            assert status == 503, f"expected 503 on in-flight hang, got {status}: {body!r}"
            assert "current query running" in body

            # Counter incremented with the structured reason — operators
            # alert on rate(ducklake_metrics_liveness_failures_total{reason="in_flight"}).
            value = registry.get_sample_value(
                "ducklake_metrics_liveness_failures_total",
                {"tenant": TENANT, "reason": dm.LIVENESS_REASON_IN_FLIGHT},
            )
            assert value == 1, f"in_flight counter not incremented; got {value}"

            # Now simulate "scheduler thread died silently" — clear the
            # in-flight signal and stale the tick. The reason code should
            # change to stale_tick and the counter gain a second series.
            liveness.current_query_start = 0.0
            liveness.last_tick = time.monotonic() - 5.0

            status, body = _http_get(f"http://127.0.0.1:{port}/-/healthy")
            assert status == 503
            assert "no scheduler tick" in body

            value = registry.get_sample_value(
                "ducklake_metrics_liveness_failures_total",
                {"tenant": TENANT, "reason": dm.LIVENESS_REASON_STALE_TICK},
            )
            assert value == 1, f"stale_tick counter not incremented; got {value}"

            # Recovery path: a fresh tick brings the probe back to 200.
            # Kubelet wouldn't see this in production (it would have
            # restarted the pod) but the handler must support it for the
            # transient-hiccup case where one slow scrape preceded the next.
            liveness.last_tick = time.monotonic()
            status, body = _http_get(f"http://127.0.0.1:{port}/-/healthy")
            assert status == 200
            assert body.strip() == "ok"
        finally:
            srv.shutdown()
            srv.server_close()
