"""In-container stress driver. Spawns N writer processes against the
Lakekeeper REST catalog and prints a JSON report per writer count to
stdout (prefix-tagged so the host-side test can parse them out).

This runs inside the docker network so the in-network hostnames
(`lakekeeper:8181`, `lakekeeper-minio:9000`) resolve. The host-side test
fixture brings up the compose, exec's this script, and parses stdout.

Stdout is the data channel; pretty-printed lines go to stderr.

Env vars (set by the compose):
  LK_CATALOG_URI     - e.g. http://lakekeeper:8181/catalog
  LK_MINIO_ENDPOINT  - e.g. http://lakekeeper-minio:9000
  LK_WAREHOUSE       - e.g. stress
  LK_NAMESPACE       - e.g. stress_ns
  STRESS_WRITER_COUNTS - comma-separated, e.g. "2,4,8,16,32"
  STRESS_COMMITS_PER_WRITER - e.g. "50"
  STRESS_ROWS_PER_COMMIT - e.g. "100"
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import statistics
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException

from millpond.config import Config
from millpond.iceberg import IcebergSink

CATALOG_URI = os.environ["LK_CATALOG_URI"]
MINIO_ENDPOINT = os.environ["LK_MINIO_ENDPOINT"]
WAREHOUSE = os.environ["LK_WAREHOUSE"]
NAMESPACE = os.environ["LK_NAMESPACE"]
WRITER_COUNTS = tuple(int(x) for x in os.environ["STRESS_WRITER_COUNTS"].split(","))
COMMITS_PER_WRITER = int(os.environ["STRESS_COMMITS_PER_WRITER"])
ROWS_PER_COMMIT = int(os.environ["STRESS_ROWS_PER_COMMIT"])

# Stdout = data channel; stderr = human-readable. The host wrapper parses
# stdout looking for the RESULT_PREFIX line(s) and a final SUMMARY_PREFIX.
RESULT_PREFIX = "STRESS_RESULT "
SUMMARY_PREFIX = "STRESS_SUMMARY "


def _stderr(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


@dataclass
class CommitAttempt:
    writer_id: int
    attempt_idx: int
    latency_ms: float
    status: str
    error_type: str | None


def _wait_for_http(url: str, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if 200 <= resp.status < 300:
                    return
        except Exception as e:
            last_err = e
        time.sleep(0.5)
    raise RuntimeError(f"endpoint {url} never came up within {timeout}s; last error: {last_err!r}")


def _make_cfg(table_name: str) -> Config:
    return Config(
        bootstrap_servers="unused",
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
        iceberg_catalog_uri=CATALOG_URI,
        iceberg_warehouse=WAREHOUSE,
        iceberg_namespace=NAMESPACE,
        iceberg_table=table_name,
        iceberg_table_location=None,
        iceberg_catalog_token=None,
        s3_access_key_id="minio-root-user",
        s3_secret_access_key="minio-root-password",
        s3_region="local-01",
        s3_endpoint=MINIO_ENDPOINT,
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
        kafka_config_overrides=(),
    )


def _writer_worker(
    writer_id: int,
    table_name: str,
    n_commits: int,
    barrier_path: str,
    results_queue: mp.Queue,
) -> None:
    cfg = _make_cfg(table_name)
    sink = IcebergSink(cfg)

    barrier = Path(barrier_path)
    while not barrier.exists():
        time.sleep(0.01)

    results: list[CommitAttempt] = []
    for i in range(n_commits):
        batch = pa.table(
            {
                "event": [f"e-{writer_id}-{i}-{j}" for j in range(ROWS_PER_COMMIT)],
                "team_id": [writer_id] * ROWS_PER_COMMIT,
            }
        )
        t0 = time.monotonic()
        status: str
        error_type: str | None = None
        try:
            sink.write(batch)
            status = "success"
        except CommitFailedException as e:
            status = "conflict"
            error_type = type(e).__name__
        except Exception as e:  # noqa: BLE001
            status = "error"
            error_type = type(e).__name__
        latency_ms = (time.monotonic() - t0) * 1000.0
        results.append(CommitAttempt(writer_id, i, latency_ms, status, error_type))
        # Mirror _write_with_retry's invalidate behaviour so subsequent
        # attempts don't pile up on the same stale snapshot.
        if status == "conflict":
            sink.reset_caches()

    sink.close()
    results_queue.put([(r.writer_id, r.attempt_idx, r.latency_ms, r.status, r.error_type) for r in results])


def _percentile(samples: list[float], p: float) -> float:
    if not samples:
        return float("nan")
    s = sorted(samples)
    k = max(0, min(len(s) - 1, int(round(p / 100.0 * (len(s) - 1)))))
    return s[k]


def _report(n_writers: int, attempts: list[CommitAttempt]) -> dict:
    by_status: dict[str, list[float]] = {"success": [], "conflict": [], "error": []}
    error_types: dict[str, int] = {}
    for a in attempts:
        by_status.setdefault(a.status, []).append(a.latency_ms)
        if a.error_type:
            error_types[a.error_type] = error_types.get(a.error_type, 0) + 1

    total = len(attempts)
    n_success = len(by_status["success"])
    n_conflict = len(by_status["conflict"])
    n_error = len(by_status["error"])

    all_latencies = [a.latency_ms for a in attempts]
    success_latencies = by_status["success"]

    return {
        "n_writers": n_writers,
        "total_attempts": total,
        "success": n_success,
        "conflict": n_conflict,
        "error": n_error,
        "error_types": error_types,
        "success_rate": n_success / total if total else 0.0,
        "conflict_rate": n_conflict / total if total else 0.0,
        "all_p50_ms": _percentile(all_latencies, 50),
        "all_p95_ms": _percentile(all_latencies, 95),
        "all_p99_ms": _percentile(all_latencies, 99),
        "success_p50_ms": _percentile(success_latencies, 50),
        "success_p95_ms": _percentile(success_latencies, 95),
        "success_p99_ms": _percentile(success_latencies, 99),
        "success_mean_ms": statistics.fmean(success_latencies) if success_latencies else float("nan"),
    }


def _run_one(n_writers: int) -> dict:
    table_name = f"events_n{n_writers}"

    # Seed the table with one commit so all workers start at a known snapshot.
    seed_sink = IcebergSink(_make_cfg(table_name))
    seed_sink.write(pa.table({"event": ["seed"], "team_id": [0]}))
    seed_sink.close()

    barrier_file = Path(f"/tmp/stress-barrier-n{n_writers}")
    if barrier_file.exists():
        barrier_file.unlink()
    ctx = mp.get_context("spawn")
    # Queue must come from the same context as Process — crossing contexts
    # (e.g. default fork queue + spawn process) raises in Python 3.12+.
    results_queue: mp.Queue = ctx.Queue()
    procs = []
    for w in range(n_writers):
        p = ctx.Process(
            target=_writer_worker,
            args=(w, table_name, COMMITS_PER_WRITER, str(barrier_file), results_queue),
        )
        p.start()
        procs.append(p)

    # Let workers construct their IcebergSinks before releasing the barrier.
    time.sleep(2.0)
    barrier_file.write_text("go")

    all_attempts: list[CommitAttempt] = []
    for _ in procs:
        rows = results_queue.get(timeout=600)
        all_attempts.extend(CommitAttempt(*r) for r in rows)

    for p in procs:
        p.join(timeout=30)
        if p.is_alive():
            p.terminate()

    return _report(n_writers, all_attempts)


def main() -> int:
    _wait_for_http(f"{CATALOG_URI}/v1/config?warehouse={WAREHOUSE}")

    summary = []
    for n_writers in WRITER_COUNTS:
        _stderr(f"--- running n_writers={n_writers} ---")
        report = _run_one(n_writers)
        # One self-contained JSON line per writer count.
        print(RESULT_PREFIX + json.dumps(report), flush=True)
        _stderr(
            f"n={n_writers}: success={report['success']}/{report['total_attempts']}  "
            f"conflict={report['conflict']}  errors={report['error']}  "
            f"p50={report['success_p50_ms']:.0f}ms  p95={report['success_p95_ms']:.0f}ms"
        )
        summary.append(report)

    print(SUMMARY_PREFIX + json.dumps(summary), flush=True)
    _stderr("--- done ---")
    return 0


if __name__ == "__main__":
    sys.exit(main())
