#!/usr/bin/env python3
"""DuckLake state-metrics daemon.

Long-running process that periodically runs a fixed set of catalog-side
queries against a DuckLake and exposes the results as Prometheus gauges.
Built-in queries cover lake-shape signals (size-band distribution,
compaction-tier candidate counts, pending-deletion queue depth, snapshot
age). Operators can supply additional queries via a YAML file referenced
by ``DUCKLAKE_METRICS_CONFIG``.

Reuses ``maintenance.connect()`` so the daemon inherits the same lake +
postgres ATTACH, S3 secret, and session tunables that maintenance.py runs
under. Same env vars as the maintenance script:
  DUCKLAKE_RDS_HOST, DUCKLAKE_RDS_PORT, DUCKLAKE_RDS_DATABASE,
  DUCKLAKE_RDS_USERNAME, DUCKLAKE_RDS_PASSWORD, DUCKLAKE_DATA_PATH,
  DUCKDB_S3_REGION, DUCKDB_S3_ACCESS_KEY_ID, DUCKDB_S3_SECRET_ACCESS_KEY
  (plus optional DUCKDB_S3_ENDPOINT, DUCKDB_S3_USE_SSL, DUCKDB_S3_URL_STYLE)

Optional:
  DUCKLAKE_METRICS_PORT     — HTTP listen port (default 9100)
  DUCKLAKE_METRICS_CONFIG   — path to user-supplied queries YAML
  DUCKLAKE_METRICS_DISABLE  — comma-separated query names to skip
"""

from __future__ import annotations

import argparse
import contextlib
import heapq
import itertools
import logging
import os
import re
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb
import maintenance
import yaml
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

log = logging.getLogger("ducklake_metrics")


# Built-in queries are embedded so the binary is self-contained; they parse
# through the same loader as user YAML so the two paths can't diverge.
#
# SQL refers to the metadata schema by its literal name. maintenance.connect()
# always ATTACHes the lake as ``lake``, which fixes the schema as
# ``__ducklake_metadata_lake`` (per maintenance.py's METADATA_SCHEMA). Built-in
# queries hardcode that name; user-supplied queries are passed through verbatim
# and may reference whatever schema they like.
BUILTIN_YAML = """
queries:
  - name: ducklake_pending_deletes
    help: Pending-deletion queue depth and duplicate-row pathology.
    interval_mins: 1
    values: [total, unique_paths, dup_rows]
    sql: |
      SELECT
        COUNT(*) AS total,
        COUNT(DISTINCT path) AS unique_paths,
        COUNT(*) - COUNT(DISTINCT path) AS dup_rows
      FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion

  - name: ducklake_files_per_band
    help: Live data files grouped into byte-size bands.
    interval_mins: 1
    labels: [band]
    values: [count, bytes]
    sql: |
      SELECT
        CASE
          WHEN file_size_bytes < 1048576    THEN 'lt1mib'
          WHEN file_size_bytes < 5242880    THEN '1to5mib'
          WHEN file_size_bytes < 10485760   THEN '5to10mib'
          WHEN file_size_bytes < 33554432   THEN '10to32mib'
          WHEN file_size_bytes < 67108864   THEN '32to64mib'
          WHEN file_size_bytes < 134217728  THEN '64to128mib'
          ELSE 'gt128mib'
        END AS band,
        COUNT(*) AS count,
        COALESCE(SUM(file_size_bytes), 0) AS bytes
      FROM __ducklake_metadata_lake.ducklake_data_file
      WHERE end_snapshot IS NULL
      GROUP BY band

  - name: ducklake_compaction_candidates
    help: Live file counts bucketed to match maintenance.py's TIERS spec.
    interval_mins: 1
    labels: [tier]
    values: [count]
    sql: |
      SELECT tier, COUNT(*) AS count
      FROM (
        SELECT CASE
          WHEN file_size_bytes < 1048576   THEN 'tier1'
          WHEN file_size_bytes < 10485760  THEN 'tier2'
          WHEN file_size_bytes < 67108864  THEN 'tier3'
          ELSE 'large'
        END AS tier
        FROM __ducklake_metadata_lake.ducklake_data_file
        WHERE end_snapshot IS NULL
      ) t
      GROUP BY tier
      UNION ALL
      SELECT 'total' AS tier, COUNT(*) AS count
      FROM __ducklake_metadata_lake.ducklake_data_file
      WHERE end_snapshot IS NULL

  - name: ducklake_snapshots
    help: Snapshot count plus age of oldest/newest snapshot in seconds.
    interval_mins: 1
    values: [count, oldest_seconds_ago, newest_seconds_ago]
    sql: |
      SELECT
        COUNT(*) AS count,
        COALESCE(EXTRACT(EPOCH FROM (now() - MIN(CAST(snapshot_time AS TIMESTAMPTZ)))), 0) AS oldest_seconds_ago,
        COALESCE(EXTRACT(EPOCH FROM (now() - MAX(CAST(snapshot_time AS TIMESTAMPTZ)))), 0) AS newest_seconds_ago
      FROM __ducklake_metadata_lake.ducklake_snapshot

  - name: ducklake_files_per_partition_top20
    help: Twenty heaviest partitions by live data-file count (composite values joined with '/').
    interval_mins: 1
    labels: [partition]
    values: [count]
    sql: |
      WITH labels AS (
        SELECT data_file_id,
               string_agg(partition_value, '/' ORDER BY partition_key_index) AS partition
        FROM __ducklake_metadata_lake.ducklake_file_partition_value
        GROUP BY data_file_id
      )
      SELECT
        COALESCE(l.partition, '<none>') AS partition,
        COUNT(*) AS count
      FROM __ducklake_metadata_lake.ducklake_data_file df
      LEFT JOIN labels l USING (data_file_id)
      WHERE df.end_snapshot IS NULL
      GROUP BY partition
      ORDER BY count DESC
      LIMIT 20
"""

# Intervals are specified in whole minutes via the YAML field `interval_mins`
# (integer, >= 1). The unit is encoded in the field name so the value is just
# a number — no "1m" suffix parsing, no ambiguity. Sub-minute polling buys
# nothing for the catalog signals this daemon publishes.
_MIN_INTERVAL_MINS = 1

_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@dataclass
class Query:
    name: str
    help: str
    sql: str
    interval_seconds: int
    labels: list[str] = field(default_factory=list)
    values: list[str] = field(default_factory=list)


def _validate_interval_mins(v: object) -> int:
    """Validate the YAML ``interval_mins`` field; return seconds.

    Must be a positive integer. ``bool`` is rejected explicitly because
    it's an ``int`` subclass and would otherwise sneak through.
    """
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError(f"interval_mins must be an integer (whole minutes), got {type(v).__name__}: {v!r}")
    if v < _MIN_INTERVAL_MINS:
        raise ValueError(f"interval_mins must be >= {_MIN_INTERVAL_MINS}, got {v!r}")
    return v * 60


def _load_yaml_doc(text: str, source: str) -> list[dict]:
    doc = yaml.safe_load(text) or {}
    if not isinstance(doc, dict) or "queries" not in doc:
        raise ValueError(f"{source}: top-level must be a mapping with a 'queries' key")
    queries = doc["queries"]
    if queries is None:
        return []
    if not isinstance(queries, list):
        raise ValueError(f"{source}: 'queries' must be a list")
    return queries


def _query_from_dict(d: dict, source: str) -> Query:
    for key in ("name", "help", "interval_mins", "sql"):
        if key not in d:
            raise ValueError(f"{source}: query missing required key {key!r}")
    name = d["name"]
    if not isinstance(name, str) or not _NAME_RE.match(name):
        raise ValueError(f"{source}: query name {name!r} must match {_NAME_RE.pattern}")
    labels = d.get("labels") or []
    values = d.get("values") or []
    if not isinstance(labels, list) or not all(isinstance(x, str) for x in labels):
        raise ValueError(f"{source}: query {name!r} labels must be a list of strings")
    if not isinstance(values, list) or not all(isinstance(x, str) for x in values):
        raise ValueError(f"{source}: query {name!r} values must be a list of strings")
    return Query(
        name=name,
        help=d["help"],
        sql=d["sql"],
        interval_seconds=_validate_interval_mins(d["interval_mins"]),
        labels=labels,
        values=values,
    )


def load_queries(user_yaml_path: str | None, disable: set[str]) -> list[Query]:
    """Load built-in queries, then merge user queries by name (user wins)."""
    by_name: dict[str, Query] = {}
    for raw in _load_yaml_doc(BUILTIN_YAML, "builtin"):
        q = _query_from_dict(raw, "builtin")
        by_name[q.name] = q
    if user_yaml_path:
        with open(user_yaml_path) as f:
            text = f.read()
        for raw in _load_yaml_doc(text, user_yaml_path):
            q = _query_from_dict(raw, user_yaml_path)
            by_name[q.name] = q
    for name in disable:
        by_name.pop(name, None)
    return list(by_name.values())


@dataclass
class SelfMetrics:
    duration: Gauge
    errors: Counter
    last_success: Gauge
    up: Gauge


def _build_self_metrics(registry: CollectorRegistry | None = None) -> SelfMetrics:
    kwargs = {"registry": registry} if registry is not None else {}
    return SelfMetrics(
        duration=Gauge(
            "ducklake_metrics_query_duration_seconds",
            "Wall-clock duration of the most recent run for each query.",
            ["query"],
            **kwargs,
        ),
        errors=Counter(
            "ducklake_metrics_query_errors_total",
            "Cumulative count of failed query runs.",
            ["query"],
            **kwargs,
        ),
        last_success=Gauge(
            "ducklake_metrics_query_last_success_timestamp",
            "Unix timestamp of the most recent successful run for each query.",
            ["query"],
            **kwargs,
        ),
        up=Gauge(
            "ducklake_metrics_up",
            "1 while the daemon has a live catalog connection; 0 during reconnect.",
            **kwargs,
        ),
    )


def _build_query_gauges(
    queries: list[Query],
    registry: CollectorRegistry | None = None,
) -> dict[str, dict[str, Gauge]]:
    """For each query, register one Gauge per value column.

    Metric name is ``<query_name>_<value>``. Labels come from ``query.labels``.
    Always suffixes (no special-case for single-value queries) so the metric
    name shape is uniform across the daemon.
    """
    kwargs = {"registry": registry} if registry is not None else {}
    out: dict[str, dict[str, Gauge]] = {}
    for q in queries:
        gs: dict[str, Gauge] = {}
        for v in q.values:
            gs[v] = Gauge(f"{q.name}_{v}", q.help, q.labels, **kwargs)
        out[q.name] = gs
    return out


class _ReconnectNeeded(Exception):
    """Raised by the scheduler when it wants the outer loop to drop and re-establish the catalog connection."""


# When this many query runs in a row fail, assume the connection is
# wedged (vs. a single bad query) and force a reconnect. Tuned higher
# than the number of built-in queries so a single sticky query name
# can't trip the reset on its own.
CONSECUTIVE_FAILURE_THRESHOLD = 10


def _run_query(
    conn: duckdb.DuckDBPyConnection,
    q: Query,
    gauges: dict[str, Gauge],
    self_metrics: SelfMetrics,
) -> bool:
    """Execute one query and update its gauges. Returns True on success.

    On success: clears each value gauge before re-populating so label
    combinations that drop out between runs don't linger as stale series.
    On failure: increments the error counter and logs; the daemon stays
    up. Catalog flap is the expected steady-state failure mode. The
    outer scheduler watches the sequence of return values to decide
    whether to escalate to a reconnect.
    """
    t0 = time.monotonic()
    try:
        cur = conn.execute(q.sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        try:
            label_idx = [cols.index(name) for name in q.labels]
            value_idx = [cols.index(name) for name in q.values]
        except ValueError as e:
            raise RuntimeError(
                f"query {q.name}: SQL must return columns named in labels+values; "
                f"got cols={cols} labels={q.labels} values={q.values}"
            ) from e
        if q.labels:
            # Only labeled gauges support clear(); for unlabeled the .set()
            # below is itself the full state update.
            for g in gauges.values():
                g.clear()
        for row in rows:
            label_vals = [str(row[i]) if row[i] is not None else "" for i in label_idx]
            for v_name, v_i in zip(q.values, value_idx):
                v = row[v_i]
                if v is None:
                    continue
                g = gauges[v_name]
                if q.labels:
                    g.labels(*label_vals).set(float(v))
                else:
                    g.set(float(v))
        elapsed = time.monotonic() - t0
        self_metrics.duration.labels(q.name).set(elapsed)
        self_metrics.last_success.labels(q.name).set_to_current_time()
        log.debug("query %s: %d rows in %.3fs", q.name, len(rows), elapsed)
        return True
    except Exception:
        log.exception("query %s failed", q.name)
        self_metrics.errors.labels(q.name).inc()
        return False


def _scheduler_loop(
    conn: duckdb.DuckDBPyConnection,
    queries: list[Query],
    gauges: dict[str, dict[str, Gauge]],
    self_metrics: SelfMetrics,
    stop: threading.Event,
) -> None:
    """Run queries on per-query intervals until stop is set.

    Min-heap of ``(next_monotonic_ts, seq, idx)``. ``seq`` is a strict
    tiebreaker so the heap never compares Query objects (which would
    fail). Initial schedule fires every query at startup so /metrics
    populates as quickly as the catalog will respond. Catalog reads are
    short and serial — no need for parallel execution; one duckdb
    connection isn't safe for concurrent calls anyway.

    Tracks consecutive query failures across all queries; once the
    threshold is reached the scheduler raises ``_ReconnectNeeded`` so
    the outer loop can drop and re-establish the connection. Any
    successful run resets the counter — a single misbehaving query
    won't trip the reset on its own (threshold is well above the
    number of built-in queries).
    """
    seq = itertools.count()
    heap: list[tuple[float, int, int]] = []
    now = time.monotonic()
    for idx in range(len(queries)):
        heapq.heappush(heap, (now, next(seq), idx))
    consecutive_failures = 0
    while not stop.is_set():
        next_ts, _, idx = heap[0]
        wait_for = next_ts - time.monotonic()
        if wait_for > 0 and stop.wait(wait_for):
            return
        heapq.heappop(heap)
        q = queries[idx]
        ok = _run_query(conn, q, gauges[q.name], self_metrics)
        if ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= CONSECUTIVE_FAILURE_THRESHOLD:
                raise _ReconnectNeeded(
                    f"{consecutive_failures} consecutive query failures; reconnecting"
                )
        heapq.heappush(heap, (time.monotonic() + q.interval_seconds, next(seq), idx))


class _HealthHandler(BaseHTTPRequestHandler):
    """Serve /metrics, /-/healthy, /-/ready.

    Liveness (/-/healthy): 200 as long as the process answers — k8s should
    not restart on transient catalog flap. Readiness (/-/ready): 200 once
    the scheduler has started; we deliberately do NOT gate on any
    individual query completing because some queries can be slow and
    blocking readiness on them would block rollout.
    """

    server_version = "ducklake-metrics/0"

    def log_message(self, format: str, *args) -> None:  # noqa: A002 - stdlib signature
        log.debug("http: " + format, *args)

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - stdlib signature
        if self.path == "/metrics":
            reg = getattr(self.server, "registry", REGISTRY)
            body = generate_latest(reg)
            self._send(200, body, CONTENT_TYPE_LATEST)
        elif self.path == "/-/healthy":
            self._send(200, b"ok\n", "text/plain; charset=utf-8")
        elif self.path == "/-/ready":
            ready = getattr(self.server, "ready", False)
            self._send(200 if ready else 503, b"ok\n" if ready else b"not ready\n", "text/plain; charset=utf-8")
        else:
            self._send(404, b"not found\n", "text/plain; charset=utf-8")


def _start_http(
    port: int,
    registry: CollectorRegistry | None = None,
    host: str = "",
) -> ThreadingHTTPServer:
    srv = ThreadingHTTPServer((host, port), _HealthHandler)
    srv.ready = False  # type: ignore[attr-defined]
    srv.registry = registry if registry is not None else REGISTRY  # type: ignore[attr-defined]
    threading.Thread(target=srv.serve_forever, name="http", daemon=True).start()
    log.info("HTTP server listening on %s:%d", host or "*", port)
    return srv


_BACKOFF_INITIAL_SECONDS = 1.0
_BACKOFF_MAX_SECONDS = 60.0


def _connect_with_backoff(
    stop: threading.Event,
    self_metrics: SelfMetrics,
) -> duckdb.DuckDBPyConnection | None:
    """Call maintenance.connect() with exponential backoff until it succeeds or stop is set.

    Returns the new connection on success, or None if the daemon was asked
    to shut down before connect succeeded. ``self_metrics.up`` is held at 0
    while we're trying. Backoff starts at 1s and caps at 60s; the cap is
    intentional so that a long catalog outage doesn't grow into 30-minute
    sleeps that miss the recovery window.
    """
    delay = _BACKOFF_INITIAL_SECONDS
    self_metrics.up.set(0)
    while not stop.is_set():
        try:
            conn = maintenance.connect()
            log.info("Connected to DuckLake catalog")
            return conn
        except Exception:
            log.exception("connect to DuckLake failed; retrying in %.0fs", delay)
            if stop.wait(delay):
                return None
            delay = min(delay * 2, _BACKOFF_MAX_SECONDS)
    return None


def _setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-5s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="DuckLake state-metrics daemon")
    p.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("DUCKLAKE_METRICS_PORT", "9100")),
        help="HTTP listen port (default 9100)",
    )
    p.add_argument(
        "--config",
        default=os.environ.get("DUCKLAKE_METRICS_CONFIG"),
        help="Path to user-supplied queries YAML (extends built-ins)",
    )
    p.add_argument(
        "--disable",
        default=os.environ.get("DUCKLAKE_METRICS_DISABLE", ""),
        help="Comma-separated query names to skip from built-ins",
    )
    p.add_argument(
        "--list-queries",
        action="store_true",
        help="Print resolved query list and exit (validates config without connecting)",
    )
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    _setup_logging()

    disable = {s.strip() for s in args.disable.split(",") if s.strip()}
    queries = load_queries(args.config, disable)
    log.info("Loaded %d queries: %s", len(queries), [q.name for q in queries])

    if args.list_queries:
        for q in queries:
            print(f"{q.name}\t{q.interval_seconds}s\t{q.help}")
        return

    self_metrics = _build_self_metrics()
    gauges = _build_query_gauges(queries)

    stop = threading.Event()

    def _handle_signal(signum: int, _frame: object) -> None:
        log.info("Received signal %d; shutting down", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    srv = _start_http(args.port)
    log.info("Scheduler starting; %d query(ies) registered", len(queries))

    # Outer reconnect loop: every iteration establishes a fresh connection
    # (with backoff) and runs the scheduler against it. The scheduler exits
    # cleanly only on stop; on _ReconnectNeeded we drop the connection and
    # loop again. Readiness flips true after the first successful connect
    # and stays true thereafter — k8s shouldn't yank metrics traffic on
    # transient catalog flap, and there's no real "traffic" anyway.
    while not stop.is_set():
        conn = _connect_with_backoff(stop, self_metrics)
        if conn is None:
            break
        self_metrics.up.set(1)
        srv.ready = True  # type: ignore[attr-defined]
        try:
            _scheduler_loop(conn, queries, gauges, self_metrics, stop)
        except _ReconnectNeeded as e:
            log.warning("%s", e)
        finally:
            self_metrics.up.set(0)
            with contextlib.suppress(Exception):
                conn.close()


if __name__ == "__main__":
    main()
