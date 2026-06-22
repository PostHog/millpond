#!/usr/bin/env python3
"""DuckLake state-metrics daemon.

Long-running process that periodically runs a fixed set of catalog-side
queries against a DuckLake and exposes the results as Prometheus gauges.
Built-in queries cover lake-shape signals (size-band distribution,
compaction-tier candidate counts, pending-deletion queue depth, snapshot
age). Operators can supply additional queries via a YAML file referenced
by ``DUCKLAKE_METRICS_CONFIG``.

Reuses ``ducklake_maintenance.connect()`` so the daemon inherits the same lake +
postgres ATTACH, S3 secret, and session tunables that ducklake_maintenance.py runs
under — see that module's docstring for the full env-var contract (RDS_* and
DUCKDB_S3_REGION required; DUCKDB_S3_ACCESS_KEY_ID/_SECRET_ACCESS_KEY optional —
omit both to use DuckDB's credential_chain provider).

Caveat for long-running deployments: ``connect()`` resolves S3 credentials via
``CREATE SECRET`` at startup. Under credential_chain, the SDK-resolved temporary
credentials (e.g. an IRSA STS token) are valid for ~1h and are NOT refreshed by
the secret over the connection lifetime. Once the underlying creds expire the
daemon will see ExpiredToken errors and only recover after the consecutive-
failure threshold trips a reconnect. The compactor CronJob is unaffected
(short-lived). Refresh handling is a known follow-up.

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
import ducklake_maintenance
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
# SQL refers to the metadata schema by its literal name. ducklake_maintenance.connect()
# always ATTACHes the lake as ``lake``, which fixes the schema as
# ``__ducklake_metadata_lake`` (per ducklake_maintenance.py's METADATA_SCHEMA). Built-in
# queries hardcode that name; user-supplied queries are passed through verbatim
# and may reference whatever schema they like.
BUILTIN_YAML = """
queries:
  - name: ducklake_pending_deletes
    help: |
      Pending-deletion queue depth. `total` is the row count
      (`ducklake_files_scheduled_for_deletion`); each row references one file
      path. `unique_paths` is the distinct file count (use this for "how many
      files are queued"). `dup_rows = total - unique_paths` surfaces the
      duplicate-row pathology that self-poisons the next cleanup
      (DuckLake upstream bug c5).
    interval_mins: 1
    values: [total, unique_paths, dup_rows]
    sql: |
      SELECT
        COUNT(*) AS total,
        COUNT(DISTINCT path) AS unique_paths,
        COUNT(*) - COUNT(DISTINCT path) AS dup_rows
      FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion

  - name: ducklake_data_files
    help: |
      Live data-file (parquet) totals. `files` = file count, `bytes` = total
      on-disk size, `rows` = total records across all live files. "Live" =
      `end_snapshot IS NULL`. Per-band breakdown lives in
      `ducklake_files_per_band`; this is the rollup for "how big is the lake."
    interval_mins: 1
    values: [files, bytes, rows]
    sql: |
      SELECT
        COUNT(*) AS files,
        COALESCE(SUM(file_size_bytes), 0) AS bytes,
        COALESCE(SUM(record_count), 0)    AS rows
      FROM __ducklake_metadata_lake.ducklake_data_file
      WHERE end_snapshot IS NULL

  - name: ducklake_delete_files
    help: |
      Live delete-vector (puffin/parquet) totals. Same shape as
      ducklake_data_files but for the delete side. Outsized vs.
      ducklake_data_files_files = lots of unmerged deletes; ripe for
      `ducklake_rewrite_data_files` maintenance.
    interval_mins: 1
    values: [files, bytes]
    sql: |
      SELECT
        COUNT(*) AS files,
        COALESCE(SUM(file_size_bytes), 0) AS bytes
      FROM __ducklake_metadata_lake.ducklake_delete_file
      WHERE end_snapshot IS NULL

  - name: ducklake_files_per_band
    help: |
      Live data files grouped into compaction-relevant size bands. Tiers
      match `ducklake_maintenance.py`'s TIERS spec — `tier1` (<1 MiB),
      `tier2` (1-10 MiB), `tier3` (10-64 MiB) are compaction targets;
      `large` (>=64 MiB) is past the compaction threshold and is just
      reported for shape. Sum across bands equals `ducklake_data_files_files`.
    interval_mins: 1
    labels: [band]
    values: [count, bytes]
    sql: |
      SELECT
        CASE
          WHEN file_size_bytes < 1048576   THEN 'tier1'
          WHEN file_size_bytes < 10485760  THEN 'tier2'
          WHEN file_size_bytes < 67108864  THEN 'tier3'
          ELSE 'large'
        END AS band,
        COUNT(*) AS count,
        COALESCE(SUM(file_size_bytes), 0) AS bytes
      FROM __ducklake_metadata_lake.ducklake_data_file
      WHERE end_snapshot IS NULL
      GROUP BY band

  - name: ducklake_snapshots
    help: |
      Snapshot population, ages (seconds), and id bounds. Expose the raw
      `oldest_id`/`newest_id` (counter-like, monotonic) so PromQL can
      derive the commit rate via `deriv(ducklake_snapshots_newest_id[5m])`
      — no per-sample storage in the daemon.
    interval_mins: 1
    values: [count, oldest_seconds_ago, newest_seconds_ago, oldest_id, newest_id]
    sql: |
      SELECT
        COUNT(*) AS count,
        COALESCE(EXTRACT(EPOCH FROM (now() - MIN(CAST(snapshot_time AS TIMESTAMPTZ)))), 0) AS oldest_seconds_ago,
        COALESCE(EXTRACT(EPOCH FROM (now() - MAX(CAST(snapshot_time AS TIMESTAMPTZ)))), 0) AS newest_seconds_ago,
        COALESCE(MIN(snapshot_id), 0) AS oldest_id,
        COALESCE(MAX(snapshot_id), 0) AS newest_id
      FROM __ducklake_metadata_lake.ducklake_snapshot

  - name: ducklake_inlined_data_tables
    help: |
      Count of rows in `ducklake_inlined_data_tables` (the registry of
      per-table inline-data tables that DuckLake creates when a write goes
      through the inlining path). Should stay small in steady state;
      runaway growth indicates `data_inlining_row_limit` isn't being
      honored by writers, or writers are creating tables faster than
      maintenance can drop them (see `ducklake_unreachable_inline_tables`).
    interval_mins: 1
    values: [total]
    sql: |
      SELECT COUNT(*) AS total
      FROM __ducklake_metadata_lake.ducklake_inlined_data_tables

  - name: ducklake_unreachable_inline_tables
    help: |
      Count of `ducklake_inlined_data_tables` entries whose parent
      `ducklake_table` has no snapshot-reachable row. These are orphans
      that DuckLake's own GC (DropEmptySupersededInlinedTables) won't
      reach because it only acts on superseded-and-empty, not
      parent-dropped. Should be 0 or near-0 after cleanup; growing trend
      means the data_imports DROP+CREATE pattern is producing orphans.
      Uses the range-overlap predicate (strict superset of "actually
      reachable"; safe-conservative — won't false-positive an unreachable
      table). See INCIDENT.md.
    interval_mins: 5
    values: [total]
    sql: |
      WITH bounds AS (
        SELECT MIN(snapshot_id) AS lo, MAX(snapshot_id) AS hi
        FROM __ducklake_metadata_lake.ducklake_snapshot
      ),
      reachable AS (
        SELECT DISTINCT t.table_id
        FROM __ducklake_metadata_lake.ducklake_table t, bounds
        WHERE t.begin_snapshot <= bounds.hi
          AND (t.end_snapshot IS NULL OR t.end_snapshot > bounds.lo)
      )
      SELECT COUNT(*) AS total
      FROM __ducklake_metadata_lake.ducklake_inlined_data_tables idt
      WHERE NOT EXISTS (SELECT 1 FROM reachable r WHERE r.table_id = idt.table_id)

  - name: ducklake_tables
    help: |
      Count of `ducklake_table` rows split by lifecycle state.
      `state="live"` = `end_snapshot IS NULL` (currently visible to the
      latest snapshot). `state="dropped"` = `end_snapshot` set (still
      readable by historical snapshots until expire+cleanup reaps them).
      Imbalance — many dropped, few live — indicates DROP+CREATE churn
      (data_imports anti-pattern).
    interval_mins: 1
    labels: [state]
    values: [count]
    sql: |
      SELECT
        CASE WHEN end_snapshot IS NULL THEN 'live' ELSE 'dropped' END AS state,
        COUNT(*) AS count
      FROM __ducklake_metadata_lake.ducklake_table
      GROUP BY state

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

  - name: ducklake_catalog
    help: |
      DuckLake catalog format version. The numeric major.minor lands in the
      gauge value (1.0, 1.1, 2.0…); any trailing non-numeric tag DuckLake
      attaches (currently '-dev1' on main after MigrateV10, future '-rcN' /
      '-betaN' shapes welcome) lands in the `suffix` label so dashboards
      can flag dev/pre-release builds without losing PromQL ordering on
      the value. Empty `suffix=""` means a clean release. Pure-junk values
      like 'foo' fail loudly via the error counter — the correct signal.
    interval_mins: 60
    labels: [suffix]
    values: [format_version]
    sql: |
      SELECT
        CAST(regexp_extract(value, '^[0-9]+(\\.[0-9]+)?') AS DOUBLE) AS format_version,
        regexp_replace(value, '^[0-9]+(\\.[0-9]+)?', '') AS suffix
      FROM __ducklake_metadata_lake.ducklake_metadata
      WHERE key = 'version' AND scope IS NULL

  - name: ducklake_config
    help: |
      Operationally-relevant DuckLake catalog config values from
      `ducklake_metadata`. Currently tracks `auto_compact` and
      `data_inlining_row_limit` at every scope (global / schema / table) —
      `auto_compact != 'true'` silently disables every maintenance
      function call against the affected table, and a non-zero
      `data_inlining_row_limit` is what generates the inline-table
      backlog this daemon watches. Boolean strings ('true'/'false') map
      to 1/0; numeric strings cast directly; anything else returns NULL
      and the sample is dropped (error counter still ticks, surfacing
      the bad value).
    interval_mins: 5
    labels: [key, scope, scope_id]
    values: [value]
    sql: |
      SELECT
        key,
        COALESCE(scope, '') AS scope,
        COALESCE(CAST(scope_id AS VARCHAR), '') AS scope_id,
        CASE
          WHEN value = 'true'  THEN 1.0
          WHEN value = 'false' THEN 0.0
          WHEN regexp_matches(value, '^-?[0-9]+(\\.[0-9]+)?$') THEN CAST(value AS DOUBLE)
          ELSE NULL
        END AS value
      FROM __ducklake_metadata_lake.ducklake_metadata
      WHERE key IN ('auto_compact', 'data_inlining_row_limit')
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
    """Construct the daemon's own health metrics.

    Every metric carries a ``tenant`` label (injected at sample time from
    the daemon's resolved tenant identity, see main()) so a Prometheus
    instance scraping many ducklake_metrics deployments can distinguish
    series by catalog without relying on per-target relabeling.
    """
    kwargs = {"registry": registry} if registry is not None else {}
    return SelfMetrics(
        duration=Gauge(
            "ducklake_metrics_query_duration_seconds",
            "Wall-clock duration of the most recent run for each query.",
            ["tenant", "query"],
            **kwargs,
        ),
        errors=Counter(
            "ducklake_metrics_query_errors_total",
            "Cumulative count of failed query runs.",
            ["tenant", "query"],
            **kwargs,
        ),
        last_success=Gauge(
            "ducklake_metrics_query_last_success_timestamp",
            "Unix timestamp of the most recent successful run for each query.",
            ["tenant", "query"],
            **kwargs,
        ),
        up=Gauge(
            "ducklake_metrics_up",
            "1 while the daemon has a live catalog connection; 0 during reconnect.",
            ["tenant"],
            **kwargs,
        ),
    )


def _build_query_gauges(
    queries: list[Query],
    registry: CollectorRegistry | None = None,
) -> dict[str, dict[str, Gauge]]:
    """For each query, register one Gauge per value column.

    Metric name is ``<query_name>_<value>``. Labels come from
    ``["tenant"] + query.labels`` — ``tenant`` is prepended so every
    series the daemon emits is tenant-scoped, matching PostHog's
    per-team labeling convention. Always suffixes (no special-case for
    single-value queries) so the metric name shape is uniform across
    the daemon.
    """
    kwargs = {"registry": registry} if registry is not None else {}
    out: dict[str, dict[str, Gauge]] = {}
    for q in queries:
        gs: dict[str, Gauge] = {}
        for v in q.values:
            gs[v] = Gauge(f"{q.name}_{v}", q.help, ["tenant", *q.labels], **kwargs)
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
    tenant: str,
) -> bool:
    """Execute one query and update its gauges. Returns True on success.

    ``tenant`` is prepended to every gauge sample's label tuple — see
    _build_query_gauges / _build_self_metrics. The query's SQL itself
    is tenant-agnostic; the daemon-level tenant identity gets stitched
    on at emit time so user-supplied YAML doesn't need to know about
    the multi-tenant deployment model.

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
        # Always clear since every gauge has at least the tenant label
        # — clear() drops all (tenant, ...) label combinations registered
        # so far for this gauge, so per-run label set churn doesn't leak
        # stale series.
        for g in gauges.values():
            g.clear()
        for row in rows:
            label_vals = [str(row[i]) if row[i] is not None else "" for i in label_idx]
            for v_name, v_i in zip(q.values, value_idx):
                v = row[v_i]
                if v is None:
                    continue
                gauges[v_name].labels(tenant, *label_vals).set(float(v))
        elapsed = time.monotonic() - t0
        self_metrics.duration.labels(tenant, q.name).set(elapsed)
        self_metrics.last_success.labels(tenant, q.name).set_to_current_time()
        log.debug("query %s: %d rows in %.3fs", q.name, len(rows), elapsed)
        return True
    except Exception:
        log.exception("query %s failed", q.name)
        self_metrics.errors.labels(tenant, q.name).inc()
        return False


def _scheduler_loop(
    conn: duckdb.DuckDBPyConnection,
    queries: list[Query],
    gauges: dict[str, dict[str, Gauge]],
    self_metrics: SelfMetrics,
    stop: threading.Event,
    tenant: str,
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
        ok = _run_query(conn, q, gauges[q.name], self_metrics, tenant)
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
    tenant: str,
) -> duckdb.DuckDBPyConnection | None:
    """Call ducklake_maintenance.connect() with exponential backoff until it succeeds or stop is set.

    Returns the new connection on success, or None if the daemon was asked
    to shut down before connect succeeded. ``self_metrics.up`` is held at 0
    while we're trying. Backoff starts at 1s and caps at 60s; the cap is
    intentional so that a long catalog outage doesn't grow into 30-minute
    sleeps that miss the recovery window.
    """
    delay = _BACKOFF_INITIAL_SECONDS
    self_metrics.up.labels(tenant).set(0)
    while not stop.is_set():
        try:
            conn = ducklake_maintenance.connect()
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
        "--tenant",
        default=os.environ.get("DUCKLAKE_TENANT"),
        help=(
            "Tenant identity stamped onto every emitted metric as the "
            "`tenant` label. Required for the multi-tenant deployment "
            "model — one daemon per tenant, distinguished in Prometheus "
            "via this label. Falls back to DUCKLAKE_TENANT env."
        ),
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

    if not args.tenant:
        sys.exit(
            "tenant identity required: pass --tenant <name> or set DUCKLAKE_TENANT "
            "env. Stamped onto every emitted metric as the `tenant` label."
        )
    tenant: str = args.tenant
    log.info("Tenant: %s", tenant)

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
        conn = _connect_with_backoff(stop, self_metrics, tenant)
        if conn is None:
            break
        self_metrics.up.labels(tenant).set(1)
        srv.ready = True  # type: ignore[attr-defined]
        try:
            _scheduler_loop(conn, queries, gauges, self_metrics, stop, tenant)
        except _ReconnectNeeded as e:
            log.warning("%s", e)
        finally:
            self_metrics.up.labels(tenant).set(0)
            with contextlib.suppress(Exception):
                conn.close()


if __name__ == "__main__":
    main()
