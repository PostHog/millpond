#!/usr/bin/env python3
"""DuckLake state-metrics daemon / one-shot exporter.

Two modes over the same query set:

- Daemon (default): long-running scheduler + /metrics HTTP server, one
  process per tenant (the legacy deployment model).
- ``--once``: run every query once and POST the exposition payload to a
  Prometheus-import endpoint (``--push-url``, e.g. VictoriaMetrics
  vmagent ``/api/v1/import/prometheus``) — the per-tenant metrics
  CronJob's mode, mirroring the maintenance cron's scoped-lifecycle
  model. Metric names and the ``tenant`` label are identical across
  modes, so dashboards don't care which produced the samples.

Periodically (or once) runs a fixed set of catalog-side
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
import urllib.error
import urllib.request
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
    interval_mins: 5
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
    interval_mins: 5
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
    interval_mins: 5
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
    interval_mins: 5
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
    interval_mins: 5
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
    interval_mins: 5
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
    interval_mins: 5
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
    interval_mins: 5
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
    liveness_failures: Counter


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
        liveness_failures=Counter(
            "ducklake_metrics_liveness_failures_total",
            (
                "Cumulative count of /-/healthy responses that returned 503, by reason. "
                "`in_flight` = a single query exceeded the liveness timeout; "
                "`stale_tick` = no scheduler tick (loop iteration or reconnect retry) "
                "in that long. Alert on rate(...) > 0 to catch a wedged daemon BEFORE "
                "kubelet's failureThreshold restarts the pod."
            ),
            ["tenant", "reason"],
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


# Default liveness ceiling. A single catalog query stuck in flight, or a
# scheduler thread that has stopped ticking entirely, will trip /-/healthy
# 503 after this many seconds and let k8s restart the pod. Sized well above
# the slowest realistic catalog query so a genuinely long query (e.g. counting
# inlined-data tables on a multi-million-row catalog) doesn't flap the probe.
# Tunable via --liveness-timeout-seconds / DUCKLAKE_METRICS_LIVENESS_TIMEOUT.
_DEFAULT_LIVENESS_TIMEOUT_SECONDS = 300.0


@dataclass
class _Liveness:
    """Shared timing state between the scheduler loop and the health handler.

    The scheduler updates ``current_query_start`` / ``last_tick`` from one
    thread; the HTTP handler reads them from another. Both fields are plain
    floats, and CPython's bytecode-level atomicity for single-attribute
    float load/store is enough here — we only need approximate timing for
    a liveness signal, not strict consistency, and any read that races a
    write either sees the old or the new value (both bounded by the
    timeout) so the worst case is one extra grace period before /-/healthy
    flips. A Lock would just add overhead without changing the outcome.

    ``scheduler_started`` flips True once the scheduler loop has begun
    iterating. While False, /-/healthy is unconditionally 200 so the
    startup probe doesn't kill the pod during the initial connect backoff
    (which can be minutes if the catalog is cold).

    ``timeout`` is per-instance so tests can construct short-deadline
    instances without monkeypatching the module constant.
    """

    timeout: float = _DEFAULT_LIVENESS_TIMEOUT_SECONDS
    scheduler_started: bool = False
    current_query_start: float = 0.0  # monotonic; 0 means no query in flight
    last_tick: float = 0.0  # monotonic; 0 means scheduler hasn't ticked yet


# Stable reason codes for the /-/healthy decision. Kept as a small enum-ish
# set rather than ad-hoc strings so the `ducklake_metrics_liveness_failures_total`
# counter can label by reason without scraping free text. The free-text
# message returned alongside the code is for humans (probe body, log lines)
# and may evolve; the code is part of the metrics contract.
LIVENESS_REASON_STARTING = "starting"
LIVENESS_REASON_OK = "ok"
LIVENESS_REASON_IN_FLIGHT = "in_flight"
LIVENESS_REASON_STALE_TICK = "stale_tick"


def _liveness_status(liveness: _Liveness | None, now: float) -> tuple[bool, str, str]:
    """Return (is_healthy, reason_code, message). Pure function — drives both probes + tests + metrics.

    `reason_code` is one of LIVENESS_REASON_* (stable; safe as a metric label).
    `message` is human-readable (probe body, log lines; may evolve).
    """
    if liveness is None or not liveness.scheduler_started:
        # Pre-scheduler: process is up, that's enough for liveness. Readiness
        # is gated separately on the catalog connect.
        return True, LIVENESS_REASON_STARTING, "starting"
    cur_start = liveness.current_query_start
    if cur_start > 0.0 and now - cur_start > liveness.timeout:
        return (
            False,
            LIVENESS_REASON_IN_FLIGHT,
            f"current query running >{liveness.timeout:.0f}s ({now - cur_start:.0f}s)",
        )
    last_tick = liveness.last_tick
    if last_tick > 0.0 and now - last_tick > liveness.timeout:
        return False, LIVENESS_REASON_STALE_TICK, f"no scheduler tick in {now - last_tick:.0f}s"
    return True, LIVENESS_REASON_OK, "ok"


def _run_query(
    conn: duckdb.DuckDBPyConnection,
    q: Query,
    gauges: dict[str, Gauge],
    self_metrics: SelfMetrics,
    tenant: str,
    liveness: _Liveness | None = None,
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
    if liveness is not None:
        liveness.current_query_start = t0
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
    finally:
        if liveness is not None:
            # Tick before clearing current_query_start so a concurrent reader
            # can never see "no query in flight AND no recent tick" — that
            # combination should only ever mean "scheduler dead".
            liveness.last_tick = time.monotonic()
            liveness.current_query_start = 0.0


def _scheduler_loop(
    conn: duckdb.DuckDBPyConnection,
    queries: list[Query],
    gauges: dict[str, dict[str, Gauge]],
    self_metrics: SelfMetrics,
    stop: threading.Event,
    tenant: str,
    liveness: _Liveness | None = None,
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
    if liveness is not None:
        # Flip BEFORE the first query runs so /-/healthy starts gating on
        # actual scheduler progress immediately. Without this the handler
        # stays in "starting → 200" mode until the first query completes,
        # masking a hang on the very first query.
        liveness.scheduler_started = True
    consecutive_failures = 0
    while not stop.is_set():
        next_ts, _, idx = heap[0]
        wait_for = next_ts - time.monotonic()
        if wait_for > 0 and stop.wait(wait_for):
            return
        heapq.heappop(heap)
        q = queries[idx]
        ok = _run_query(conn, q, gauges[q.name], self_metrics, tenant, liveness)
        if ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= CONSECUTIVE_FAILURE_THRESHOLD:
                raise _ReconnectNeeded(f"{consecutive_failures} consecutive query failures; reconnecting")
        heapq.heappush(heap, (time.monotonic() + q.interval_seconds, next(seq), idx))


class _HealthHandler(BaseHTTPRequestHandler):
    """Serve /metrics, /-/healthy, /-/ready.

    Liveness (/-/healthy): 200 while the scheduler is making progress
    (either ticking on schedule or currently running a query within the
    liveness timeout). 503 if a single query has been in flight past the
    timeout OR no query has ticked in that long — both mean the catalog
    connection or scheduler thread is wedged and a restart is the only
    recovery. Pre-scheduler-start the endpoint is unconditionally 200 so
    the startup probe doesn't kill the pod during initial connect backoff.

    Readiness (/-/ready): 200 once the scheduler has started; we
    deliberately do NOT gate on any individual query completing because
    some queries can be slow and blocking readiness on them would block
    rollout.
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
            liveness = getattr(self.server, "liveness", None)
            alive, reason_code, message = _liveness_status(liveness, time.monotonic())
            body = f"{message}\n".encode()
            if not alive:
                # Fire the counter + the (debounced) log line before sending
                # the response so the signal goes out even if the kubelet
                # connection dies mid-write. See _on_unhealthy in main() for
                # the bound callback.
                cb = getattr(self.server, "on_unhealthy", None)
                if cb is not None:
                    cb(reason_code, message)
            self._send(200 if alive else 503, body, "text/plain; charset=utf-8")
        elif self.path == "/-/ready":
            ready = getattr(self.server, "ready", False)
            self._send(200 if ready else 503, b"ok\n" if ready else b"not ready\n", "text/plain; charset=utf-8")
        else:
            self._send(404, b"not found\n", "text/plain; charset=utf-8")


def _start_http(
    port: int,
    registry: CollectorRegistry | None = None,
    host: str = "",
    liveness: _Liveness | None = None,
    on_unhealthy: object | None = None,
) -> ThreadingHTTPServer:
    srv = ThreadingHTTPServer((host, port), _HealthHandler)
    srv.ready = False  # type: ignore[attr-defined]
    srv.registry = registry if registry is not None else REGISTRY  # type: ignore[attr-defined]
    srv.liveness = liveness  # type: ignore[attr-defined]
    srv.on_unhealthy = on_unhealthy  # type: ignore[attr-defined]
    threading.Thread(target=srv.serve_forever, name="http", daemon=True).start()
    log.info("HTTP server listening on %s:%d", host or "*", port)
    return srv


_BACKOFF_INITIAL_SECONDS = 1.0
_BACKOFF_MAX_SECONDS = 60.0


def _connect_with_backoff(
    stop: threading.Event,
    self_metrics: SelfMetrics,
    tenant: str,
    liveness: _Liveness | None = None,
    memory_limit: str | None = None,
) -> duckdb.DuckDBPyConnection | None:
    """Call ducklake_maintenance.connect() with exponential backoff until it succeeds or stop is set.

    Returns the new connection on success, or None if the daemon was asked
    to shut down before connect succeeded. ``self_metrics.up`` is held at 0
    while we're trying. Backoff starts at 1s and caps at 60s; the cap is
    intentional so that a long catalog outage doesn't grow into 30-minute
    sleeps that miss the recovery window.

    Each retry stamps ``liveness.last_tick`` so the /-/healthy probe doesn't
    report "no scheduler tick" while we're legitimately waiting on the
    catalog to come back. Semantically: a daemon retrying connect IS doing
    useful work, even though no query has run yet. Without this stamp a
    >5min outage would flip the probe and k8s would restart the pod with a
    misleading reason that points at the scheduler thread instead of the
    catalog.

    ``memory_limit`` (when set) is applied via ``SET memory_limit = '<v>'``
    immediately after connect — DuckDB's default budget is ~75% of detected
    RAM, which inside a constrained pod (cgroup limit small, host RAM
    large) means DuckDB tries to grow well past the cgroup and the kernel
    OOM-kills the pod before the daemon can do anything useful. Setting it
    explicitly to a number that fits within `resources.limits.memory` (less
    Python interpreter + extension overhead, ~250-500Mi) keeps DuckDB inside
    the budget — e.g. '1GB' for a pod limit of 1.5Gi. The value is
    validated ONCE up-front (outside the retry loop) so a bad value fails
    fast at daemon startup rather than retrying forever in a tight loop
    that the broad except-clause below would otherwise swallow.
    """
    if memory_limit is not None:
        ducklake_maintenance._sanitize_setting_value(memory_limit)
    delay = _BACKOFF_INITIAL_SECONDS
    self_metrics.up.labels(tenant).set(0)
    while not stop.is_set():
        if liveness is not None:
            liveness.last_tick = time.monotonic()
        try:
            conn = ducklake_maintenance.connect()
            if memory_limit is not None:
                conn.execute(f"SET memory_limit = '{memory_limit}'")
                log.info("Connected to DuckLake catalog (memory_limit=%s)", memory_limit)
            else:
                log.info("Connected to DuckLake catalog (memory_limit=DuckDB default)")
            return conn
        except Exception:
            log.exception("connect to DuckLake failed; retrying in %.0fs", delay)
            if stop.wait(delay):
                return None
            delay = min(delay * 2, _BACKOFF_MAX_SECONDS)
    return None


def _connect_once(memory_limit: str | None) -> duckdb.DuckDBPyConnection:
    """Single connect attempt for --once mode: no backoff — the CronJob's
    next tick IS the retry, and a loud fast failure is the signal we want
    (kube_job_status_failed is the alert surface, same as the maintenance
    cron)."""
    if memory_limit is not None:
        ducklake_maintenance._sanitize_setting_value(memory_limit)
    conn = ducklake_maintenance.connect()
    if memory_limit is not None:
        conn.execute(f"SET memory_limit = '{memory_limit}'")
    log.info(
        "Connected to DuckLake catalog (memory_limit=%s)",
        memory_limit if memory_limit is not None else "DuckDB default",
    )
    return conn


def _push_metrics(url: str, registry: CollectorRegistry) -> None:
    """POST the registry in exposition format to a Prometheus-import
    endpoint (VictoriaMetrics vmagent/vminsert `/api/v1/import/prometheus`).

    Plain exposition text over HTTP — the payload is byte-identical to what
    a scrape of the daemon would have produced, which is what keeps every
    existing dashboard selector working. urllib raises on >=400; VM answers
    204 on success."""
    body = generate_latest(registry)
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": CONTENT_TYPE_LATEST},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - operator-supplied cluster URL
        log.info("Pushed %d bytes to %s (HTTP %d)", len(body), url, resp.status)


def _run_once(
    queries: list[Query],
    tenant: str,
    push_url: str,
    memory_limit: str | None,
) -> int:
    """One-shot mode: connect, run every query once, push, exit.

    Per-query failures do NOT fail the run — they are themselves pushed
    (ducklake_metrics_query_errors_total) and therefore visible where the
    dashboards already look. The run exits nonzero only when nothing useful
    could be produced or delivered: connect failure, zero successful
    queries, or push failure.

    A DEDICATED registry, never the global default: the default registry
    carries the untenanted python_*/process_* collectors, and pushing
    those through an import endpoint (no scrape instance label) would make
    every tenant's job fight over the same series. The push must contain
    exactly the tenant-labeled metrics and nothing else.
    """
    registry = CollectorRegistry()
    self_metrics = _build_self_metrics(registry)
    gauges = _build_query_gauges(queries, registry)

    try:
        conn = _connect_once(memory_limit)
    except Exception:
        log.exception("connect to DuckLake failed")
        return 1
    try:
        self_metrics.up.labels(tenant).set(1)
        succeeded = 0
        for q in queries:
            if _run_query(conn, q, gauges[q.name], self_metrics, tenant):
                succeeded += 1
        log.info("Ran %d/%d queries successfully", succeeded, len(queries))
    finally:
        with contextlib.suppress(Exception):
            conn.close()

    if succeeded == 0:
        log.error("no query succeeded; not pushing a metrics payload of pure errors")
        return 1
    try:
        _push_metrics(push_url, registry)
    except Exception:
        log.exception("metrics push failed")
        return 1
    return 0


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
        "--liveness-timeout-seconds",
        type=float,
        default=float(os.environ.get("DUCKLAKE_METRICS_LIVENESS_TIMEOUT", _DEFAULT_LIVENESS_TIMEOUT_SECONDS)),
        help=(
            "Liveness ceiling for /-/healthy: 503 returned when a single query has "
            "been running, OR no scheduler tick has happened, for this many seconds. "
            "Sized to allow the slowest legitimate query a comfortable margin. "
            f"Default {_DEFAULT_LIVENESS_TIMEOUT_SECONDS:.0f}s."
        ),
    )
    p.add_argument(
        "--duckdb-memory-limit",
        default=os.environ.get("DUCKLAKE_METRICS_MEMORY_LIMIT"),
        help=(
            "DuckDB `memory_limit` applied right after connect (e.g. '512MB', '1GB'). "
            "DuckDB's default is ~75%% of detected RAM; inside a cgroup-limited pod "
            "that often resolves to the host RAM rather than the cgroup limit, and "
            "the kernel OOM-kills the pod once DuckDB tries to grow into "
            "non-existent memory. Size this WELL UNDER the pod's "
            "resources.limits.memory (leave headroom for the Python interpreter, "
            "the ducklake extension's in-memory catalog model, and HTTP server "
            "buffers — ~150-200Mi typical). Unset = DuckDB default (only safe on "
            "a host where DuckDB can actually use ~75%% of the reported RAM)."
        ),
    )
    p.add_argument(
        "--list-queries",
        action="store_true",
        help="Print resolved query list and exit (validates config without connecting)",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help=(
            "One-shot mode for the per-tenant metrics CronJob: connect, run "
            "every query once, POST the exposition-format payload to "
            "--push-url, exit. No HTTP server, no scheduler, no backoff — "
            "the cron's next tick is the retry and a failed Job is the "
            "alert surface. Requires --push-url."
        ),
    )
    p.add_argument(
        "--push-url",
        default=os.environ.get("DUCKLAKE_METRICS_PUSH_URL"),
        help=(
            "Prometheus-import endpoint for --once mode, e.g. a "
            "VictoriaMetrics vmagent's /api/v1/import/prometheus. Falls "
            "back to DUCKLAKE_METRICS_PUSH_URL env."
        ),
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

    if args.once:
        if not args.push_url:
            sys.exit("--once requires --push-url (or DUCKLAKE_METRICS_PUSH_URL env)")
        sys.exit(_run_once(queries, tenant, args.push_url, args.duckdb_memory_limit))

    self_metrics = _build_self_metrics()
    gauges = _build_query_gauges(queries)

    stop = threading.Event()

    def _handle_signal(signum: int, _frame: object) -> None:
        log.info("Received signal %d; shutting down", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    liveness = _Liveness(timeout=args.liveness_timeout_seconds)

    # Debounce so kubelet's per-N-seconds /-/healthy scrape doesn't spam
    # the log while the probe stays 503 — log on the first transition into
    # an unhealthy reason and again only when the reason CHANGES. Counter
    # increments every time (operators alert off rate(); they don't want
    # debounce baked into the metric).
    last_logged_reason: dict[str, str | None] = {"reason": None}

    def _on_unhealthy(reason_code: str, message: str) -> None:
        self_metrics.liveness_failures.labels(tenant, reason_code).inc()
        if last_logged_reason["reason"] != reason_code:
            log.warning("liveness probe 503 (%s): %s", reason_code, message)
            last_logged_reason["reason"] = reason_code

    srv = _start_http(args.port, liveness=liveness, on_unhealthy=_on_unhealthy)
    log.info(
        "Scheduler starting; %d query(ies) registered; liveness timeout %.0fs",
        len(queries),
        liveness.timeout,
    )

    # Outer reconnect loop: every iteration establishes a fresh connection
    # (with backoff) and runs the scheduler against it. The scheduler exits
    # cleanly only on stop; on _ReconnectNeeded we drop the connection and
    # loop again. Readiness flips true after the first successful connect
    # and stays true thereafter — k8s shouldn't yank metrics traffic on
    # transient catalog flap, and there's no real "traffic" anyway.
    while not stop.is_set():
        conn = _connect_with_backoff(stop, self_metrics, tenant, liveness, args.duckdb_memory_limit)
        if conn is None:
            break
        self_metrics.up.labels(tenant).set(1)
        srv.ready = True  # type: ignore[attr-defined]
        try:
            _scheduler_loop(conn, queries, gauges, self_metrics, stop, tenant, liveness)
        except _ReconnectNeeded as e:
            log.warning("%s", e)
        finally:
            self_metrics.up.labels(tenant).set(0)
            with contextlib.suppress(Exception):
                conn.close()


if __name__ == "__main__":
    main()
