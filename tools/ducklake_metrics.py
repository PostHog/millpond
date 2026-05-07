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
import logging
import os
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import maintenance
import yaml
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest

log = logging.getLogger("ducklake_metrics")


# Built-in queries are embedded so the binary is self-contained; they parse
# through the same loader as user YAML so the two paths can't diverge.
BUILTIN_YAML = """
queries: []
"""

# Intervals are constrained to whole minutes (suffix "m") with a 1-minute
# floor. Catalog reads are cheap but not free — sub-minute polling buys
# nothing for the signals this daemon publishes.
_INTERVAL_RE = re.compile(r"^(\d+)m$")
_MIN_INTERVAL_MIN = 1

_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@dataclass
class Query:
    name: str
    help: str
    sql: str
    interval_seconds: int
    labels: list[str] = field(default_factory=list)
    values: list[str] = field(default_factory=list)


def parse_interval(s: str) -> int:
    """Parse a "Nm" interval string to seconds; only minutes, minimum 1."""
    m = _INTERVAL_RE.match(s.strip())
    if not m:
        raise ValueError(f"interval must match '<n>m' (whole minutes), got {s!r}")
    minutes = int(m.group(1))
    if minutes < _MIN_INTERVAL_MIN:
        raise ValueError(f"interval must be >= {_MIN_INTERVAL_MIN}m, got {s!r}")
    return minutes * 60


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
    for key in ("name", "help", "interval", "sql"):
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
        interval_seconds=parse_interval(d["interval"]),
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
            body = generate_latest(REGISTRY)
            self._send(200, body, CONTENT_TYPE_LATEST)
        elif self.path == "/-/healthy":
            self._send(200, b"ok\n", "text/plain; charset=utf-8")
        elif self.path == "/-/ready":
            ready = getattr(self.server, "ready", False)
            self._send(200 if ready else 503, b"ok\n" if ready else b"not ready\n", "text/plain; charset=utf-8")
        else:
            self._send(404, b"not found\n", "text/plain; charset=utf-8")


def _start_http(port: int) -> ThreadingHTTPServer:
    srv = ThreadingHTTPServer(("", port), _HealthHandler)
    srv.ready = False  # type: ignore[attr-defined]
    threading.Thread(target=srv.serve_forever, name="http", daemon=True).start()
    log.info("HTTP server listening on :%d", port)
    return srv


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

    srv = _start_http(args.port)
    conn = maintenance.connect()
    try:
        srv.ready = True  # type: ignore[attr-defined]
        # Scheduler loop lands in a follow-up commit; for now just block so
        # the HTTP server stays up and serves /metrics + health endpoints.
        log.info("Daemon ready (scheduler not yet wired)")
        while True:
            time.sleep(60)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
