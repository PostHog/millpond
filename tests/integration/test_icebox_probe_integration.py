"""Integration tests for the icebox probe HTTP server (B4 / QE-M1).

Starts the real ThreadingHTTPServer with the daemon's handler factory,
scrapes /healthz and /metrics over HTTP, and verifies the staleness
boundary that the k8s liveness probe relies on.
"""
from __future__ import annotations

import threading
import urllib.request
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError

import pytest

from icebox import postgres_sync as ps
from icebox.main import _make_probe_handler

pytestmark = pytest.mark.integration


def _start_probe_server(cfg, pool) -> tuple[ThreadingHTTPServer, threading.Thread, str]:
    """Bind on port 0 (OS-assigned) and return the server, thread, and
    base URL. Avoids the bind-then-close race of pre-allocating a port."""
    handler_cls = _make_probe_handler(cfg=cfg, pg_pool=pool)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{port}"


def _stop_probe_server(server: ThreadingHTTPServer, thread: threading.Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5.0)


@pytest.fixture
def probe_server(cfg, pool):
    """Run the real probe handler against the cfg/pool fixtures. Yields
    the bound base URL; tears the server down on exit."""
    server, thread, base_url = _start_probe_server(cfg, pool)
    try:
        yield base_url
    finally:
        _stop_probe_server(server, thread)


def _get(url: str) -> tuple[int, bytes, str]:
    """HTTP GET that captures status, body, and Content-Type — without
    raising on 4xx/5xx (urllib.urlopen raises HTTPError for those)."""
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status, resp.read(), resp.headers.get("Content-Type", "")
    except HTTPError as e:
        return e.code, e.read(), e.headers.get("Content-Type", "")


# ---------------------------------------------------------------------------
# /healthz
# ---------------------------------------------------------------------------


def test_healthz_200_when_heartbeat_fresh(probe_server, pool):
    """Fresh heartbeat (stamped just now) → /healthz returns 200 with
    a body that includes the heartbeat age."""
    with pool.connection() as conn:
        with conn.transaction():
            ps.update_heartbeat(conn)

    status, body, ct = _get(f"{probe_server}/healthz")
    assert status == 200
    assert b"ok" in body
    assert b"heartbeat_age=" in body
    assert ct.startswith("text/plain")


def test_healthz_503_when_no_heartbeat_seeded(cfg, pool):
    """Fresh status row with last_committer_heartbeat=NULL → /healthz
    returns 503. This is the failure mode B1 fixes: production must
    seed the heartbeat at boot or the probe never returns 200."""
    # Wipe the heartbeat to NULL to simulate the un-seeded state.
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE status SET last_committer_heartbeat = NULL WHERE id = 1")
            conn.commit()

    server, thread, base_url = _start_probe_server(cfg, pool)
    try:
        status, body, _ = _get(f"{base_url}/healthz")
    finally:
        _stop_probe_server(server, thread)

    assert status == 503
    assert b"no heartbeat" in body


def test_healthz_503_when_heartbeat_stale(cfg, pool):
    """Stale heartbeat (older than cadence × stale_multiple) → 503.
    With cadence=1s and stale_multiple=3.0, stale_after=3s. Set the
    heartbeat to 10s ago and verify the probe fires."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE status SET last_committer_heartbeat = "
                "now() - interval '10 seconds' WHERE id = 1"
            )
            conn.commit()

    server, thread, base_url = _start_probe_server(cfg, pool)
    try:
        status, body, _ = _get(f"{base_url}/healthz")
    finally:
        _stop_probe_server(server, thread)

    assert status == 503
    assert b"heartbeat stale" in body


def test_healthz_503_when_pg_unreachable(cfg, pool):
    """The probe handler reads heartbeat from PG; if the pool is
    closed (PG unreachable, mid-shutdown, etc.), _read_heartbeat
    returns None and /healthz returns 503. This is what kubelet uses
    to restart the pod when PG goes dark."""
    pool.close()  # simulate PG unreachable
    server, thread, base_url = _start_probe_server(cfg, pool)
    try:
        status, body, _ = _get(f"{base_url}/healthz")
    finally:
        _stop_probe_server(server, thread)

    assert status == 503
    assert b"no heartbeat" in body


# ---------------------------------------------------------------------------
# /metrics
# ---------------------------------------------------------------------------


def test_metrics_returns_200_with_prometheus_content_type(probe_server, pool):
    """Prometheus expects text/plain; version=0.0.4. The handler uses
    `prometheus_client.CONTENT_TYPE_LATEST` which equals exactly that."""
    status, body, ct = _get(f"{probe_server}/metrics")
    assert status == 200
    assert ct.startswith("text/plain")
    # Spot-check that the daemon's metrics are exposed.
    text = body.decode()
    assert "icebox_files_count" in text
    assert "icebox_ticks_total" in text


def test_unknown_path_returns_404(probe_server):
    """The handler only knows /healthz + /metrics; everything else
    returns 404. Pins the fall-through case so a future helpful
    addition that handles /version (say) doesn't accidentally swallow
    misrouted probe traffic."""
    status, body, _ = _get(f"{probe_server}/nope")
    assert status == 404
    assert b"not found" in body
