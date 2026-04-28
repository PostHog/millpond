import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

log = logging.getLogger(__name__)


class _HealthState:
    """Tracks recency of poll and flush for health checks."""

    def __init__(self, max_poll_age_s: float = 300):
        self.max_poll_age_s = max_poll_age_s
        self._last_poll: float = 0
        self._last_flush: float = 0
        self._started: bool = False
        self._has_flushed: bool = False

    def record_poll(self) -> None:
        self._last_poll = time.monotonic()

    def record_flush(self) -> None:
        self._last_flush = time.monotonic()
        self._has_flushed = True

    def mark_started(self) -> None:
        now = time.monotonic()
        self._last_poll = now
        self._started = True

    def is_alive(self) -> bool:
        """Liveness: is the process started and actively polling?"""
        if not self._started:
            return False
        now = time.monotonic()
        return (now - self._last_poll) < self.max_poll_age_s

    def is_ready(self) -> bool:
        """Readiness: is the process started and actively polling?

        A consumer with no incoming data is still ready — it's sitting in its
        consume-write loop waiting for messages.  Flush recency is not checked
        because topics can legitimately receive no data for extended periods
        (e.g. during ingestion cutover).
        """
        return self.is_alive()

    def is_healthy(self) -> bool:
        """Backward-compatible: same as is_ready()."""
        return self.is_ready()

    def status_body(self) -> str:
        """Diagnostic string for health check responses."""
        now = time.monotonic()
        poll_age = f"{now - self._last_poll:.1f}s ago" if self._started else "never"
        flush_age = f"{now - self._last_flush:.1f}s ago" if self._has_flushed else "never"
        return f"poll={poll_age} flush={flush_age}"


health = _HealthState()


def _make_handler():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/metrics":
                payload = generate_latest()
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            elif self.path == "/healthz":
                body = health.status_body()
                if health.is_alive():
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(f"ok {body}\n".encode())
                else:
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(f"unhealthy {body}\n".encode())
            elif self.path == "/readyz":
                body = health.status_body()
                if health.is_ready():
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(f"ok {body}\n".encode())
                else:
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(f"not ready {body}\n".encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            # Suppress default stderr logging for each request
            pass

    return Handler


def start(port: int = 8000) -> HTTPServer:
    server = HTTPServer(("", port), _make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info("HTTP server listening on port %d (/metrics, /healthz, /readyz)", port)
    return server
