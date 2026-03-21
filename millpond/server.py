import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

log = logging.getLogger(__name__)


class _HealthState:
    """Tracks recency of poll and flush for health checks."""

    def __init__(self, max_poll_age_s: float = 300, max_flush_age_s: float = 600):
        self.max_poll_age_s = max_poll_age_s
        self.max_flush_age_s = max_flush_age_s
        self._last_poll: float = 0
        self._last_flush: float = 0
        self._started: bool = False

    def record_poll(self) -> None:
        self._last_poll = time.monotonic()

    def record_flush(self) -> None:
        self._last_flush = time.monotonic()

    def mark_started(self) -> None:
        now = time.monotonic()
        self._last_poll = now
        self._last_flush = now
        self._started = True

    def is_healthy(self) -> bool:
        if not self._started:
            return False
        now = time.monotonic()
        poll_ok = (now - self._last_poll) < self.max_poll_age_s
        flush_ok = (now - self._last_flush) < self.max_flush_age_s
        return poll_ok and flush_ok


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
                if health.is_healthy():
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"ok\n")
                else:
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(b"unhealthy\n")
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
    log.info("HTTP server listening on port %d (/metrics, /healthz)", port)
    return server
