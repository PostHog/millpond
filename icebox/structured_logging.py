"""Structured-logging setup for the icebox.

Provides:
  - ``cycle_id_var`` — a ``ContextVar`` the committer sets at the top
    of ``run_cycle`` so every log line emitted during that cycle is
    automatically stamped with the cycle's UUID.
  - ``JsonFormatter`` — emits one JSON object per log record. Pulls
    ``cycle_id`` from the ContextVar, includes the standard fields
    (ts, level, logger, msg, exc), and inlines any ``extra=`` keyword
    fields passed to the log call.
  - ``setup_logging`` — installs the stdout handler with either the
    JSON formatter or a plain text formatter, and (when
    ``POSTHOG_PROJECT_TOKEN`` is set) attaches an OpenTelemetry
    OTLP/HTTP log handler that ships records to PostHog Logs.

The OTel path uses standard ``opentelemetry-sdk`` +
``opentelemetry-exporter-otlp-proto-http``; PostHog terminates OTLP
at ``/i/v1/logs`` so there is no PostHog-specific package needed
(verified against ``posthog`` 7.16.x in the SDK research).
"""
from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

# ContextVar carried through every committer cycle. The JsonFormatter
# reads from it when rendering log records, so all logs emitted
# during ``run_cycle`` are automatically stamped with the cycle's UUID.
cycle_id_var: ContextVar[str | None] = ContextVar("icebox_cycle_id", default=None)


# Standard ``LogRecord`` attrs we do NOT want to inline as JSON keys
# (they're already in the top-level shape or are noise).
_STANDARD_LOGRECORD_ATTRS = frozenset({
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "taskName",
})


class JsonFormatter(logging.Formatter):
    """JSON log formatter.

    Output shape (one line per record):
        {"ts": "...", "level": "INFO", "logger": "...", "msg": "...",
         "cycle_id": "...", "<extra_key>": <extra_value>, ...,
         "exc": "<traceback>"}

    Extras passed to ``log.info("...", extra={"foo": 1})`` are inlined
    at the top level. Values that aren't JSON-serializable are coerced
    via ``repr``.
    """

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        cid = cycle_id_var.get()
        if cid is not None:
            out["cycle_id"] = cid
        for key, value in record.__dict__.items():
            if key in _STANDARD_LOGRECORD_ATTRS or key.startswith("_"):
                continue
            try:
                json.dumps(value)
                out[key] = value
            except (TypeError, ValueError):
                out[key] = repr(value)
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def setup_logging(
    *,
    level: str = "INFO",
    fmt: str = "json",
    posthog_token: str | None = None,
    posthog_endpoint: str = "https://us.i.posthog.com/i/v1/logs",
    service_name: str = "icebox",
    service_namespace: str = "default",
    service_version: str = "unknown",
) -> Any | None:
    """Configure the root logger and (optionally) PostHog OTLP export.

    Returns the OTel ``LoggerProvider`` when PostHog logging is
    enabled, so the caller can register ``provider.shutdown()`` on
    the SIGTERM drain path. Returns ``None`` when disabled.

    Idempotent: re-running clears the root logger's handlers first
    so tests and re-imports don't stack duplicate sinks.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.setLevel(level)

    formatter: logging.Formatter
    if fmt == "json":
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    if not posthog_token:
        return None

    # Lazy import: bringing OTel in only when we're actually exporting
    # keeps the cold-start cost off the path for tests / dev that
    # haven't set the token. The deps ARE installed (we ship them so
    # the prod chart can opt in by setting env vars without an image
    # rebuild), but the modules pull in a fair amount of code.
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.resources import Resource

    resource = Resource.create({
        "service.name": service_name,
        "service.namespace": service_namespace,
        "service.version": service_version,
    })
    provider = LoggerProvider(resource=resource)
    set_logger_provider(provider)
    provider.add_log_record_processor(
        BatchLogRecordProcessor(
            OTLPLogExporter(
                endpoint=posthog_endpoint,
                headers={"Authorization": f"Bearer {posthog_token}"},
            )
        )
    )
    # Same formatter so the OTel body has consistent JSON shape with
    # what lands in stdout. OTel will wrap the record in its own
    # envelope on the wire, but the body field will be parseable.
    otel_handler = LoggingHandler(level=level, logger_provider=provider)
    otel_handler.setFormatter(formatter)
    root.addHandler(otel_handler)
    return provider
