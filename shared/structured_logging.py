"""Shared structured-logging building blocks for millpond + icebox.

Both services emit JSON logs to stdout and (optionally) export OTLP/HTTP
log records to PostHog Logs. This module provides the service-agnostic
pieces:

  - ``JsonFormatter`` — base JSON formatter. Subclasses override
    ``extra_context()`` to inject per-record fields from ContextVars
    or similar.
  - ``text_formatter`` — plain-text formatter for local dev.
  - ``install_root_handlers`` — clear + reattach the root logger.
  - ``build_otel_logger_provider`` — construct a LoggerProvider with
    the OTLP/HTTP exporter bound to PostHog Logs.
  - ``attach_otel_handler`` — wire the OTel handler onto the root
    logger, optionally with extra filters (e.g. ContextVar-stamping).
  - ``silence_logger`` — pin a noisy logger at a given level.

Service-specific concerns (icebox ``cycle_id``, millpond pod-ordinal
prefix, per-service resource-attr taxonomy) stay in
``icebox/structured_logging.py`` and ``millpond/logging_config.py``;
they compose these blocks rather than reinventing them.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any


# Standard ``LogRecord`` attrs we do NOT want to inline as JSON keys
# (they're already in the top-level shape or are noise).
_STANDARD_LOGRECORD_ATTRS = frozenset({
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "taskName",
})


class JsonFormatter(logging.Formatter):
    """Base JSON log formatter.

    Output shape (one line per record):
        {"ts": "...", "level": "INFO", "logger": "...", "msg": "...",
         "<extra_key>": <extra_value>, ..., "exc": "<traceback>"}

    Extras passed to ``log.info("...", extra={"foo": 1})`` are inlined
    at the top level. Values that aren't JSON-serializable are coerced
    via ``repr``.

    Subclasses can override ``extra_context()`` to add fields pulled
    from ContextVars or other per-call-site sources (e.g. the icebox's
    ``cycle_id``).
    """

    def extra_context(self) -> dict[str, Any]:  # pragma: no cover - trivial default
        return {}

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in self.extra_context().items():
            if value is not None:
                out[key] = value
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


def text_formatter() -> logging.Formatter:
    """Plain-text formatter for local-dev readability."""
    return logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")


def install_root_handlers(
    *,
    level: str,
    formatter: logging.Formatter,
    root: logging.Logger | None = None,
) -> logging.Logger:
    """Reset the root logger and attach a single stdout handler.

    Idempotent: clears existing handlers so tests, re-imports, and
    process forks don't stack duplicate sinks. Returns the root
    logger so callers can use it for additional handler attachment
    (e.g. the OTel handler).
    """
    root = root or logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.setLevel(level)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)
    return root


def silence_logger(name: str, level: int = logging.WARNING) -> None:
    """Pin a noisy logger at a given level.

    Pre-existing nuisance loggers we know about:
      - ``uvicorn.access`` (icebox): probes + Prometheus scrape + every
        POST emit one line each at INFO.
      - ``confluent_kafka`` (millpond): librdkafka chatter at INFO.

    Idempotent — safe to call if the logger hasn't been created yet
    (Python lazy-creates loggers on first ``getLogger`` reference).
    """
    logging.getLogger(name).setLevel(level)


def build_otel_logger_provider(
    *,
    posthog_token: str,
    posthog_endpoint: str,
    resource_attrs: dict[str, str],
    batch_schedule_delay_millis: int = 5000,
    batch_max_export_size: int = 512,
    batch_max_queue_size: int = 4096,
) -> Any:
    """Build a LoggerProvider with the OTLP/HTTP exporter to PostHog Logs.

    Resource attrs are passed verbatim; ``Resource.create`` additionally
    merges in ``OTEL_RESOURCE_ATTRIBUTES`` from env (the standard SDK
    behavior, which the chart relies on for ``deployment.environment``
    / ``k8s.*`` / ``host.hostname``).

    Batch knobs are pinned to explicit values rather than letting the
    SDK pick. At 32 writer + 6+ icebox pods exporting at the SDK
    defaults (``schedule_delay_millis=1000``, ``max_export_batch_size=512``,
    ``max_queue_size=2048``), the aggregate is ~38 batched exports/sec
    to a single PostHog Logs endpoint from one deployment family. Our
    defaults are tuned for PostHog Logs ingest: 5s schedule delay (≈ a
    fifth of SDK chatter), the standard 512-record batch, and a 2× queue
    so a stalled exporter doesn't drop logs during transient PostHog
    ingress backpressure. Operators can override per-service via the
    Config fields ``posthog_logs_schedule_delay_ms`` /
    ``posthog_logs_max_batch_size`` / ``posthog_logs_max_queue_size``.

    Caller owns the returned provider — register
    ``provider.shutdown()`` on the SIGTERM drain path so the
    BatchLogRecordProcessor flushes in-flight batches.

    Lazy imports: bringing OTel modules in only when the caller has
    decided to export keeps the cold-start cost off the path for tests
    / dev that haven't set the token.
    """
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.resources import Resource

    resource = Resource.create(resource_attrs)
    provider = LoggerProvider(resource=resource)
    set_logger_provider(provider)
    provider.add_log_record_processor(
        BatchLogRecordProcessor(
            OTLPLogExporter(
                endpoint=posthog_endpoint,
                headers={"Authorization": f"Bearer {posthog_token}"},
            ),
            schedule_delay_millis=batch_schedule_delay_millis,
            max_export_batch_size=batch_max_export_size,
            max_queue_size=batch_max_queue_size,
        )
    )
    return provider


def attach_otel_handler(
    *,
    provider: Any,
    root: logging.Logger,
    level: str,
    extra_filters: list[logging.Filter] | None = None,
) -> Any:
    """Attach the OTel logging handler to the root logger.

    ``extra_filters`` (optional) run on the OTel side only, so
    services can stamp per-record attrs (e.g. icebox's
    ``icebox.cycle_id``) without touching the stdout shape.

    The handler comes from ``opentelemetry-instrumentation-logging``;
    the older ``opentelemetry.sdk._logs.LoggingHandler`` was deprecated
    in SDK 1.42. ``log_code_attributes=False`` is deliberate — the
    code.{file,function,line} attrs are pure noise in PostHog Logs.
    """
    from opentelemetry.instrumentation.logging.handler import LoggingHandler

    otel_handler = LoggingHandler(
        level=level,
        logger_provider=provider,
        log_code_attributes=False,
    )
    for flt in extra_filters or ():
        otel_handler.addFilter(flt)
    root.addHandler(otel_handler)
    return otel_handler
