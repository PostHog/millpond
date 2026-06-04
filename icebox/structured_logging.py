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


class _CycleIdAttrFilter(logging.Filter):
    """Stamp ``icebox.cycle_id`` onto records inside a cycle context.

    Attached to the OTel handler only. The stdout JsonFormatter keeps
    its own ContextVar read (preserves the legacy ``cycle_id`` body
    field), while OTel record attributes get the namespaced key so
    PostHog Logs can filter on it like any other resource/record dim.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        cid = cycle_id_var.get()
        if cid is not None:
            # Dotted attribute names are legal as dict keys via setattr
            # — OTel's LoggingHandler picks these up into record attrs
            # without further translation.
            record.__dict__["icebox.cycle_id"] = cid
        return True


def setup_logging(
    *,
    level: str = "INFO",
    fmt: str = "json",
    posthog_token: str | None = None,
    posthog_endpoint: str = "https://us.i.posthog.com/i/v1/logs",
    service_name: str = "icebox",
    service_namespace: str = "millpond",
    service_version: str = "unknown",
    service_instance_id: str | None = None,
    iceberg_warehouse: str | None = None,
    iceberg_namespace: str | None = None,
    iceberg_table: str | None = None,
    kafka_topic: str | None = None,
    kafka_group_id: str | None = None,
) -> Any | None:
    """Configure the root logger and (optionally) PostHog OTLP export.

    Resource-attr taxonomy on the OTLP side, split by ownership:

    **App-owned** (passed here):
      - ``service.name`` (constant ``icebox``): one binary, one service.
        Per-instance differentiation is on ``service.instance.id``.
      - ``service.namespace`` (default ``millpond``): release family.
        OTel semconv intends this for logical service grouping — NOT
        data-side namespacing. (Earlier versions misused it for the
        PG schema; see migration note in icebox/README.md.)
      - ``service.instance.id`` (per-consumer key, e.g.
        ``events-icebox``): differentiates instances of the same
        service. This IS the per-pod / per-(namespace,table) axis.
      - ``service.version``: package version.
      - ``messaging.system``, ``messaging.destination.name``,
        ``messaging.kafka.consumer.group``: OTel semconv standard for
        Kafka attrs. Picked over vendor-prefixed
        ``icebox.kafka.*`` so future OTel-aware tooling (collector
        processors, dashboard packs, alert templates) Just Works.
      - ``icebox.iceberg.{warehouse,namespace,table}``: vendor-prefixed
        because OTel semconv has no Iceberg coverage today.

    **Chart-owned** (NOT passed here; supplied via
    ``OTEL_RESOURCE_ATTRIBUTES`` env, auto-merged by ``Resource.create``):
      - ``deployment.environment``
      - ``k8s.cluster.name``, ``k8s.namespace.name``, ``k8s.pod.name``,
        ``k8s.deployment.name``
      - ``host.hostname``

    Split by ownership, not by precedence — there's no key the app and
    the chart both set, so we never depend on the implicit
    last-arg-wins rule of ``Resource.merge``.

    ``cycle_id`` is intentionally a per-record attribute
    (``icebox.cycle_id``), NOT a Resource attribute, because it's
    scoped to a single cycle inside the process — not a property of
    the process itself.

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

    # Silence uvicorn's per-request access log. kubelet probes
    # (/readyz, /healthz) + Prometheus scrape (/metrics) + every
    # writer POST (/v1/files) generate one line each here. None of it
    # carries operational signal that isn't already in our metrics
    # — but together they swamp the useful committer logs (~63% of
    # all icebox log volume per a 1h prod sample). WARNING-level
    # keeps the door open for uvicorn to surface a startup failure.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

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

    # The handler from opentelemetry.sdk._logs is deprecated since
    # opentelemetry-sdk 1.42 in favor of the one from
    # opentelemetry-instrumentation-logging. Same constructor signature
    # (level, logger_provider), same wire behavior — the
    # instrumentation-package version additionally defaults to NOT
    # emitting code.{file,function,line} attributes per record, which
    # is exactly what we want for PostHog Logs (those attrs were pure
    # noise in the export).
    from opentelemetry.instrumentation.logging.handler import LoggingHandler
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.resources import Resource

    # Build the resource-attr dict, dropping None values so unset
    # optionals don't render as empty strings on the wire.
    resource_attrs: dict[str, str] = {
        "service.name": service_name,
        "service.namespace": service_namespace,
        "service.version": service_version,
    }
    # Kafka attrs use OTel messaging semconv (see
    # https://opentelemetry.io/docs/specs/semconv/messaging/) so
    # generic OTel tooling can interpret them. Iceberg attrs stay
    # vendor-prefixed (no semconv coverage today).
    for key, value in (
        ("service.instance.id", service_instance_id),
        ("messaging.destination.name", kafka_topic),
        ("messaging.kafka.consumer.group", kafka_group_id),
        ("icebox.iceberg.warehouse", iceberg_warehouse),
        ("icebox.iceberg.namespace", iceberg_namespace),
        ("icebox.iceberg.table", iceberg_table),
    ):
        if value is not None:
            resource_attrs[key] = value
    # ``messaging.system`` is the constant axis for the system itself;
    # only emit it if we actually have Kafka attrs to qualify.
    if kafka_topic is not None or kafka_group_id is not None:
        resource_attrs["messaging.system"] = "kafka"

    # Resource.create() merges in OTEL_RESOURCE_ATTRIBUTES env so the
    # chart can supply deployment.environment, k8s.*, host.hostname
    # without an app code change. We split by key ownership (app vs.
    # chart) so the implicit user-attrs-win precedence of merge() is
    # never load-bearing — but a test still pins it as a safety net
    # against future SDK refactors.
    resource = Resource.create(resource_attrs)
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
    # log_code_attributes=False is the explicit (and default) request to
    # NOT stamp code.{file,function,line} on every record. The previous
    # opentelemetry-sdk handler stamped them unconditionally; the new
    # instrumentation-package handler defaults to off, but pinning the
    # kwarg makes the choice deliberate code rather than implicit
    # default — future readers see we want them off.
    otel_handler = LoggingHandler(
        level=level,
        logger_provider=provider,
        log_code_attributes=False,
    )
    # Filter is OTel-side only so the stdout JSON shape stays stable
    # (existing ``cycle_id`` body field via the formatter's
    # ContextVar read) while OTel records gain ``icebox.cycle_id`` as
    # a typed record attribute for PostHog Logs filtering.
    otel_handler.addFilter(_CycleIdAttrFilter())
    root.addHandler(otel_handler)
    return provider
