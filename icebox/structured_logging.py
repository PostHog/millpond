"""Structured-logging setup for the icebox.

Composes the shared building blocks in ``shared.structured_logging``
with icebox-specific bits:

  - ``cycle_id_var`` — a ``ContextVar`` the committer sets at the top
    of ``run_cycle`` so every log line emitted during that cycle is
    automatically stamped with the cycle's UUID.
  - ``IceboxJsonFormatter`` — JsonFormatter that pulls ``cycle_id``
    from the ContextVar into the stdout JSON body.
  - ``_CycleIdAttrFilter`` — OTel-side filter that stamps
    ``icebox.cycle_id`` onto each log record as a typed attribute.
  - ``setup_logging`` — assembles the icebox-flavored configuration
    on top of the shared helpers.
"""
from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Any

from shared import structured_logging as sl

# ContextVar carried through every committer cycle. The formatter
# reads from it when rendering log records, so all logs emitted
# during ``run_cycle`` are automatically stamped with the cycle's UUID.
cycle_id_var: ContextVar[str | None] = ContextVar("icebox_cycle_id", default=None)


class IceboxJsonFormatter(sl.JsonFormatter):
    """JSON formatter that inlines ``cycle_id`` into the stdout body.

    Kept separate from the OTLP-side stamping (which uses
    ``_CycleIdAttrFilter`` with the namespaced key ``icebox.cycle_id``)
    so the stdout JSON shape stays backward-compatible with the
    pre-existing ``cycle_id`` body field.
    """

    def extra_context(self) -> dict[str, Any]:
        return {"cycle_id": cycle_id_var.get()}


# Backward-compat alias: tests and the rest of the codebase reference
# ``JsonFormatter`` directly. The icebox flavor IS the default.
JsonFormatter = IceboxJsonFormatter


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
    """
    formatter = IceboxJsonFormatter() if fmt == "json" else sl.text_formatter()
    root = sl.install_root_handlers(level=level, formatter=formatter)

    # Belt-and-suspenders against uvicorn.access leakage. The
    # load-bearing silencing happens at ``uvicorn.run(access_log=False)``
    # in icebox/main.py — verified in prod after the level-only approach
    # turned out to be defeated by uvicorn's startup logger-config reset.
    # This setLevel still catches any future ``uvicorn.access`` records
    # constructed outside the standard access-log path (e.g. a
    # middleware hook), so leaving it in place is cheap insurance.
    sl.silence_logger("uvicorn.access", logging.WARNING)

    if not posthog_token:
        return None

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

    provider = sl.build_otel_logger_provider(
        posthog_token=posthog_token,
        posthog_endpoint=posthog_endpoint,
        resource_attrs=resource_attrs,
    )
    sl.attach_otel_handler(
        provider=provider,
        root=root,
        level=level,
        extra_filters=[_CycleIdAttrFilter()],
    )
    return provider
