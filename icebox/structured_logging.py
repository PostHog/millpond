"""Structured-logging setup for the icebox daemon.

Composes the shared building blocks in ``shared.structured_logging``
with icebox-specific bits:

  - ``IceboxJsonFormatter`` — JsonFormatter for the stdout body.
    Subclass exists so callers can extend ``extra_context()`` later
    without re-declaring the JSON shape.
  - ``setup_logging`` — assembles the icebox-flavored configuration
    on top of the shared helpers.
"""
from __future__ import annotations

import logging
from typing import Any

from shared import structured_logging as sl


class IceboxJsonFormatter(sl.JsonFormatter):
    """JSON formatter for icebox stdout logs.

    Currently a thin subclass; kept distinct from `sl.JsonFormatter` so
    callers that import `IceboxJsonFormatter` keep working as the
    formatter evolves.
    """

    def extra_context(self) -> dict[str, Any]:
        return {}


# Backward-compat alias: tests and the rest of the codebase reference
# ``JsonFormatter`` directly. The icebox flavor IS the default.
JsonFormatter = IceboxJsonFormatter


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

    Resource-attr taxonomy on the OTLP side is split by ownership:

    **App-owned** (passed here):
      - ``service.name`` (constant ``icebox``).
      - ``service.namespace`` (default ``millpond``): release family.
      - ``service.instance.id`` (per-consumer key, e.g.
        ``events-icebox``).
      - ``service.version``.
      - ``messaging.system``, ``messaging.destination.name``,
        ``messaging.kafka.consumer.group`` (OTel semconv).
      - ``icebox.iceberg.{warehouse,namespace,table}`` (vendor-prefixed).

    **Chart-owned** (NOT passed here; supplied via
    ``OTEL_RESOURCE_ATTRIBUTES`` env, auto-merged by ``Resource.create``):
      - ``deployment.environment``
      - ``k8s.cluster.name``, ``k8s.namespace.name``, ``k8s.pod.name``,
        ``k8s.deployment.name``
      - ``host.hostname``

    Split by ownership, not by precedence — there's no key the app and
    the chart both set, so we never depend on the implicit
    last-arg-wins rule of ``Resource.merge``.

    Returns the OTel ``LoggerProvider`` when PostHog logging is
    enabled, so the caller can register ``provider.shutdown()`` on
    the SIGTERM drain path. Returns ``None`` when disabled.
    """
    formatter = IceboxJsonFormatter() if fmt == "json" else sl.text_formatter()
    root = sl.install_root_handlers(level=level, formatter=formatter)

    if not posthog_token:
        return None

    resource_attrs: dict[str, str] = {
        "service.name": service_name,
        "service.namespace": service_namespace,
        "service.version": service_version,
    }
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
    )
    return provider


# Backward-compat re-export so the daemon (and anyone else) can call
# `silence_logger` via this module if they were already importing from
# here.
silence_logger = sl.silence_logger
