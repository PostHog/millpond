"""Structured-logging setup for the millpond writer.

Two-phase by design:

  - ``setup_stdout()`` — called BEFORE ``config.load()`` so that
    config-load errors land on a structured stdout sink rather than
    Python's default plain-text root.
  - ``attach_posthog_otlp(cfg)`` — called AFTER ``config.load()``
    once we have the resource-attr inputs. Returns the
    ``LoggerProvider`` so the caller can register
    ``provider.shutdown()`` on SIGTERM. Returns ``None`` when
    ``cfg.posthog_project_token`` is unset.

The two-phase split is intentional: the OTLP path needs config to
build the resource attrs (topic, table, etc.), but we want logging
up before config-load can fail loudly.

Building blocks live in ``millpond.structured_logging``; this
module adds the service-specific pieces (pod-ordinal prefix on the
text formatter, ``confluent_kafka`` chatter silencing, and the
millpond resource-attr taxonomy: ``millpond`` as ``service.name``
plus the ``millpond.ducklake.*`` custom attrs).
"""

from __future__ import annotations

import logging
import os
from typing import Any

from millpond import structured_logging as sl


def _pod_ordinal() -> str:
    """Best-effort pod ordinal extraction for log prefix.

    Falls back to the full pod name if no `-N` suffix, and to an empty
    string if neither POD_NAME nor HOSTNAME is set.
    """
    pod_name = os.environ.get("POD_NAME") or os.environ.get("HOSTNAME", "")
    if "-" in pod_name:
        return pod_name.rsplit("-", 1)[-1]
    return pod_name


def setup_stdout() -> None:
    """Phase 1: install the stdout handler with the configured format.

    Called before config.load() so config-load errors are structured.
    Reads LOG_LEVEL + MILLPOND_LOG_FORMAT (json|text) directly from
    env — the Config dataclass isn't built yet at this point. Also
    silences confluent_kafka chatter (per-message INFO at WARNING+).
    """
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    fmt = os.environ.get("MILLPOND_LOG_FORMAT", "json").lower()
    if fmt == "json":
        formatter: logging.Formatter = sl.JsonFormatter()
    else:
        # Preserve the historical [ordinal][logger] prefix for text mode
        # so dev tail-style consumers stay readable.
        ordinal = _pod_ordinal()
        prefix = f"[{ordinal}]" if ordinal else ""
        formatter = logging.Formatter(
            f"%(asctime)s %(levelname)-5s {prefix}[%(name)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    sl.install_root_handlers(level=level, formatter=formatter)
    sl.silence_logger("confluent_kafka", logging.WARNING)


def attach_posthog_otlp(cfg) -> Any | None:
    """Phase 2: attach the OTLP/HTTP handler to PostHog Logs.

    Returns the OTel LoggerProvider when enabled (so caller can
    register ``provider.shutdown()`` on SIGTERM) or None when
    ``cfg.posthog_project_token`` is unset.

    Resource-attr taxonomy:

    **App-owned** (passed here):
      - ``service.name`` = constant ``millpond``
      - ``service.namespace`` (default ``millpond``)
      - ``service.instance.id`` = consumer key set by chart (e.g.
        ``events``)
      - ``service.version``
      - ``messaging.system=kafka``, ``messaging.destination.name``,
        ``messaging.kafka.consumer.group`` — OTel messaging semconv.
      - ``millpond.ducklake.table``, ``millpond.ducklake.data_path``.

    **Chart-owned** (NOT passed; auto-merged via
    ``OTEL_RESOURCE_ATTRIBUTES``): ``deployment.environment``,
    ``k8s.*``, ``host.hostname``.
    """
    if not cfg.posthog_project_token:
        return None

    resource_attrs: dict[str, str] = {
        "service.name": "millpond",
        "service.namespace": cfg.service_namespace,
        "service.version": cfg.service_version,
        "messaging.system": "kafka",
        "messaging.destination.name": cfg.topic,
        "messaging.kafka.consumer.group": cfg.group_id,
        "millpond.ducklake.table": cfg.ducklake_table,
        "millpond.ducklake.data_path": cfg.ducklake_data_path,
    }
    if cfg.service_instance_id is not None:
        resource_attrs["service.instance.id"] = cfg.service_instance_id

    provider = sl.build_otel_logger_provider(
        posthog_token=cfg.posthog_project_token,
        posthog_endpoint=cfg.posthog_logs_endpoint,
        resource_attrs=resource_attrs,
    )
    sl.attach_otel_handler(
        provider=provider,
        root=logging.getLogger(),
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    )
    return provider
