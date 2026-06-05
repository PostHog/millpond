"""Unit tests for icebox/structured_logging.py — the JSON formatter,
and the setup_logging() builder.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from unittest.mock import patch

import pytest

from icebox.structured_logging import (
    JsonFormatter,
    setup_logging,
)


def _make_record(
    *,
    msg: str = "hello",
    level: int = logging.INFO,
    logger_name: str = "test",
    extra: dict[str, object] | None = None,
    exc_info=None,
) -> logging.LogRecord:
    """Build a LogRecord with the same shape that logging.makeRecord
    + an ``extra`` would produce."""
    record = logging.LogRecord(
        name=logger_name,
        level=level,
        pathname="t.py",
        lineno=1,
        msg=msg,
        args=(),
        exc_info=exc_info,
    )
    if extra:
        for k, v in extra.items():
            setattr(record, k, v)
    return record


# ---------------------------------------------------------------------------
# JsonFormatter
# ---------------------------------------------------------------------------


def test_json_formatter_emits_required_fields():
    record = _make_record(msg="boot complete")
    out = json.loads(JsonFormatter().format(record))
    assert set(out.keys()) >= {"ts", "level", "logger", "msg"}
    assert out["level"] == "INFO"
    assert out["logger"] == "test"
    assert out["msg"] == "boot complete"
    # ts must be parseable as ISO-8601 UTC.
    datetime.fromisoformat(out["ts"])




def test_json_formatter_inlines_extra_fields():
    record = _make_record(msg="cycle done", extra={"file_count": 3, "snapshot_id": 42})
    out = json.loads(JsonFormatter().format(record))
    assert out["file_count"] == 3
    assert out["snapshot_id"] == 42


def test_json_formatter_handles_non_serializable_extra_via_repr():
    class NotJSON:
        def __repr__(self) -> str:
            return "NotJSON(x=1)"

    record = _make_record(msg="...", extra={"thing": NotJSON()})
    out = json.loads(JsonFormatter().format(record))
    assert out["thing"] == "NotJSON(x=1)"


def test_json_formatter_includes_exc_info_when_present():
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        import sys

        record = _make_record(msg="cycle failed", level=logging.ERROR, exc_info=sys.exc_info())
    out = json.loads(JsonFormatter().format(record))
    assert "exc" in out
    assert "RuntimeError" in out["exc"]
    assert "boom" in out["exc"]


def test_json_formatter_renders_one_line_per_record():
    record = _make_record(msg="multi\nline\nmessage")
    rendered = JsonFormatter().format(record)
    assert "\n" not in rendered  # JSON dumps escapes; one line per record
    out = json.loads(rendered)
    assert out["msg"] == "multi\nline\nmessage"


# ---------------------------------------------------------------------------
# setup_logging()
# ---------------------------------------------------------------------------


def test_setup_logging_json_mode_installs_json_formatter():
    setup_logging(level="INFO", fmt="json")
    root = logging.getLogger()
    assert len(root.handlers) == 1  # only the stream handler
    assert isinstance(root.handlers[0].formatter, JsonFormatter)


def test_setup_logging_text_mode_installs_plain_formatter():
    setup_logging(level="INFO", fmt="text")
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert not isinstance(root.handlers[0].formatter, JsonFormatter)



def test_setup_logging_clears_prior_handlers_on_repeat_call():
    # Re-running should not stack handlers (matters for tests +
    # process forks / reloaders).
    setup_logging(level="INFO", fmt="json")
    first_handler_count = len(logging.getLogger().handlers)
    setup_logging(level="INFO", fmt="json")
    assert len(logging.getLogger().handlers) == first_handler_count


def test_setup_logging_returns_none_when_no_posthog_token():
    provider = setup_logging(level="INFO", fmt="json", posthog_token=None)
    assert provider is None


def test_setup_logging_with_posthog_token_returns_provider_and_adds_handler():
    # Replace the OTLP exporter with a MagicMock so the test doesn't
    # open a network connection or background batch thread. Use the
    # full class replacement (not __init__ only) so the LoggerProvider's
    # atexit shutdown can call .shutdown() on it without AttributeError.
    from unittest.mock import MagicMock

    with patch(
        "opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter",
        return_value=MagicMock(),
    ):
        provider = setup_logging(
            level="INFO",
            fmt="json",
            posthog_token="phc_test",
            posthog_endpoint="https://example.test/i/v1/logs",
            service_name="icebox",
            service_namespace="icebox_test",
            service_version="0.0.0",
        )
    assert provider is not None
    # provider exposes shutdown — caller wires that into the SIGTERM
    # drain path in main.py
    assert hasattr(provider, "shutdown")
    # Two handlers now: stream (stdout) + OTel LoggingHandler.
    assert len(logging.getLogger().handlers) == 2
    # Explicitly shut down the provider so atexit doesn't try to drain
    # it later and produce a noisy traceback.
    provider.shutdown()
    # Cleanup so subsequent tests get a fresh root logger.
    setup_logging(level="INFO", fmt="json")


def test_setup_logging_resource_attrs_use_semconv_for_kafka_and_vendor_for_iceberg():
    """OTLP resource attrs split: Kafka uses OTel messaging semconv
    (interop with future tooling), Iceberg stays vendor-prefixed
    (no semconv coverage). No `source_type` — service.name carries
    the axis."""
    from unittest.mock import MagicMock

    with patch(
        "opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter",
        return_value=MagicMock(),
    ):
        provider = setup_logging(
            level="INFO",
            fmt="json",
            posthog_token="phc_test",
            service_instance_id="events-icebox",
            iceberg_warehouse="ingest",
            iceberg_namespace="kafka",
            iceberg_table="events",
            kafka_topic="clickhouse_events_json",
            kafka_group_id="millpond-icebox-clickhouse_events_json-events",
        )
    try:
        attrs = dict(provider.resource.attributes)
        assert attrs["service.name"] == "icebox"
        assert attrs["service.namespace"] == "millpond"
        assert attrs["service.instance.id"] == "events-icebox"
        # Kafka: messaging.* semconv
        assert attrs["messaging.system"] == "kafka"
        assert attrs["messaging.destination.name"] == "clickhouse_events_json"
        assert (
            attrs["messaging.kafka.consumer.group"]
            == "millpond-icebox-clickhouse_events_json-events"
        )
        # Iceberg: vendor-prefixed (no semconv exists today)
        assert attrs["icebox.iceberg.warehouse"] == "ingest"
        assert attrs["icebox.iceberg.namespace"] == "kafka"
        assert attrs["icebox.iceberg.table"] == "events"
        # Negative assertions: pre-rename names and dropped attrs
        assert "icebox.kafka.topic" not in attrs
        assert "icebox.kafka.group_id" not in attrs
        assert "source_type" not in attrs
    finally:
        provider.shutdown()
        setup_logging(level="INFO", fmt="json")


def test_setup_logging_omits_messaging_system_when_no_kafka_attrs():
    """messaging.system=kafka is only emitted when there are actual
    Kafka attrs to qualify — don't lie about the system if we have
    nothing to say about it."""
    from unittest.mock import MagicMock

    with patch(
        "opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter",
        return_value=MagicMock(),
    ):
        provider = setup_logging(
            level="INFO",
            fmt="json",
            posthog_token="phc_test",
            iceberg_warehouse="ingest",
            iceberg_namespace="kafka",
            iceberg_table="events",
        )
    try:
        attrs = dict(provider.resource.attributes)
        assert "messaging.system" not in attrs
        assert "messaging.destination.name" not in attrs
    finally:
        provider.shutdown()
        setup_logging(level="INFO", fmt="json")


def test_setup_logging_omits_none_optional_resource_attrs():
    """Unset optional attrs must not render as empty strings or null
    on the wire — they should be absent from the resource entirely."""
    from unittest.mock import MagicMock

    with patch(
        "opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter",
        return_value=MagicMock(),
    ):
        provider = setup_logging(
            level="INFO",
            fmt="json",
            posthog_token="phc_test",
        )
    try:
        attrs = dict(provider.resource.attributes)
        # Required attrs present, optionals absent.
        assert "service.name" in attrs
        assert "service.instance.id" not in attrs
        assert "icebox.iceberg.warehouse" not in attrs
        assert "messaging.destination.name" not in attrs
    finally:
        provider.shutdown()
        setup_logging(level="INFO", fmt="json")


def test_setup_logging_app_passed_attrs_win_over_otel_resource_attributes_env(monkeypatch):
    """Lock the user-attrs-win contract for ``Resource.create``. Even
    though we split by key ownership (app vs. chart) so the precedence
    rule is never load-bearing in production, an accidental future
    chart change that sets a key the app also sets would silently
    swap winners if this contract reverses — pin it with a test."""
    from unittest.mock import MagicMock

    monkeypatch.setenv(
        "OTEL_RESOURCE_ATTRIBUTES",
        "service.namespace=env-wins,deployment.environment=prod",
    )
    with patch(
        "opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter",
        return_value=MagicMock(),
    ):
        provider = setup_logging(
            level="INFO",
            fmt="json",
            posthog_token="phc_test",
            service_namespace="app-wins",
        )
    try:
        attrs = dict(provider.resource.attributes)
        # Same-key conflict: app value wins (this is the contract).
        assert attrs["service.namespace"] == "app-wins"
        # Distinct key supplied only by env still flows through.
        assert attrs["deployment.environment"] == "prod"
    finally:
        provider.shutdown()
        setup_logging(level="INFO", fmt="json")




