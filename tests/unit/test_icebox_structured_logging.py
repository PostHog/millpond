"""Unit tests for icebox/structured_logging.py — the JSON formatter,
the cycle_id ContextVar, and the setup_logging() builder.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from unittest.mock import patch

import pytest

from icebox.structured_logging import (
    JsonFormatter,
    cycle_id_var,
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


def test_json_formatter_includes_cycle_id_when_contextvar_set():
    token = cycle_id_var.set("c-123")
    try:
        record = _make_record(msg="claiming files")
        out = json.loads(JsonFormatter().format(record))
        assert out["cycle_id"] == "c-123"
    finally:
        cycle_id_var.reset(token)


def test_json_formatter_omits_cycle_id_when_unset():
    # Make sure no leakage from prior tests; reset is unconditional.
    if cycle_id_var.get() is not None:
        cycle_id_var.set(None)
    record = _make_record(msg="no cycle context here")
    out = json.loads(JsonFormatter().format(record))
    assert "cycle_id" not in out


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


def test_setup_logging_silences_uvicorn_access_logger():
    """uvicorn.access ships one line per probe + per writer POST and
    swamps the useful committer logs. Effective behavior: an INFO
    record on that logger after setup_logging() must NOT reach the
    captured stream. Asserting on the logger.level value alone would
    miss the case where uvicorn (or anything else) flips the level
    back at server start."""
    import io

    setup_logging(level="DEBUG", fmt="json")
    # Add a capture sink to the same root handler chain
    captured = io.StringIO()
    root = logging.getLogger()
    capture_handler = logging.StreamHandler(captured)
    capture_handler.setLevel(logging.DEBUG)
    root.addHandler(capture_handler)
    try:
        logging.getLogger("uvicorn.access").info(
            'GET /healthz HTTP/1.1" 200'
        )
        assert "/healthz" not in captured.getvalue()
    finally:
        root.removeHandler(capture_handler)


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


def test_cycle_id_attr_filter_stamps_record_when_contextvar_set():
    """The OTel-side filter must surface ``icebox.cycle_id`` as a
    record attribute so PostHog Logs can filter on it directly,
    independent of the JSON body shape on stdout."""
    from icebox.structured_logging import _CycleIdAttrFilter

    flt = _CycleIdAttrFilter()
    token = cycle_id_var.set("c-xyz")
    try:
        record = _make_record(msg="inside cycle")
        flt.filter(record)
        # Dot-keyed attrs aren't accessible via getattr-with-dots syntax;
        # OTel reads them from __dict__ directly.
        assert record.__dict__["icebox.cycle_id"] == "c-xyz"
    finally:
        cycle_id_var.reset(token)


def test_cycle_id_attr_filter_noop_when_contextvar_unset():
    from icebox.structured_logging import _CycleIdAttrFilter

    if cycle_id_var.get() is not None:
        cycle_id_var.set(None)
    flt = _CycleIdAttrFilter()
    record = _make_record(msg="no cycle context")
    flt.filter(record)
    assert "icebox.cycle_id" not in record.__dict__


def test_cycle_id_survives_logging_makerecord_plumbing():
    """Records produced via the public logging API path
    (`logger.info(..., extra={...})` → `Logger.makeRecord` →
    `LogRecord.__init__`) must carry the dotted attribute the filter
    sets — not just hand-built `LogRecord` instances. Some
    implementations of `LogRecord` go through `__dict__.update(extra)`
    which can mishandle dotted keys; this test exercises the real
    plumbing once so a regression there can't slip through."""
    from icebox.structured_logging import _CycleIdAttrFilter

    token = cycle_id_var.set("c-from-real-logger")
    try:
        flt = _CycleIdAttrFilter()
        logger = logging.getLogger("test_cycle_id_real_logger")
        logger.addFilter(flt)
        # Add a capturing handler whose emit() inspects the record
        # AFTER the filter runs — same lifecycle the OTel handler sees.
        captured: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured.append(record)

        cap = _Capture()
        logger.addHandler(cap)
        logger.setLevel(logging.INFO)
        try:
            logger.info("hello from a real logger", extra={"file_count": 7})
            assert len(captured) == 1
            rec = captured[0]
            assert rec.__dict__["icebox.cycle_id"] == "c-from-real-logger"
            # The non-dotted `extra` field came through normally too.
            assert rec.__dict__["file_count"] == 7
        finally:
            logger.removeHandler(cap)
            logger.removeFilter(flt)
    finally:
        cycle_id_var.reset(token)


@pytest.fixture(autouse=True)
def _reset_root_logger():
    """Reset root logger handlers between tests so log lines from one
    test don't pile up in another's handler chain."""
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
