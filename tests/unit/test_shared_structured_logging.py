"""Unit tests for shared/structured_logging.py — the building blocks
both millpond and icebox compose from."""
from __future__ import annotations

import io
import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from shared import structured_logging as sl


@pytest.fixture(autouse=True)
def _reset_root_logger():
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)


def _record(msg: str = "hello", extra: dict | None = None) -> logging.LogRecord:
    r = logging.LogRecord(
        name="t", level=logging.INFO, pathname="t.py", lineno=1, msg=msg, args=(), exc_info=None
    )
    if extra:
        for k, v in extra.items():
            setattr(r, k, v)
    return r


# ---------------------------------------------------------------------------
# JsonFormatter
# ---------------------------------------------------------------------------


def test_json_formatter_emits_required_fields():
    out = json.loads(sl.JsonFormatter().format(_record(msg="boot")))
    assert set(out.keys()) >= {"ts", "level", "logger", "msg"}
    assert out["msg"] == "boot"


def test_json_formatter_inlines_extra_kwargs():
    out = json.loads(sl.JsonFormatter().format(_record(extra={"file_count": 7})))
    assert out["file_count"] == 7


def test_json_formatter_extra_context_subclass_hook():
    """Subclasses (e.g. icebox's IceboxJsonFormatter) inject per-record
    fields by overriding extra_context()."""

    class _Fmt(sl.JsonFormatter):
        def extra_context(self):
            return {"injected": "yes"}

    out = json.loads(_Fmt().format(_record()))
    assert out["injected"] == "yes"


def test_json_formatter_skips_none_extra_context_values():
    """A subclass returning None values (e.g. an unset ContextVar)
    must not render the key with a null."""

    class _Fmt(sl.JsonFormatter):
        def extra_context(self):
            return {"maybe": None}

    out = json.loads(_Fmt().format(_record()))
    assert "maybe" not in out


def test_json_formatter_handles_non_serializable_via_repr():
    class NotJSON:
        def __repr__(self):
            return "NotJSON(x=1)"

    out = json.loads(sl.JsonFormatter().format(_record(extra={"obj": NotJSON()})))
    assert out["obj"] == "NotJSON(x=1)"


# ---------------------------------------------------------------------------
# install_root_handlers + silence_logger
# ---------------------------------------------------------------------------


def test_install_root_handlers_replaces_existing_handlers():
    root = logging.getLogger()
    pre_existing = logging.StreamHandler()
    root.addHandler(pre_existing)
    sl.install_root_handlers(level="INFO", formatter=sl.text_formatter())
    assert pre_existing not in root.handlers
    assert len(root.handlers) == 1


def test_silence_logger_pins_level():
    sl.silence_logger("test.noisy.logger", logging.ERROR)
    assert logging.getLogger("test.noisy.logger").level == logging.ERROR


def test_silence_logger_effectively_drops_records_below_threshold():
    """Asserting on logger.level alone isn't enough — pin the
    effective behavior with a captured stream."""
    sl.install_root_handlers(level="DEBUG", formatter=sl.JsonFormatter())
    captured = io.StringIO()
    root = logging.getLogger()
    capture_handler = logging.StreamHandler(captured)
    capture_handler.setLevel(logging.DEBUG)
    root.addHandler(capture_handler)
    sl.silence_logger("noisy.thing", logging.WARNING)
    try:
        logging.getLogger("noisy.thing").info("should-not-appear")
        logging.getLogger("noisy.thing").warning("should-appear")
        out = captured.getvalue()
        assert "should-not-appear" not in out
        assert "should-appear" in out
    finally:
        root.removeHandler(capture_handler)


# ---------------------------------------------------------------------------
# OTLP provider + handler
# ---------------------------------------------------------------------------


def test_build_otel_logger_provider_attaches_resource_attrs():
    with patch(
        "opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter",
        return_value=MagicMock(),
    ):
        provider = sl.build_otel_logger_provider(
            posthog_token="phc_test",
            posthog_endpoint="https://example.test/i/v1/logs",
            resource_attrs={"service.name": "test-svc", "k": "v"},
        )
    try:
        attrs = dict(provider.resource.attributes)
        assert attrs["service.name"] == "test-svc"
        assert attrs["k"] == "v"
    finally:
        provider.shutdown()


def test_attach_otel_handler_applies_extra_filters():
    """Filters passed to attach_otel_handler must be wired onto the
    OTel-side handler (not the stdout handler) so per-record stamping
    only affects the OTLP export path."""
    with patch(
        "opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter",
        return_value=MagicMock(),
    ):
        provider = sl.build_otel_logger_provider(
            posthog_token="phc_test",
            posthog_endpoint="https://example.test/i/v1/logs",
            resource_attrs={"service.name": "test"},
        )
    try:
        seen: list[logging.LogRecord] = []

        class _Capture(logging.Filter):
            def filter(self, record):
                seen.append(record)
                return True

        root = sl.install_root_handlers(level="INFO", formatter=sl.JsonFormatter())
        sl.attach_otel_handler(
            provider=provider, root=root, level="INFO", extra_filters=[_Capture()]
        )
        logging.getLogger("test.otel.path").info("emit me")
        assert any(r.getMessage() == "emit me" for r in seen)
    finally:
        provider.shutdown()
