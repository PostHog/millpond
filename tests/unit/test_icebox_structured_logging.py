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


@pytest.fixture(autouse=True)
def _reset_root_logger():
    """Reset root logger handlers between tests so log lines from one
    test don't pile up in another's handler chain."""
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
