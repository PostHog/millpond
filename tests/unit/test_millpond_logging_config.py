"""Unit tests for millpond/logging_config.py — the two-phase
setup_stdout() / attach_posthog_otlp() flow and the resource-attr
taxonomy emitted per destination (ducklake / iceberg / icebox).
"""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from millpond import logging_config


@pytest.fixture(autouse=True)
def _reset_root_logger():
    """Clear root-handler state between tests so streams + filters
    from one test don't leak into the next."""
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)


def _make_cfg(**overrides):
    """Minimal cfg-like object that satisfies attach_posthog_otlp's
    attribute access. Saves importing config.Config and wiring every
    field — we only need what the function reads."""
    base = dict(
        posthog_project_token=None,
        posthog_logs_endpoint="https://us.i.posthog.com/i/v1/logs",
        service_namespace="millpond",
        service_instance_id=None,
        service_version="0.0.0",
        topic="clickhouse_events_json",
        group_id="millpond-test",
        destination="ducklake",
        ducklake_table=None,
        ducklake_data_path=None,
        iceberg_warehouse=None,
        iceberg_namespace=None,
        iceberg_table=None,
        icebox_url=None,
        icebox_bucket=None,
    )
    base.update(overrides)
    return type("Cfg", (), base)()


# ---------------------------------------------------------------------------
# setup_stdout
# ---------------------------------------------------------------------------


def test_setup_stdout_installs_json_formatter_by_default(monkeypatch):
    from shared.structured_logging import JsonFormatter

    monkeypatch.delenv("MILLPOND_LOG_FORMAT", raising=False)
    logging_config.setup_stdout()
    handlers = logging.getLogger().handlers
    assert len(handlers) == 1
    assert isinstance(handlers[0].formatter, JsonFormatter)


def test_setup_stdout_text_mode_includes_pod_ordinal_prefix(monkeypatch):
    monkeypatch.setenv("MILLPOND_LOG_FORMAT", "text")
    monkeypatch.setenv("POD_NAME", "millpond-events-7")
    logging_config.setup_stdout()
    formatter = logging.getLogger().handlers[0].formatter
    # The historical text format embedded [<ordinal>][<logger>]; check
    # the format string carries the ordinal we set via POD_NAME.
    assert "[7]" in formatter._fmt


def test_setup_stdout_silences_confluent_kafka_logger():
    logging_config.setup_stdout()
    assert logging.getLogger("confluent_kafka").level == logging.WARNING


# ---------------------------------------------------------------------------
# attach_posthog_otlp — disabled path
# ---------------------------------------------------------------------------


def test_attach_posthog_otlp_returns_none_when_token_unset():
    cfg = _make_cfg()  # token=None
    assert logging_config.attach_posthog_otlp(cfg) is None


# ---------------------------------------------------------------------------
# attach_posthog_otlp — resource attrs per destination
# ---------------------------------------------------------------------------


def _resource_attrs_for(cfg) -> dict[str, str]:
    """Run attach_posthog_otlp with a mocked exporter and return the
    resource-attr dict that landed on the LoggerProvider."""
    with patch(
        "opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter",
        return_value=MagicMock(),
    ):
        provider = logging_config.attach_posthog_otlp(cfg)
    try:
        return dict(provider.resource.attributes)
    finally:
        provider.shutdown()


def test_resource_attrs_ducklake_destination():
    cfg = _make_cfg(
        posthog_project_token="phc_test",
        service_instance_id="events",
        destination="ducklake",
        ducklake_table="events",
        ducklake_data_path="s3://posthog-megaduck-mw-dev/",
    )
    attrs = _resource_attrs_for(cfg)
    # Service identity
    assert attrs["service.name"] == "millpond"
    assert attrs["service.namespace"] == "millpond"
    assert attrs["service.instance.id"] == "events"
    # OTel messaging semconv
    assert attrs["messaging.system"] == "kafka"
    assert attrs["messaging.destination.name"] == "clickhouse_events_json"
    assert attrs["messaging.kafka.consumer.group"] == "millpond-test"
    # Destination dimension + ducklake-specific custom attrs
    assert attrs["millpond.destination"] == "ducklake"
    assert attrs["millpond.ducklake.table"] == "events"
    assert attrs["millpond.ducklake.data_path"] == "s3://posthog-megaduck-mw-dev/"
    # Iceberg / icebox attrs absent
    assert "millpond.iceberg.warehouse" not in attrs
    assert "millpond.icebox.url" not in attrs


def test_resource_attrs_iceberg_destination():
    cfg = _make_cfg(
        posthog_project_token="phc_test",
        service_instance_id="events-iceberg",
        destination="iceberg",
        iceberg_warehouse="ingest",
        iceberg_namespace="kafka",
        iceberg_table="events",
    )
    attrs = _resource_attrs_for(cfg)
    assert attrs["millpond.destination"] == "iceberg"
    assert attrs["millpond.iceberg.warehouse"] == "ingest"
    assert attrs["millpond.iceberg.namespace"] == "kafka"
    assert attrs["millpond.iceberg.table"] == "events"
    # icebox-only attrs absent for plain iceberg
    assert "millpond.icebox.url" not in attrs
    assert "millpond.icebox.bucket" not in attrs
    # ducklake attrs absent
    assert "millpond.ducklake.table" not in attrs


def test_resource_attrs_icebox_destination():
    cfg = _make_cfg(
        posthog_project_token="phc_test",
        service_instance_id="events-icebox",
        destination="icebox",
        iceberg_warehouse="ingest",
        iceberg_namespace="kafka",
        iceberg_table="events",
        icebox_url="http://millpond-events-icebox-coord:8000",
        icebox_bucket="posthog-megaberg-mw-dev",
    )
    attrs = _resource_attrs_for(cfg)
    assert attrs["millpond.destination"] == "icebox"
    # Iceberg attrs carried over (icebox writers still target Iceberg)
    assert attrs["millpond.iceberg.warehouse"] == "ingest"
    assert attrs["millpond.iceberg.namespace"] == "kafka"
    assert attrs["millpond.iceberg.table"] == "events"
    # Icebox-specific routing attrs present
    assert attrs["millpond.icebox.url"] == "http://millpond-events-icebox-coord:8000"
    assert attrs["millpond.icebox.bucket"] == "posthog-megaberg-mw-dev"
    # ducklake attrs absent
    assert "millpond.ducklake.table" not in attrs


def test_resource_attrs_omit_unset_optionals():
    """service.instance.id is the per-consumer axis — omit it
    entirely if unset rather than rendering as null/empty."""
    cfg = _make_cfg(
        posthog_project_token="phc_test",
        service_instance_id=None,
        destination="ducklake",
        ducklake_table=None,  # also unset
        ducklake_data_path=None,
    )
    attrs = _resource_attrs_for(cfg)
    assert "service.instance.id" not in attrs
    assert "millpond.ducklake.table" not in attrs
    assert "millpond.ducklake.data_path" not in attrs
    # But destination IS always emitted — it's the dimension's whole point.
    assert attrs["millpond.destination"] == "ducklake"
