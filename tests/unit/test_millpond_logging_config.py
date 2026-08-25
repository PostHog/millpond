"""Unit tests for millpond/logging_config.py — the two-phase
setup_stdout() / attach_posthog_otlp() flow and the resource-attr
taxonomy.
"""

from __future__ import annotations

import dataclasses
import logging
from unittest.mock import MagicMock, patch

import pytest

from millpond import logging_config
from millpond.config import Config


@pytest.fixture(autouse=True)
def _reset_root_logger():
    """Clear root-handler state between tests so streams + filters
    from one test don't leak into the next."""
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)


def _minimal_config() -> Config:
    """A real Config instance with the minimum required field set so a
    future rename of any field surfaces here as a TypeError, not as a
    silent green-test/red-prod skew. Tests use ``dataclasses.replace``
    to override just the field(s) they care about.

    Per attach_posthog_otlp, posthog_project_token=None disables the
    OTLP path — the OFF state is the default; tests turning it ON
    pass an explicit token via replace().
    """
    return Config(
        bootstrap_servers="localhost:9092",
        topic="clickhouse_events_json",
        group_id="millpond-test",
        replica_count=1,
        ordinal=0,
        ducklake_schema="main",
        ducklake_table="events",
        ducklake_data_path="s3://bucket/data",
        ducklake_connection=":memory:",
        rds_host="localhost",
        rds_port="5432",
        rds_database="ducklake",
        rds_username="ducklake",
        rds_password="pass",
        partition_by=None,
        ducklake_max_retry_count=100,
        flush_size=104857600,
        flush_interval_ms=60000,
        fetch_min_bytes=1048576,
        fetch_max_wait_ms=500,
        consume_batch_size=1000,
        stats_interval_ms=5000,
        auto_offset_reset="earliest",
        broker_source="warpstream",
        filter_keep_field=None,
        filter_drop_field=None,
        filter_values=None,
        filter_drop_values=None,
        include_values_url=None,
        include_values_mode="shadow",
        include_values_poll_interval_s=60.0,
        include_values_removal_polls=5,
        include_values_request_timeout_s=10.0,
        include_values_startup_timeout_s=60.0,
        include_values_auth_header_name=None,
        include_values_auth_token=None,
        sort_by=None,
        typed_columns=None,
        variant_columns=None,
        variant_key_prefix=None,
        variant_keys=None,
        kafka_config_overrides=(),
    )


def _make_cfg(**overrides) -> Config:
    """Build a real Config via dataclasses.replace from the minimal
    baseline. Any rename of a Config field surfaces as a TypeError on
    this call site — structural canary that the previous
    ``type("Cfg", (), ...)`` fake lacked."""
    return dataclasses.replace(_minimal_config(), **overrides)


# ---------------------------------------------------------------------------
# setup_stdout
# ---------------------------------------------------------------------------


def test_setup_stdout_installs_json_formatter_by_default(monkeypatch):
    from millpond.structured_logging import JsonFormatter

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
# attach_posthog_otlp — resource attrs
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


def test_resource_attrs():
    cfg = _make_cfg(
        posthog_project_token="phc_test",
        service_instance_id="events",
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
    # DuckLake custom attrs
    assert attrs["millpond.ducklake.table"] == "events"
    assert attrs["millpond.ducklake.data_path"] == "s3://posthog-megaduck-mw-dev/"


def test_resource_attrs_omit_unset_optionals():
    """service.instance.id is the per-consumer axis — omit it
    entirely if unset rather than rendering as null/empty."""
    cfg = _make_cfg(
        posthog_project_token="phc_test",
        service_instance_id=None,
    )
    attrs = _resource_attrs_for(cfg)
    assert "service.instance.id" not in attrs
