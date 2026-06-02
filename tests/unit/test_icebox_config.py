"""Tests for icebox.config — env-driven Config loading."""
from __future__ import annotations

import pytest

from icebox import config as icebox_config


REQUIRED_ENV = {
    "ICEBOX_PG_HOST": "lakekeeper-pg.megaberg",
    "ICEBOX_PG_PASSWORD": "secret",
    "ICEBOX_ICEBERG_CATALOG_URI": "http://megaberg:8181/catalog",
    "ICEBOX_KAFKA_BOOTSTRAP_SERVERS": "warpstream:9092",
    "ICEBOX_KAFKA_TOPIC": "events",
    "ICEBOX_KAFKA_GROUP_ID": "millpond-events-icebox",
}


def _set_env(monkeypatch, overrides=None):
    """Wipe any pre-existing ICEBOX_* env vars then apply the required
    minimum + the caller's overrides. monkeypatch's fixture rolls back
    after the test."""
    for name in list(REQUIRED_ENV.keys()):
        monkeypatch.delenv(name, raising=False)
    # Also clear all ICEBOX_* vars to give a clean slate
    for name in list(__import__("os").environ.keys()):
        if name.startswith("ICEBOX_"):
            monkeypatch.delenv(name, raising=False)

    for k, v in REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    if overrides:
        for k, v in overrides.items():
            monkeypatch.setenv(k, v)


def test_load_with_only_required_env(monkeypatch):
    """All optional fields should fall back to defaults."""
    _set_env(monkeypatch)
    cfg = icebox_config.load()
    assert cfg.pg_host == "lakekeeper-pg.megaberg"
    assert cfg.pg_password == "secret"
    assert cfg.iceberg_catalog_uri == "http://megaberg:8181/catalog"
    assert cfg.kafka_bootstrap_servers == "warpstream:9092"
    assert cfg.kafka_topic == "events"
    assert cfg.kafka_group_id == "millpond-events-icebox"
    # Defaults
    assert cfg.pg_port == 5432
    assert cfg.pg_database == "icebox"
    assert cfg.pg_username == "lakekeeper"
    assert cfg.pg_sslmode == "require"
    assert cfg.iceberg_warehouse == "ingest"
    assert cfg.committer_cadence_seconds == 60
    assert cfg.committer_max_pending_files == 1000
    assert cfg.committer_degraded_failure_threshold == 2
    assert cfg.committer_heartbeat_stale_multiple == 3.0
    assert cfg.api_host == "0.0.0.0"
    assert cfg.api_port == 8000
    assert cfg.asyncpg_pool_min == 2
    assert cfg.asyncpg_pool_max == 8
    assert cfg.psycopg_pool_min == 1
    assert cfg.psycopg_pool_max == 2


def test_load_missing_pg_host_raises(monkeypatch):
    _set_env(monkeypatch)
    monkeypatch.delenv("ICEBOX_PG_HOST")
    with pytest.raises(RuntimeError, match="ICEBOX_PG_HOST"):
        icebox_config.load()


def test_load_missing_pg_password_raises(monkeypatch):
    _set_env(monkeypatch)
    monkeypatch.delenv("ICEBOX_PG_PASSWORD")
    with pytest.raises(RuntimeError, match="ICEBOX_PG_PASSWORD"):
        icebox_config.load()


def test_load_missing_catalog_uri_raises(monkeypatch):
    _set_env(monkeypatch)
    monkeypatch.delenv("ICEBOX_ICEBERG_CATALOG_URI")
    with pytest.raises(RuntimeError, match="ICEBOX_ICEBERG_CATALOG_URI"):
        icebox_config.load()


def test_load_missing_kafka_raises(monkeypatch):
    _set_env(monkeypatch)
    monkeypatch.delenv("ICEBOX_KAFKA_BOOTSTRAP_SERVERS")
    with pytest.raises(RuntimeError, match="ICEBOX_KAFKA_BOOTSTRAP_SERVERS"):
        icebox_config.load()


def test_load_missing_kafka_topic_raises(monkeypatch):
    _set_env(monkeypatch)
    monkeypatch.delenv("ICEBOX_KAFKA_TOPIC")
    with pytest.raises(RuntimeError, match="ICEBOX_KAFKA_TOPIC"):
        icebox_config.load()


def test_load_missing_kafka_group_id_raises(monkeypatch):
    _set_env(monkeypatch)
    monkeypatch.delenv("ICEBOX_KAFKA_GROUP_ID")
    with pytest.raises(RuntimeError, match="ICEBOX_KAFKA_GROUP_ID"):
        icebox_config.load()


def test_empty_required_env_var_treated_as_missing(monkeypatch):
    """Empty-string env vars in K8s/Helm are easy to introduce by
    accident (e.g., un-substituted template var). They should fail
    the same as omitted vars."""
    _set_env(monkeypatch, {"ICEBOX_PG_HOST": ""})
    with pytest.raises(RuntimeError, match="ICEBOX_PG_HOST"):
        icebox_config.load()


def test_load_overrides_apply(monkeypatch):
    """Each documented env var, when set, overrides the default."""
    _set_env(
        monkeypatch,
        {
            "ICEBOX_PG_PORT": "6543",
            "ICEBOX_PG_DATABASE": "icebox_test",
            "ICEBOX_PG_USERNAME": "icebox_user",
            "ICEBOX_PG_SSLMODE": "disable",
            "ICEBOX_ASYNCPG_POOL_MIN": "1",
            "ICEBOX_ASYNCPG_POOL_MAX": "16",
            "ICEBOX_PSYCOPG_POOL_MIN": "2",
            "ICEBOX_PSYCOPG_POOL_MAX": "4",
            "ICEBOX_ICEBERG_WAREHOUSE": "ingest_test",
            "ICEBOX_COMMITTER_CADENCE_SECONDS": "30",
            "ICEBOX_COMMITTER_MAX_PENDING_FILES": "500",
            "ICEBOX_COMMITTER_DEGRADED_FAILURE_THRESHOLD": "5",
            "ICEBOX_COMMITTER_HEARTBEAT_STALE_MULTIPLE": "2.5",
            "ICEBOX_API_HOST": "127.0.0.1",
            "ICEBOX_API_PORT": "9000",
            "ICEBOX_LOG_LEVEL": "DEBUG",
            "ICEBOX_KAFKA_EXTRA_CONFIG": '{"security.protocol":"PLAINTEXT"}',
        },
    )
    cfg = icebox_config.load()
    assert cfg.pg_port == 6543
    assert cfg.pg_database == "icebox_test"
    assert cfg.pg_username == "icebox_user"
    assert cfg.pg_sslmode == "disable"
    assert cfg.asyncpg_pool_min == 1
    assert cfg.asyncpg_pool_max == 16
    assert cfg.psycopg_pool_min == 2
    assert cfg.psycopg_pool_max == 4
    assert cfg.iceberg_warehouse == "ingest_test"
    assert cfg.committer_cadence_seconds == 30
    assert cfg.committer_max_pending_files == 500
    assert cfg.committer_degraded_failure_threshold == 5
    assert cfg.committer_heartbeat_stale_multiple == 2.5
    assert cfg.api_host == "127.0.0.1"
    assert cfg.api_port == 9000
    assert cfg.log_level == "DEBUG"
    assert cfg.kafka_extra_config_json == '{"security.protocol":"PLAINTEXT"}'


def test_int_env_var_with_non_integer_raises(monkeypatch):
    _set_env(monkeypatch, {"ICEBOX_PG_PORT": "not-an-int"})
    with pytest.raises(RuntimeError, match="ICEBOX_PG_PORT"):
        icebox_config.load()


def test_float_env_var_with_non_float_raises(monkeypatch):
    _set_env(monkeypatch, {"ICEBOX_COMMITTER_HEARTBEAT_STALE_MULTIPLE": "abc"})
    with pytest.raises(RuntimeError, match="ICEBOX_COMMITTER_HEARTBEAT_STALE_MULTIPLE"):
        icebox_config.load()


def test_cadence_must_be_at_least_one(monkeypatch):
    """Cadence of 0 would spin the committer loop without sleeping.
    Negative cadences are nonsense."""
    _set_env(monkeypatch, {"ICEBOX_COMMITTER_CADENCE_SECONDS": "0"})
    with pytest.raises(RuntimeError, match="must be >= 1"):
        icebox_config.load()


def test_cadence_negative_rejected(monkeypatch):
    _set_env(monkeypatch, {"ICEBOX_COMMITTER_CADENCE_SECONDS": "-5"})
    with pytest.raises(RuntimeError, match="must be >= 1"):
        icebox_config.load()


def test_config_is_frozen():
    """Config is @dataclass(frozen=True) — accidental mutation must fail."""
    import dataclasses

    assert dataclasses.is_dataclass(icebox_config.Config)
    fields = {f.name for f in dataclasses.fields(icebox_config.Config)}
    # Spot-check a few critical fields exist
    assert {
        "pg_host",
        "pg_password",
        "iceberg_catalog_uri",
        "kafka_bootstrap_servers",
        "committer_cadence_seconds",
        "committer_heartbeat_stale_multiple",
    }.issubset(fields)


def test_config_cannot_be_mutated_after_load(monkeypatch):
    _set_env(monkeypatch)
    cfg = icebox_config.load()
    with pytest.raises(Exception):  # dataclasses.FrozenInstanceError
        cfg.pg_host = "different-host"  # type: ignore[misc]


def test_default_kafka_extra_config_is_empty_json_object(monkeypatch):
    """The committer's kafka helpers parse this as JSON and merge it
    over the base config. An empty object means no overrides."""
    _set_env(monkeypatch)
    cfg = icebox_config.load()
    assert cfg.kafka_extra_config_json == "{}"


def test_pool_size_field_naming_consistent(monkeypatch):
    """Sanity that the two pool drivers are both surfaced — easy to
    drop one accidentally when adding new ones."""
    _set_env(monkeypatch)
    cfg = icebox_config.load()
    assert cfg.asyncpg_pool_min <= cfg.asyncpg_pool_max
    assert cfg.psycopg_pool_min <= cfg.psycopg_pool_max
