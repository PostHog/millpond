"""Tests for icebox.config — env-driven Config loading."""
from __future__ import annotations

import pytest

from icebox import config as icebox_config

REQUIRED_ENV = {
    "ICEBOX_PG_HOST": "lakekeeper-pg.megaberg",
    "ICEBOX_PG_PASSWORD": "secret",
    "ICEBOX_ICEBERG_CATALOG_URI": "http://megaberg:8181/catalog",
    "ICEBOX_ICEBERG_NAMESPACE": "kafka",
    "ICEBOX_ICEBERG_TABLE": "events",
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
    assert cfg.committer_max_pending_files == 100
    assert cfg.committer_heartbeat_stale_multiple == 3.0
    assert cfg.api_host == "0.0.0.0"
    assert cfg.api_port == 8000
    assert cfg.psycopg_pool_min == 1
    assert cfg.psycopg_pool_max == 2
    assert cfg.service_namespace == "millpond"
    assert cfg.service_instance_id is None


def test_service_namespace_and_instance_id_overrides(monkeypatch):
    _set_env(
        monkeypatch,
        {
            "ICEBOX_SERVICE_NAMESPACE": "icebox-shadow",
            "ICEBOX_SERVICE_INSTANCE_ID": "events-icebox",
        },
    )
    cfg = icebox_config.load()
    assert cfg.service_namespace == "icebox-shadow"
    assert cfg.service_instance_id == "events-icebox"


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
            "ICEBOX_PSYCOPG_POOL_MIN": "2",
            "ICEBOX_PSYCOPG_POOL_MAX": "4",
            "ICEBOX_ICEBERG_WAREHOUSE": "ingest_test",
            "ICEBOX_COMMITTER_CADENCE_SECONDS": "30",
            "ICEBOX_COMMITTER_MAX_PENDING_FILES": "500",
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
    assert cfg.psycopg_pool_min == 2
    assert cfg.psycopg_pool_max == 4
    assert cfg.iceberg_warehouse == "ingest_test"
    assert cfg.committer_cadence_seconds == 30
    assert cfg.committer_max_pending_files == 500
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


def test_psycopg_pool_max_below_2_rejected(monkeypatch):
    """PE review #5: psycopg_pool_max=1 deadlocks because the committer
    holds 1 connection (advisory lock) for the lifetime of the thread,
    leaving zero slots for cycle work."""
    _set_env(monkeypatch, {"ICEBOX_PSYCOPG_POOL_MAX": "1"})
    with pytest.raises(RuntimeError, match="ICEBOX_PSYCOPG_POOL_MAX must be >= 2"):
        icebox_config.load()


def test_psycopg_pool_max_zero_rejected(monkeypatch):
    _set_env(monkeypatch, {"ICEBOX_PSYCOPG_POOL_MAX": "0"})
    with pytest.raises(RuntimeError, match="ICEBOX_PSYCOPG_POOL_MAX must be >= 2"):
        icebox_config.load()


def test_psycopg_pool_max_exactly_2_accepted(monkeypatch):
    """The floor must be inclusive — 2 is the documented minimum."""
    _set_env(monkeypatch, {"ICEBOX_PSYCOPG_POOL_MAX": "2"})
    cfg = icebox_config.load()
    assert cfg.psycopg_pool_max == 2


def test_pg_schema_defaults_to_icebox(monkeypatch):
    _set_env(monkeypatch)
    cfg = icebox_config.load()
    assert cfg.pg_schema == "icebox"


def test_pg_schema_accepts_underscore_suffix(monkeypatch):
    """Per-deployment naming convention: events icebox runs against
    `icebox_events`, person against `icebox_person`, etc."""
    _set_env(monkeypatch, {"ICEBOX_PG_SCHEMA": "icebox_events"})
    cfg = icebox_config.load()
    assert cfg.pg_schema == "icebox_events"


@pytest.mark.parametrize("bad", [
    "1startswithnumber",
    "has-dashes",
    "has.dots",
    "has spaces",
    "select; drop table",  # classic injection attempt
    "x" * 64,  # over 63-char identifier limit
    "",  # empty (would fall to default via _optional, but explicit empty is rejected)
])
def test_pg_schema_rejects_invalid_identifiers(monkeypatch, bad):
    """Schema name gets interpolated into the conninfo `options=-c
    search_path=...` parameter; PG protocol doesn't allow parameter
    binding for session options. Strict identifier validation at boot
    makes injection structurally impossible — and PG itself only
    accepts these characters in unquoted identifiers."""
    _set_env(monkeypatch, {"ICEBOX_PG_SCHEMA": bad})
    with pytest.raises(RuntimeError, match="ICEBOX_PG_SCHEMA"):
        icebox_config.load()


@pytest.mark.parametrize("uppercase_schema", [
    "Icebox",  # mixed case
    "ICEBOX",  # all caps
    "icebox_Events",  # uppercase in suffix
    "icebox_events_V2",  # camelCase-ish
])
def test_pg_schema_rejects_uppercase_identifiers(monkeypatch, uppercase_schema):
    """QE-review finding: psycopg's `options=-csearch_path=<schema>`
    sends the schema unquoted, so PG folds it to lowercase. asyncpg's
    `server_settings={"search_path": <schema>}` sends it as a literal
    that PRESERVES case. Allowing uppercase would produce a split-brain
    between the two pools (one resolves to `myicebox`, the other to
    `MyIcebox`). Lock the whole pipeline to lowercase at config load."""
    _set_env(monkeypatch, {"ICEBOX_PG_SCHEMA": uppercase_schema})
    with pytest.raises(RuntimeError, match="ICEBOX_PG_SCHEMA"):
        icebox_config.load()


@pytest.mark.parametrize("reserved_schema", [
    "pg_catalog",
    "pg_toast",
    "pg_temp",
    "pg_temp_1",
    "pg_anything",  # any pg_ prefix
    "information_schema",
    "public",
])
def test_pg_schema_rejects_reserved_pg_names(monkeypatch, reserved_schema):
    """PG reserves `pg_*` prefix (CREATE SCHEMA refuses these with
    SQLSTATE 42939) and a handful of well-known schemas. `public`
    would technically succeed but commingle icebox with whatever else
    is in `public` — worse than failing fast."""
    _set_env(monkeypatch, {"ICEBOX_PG_SCHEMA": reserved_schema})
    with pytest.raises(RuntimeError, match="ICEBOX_PG_SCHEMA"):
        icebox_config.load()


@pytest.mark.parametrize("keyword_schema", [
    "select",
    "from",
    "table",
    "schema",
    "database",
    "create",
    "commit",
    "rollback",
    "user",
    "group",
])
def test_pg_schema_rejects_sql_reserved_words(monkeypatch, keyword_schema):
    """SQL reserved words would need quoting in every reference; the
    conninfo `options=-csearch_path=` interpolation can't quote them
    reliably. Reject at config load."""
    _set_env(monkeypatch, {"ICEBOX_PG_SCHEMA": keyword_schema})
    with pytest.raises(RuntimeError, match="ICEBOX_PG_SCHEMA"):
        icebox_config.load()


@pytest.mark.parametrize("bad_db", [
    "1startswithnumber",
    "has-dashes",
    "has.dots",
    "has spaces",
    "pg_anything",  # reserved prefix
    "MyDB",  # uppercase (split-brain prevention)
])
def test_pg_database_rejects_invalid_identifiers(monkeypatch, bad_db):
    """PE review #1: cfg.pg_database flows into psycopg/asyncpg conninfo
    unquoted. An operator typo otherwise boot-loops the pod with a
    cryptic libpq error; validate at config-load."""
    _set_env(monkeypatch, {"ICEBOX_PG_DATABASE": bad_db})
    with pytest.raises(RuntimeError, match="ICEBOX_PG_DATABASE"):
        icebox_config.load()


def test_iceberg_namespace_required(monkeypatch):
    """Per-schema topology: each icebox is explicit about which
    Iceberg (namespace, table) it serves. Previously parsed from
    kafka_topic; now an explicit env var."""
    _set_env(monkeypatch)
    monkeypatch.delenv("ICEBOX_ICEBERG_NAMESPACE")
    with pytest.raises(RuntimeError, match="ICEBOX_ICEBERG_NAMESPACE"):
        icebox_config.load()


def test_iceberg_table_required(monkeypatch):
    _set_env(monkeypatch)
    monkeypatch.delenv("ICEBOX_ICEBERG_TABLE")
    with pytest.raises(RuntimeError, match="ICEBOX_ICEBERG_TABLE"):
        icebox_config.load()


def test_iceberg_namespace_and_table_loaded(monkeypatch):
    _set_env(monkeypatch, {
        "ICEBOX_ICEBERG_NAMESPACE": "kafka",
        "ICEBOX_ICEBERG_TABLE": "person_distinct_id",
    })
    cfg = icebox_config.load()
    assert cfg.iceberg_namespace == "kafka"
    assert cfg.iceberg_table == "person_distinct_id"


def test_pg_schema_accepts_real_world_names(monkeypatch):
    """Sanity: the names we actually plan to deploy ALL pass."""
    for schema in (
        "icebox",
        "icebox_events",
        "icebox_person",
        "icebox_person_distinct_id",
        "icebox_groups",
        "icebox_heatmap_events",
        "icebox_ai_events",
    ):
        _set_env(monkeypatch, {"ICEBOX_PG_SCHEMA": schema})
        cfg = icebox_config.load()
        assert cfg.pg_schema == schema


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
    """Sanity that the pool config is surfaced — easy to drop fields
    accidentally when refactoring."""
    _set_env(monkeypatch)
    cfg = icebox_config.load()
    assert cfg.psycopg_pool_min <= cfg.psycopg_pool_max
