import pytest

from millpond.config import _parse_ordinal, load


class TestParseOrdinal:
    def test_standard_statefulset(self):
        assert _parse_ordinal("millpond-events-3") == 3

    def test_zero(self):
        assert _parse_ordinal("millpond-events-0") == 0

    def test_docker_compose(self):
        assert _parse_ordinal("millpond-test-1") == 1

    def test_multi_digit(self):
        assert _parse_ordinal("millpond-events-42") == 42

    def test_no_ordinal(self):
        with pytest.raises(ValueError, match="Cannot parse ordinal"):
            _parse_ordinal("millpond-events")

    def test_empty(self):
        with pytest.raises(ValueError, match="Cannot parse ordinal"):
            _parse_ordinal("")


class TestLoad:
    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_TOPIC", "test-topic")
        monkeypatch.setenv("REPLICA_COUNT", "4")
        monkeypatch.setenv("POD_NAME", "millpond-events-2")
        monkeypatch.setenv("DUCKLAKE_TABLE", "events")
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "s3://bucket/data")
        monkeypatch.setenv("DUCKLAKE_RDS_HOST", "host")
        monkeypatch.setenv("DUCKLAKE_RDS_PASSWORD", "pass")
        monkeypatch.setenv("DUCKLAKE_CONNECTION", ":memory:")

    def test_loads(self):
        cfg = load()
        assert cfg.topic == "test-topic"
        assert cfg.ordinal == 2
        assert cfg.replica_count == 4
        assert cfg.group_id == "millpond-test-topic-events"

    def test_custom_group_id(self, monkeypatch):
        monkeypatch.setenv("GROUP_ID", "custom-group")
        cfg = load()
        assert cfg.group_id == "custom-group"

    def test_defaults(self):
        cfg = load()
        assert cfg.flush_size == 104857600
        assert cfg.flush_interval_ms == 60000
        assert cfg.fetch_min_bytes == 1048576
        assert cfg.fetch_max_wait_ms == 500
        assert cfg.consume_batch_size == 1000
        assert cfg.stats_interval_ms == 5000

    def test_ordinal_exceeds_replica_count(self, monkeypatch):
        monkeypatch.setenv("POD_NAME", "millpond-events-5")
        with pytest.raises(RuntimeError, match="Ordinal 5 >= REPLICA_COUNT 4"):
            load()

    def test_missing_required(self, monkeypatch):
        monkeypatch.delenv("KAFKA_TOPIC")
        with pytest.raises(RuntimeError, match="KAFKA_TOPIC"):
            load()

    def test_unsafe_table_name_rejected(self, monkeypatch):
        monkeypatch.setenv("DUCKLAKE_TABLE", "events; DROP TABLE x")
        with pytest.raises(RuntimeError, match="unsafe characters"):
            load()

    def test_table_name_with_sql_injection(self, monkeypatch):
        monkeypatch.setenv("DUCKLAKE_TABLE", "x--")
        with pytest.raises(RuntimeError, match="unsafe characters"):
            load()

    def test_valid_table_names(self, monkeypatch):
        for name in ["events", "my_table", "_private", "Events123"]:
            monkeypatch.setenv("DUCKLAKE_TABLE", name)
            cfg = load()
            assert cfg.ducklake_table == name

    def test_partition_by_default_none(self):
        cfg = load()
        assert cfg.partition_by is None

    def test_partition_by_set(self, monkeypatch):
        monkeypatch.setenv("DUCKLAKE_PARTITION_BY", "year(timestamp),month(timestamp)")
        cfg = load()
        assert cfg.partition_by == "year(timestamp),month(timestamp)"

    def test_partition_by_valid_expressions(self, monkeypatch):
        for expr in [
            "region",
            "year(ts)",
            "year(ts),month(ts),day(ts),hour(ts)",
            "year(created_at), month(created_at)",
            "team_id,year(timestamp)",
        ]:
            monkeypatch.setenv("DUCKLAKE_PARTITION_BY", expr)
            cfg = load()
            assert cfg.partition_by == expr

    def test_partition_by_sql_injection_rejected(self, monkeypatch):
        monkeypatch.setenv("DUCKLAKE_PARTITION_BY", "year(ts); DROP TABLE x")
        with pytest.raises(RuntimeError, match="unsafe"):
            load()

    def test_partition_by_empty_string_treated_as_none(self, monkeypatch):
        monkeypatch.setenv("DUCKLAKE_PARTITION_BY", "")
        cfg = load()
        assert cfg.partition_by is None

    def test_kafka_consumer_overrides(self, monkeypatch):
        monkeypatch.setenv("KAFKA_CONSUMER_SECURITY_PROTOCOL", "SASL_SSL")
        monkeypatch.setenv("KAFKA_CONSUMER_SASL_MECHANISMS", "PLAIN")
        monkeypatch.setenv("KAFKA_CONSUMER_SASL_USERNAME", "user1")
        cfg = load()
        overrides = dict(cfg.kafka_config_overrides)
        assert overrides["security.protocol"] == "SASL_SSL"
        assert overrides["sasl.mechanisms"] == "PLAIN"
        assert overrides["sasl.username"] == "user1"

    def test_kafka_consumer_overrides_default_empty(self):
        cfg = load()
        assert cfg.kafka_config_overrides == ()

    def test_destination_defaults_to_ducklake(self):
        cfg = load()
        assert cfg.destination == "ducklake"
        assert cfg.table_label == "events"

    def test_iceberg_fields_none_when_destination_ducklake(self):
        cfg = load()
        assert cfg.iceberg_catalog_uri is None
        assert cfg.iceberg_namespace is None
        assert cfg.s3_access_key_id is None


class TestLoadIcebergDestination:
    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        # Strip any DUCKLAKE_* so a leak doesn't make this test pass by accident.
        for key in list(__import__("os").environ):
            if key.startswith("DUCKLAKE_"):
                monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_TOPIC", "test-topic")
        monkeypatch.setenv("REPLICA_COUNT", "4")
        monkeypatch.setenv("POD_NAME", "millpond-events-2")
        monkeypatch.setenv("MILLPOND_DESTINATION", "iceberg")
        monkeypatch.setenv("ICEBERG_TABLE", "events")
        monkeypatch.setenv("ICEBERG_NAMESPACE", "millpond")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://warehouse/")
        monkeypatch.setenv("MILLPOND_S3_ACCESS_KEY_ID", "akid")
        monkeypatch.setenv("MILLPOND_S3_SECRET_ACCESS_KEY", "secret")
        monkeypatch.setenv("MILLPOND_S3_REGION", "us-east-1")

    def test_loads_iceberg(self, monkeypatch):
        # Use a distinct iceberg_table name from the KAFKA_TOPIC so we can
        # actually prove dispatch — not just rely on both happening to be
        # "events". (Prior version of this test was a false-positive.)
        monkeypatch.setenv("ICEBERG_TABLE", "ice_events")
        cfg = load()
        assert cfg.destination == "iceberg"
        assert cfg.iceberg_table == "ice_events"
        assert cfg.iceberg_namespace == "millpond"
        assert cfg.iceberg_catalog_uri == "http://catalog:8181"
        assert cfg.iceberg_warehouse == "s3://warehouse/"
        assert cfg.s3_access_key_id == "akid"
        assert cfg.table_label == "millpond.ice_events"

    def test_default_group_id_uses_iceberg_table(self, monkeypatch):
        # Distinct table name so the assertion proves we're reading
        # iceberg_table (not ducklake_table or KAFKA_TOPIC).
        monkeypatch.setenv("ICEBERG_TABLE", "ice_events")
        cfg = load()
        assert cfg.group_id == "millpond-test-topic-ice_events"

    @pytest.mark.parametrize("raw", ["ICEBERG", "Iceberg", "iceberg", "IceBERG"])
    def test_iceberg_destination_is_case_insensitive(self, raw, monkeypatch):
        monkeypatch.setenv("MILLPOND_DESTINATION", raw)
        cfg = load()
        assert cfg.destination == "iceberg"

    def test_ducklake_fields_none_when_destination_iceberg(self):
        cfg = load()
        assert cfg.ducklake_table is None
        assert cfg.rds_host is None
        assert cfg.partition_by is None

    def test_stray_ducklake_env_vars_are_ignored(self, monkeypatch):
        # An operator who flipped MILLPOND_DESTINATION to iceberg may still
        # have DUCKLAKE_* set from the prior config. Those should be silently
        # ignored, not validated, not pulled into Config.
        monkeypatch.setenv("DUCKLAKE_TABLE", "leftover")
        monkeypatch.setenv("DUCKLAKE_RDS_PASSWORD", "leftover")
        cfg = load()
        assert cfg.destination == "iceberg"
        assert cfg.ducklake_table is None  # ignored, not propagated

    def test_optional_s3_endpoint_and_token(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_S3_ENDPOINT", "http://minio:9000")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "bearer-xyz")
        cfg = load()
        assert cfg.s3_endpoint == "http://minio:9000"
        assert cfg.iceberg_catalog_token == "bearer-xyz"

    @pytest.mark.parametrize(
        "missing_var",
        [
            "ICEBERG_CATALOG_URI",
            "ICEBERG_WAREHOUSE",
            "ICEBERG_NAMESPACE",
            "ICEBERG_TABLE",
            "MILLPOND_S3_ACCESS_KEY_ID",
            "MILLPOND_S3_SECRET_ACCESS_KEY",
            "MILLPOND_S3_REGION",
        ],
    )
    def test_missing_iceberg_required_field_raises(self, missing_var, monkeypatch):
        monkeypatch.delenv(missing_var)
        with pytest.raises(RuntimeError, match=missing_var):
            load()

    def test_unsafe_iceberg_table_name_rejected(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_TABLE", "events; DROP TABLE x")
        with pytest.raises(RuntimeError, match="unsafe characters"):
            load()

    def test_unsafe_iceberg_namespace_rejected(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_NAMESPACE", "evil-ns")
        with pytest.raises(RuntimeError, match="unsafe characters"):
            load()


class TestLoadDestinationValidation:
    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_TOPIC", "test-topic")
        monkeypatch.setenv("REPLICA_COUNT", "4")
        monkeypatch.setenv("POD_NAME", "millpond-events-2")
        monkeypatch.setenv("DUCKLAKE_TABLE", "events")
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "s3://bucket/data")
        monkeypatch.setenv("DUCKLAKE_RDS_HOST", "host")
        monkeypatch.setenv("DUCKLAKE_RDS_PASSWORD", "pass")
        monkeypatch.setenv("DUCKLAKE_CONNECTION", ":memory:")

    def test_unknown_destination_raises(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_DESTINATION", "snowflake")
        with pytest.raises(RuntimeError, match="MILLPOND_DESTINATION"):
            load()

    @pytest.mark.parametrize("raw", ["DUCKLAKE", "DuckLake", "ducklake", "DuckLAKE"])
    def test_ducklake_destination_is_case_insensitive(self, raw, monkeypatch):
        # `.lower()` in load() accepts any casing operators might helm-template in.
        monkeypatch.setenv("MILLPOND_DESTINATION", raw)
        cfg = load()
        assert cfg.destination == "ducklake"

    def test_destination_whitespace_tolerated(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_DESTINATION", "  ducklake  ")
        cfg = load()
        assert cfg.destination == "ducklake"

    def test_destination_empty_string_defaults_to_ducklake(self, monkeypatch):
        # Common helm-template gotcha: unset variable renders as "" rather
        # than being absent. Treat empty/whitespace as fall-back to default.
        monkeypatch.setenv("MILLPOND_DESTINATION", "")
        cfg = load()
        assert cfg.destination == "ducklake"

    def test_destination_only_whitespace_defaults_to_ducklake(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_DESTINATION", "   ")
        cfg = load()
        assert cfg.destination == "ducklake"

    def test_stray_iceberg_env_vars_ignored_when_destination_ducklake(self, monkeypatch):
        # Symmetric to the iceberg-side test: stray ICEBERG_* should not
        # affect a DuckLake deployment.
        monkeypatch.setenv("ICEBERG_TABLE", "ice_events")
        monkeypatch.setenv("ICEBERG_NAMESPACE", "millpond")
        cfg = load()
        assert cfg.destination == "ducklake"


class TestFilterConfig:
    """MILLPOND_FILTER_{KEEP,DROP}_FIELD_NAME + MILLPOND_FILTER_VALUES."""

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        # Minimal valid DuckLake config — the filter feature is destination-agnostic.
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_TOPIC", "test-topic")
        monkeypatch.setenv("REPLICA_COUNT", "1")
        monkeypatch.setenv("POD_NAME", "millpond-events-0")
        monkeypatch.setenv("DUCKLAKE_TABLE", "events")
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "s3://bucket/data")
        monkeypatch.setenv("DUCKLAKE_RDS_HOST", "host")
        monkeypatch.setenv("DUCKLAKE_RDS_PASSWORD", "pass")
        monkeypatch.setenv("DUCKLAKE_CONNECTION", ":memory:")

    def test_unset_yields_none(self):
        cfg = load()
        assert cfg.filter_keep_field is None
        assert cfg.filter_drop_field is None
        assert cfg.filter_values is None

    def test_int_allowlist_parsed_as_ints(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "2,4,1956,69")
        cfg = load()
        assert cfg.filter_keep_field == "team_id"
        assert cfg.filter_values == (2, 4, 1956, 69)
        # Homogeneous int tuple — every element must be int.
        assert all(isinstance(v, int) for v in cfg.filter_values)

    def test_string_allowlist_parsed_as_strings(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "region")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "us-east-1,us-west-2,eu-central-1")
        cfg = load()
        assert cfg.filter_keep_field == "region"
        assert cfg.filter_values == ("us-east-1", "us-west-2", "eu-central-1")

    def test_mixed_int_and_string_falls_back_to_string(self, monkeypatch):
        # Once any token fails int parsing, the *whole* tuple is strings.
        # This is the deterministic-typing contract — the apply code
        # branches on the first element's type and would mis-handle a
        # silently mixed tuple.
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "2,foo,4")
        cfg = load()
        assert cfg.filter_values == ("2", "foo", "4")

    def test_whitespace_around_values_is_trimmed(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "  2 , 4  ,1956,  69  ")
        cfg = load()
        assert cfg.filter_values == (2, 4, 1956, 69)

    def test_empty_tokens_are_dropped(self, monkeypatch):
        # Trailing commas / repeated commas are common operator slips;
        # drop empty tokens rather than fail loudly on them.
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "2,,4,")
        cfg = load()
        assert cfg.filter_values == (2, 4)

    def test_negative_ints_parsed(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "-1,0,42")
        cfg = load()
        assert cfg.filter_values == (-1, 0, 42)

    def test_keep_field_without_values_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "team_id")
        with pytest.raises(RuntimeError, match="MILLPOND_FILTER_VALUES"):
            load()

    def test_values_without_field_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "1,2,3")
        with pytest.raises(RuntimeError, match="MILLPOND_FILTER_VALUES"):
            load()

    def test_keep_and_drop_mutually_exclusive(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_DROP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "1,2")
        with pytest.raises(RuntimeError, match="mutually exclusive"):
            load()

    def test_drop_direction_rejected_until_implemented(self, monkeypatch):
        # Reserved namespace: drop is parsed and validated but explicitly
        # refused so an operator setting it today gets a clear startup
        # error rather than silently no-filtering.
        monkeypatch.setenv("MILLPOND_FILTER_DROP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "1,2")
        with pytest.raises(RuntimeError, match="reserved for a future release"):
            load()

    def test_unsafe_field_name_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "team_id; DROP TABLE x")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "1")
        with pytest.raises(RuntimeError, match="unsafe characters"):
            load()

    def test_whitespace_only_field_name_treated_as_unset(self, monkeypatch):
        # Symmetric with how DESTINATION handles whitespace — operator
        # rendering glitches shouldn't enable a filter accidentally.
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "   ")
        cfg = load()
        assert cfg.filter_keep_field is None
        assert cfg.filter_values is None

    def test_values_only_whitespace_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "   ")
        with pytest.raises(RuntimeError, match="MILLPOND_FILTER_VALUES"):
            load()


class TestSortByConfig:
    """MILLPOND_SORT_BY parsing and validation."""

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        # Minimal valid DuckLake config — sort is destination-agnostic.
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_TOPIC", "test-topic")
        monkeypatch.setenv("REPLICA_COUNT", "1")
        monkeypatch.setenv("POD_NAME", "millpond-events-0")
        monkeypatch.setenv("DUCKLAKE_TABLE", "events")
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "s3://bucket/data")
        monkeypatch.setenv("DUCKLAKE_RDS_HOST", "host")
        monkeypatch.setenv("DUCKLAKE_RDS_PASSWORD", "pass")
        monkeypatch.setenv("DUCKLAKE_CONNECTION", ":memory:")

    def test_unset_yields_none(self):
        cfg = load()
        assert cfg.sort_by is None

    def test_single_field(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_SORT_BY", "team_id")
        cfg = load()
        assert cfg.sort_by == ("team_id",)

    def test_multi_field_order_preserved(self, monkeypatch):
        # Sort order is determined by tuple position, so the parsing must
        # preserve the operator-specified order verbatim.
        monkeypatch.setenv("MILLPOND_SORT_BY", "team_id,timestamp,distinct_id")
        cfg = load()
        assert cfg.sort_by == ("team_id", "timestamp", "distinct_id")

    def test_whitespace_around_fields_trimmed(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_SORT_BY", "  team_id ,  timestamp  ")
        cfg = load()
        assert cfg.sort_by == ("team_id", "timestamp")

    def test_empty_tokens_dropped(self, monkeypatch):
        # Trailing commas / doubled commas are common operator slips —
        # drop them rather than fail loudly.
        monkeypatch.setenv("MILLPOND_SORT_BY", "team_id,,timestamp,")
        cfg = load()
        assert cfg.sort_by == ("team_id", "timestamp")

    def test_whitespace_only_value_yields_none(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_SORT_BY", "   ")
        cfg = load()
        assert cfg.sort_by is None

    def test_unsafe_field_name_rejected(self, monkeypatch):
        # Identifier safety pattern is enforced at config-load — keeps a
        # SQL-injection-flavoured config from surfacing only under load.
        monkeypatch.setenv("MILLPOND_SORT_BY", "team_id, foo; DROP TABLE x")
        with pytest.raises(RuntimeError, match="unsafe characters"):
            load()

    def test_log_says_ascending(self, monkeypatch, caplog):
        # The log line should make the (currently fixed) direction
        # explicit so operators understand what they configured.
        import logging

        monkeypatch.setenv("MILLPOND_SORT_BY", "team_id,timestamp")
        with caplog.at_level(logging.INFO, logger="millpond.config"):
            load()
        msgs = [r.message for r in caplog.records]
        assert any("Sort by: team_id, timestamp (ascending)" in m for m in msgs)
