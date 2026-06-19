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
        assert cfg.auto_offset_reset == "earliest"
        # PostHog Logs export: off by default.
        assert cfg.posthog_project_token is None
        assert cfg.posthog_logs_endpoint == "https://us.i.posthog.com/i/v1/logs"
        assert cfg.service_namespace == "millpond"
        assert cfg.service_instance_id is None

    def test_posthog_otlp_env_overrides(self, monkeypatch):
        monkeypatch.setenv("POSTHOG_PROJECT_TOKEN", "phc_test")
        monkeypatch.setenv("POSTHOG_LOGS_ENDPOINT", "https://eu.i.posthog.com/i/v1/logs")
        monkeypatch.setenv("MILLPOND_SERVICE_NAMESPACE", "my-ns")
        monkeypatch.setenv("MILLPOND_SERVICE_INSTANCE_ID", "events")
        cfg = load()
        assert cfg.posthog_project_token == "phc_test"
        assert cfg.posthog_logs_endpoint == "https://eu.i.posthog.com/i/v1/logs"
        assert cfg.service_namespace == "my-ns"
        assert cfg.service_instance_id == "events"

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

    def test_ducklake_schema_default_main(self):
        # When DUCKLAKE_SCHEMA is unset, fall back to DuckDB's default
        # schema so existing deployments keep writing the same path.
        cfg = load()
        assert cfg.ducklake_schema == "main"

    def test_ducklake_schema_empty_string_treated_as_main(self, monkeypatch):
        # Helm-template gotcha: unset values render as "" rather than
        # being absent. Treat empty/whitespace as fall-back to default
        # rather than raising — symmetric with DUCKLAKE_PARTITION_BY.
        monkeypatch.setenv("DUCKLAKE_SCHEMA", "")
        cfg = load()
        assert cfg.ducklake_schema == "main"

    def test_ducklake_schema_explicit_value(self, monkeypatch):
        monkeypatch.setenv("DUCKLAKE_SCHEMA", "posthog")
        cfg = load()
        assert cfg.ducklake_schema == "posthog"

    def test_ducklake_schema_unsafe_rejected(self, monkeypatch):
        monkeypatch.setenv("DUCKLAKE_SCHEMA", "posthog; DROP TABLE x")
        with pytest.raises(RuntimeError, match="DUCKLAKE_SCHEMA.*unsafe characters"):
            load()

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

    def test_auto_offset_reset_latest(self, monkeypatch):
        monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "latest")
        cfg = load()
        assert cfg.auto_offset_reset == "latest"

    def test_auto_offset_reset_is_case_and_whitespace_insensitive(self, monkeypatch):
        monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "  LATEST ")
        cfg = load()
        assert cfg.auto_offset_reset == "latest"

    def test_auto_offset_reset_invalid_rejected(self, monkeypatch):
        monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "error")
        with pytest.raises(RuntimeError, match="KAFKA_AUTO_OFFSET_RESET"):
            load()

    def test_auto_offset_reset_empty_string_rejected(self, monkeypatch):
        """Empty string fails validation rather than falling back to default.

        Fail-loud matches the rest of the validation philosophy in this file —
        an unintentionally-empty env var (e.g. unset chart value rendering as
        "") is a config bug, not a silent default trigger."""
        monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "")
        with pytest.raises(RuntimeError, match="KAFKA_AUTO_OFFSET_RESET"):
            load()

    def test_auto_offset_reset_collision_with_kafka_consumer_passthrough(self, monkeypatch):
        """KAFKA_CONSUMER_AUTO_OFFSET_RESET would silently lose to the explicit
        cfg.auto_offset_reset write in consumer.create(); refuse the ambiguity."""
        monkeypatch.setenv("KAFKA_CONSUMER_AUTO_OFFSET_RESET", "latest")
        with pytest.raises(RuntimeError, match="KAFKA_CONSUMER_AUTO_OFFSET_RESET is not honored"):
            load()

    def test_table_label(self):
        cfg = load()
        assert cfg.table_label == "events"


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

    @pytest.mark.parametrize("raw", ["iceberg", "icebox", "snowflake"])
    def test_removed_destination_raises(self, raw, monkeypatch):
        # The iceberg/icebox sinks were removed (tag: final-iceberg). A pod
        # deployed with stale config must fail at startup, loudly.
        monkeypatch.setenv("MILLPOND_DESTINATION", raw)
        with pytest.raises(RuntimeError, match="MILLPOND_DESTINATION"):
            load()

    @pytest.mark.parametrize("raw", ["DUCKLAKE", "DuckLake", "ducklake", "DuckLAKE", "  ducklake  "])
    def test_ducklake_destination_accepted_any_casing(self, raw, monkeypatch):
        # `.strip().lower()` in load() accepts any casing/whitespace
        # operators might helm-template in.
        monkeypatch.setenv("MILLPOND_DESTINATION", raw)
        cfg = load()
        assert cfg.table_label == "events"

    @pytest.mark.parametrize("raw", ["", "   "])
    def test_destination_empty_string_defaults_to_ducklake(self, raw, monkeypatch):
        # Common helm-template gotcha: unset variable renders as "" rather
        # than being absent. Treat empty/whitespace as fall-back to default.
        monkeypatch.setenv("MILLPOND_DESTINATION", raw)
        cfg = load()
        assert cfg.table_label == "events"

    def test_stray_iceberg_env_vars_ignored(self, monkeypatch):
        # Stray ICEBERG_* from a pre-removal deployment should not affect
        # a DuckLake deployment.
        monkeypatch.setenv("ICEBERG_TABLE", "ice_events")
        monkeypatch.setenv("ICEBERG_NAMESPACE", "millpond")
        cfg = load()
        assert cfg.table_label == "events"


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
