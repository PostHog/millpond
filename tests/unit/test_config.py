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

    def test_http_port_default(self):
        # The health/metrics port lives on Config with every other knob —
        # server.start() no longer reads the environment behind config's back.
        assert load().http_port == 8000

    def test_http_port_env_override(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_HTTP_PORT", "28000")
        assert load().http_port == 28000

    def test_http_port_non_numeric_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_HTTP_PORT", "eight thousand")
        with pytest.raises((RuntimeError, ValueError)):
            load()

    def test_group_id_default_unchanged_for_ducklake(self):
        # Deliberately NOT prefixed with the destination: every deployed
        # DuckLake pipeline stores its offsets under this exact group id,
        # and changing it would replay the whole retention window.
        assert load().group_id == "millpond-test-topic-events"

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

    def test_ducklake_max_retry_count_default_100(self):
        # DuckLake's own default is 10; we bump to 100 to absorb
        # snapshot-id allocation contention from multi-writer deployments
        # (see PR description). Lock the default so a future env-loader
        # refactor doesn't silently regress concurrency behaviour.
        cfg = load()
        assert cfg.ducklake_max_retry_count == 100

    def test_ducklake_max_retry_count_explicit_value(self, monkeypatch):
        monkeypatch.setenv("DUCKLAKE_MAX_RETRY_COUNT", "250")
        cfg = load()
        assert cfg.ducklake_max_retry_count == 250

    def test_ducklake_max_retry_count_zero_rejected(self, monkeypatch):
        # 0 disables retries entirely — under multi-writer load it
        # degenerates straight to the PK-collision failure mode we're
        # raising the default to avoid. Refuse loudly rather than accept.
        monkeypatch.setenv("DUCKLAKE_MAX_RETRY_COUNT", "0")
        with pytest.raises(RuntimeError, match="DUCKLAKE_MAX_RETRY_COUNT.*positive integer"):
            load()

    def test_ducklake_max_retry_count_negative_rejected(self, monkeypatch):
        monkeypatch.setenv("DUCKLAKE_MAX_RETRY_COUNT", "-5")
        with pytest.raises(RuntimeError, match="DUCKLAKE_MAX_RETRY_COUNT.*positive integer"):
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

    def test_drop_alone_parsed(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_DROP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_DROP_VALUES", "47074")
        cfg = load()
        assert cfg.filter_drop_field == "team_id"
        assert cfg.filter_drop_values == (47074,)
        assert cfg.filter_keep_field is None
        assert cfg.filter_values is None

    def test_keep_and_drop_compose(self, monkeypatch):
        # The load-shedding case this exists for: CP-driven keep-filter
        # stays authoritative while an operator blacklist subtracts one
        # tenant. Each direction pairs with its OWN values var.
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "1,2,47074")
        monkeypatch.setenv("MILLPOND_FILTER_DROP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_DROP_VALUES", "47074")
        cfg = load()
        assert cfg.filter_keep_field == "team_id"
        assert cfg.filter_values == (1, 2, 47074)
        assert cfg.filter_drop_field == "team_id"
        assert cfg.filter_drop_values == (47074,)

    def test_drop_field_without_drop_values_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_DROP_FIELD_NAME", "team_id")
        with pytest.raises(RuntimeError, match="MILLPOND_FILTER_DROP_VALUES"):
            load()

    def test_drop_values_without_drop_field_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_DROP_VALUES", "1,2")
        with pytest.raises(RuntimeError, match="MILLPOND_FILTER_DROP_VALUES"):
            load()

    def test_drop_does_not_pair_with_keep_values(self, monkeypatch):
        # The original reservation shared MILLPOND_FILTER_VALUES; the
        # implementation deliberately does not — a drop field with only
        # the keep-values var set is half-configured and must refuse.
        monkeypatch.setenv("MILLPOND_FILTER_DROP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "1,2")
        with pytest.raises(RuntimeError, match="MILLPOND_FILTER"):
            load()

    def test_unsafe_drop_field_name_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_FILTER_DROP_FIELD_NAME", "team_id; DROP TABLE x")
        monkeypatch.setenv("MILLPOND_FILTER_DROP_VALUES", "1")
        with pytest.raises(RuntimeError, match="unsafe characters"):
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


class TestTypedColumnsConfig:
    """MILLPOND_TYPED_COLUMNS parsing and validation."""

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
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
        assert cfg.typed_columns is None

    def test_single_pair(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", "timestamp:timestamptz")
        cfg = load()
        assert cfg.typed_columns == (("timestamp", "timestamptz"),)

    def test_events_column_set_with_project_id(self, monkeypatch):
        # The full re-point set: 8 TIMESTAMPTZ columns + project_id BIGINT.
        spec = "timestamp:timestamptz,created_at:timestamptz,project_id:bigint"
        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", spec)
        cfg = load()
        assert cfg.typed_columns == (
            ("timestamp", "timestamptz"),
            ("created_at", "timestamptz"),
            ("project_id", "bigint"),
        )

    def test_type_name_lowercased_and_whitespace_trimmed(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", " timestamp : TIMESTAMPTZ , , project_id:BigInt ,")
        cfg = load()
        assert cfg.typed_columns == (("timestamp", "timestamptz"), ("project_id", "bigint"))

    def test_order_preserved(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", "b:bigint,a:timestamptz")
        cfg = load()
        assert cfg.typed_columns == (("b", "bigint"), ("a", "timestamptz"))

    def test_duplicate_same_type_deduped(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", "timestamp:timestamptz,timestamp:timestamptz")
        cfg = load()
        assert cfg.typed_columns == (("timestamp", "timestamptz"),)

    def test_duplicate_conflicting_type_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", "x:bigint,x:timestamptz")
        with pytest.raises(RuntimeError, match="both"):
            load()

    def test_whitespace_only_value_yields_none(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", "   ")
        cfg = load()
        assert cfg.typed_columns is None

    def test_missing_colon_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", "timestamp")
        with pytest.raises(RuntimeError, match="must be 'column:type'"):
            load()

    def test_unknown_type_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", "timestamp:datetime")
        with pytest.raises(RuntimeError, match="must be one of"):
            load()

    def test_unsafe_column_name_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", "foo; DROP TABLE x:bigint")
        with pytest.raises(RuntimeError, match="unsafe characters"):
            load()

    def test_log_lists_pairs(self, monkeypatch, caplog):
        import logging

        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", "timestamp:timestamptz,project_id:bigint")
        with caplog.at_level(logging.INFO, logger="millpond.config"):
            load()
        msgs = [r.message for r in caplog.records]
        assert any("Coerce typed columns: timestamp:timestamptz, project_id:bigint" in m for m in msgs)


class TestVariantColumnsConfig:
    """MILLPOND_VARIANT_COLUMNS — dual-write JSON/VARCHAR sources as VARIANT."""

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
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
        assert cfg.variant_columns is None

    def test_single_column(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_VARIANT_COLUMNS", "properties")
        cfg = load()
        assert cfg.variant_columns == ("properties",)

    def test_multiple_columns_order_preserved(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_VARIANT_COLUMNS", "properties,person_properties")
        cfg = load()
        assert cfg.variant_columns == ("properties", "person_properties")

    def test_whitespace_and_dedup(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_VARIANT_COLUMNS", " properties , , properties , person_properties ")
        cfg = load()
        assert cfg.variant_columns == ("properties", "person_properties")

    def test_whitespace_only_yields_none(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_VARIANT_COLUMNS", "   ")
        cfg = load()
        assert cfg.variant_columns is None

    def test_unsafe_name_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_VARIANT_COLUMNS", "foo; DROP")
        with pytest.raises(RuntimeError, match="unsafe characters"):
            load()

    def test_suffix_name_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_VARIANT_COLUMNS", "properties_variant")
        with pytest.raises(RuntimeError, match="already ends with"):
            load()

    def test_suffix_name_rejected_case_insensitive(self, monkeypatch):
        # DuckDB identifiers are case-insensitive, so properties_VARIANT names
        # the same sink column — the misconfiguration guard must still fire.
        monkeypatch.setenv("MILLPOND_VARIANT_COLUMNS", "properties_VARIANT")
        with pytest.raises(RuntimeError, match="already ends with"):
            load()

    def test_log_lists_mappings(self, monkeypatch, caplog):
        import logging

        monkeypatch.setenv("MILLPOND_VARIANT_COLUMNS", "properties,person_properties")
        with caplog.at_level(logging.INFO, logger="millpond.config"):
            load()
        msgs = [r.message for r in caplog.records]
        assert any(
            "Dual-write VARIANT columns: properties -> properties_variant, "
            "person_properties -> person_properties_variant" in m
            for m in msgs
        )


class TestIncludeValuesConfig:
    """MILLPOND_INCLUDE_VALUES_* — the dynamic include-set source knobs and
    their validation against the static filter config."""

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_TOPIC", "test-topic")
        monkeypatch.setenv("REPLICA_COUNT", "1")
        monkeypatch.setenv("POD_NAME", "millpond-events-0")
        monkeypatch.setenv("DUCKLAKE_TABLE", "events")
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "s3://bucket/data")
        monkeypatch.setenv("DUCKLAKE_RDS_HOST", "host")
        monkeypatch.setenv("DUCKLAKE_RDS_PASSWORD", "pass")
        monkeypatch.setenv("DUCKLAKE_CONNECTION", ":memory:")
        monkeypatch.setenv("MILLPOND_FILTER_KEEP_FIELD_NAME", "team_id")
        monkeypatch.setenv("MILLPOND_FILTER_VALUES", "2,50689")

    def test_unset_url_yields_static_only_defaults(self):
        cfg = load()
        assert cfg.include_values_url is None
        assert cfg.include_values_mode == "static"

    def test_mode_without_url_rejected(self, monkeypatch):
        # A set MODE means a dynamic source was intended; a typo'd/missing
        # URL must not silently degrade to static-only.
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_MODE", "authoritative")
        with pytest.raises(RuntimeError, match="MODE is set but"):
            load()

    def test_timeout_knobs(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_URL", "http://cp/values")
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_REQUEST_TIMEOUT_S", "5")
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_STARTUP_TIMEOUT_S", "120")
        cfg = load()
        assert cfg.include_values_request_timeout_s == 5.0
        assert cfg.include_values_startup_timeout_s == 120.0

    def test_non_numeric_knob_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_URL", "http://cp/values")
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_POLL_INTERVAL_S", "soon")
        with pytest.raises(RuntimeError, match="must be a number"):
            load()

    def test_keep_filter_chain_guarantees_bootstrap(self, monkeypatch):
        # The include-values source's no-bootstrap path must stay
        # unreachable through config: URL requires the keep field, and the
        # keep field requires non-empty static values. If this chain is
        # ever loosened, the empty-seed refusal in include_values.py
        # becomes the only guard — see that module's docstring.
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_URL", "http://cp/values")
        cfg = load()
        assert cfg.filter_values, "static filter_values must be non-empty whenever a URL is configured"

    def test_url_with_defaults(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_URL", "http://cp/values")
        cfg = load()
        assert cfg.include_values_url == "http://cp/values"
        assert cfg.include_values_mode == "shadow"
        assert cfg.include_values_poll_interval_s == 60.0
        assert cfg.include_values_removal_polls == 5

    def test_authoritative_mode(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_URL", "http://cp/values")
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_MODE", "authoritative")
        assert load().include_values_mode == "authoritative"

    def test_bad_mode_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_URL", "http://cp/values")
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_MODE", "yolo")
        with pytest.raises(RuntimeError, match="MODE"):
            load()

    def test_url_requires_keep_filter(self, monkeypatch):
        monkeypatch.delenv("MILLPOND_FILTER_KEEP_FIELD_NAME")
        monkeypatch.delenv("MILLPOND_FILTER_VALUES")
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_URL", "http://cp/values")
        with pytest.raises(RuntimeError, match="FILTER_KEEP_FIELD_NAME"):
            load()

    def test_auth_header_requires_both_parts(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_URL", "http://cp/values")
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_AUTH_HEADER_NAME", "X-Secret")
        with pytest.raises(RuntimeError, match="set together"):
            load()

    def test_auth_without_url_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_AUTH_TOKEN", "tok")
        with pytest.raises(RuntimeError, match="requires MILLPOND_INCLUDE_VALUES_URL"):
            load()

    def test_nonpositive_interval_rejected(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_URL", "http://cp/values")
        monkeypatch.setenv("MILLPOND_INCLUDE_VALUES_POLL_INTERVAL_S", "0")
        with pytest.raises(RuntimeError, match="must be positive"):
            load()


class TestDuckdbMemoryLimit:
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

    def test_default_none(self):
        cfg = load()
        assert cfg.duckdb_memory_limit is None

    def test_explicit_value(self, monkeypatch):
        monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "6GB")
        cfg = load()
        assert cfg.duckdb_memory_limit == "6GB"

    def test_empty_string_treated_as_unset(self, monkeypatch):
        # A chart rendering `value: ""` must not produce SET memory_limit = ''.
        monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "")
        cfg = load()
        assert cfg.duckdb_memory_limit is None


class TestHoglakeConfig:
    """destination=hoglake config block: required fields, identifier rules,
    partition-expression mapping. The DuckLake block must NOT be required
    when hoglake is selected (and vice versa: stray HOGLAKE_* vars must
    not affect a ducklake deployment)."""

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_TOPIC", "test-topic")
        monkeypatch.setenv("REPLICA_COUNT", "4")
        monkeypatch.setenv("POD_NAME", "millpond-events-2")
        monkeypatch.setenv("MILLPOND_DESTINATION", "hoglake")
        monkeypatch.setenv("HOGLAKE_URL", "http://localhost:28080")
        monkeypatch.setenv("HOGLAKE_CATALOG", "millpond")
        monkeypatch.setenv("HOGLAKE_NAMESPACE", "analytics")
        monkeypatch.setenv("HOGLAKE_TABLE", "events")
        monkeypatch.setenv("HOGLAKE_S3_ACCESS_KEY", "ak")
        monkeypatch.setenv("HOGLAKE_S3_SECRET_KEY", "sk")

    def test_loads_without_any_ducklake_vars(self):
        cfg = load()
        assert cfg.destination == "hoglake"
        assert cfg.hoglake_url == "http://localhost:28080"
        assert cfg.hoglake_catalog == "millpond"
        assert cfg.hoglake_namespace == "analytics"
        assert cfg.hoglake_table == "events"
        assert cfg.hoglake_s3_access_key == "ak"
        assert cfg.hoglake_s3_secret_key == "sk"
        # DuckLake block untouched and unrequired.
        assert cfg.ducklake_table is None
        assert cfg.rds_host is None
        assert cfg.partition_by is None

    def test_table_label_and_group_id_use_hoglake_table(self):
        # The destination is part of the default group id on the hoglake
        # side: a shadow deployment (same topic, same table name, other
        # destination) must not share an offset namespace with the
        # DuckLake pipeline it shadows — they would split the partitions
        # between them and each would write half the rows.
        cfg = load()
        assert cfg.table_label == "events"
        assert cfg.group_id == "millpond-hoglake-test-topic-events"

    def test_optionals_default_to_none(self):
        cfg = load()
        assert cfg.hoglake_data_path is None
        assert cfg.hoglake_s3_endpoint is None
        assert cfg.hoglake_s3_region is None
        assert cfg.hoglake_partition_by is None

    def test_retry_and_timeout_defaults(self):
        # The pair bounds the worst case a single flush can spend inside
        # sink.write(); see the comment in _load_hoglake_fields.
        cfg = load()
        assert cfg.hoglake_max_retry_count == 8
        # 45, not pyhoglake's 30: the server's commit-lock admission
        # bound is 30s, so an equal client timeout gave up at the same
        # instant the server would have answered 503 + Retry-After, and
        # its explicit backpressure signal surfaced as a
        # transport-uncertain failure instead.
        assert cfg.hoglake_request_timeout_s == 45.0

    def test_a_retry_budget_that_outlives_liveness_is_refused(self, monkeypatch):
        # HOGLAKE_MAX_RETRY_COUNT was unbounded while its interaction
        # with the liveness deadline lived in a comment — so the
        # documented trap was one values-file edit away, and springing it
        # looks like a pod SIGKILLed mid-flush with nothing in its logs.
        monkeypatch.setenv("HOGLAKE_MAX_RETRY_COUNT", "40")
        with pytest.raises(RuntimeError, match="liveness deadline"):
            load()

    def test_a_long_timeout_with_few_retries_is_also_refused(self, monkeypatch):
        # It is the PRODUCT that matters, not either knob alone.
        monkeypatch.setenv("HOGLAKE_MAX_RETRY_COUNT", "4")
        monkeypatch.setenv("HOGLAKE_REQUEST_TIMEOUT_S", "200")
        with pytest.raises(RuntimeError, match="liveness deadline"):
            load()

    def test_the_defaults_fit_inside_the_liveness_budget(self):
        from millpond.config import _LIVENESS_BUDGET_S, _hoglake_worst_case_flush_s

        cfg = load()
        worst = _hoglake_worst_case_flush_s(cfg.hoglake_max_retry_count, cfg.hoglake_request_timeout_s)
        assert worst <= _LIVENESS_BUDGET_S

    @pytest.mark.parametrize(
        "value",
        [
            "/bucket/millpond/",
            "bucket/millpond/",
            "https://bucket.s3.amazonaws.com/millpond/",
            "s3:/bucket/millpond/",
            "s3://",
        ],
    )
    def test_a_data_path_that_is_not_an_s3_uri_is_refused(self, value, monkeypatch):
        # Frozen into the catalog row at creation, and hoglake has no
        # route to delete a catalog: a typo mints a permanently unusable
        # catalog under a name nobody can reuse.
        monkeypatch.setenv("HOGLAKE_DATA_PATH", value)
        with pytest.raises(RuntimeError, match="HOGLAKE_DATA_PATH"):
            load()

    @pytest.mark.parametrize(
        "value",
        ["s3://bucket", "s3://bucket/", "s3://bucket/millpond/", "s3://my-lake.prod/a/b/c"],
    )
    def test_valid_s3_data_paths_are_accepted(self, value, monkeypatch):
        monkeypatch.setenv("HOGLAKE_DATA_PATH", value)
        assert load().hoglake_data_path == value

    def test_retry_and_timeout_overrides(self, monkeypatch):
        monkeypatch.setenv("HOGLAKE_MAX_RETRY_COUNT", "3")
        monkeypatch.setenv("HOGLAKE_REQUEST_TIMEOUT_S", "5.5")
        cfg = load()
        assert cfg.hoglake_max_retry_count == 3
        assert cfg.hoglake_request_timeout_s == 5.5

    @pytest.mark.parametrize(
        ("var", "value"),
        [
            ("HOGLAKE_MAX_RETRY_COUNT", "0"),
            ("HOGLAKE_MAX_RETRY_COUNT", "-1"),
            ("HOGLAKE_MAX_RETRY_COUNT", "lots"),
            ("HOGLAKE_REQUEST_TIMEOUT_S", "0"),
            ("HOGLAKE_REQUEST_TIMEOUT_S", "soon"),
        ],
    )
    def test_nonsense_retry_knobs_refused(self, var, value, monkeypatch):
        monkeypatch.setenv(var, value)
        with pytest.raises(RuntimeError, match=var):
            load()

    def test_retry_knobs_nulled_for_ducklake(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_DESTINATION", "ducklake")
        monkeypatch.setenv("DUCKLAKE_TABLE", "events")
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "s3://bucket/data")
        monkeypatch.setenv("DUCKLAKE_RDS_HOST", "host")
        monkeypatch.setenv("DUCKLAKE_RDS_PASSWORD", "pass")
        monkeypatch.setenv("DUCKLAKE_CONNECTION", ":memory:")
        monkeypatch.setenv("HOGLAKE_MAX_RETRY_COUNT", "99")
        cfg = load()
        assert cfg.hoglake_max_retry_count is None
        assert cfg.hoglake_request_timeout_s is None

    def test_optionals_pass_through(self, monkeypatch):
        monkeypatch.setenv("HOGLAKE_DATA_PATH", "s3://bucket/millpond/")
        monkeypatch.setenv("HOGLAKE_S3_ENDPOINT", "http://localhost:29000")
        monkeypatch.setenv("HOGLAKE_S3_REGION", "us-east-1")
        cfg = load()
        assert cfg.hoglake_data_path == "s3://bucket/millpond/"
        assert cfg.hoglake_s3_endpoint == "http://localhost:29000"
        assert cfg.hoglake_s3_region == "us-east-1"

    @pytest.mark.parametrize(
        "missing",
        [
            "HOGLAKE_URL",
            "HOGLAKE_CATALOG",
            "HOGLAKE_NAMESPACE",
            "HOGLAKE_TABLE",
            "HOGLAKE_S3_ACCESS_KEY",
            "HOGLAKE_S3_SECRET_KEY",
        ],
    )
    def test_missing_required_var_raises_naming_it(self, missing, monkeypatch):
        monkeypatch.delenv(missing)
        with pytest.raises(RuntimeError, match=missing):
            load()

    @pytest.mark.parametrize(
        "name",
        [
            "Millpond",  # catalog names must start lower-case
            "9lake",
            "bad.dot",
            "x" * 64,  # catalog max 63
        ],
    )
    def test_bad_catalog_name_rejected(self, name, monkeypatch):
        monkeypatch.setenv("HOGLAKE_CATALOG", name)
        with pytest.raises(RuntimeError, match="HOGLAKE_CATALOG"):
            load()

    @pytest.mark.parametrize("var", ["HOGLAKE_NAMESPACE", "HOGLAKE_TABLE"])
    @pytest.mark.parametrize("name", ["9abc", "a.b", "a b", "x" * 129])
    def test_bad_namespace_or_table_rejected(self, var, name, monkeypatch):
        monkeypatch.setenv(var, name)
        with pytest.raises(RuntimeError, match=var):
            load()

    def test_variant_columns_refused_for_hoglake(self, monkeypatch):
        # Hoglake has no VARIANT column type; a configured dual-write must
        # refuse at startup rather than silently no-op (mixed fleets rot
        # on silent config no-ops).
        monkeypatch.setenv("MILLPOND_VARIANT_COLUMNS", "properties")
        with pytest.raises(RuntimeError, match="MILLPOND_VARIANT_COLUMNS"):
            load()

    def test_ducklake_partitioning_without_a_hoglake_spec_refused(self, monkeypatch):
        # Flipping MILLPOND_DESTINATION on an existing values file leaves
        # DUCKLAKE_PARTITION_BY behind, and the hoglake block nulls it —
        # so the pipeline that was partitioned yesterday creates an
        # UNPARTITIONED hoglake table today and nothing says a word.
        # Same treatment as MILLPOND_VARIANT_COLUMNS: refuse, and make
        # the operator state the intent.
        monkeypatch.setenv("DUCKLAKE_PARTITION_BY", "year(timestamp),month(timestamp)")
        with pytest.raises(RuntimeError, match="HOGLAKE_PARTITION_BY"):
            load()

    def test_ducklake_partitioning_ignored_once_a_hoglake_spec_is_given(self, monkeypatch):
        monkeypatch.setenv("DUCKLAKE_PARTITION_BY", "year(timestamp)")
        monkeypatch.setenv("HOGLAKE_PARTITION_BY", "month(_inserted_at)")
        cfg = load()
        assert cfg.hoglake_partition_by == (("_inserted_at", "month", None),)
        assert cfg.partition_by is None

    def test_typed_columns_allowed_for_hoglake(self, monkeypatch):
        # Typed pins act upstream in arrow_converter — destination-neutral.
        monkeypatch.setenv("MILLPOND_TYPED_COLUMNS", "timestamp:timestamptz")
        cfg = load()
        assert cfg.typed_columns == (("timestamp", "timestamptz"),)

    def test_hyphenated_table_accepted(self, monkeypatch):
        # hoglake identifiers allow hyphens (unlike DuckLake's table regex).
        monkeypatch.setenv("HOGLAKE_TABLE", "events-v2")
        cfg = load()
        assert cfg.hoglake_table == "events-v2"

    def test_stray_hoglake_vars_ignored_for_ducklake(self, monkeypatch):
        monkeypatch.setenv("MILLPOND_DESTINATION", "ducklake")
        monkeypatch.setenv("DUCKLAKE_TABLE", "events")
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "s3://bucket/data")
        monkeypatch.setenv("DUCKLAKE_RDS_HOST", "host")
        monkeypatch.setenv("DUCKLAKE_RDS_PASSWORD", "pass")
        monkeypatch.setenv("DUCKLAKE_CONNECTION", ":memory:")
        # Even a malformed hoglake partition expression must not matter.
        monkeypatch.setenv("HOGLAKE_PARTITION_BY", "truncate(")
        cfg = load()
        assert cfg.destination == "ducklake"
        assert cfg.hoglake_url is None
        assert cfg.hoglake_partition_by is None


class TestHoglakePartitionBy:
    """HOGLAKE_PARTITION_BY maps millpond's partition-expression grammar to
    hoglake's transform vocabulary (identity, year, month, day, hour,
    bucket) AT STARTUP. Unmappable expressions are refused with a clear
    error — never deferred to a per-batch failure."""

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_TOPIC", "test-topic")
        monkeypatch.setenv("REPLICA_COUNT", "4")
        monkeypatch.setenv("POD_NAME", "millpond-events-2")
        monkeypatch.setenv("MILLPOND_DESTINATION", "hoglake")
        monkeypatch.setenv("HOGLAKE_URL", "http://localhost:28080")
        monkeypatch.setenv("HOGLAKE_CATALOG", "millpond")
        monkeypatch.setenv("HOGLAKE_NAMESPACE", "analytics")
        monkeypatch.setenv("HOGLAKE_TABLE", "events")
        monkeypatch.setenv("HOGLAKE_S3_ACCESS_KEY", "ak")
        monkeypatch.setenv("HOGLAKE_S3_SECRET_KEY", "sk")

    def test_bare_column_is_identity(self, monkeypatch):
        monkeypatch.setenv("HOGLAKE_PARTITION_BY", "team_id")
        cfg = load()
        assert cfg.hoglake_partition_by == (("team_id", "identity", None),)

    def test_temporal_transforms(self, monkeypatch):
        monkeypatch.setenv(
            "HOGLAKE_PARTITION_BY",
            "year(_inserted_at), month(_inserted_at), day(_inserted_at), hour(_inserted_at)",
        )
        cfg = load()
        assert cfg.hoglake_partition_by == (
            ("_inserted_at", "year", None),
            ("_inserted_at", "month", None),
            ("_inserted_at", "day", None),
            ("_inserted_at", "hour", None),
        )

    def test_bucket_with_param(self, monkeypatch):
        monkeypatch.setenv("HOGLAKE_PARTITION_BY", "bucket(team_id, 16)")
        cfg = load()
        assert cfg.hoglake_partition_by == (("team_id", "bucket", 16),)

    def test_mixed_expression(self, monkeypatch):
        monkeypatch.setenv("HOGLAKE_PARTITION_BY", "team_id,month(_inserted_at)")
        cfg = load()
        assert cfg.hoglake_partition_by == (
            ("team_id", "identity", None),
            ("_inserted_at", "month", None),
        )

    @pytest.mark.parametrize(
        "expr",
        [
            "truncate(ts)",  # client-side-only transform; server refuses it
            "void(ts)",
            "date_trunc(day, ts)",
        ],
    )
    def test_unknown_transform_refused_listing_vocabulary(self, expr, monkeypatch):
        monkeypatch.setenv("HOGLAKE_PARTITION_BY", expr)
        with pytest.raises(RuntimeError, match="identity, year, month, day, hour, bucket"):
            load()

    @pytest.mark.parametrize(
        "expr",
        [
            "year(",  # unbalanced
            "year()",  # no column
            "year(a,b)",  # arity
            "bucket(team_id)",  # bucket needs a param
            "bucket(team_id, 0)",  # param must be positive
            "bucket(team_id, x)",  # param must be an int
            "hour(bad.col)",  # unsafe column name
            ",",  # empty entries only
        ],
    )
    def test_malformed_expression_refused(self, expr, monkeypatch):
        monkeypatch.setenv("HOGLAKE_PARTITION_BY", expr)
        with pytest.raises(RuntimeError, match="HOGLAKE_PARTITION_BY"):
            load()
