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
