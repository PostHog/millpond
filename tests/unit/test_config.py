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
        monkeypatch.setenv("ICEBERG_TABLE", "events")
        monkeypatch.setenv("ICEBERG_NAMESPACE", "millpond")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "https://catalog.example/v1")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "production")
        monkeypatch.setenv("MILLPOND_S3_ACCESS_KEY_ID", "AKIA0000")
        monkeypatch.setenv("MILLPOND_S3_SECRET_ACCESS_KEY", "secret0")
        monkeypatch.setenv("MILLPOND_S3_REGION", "us-east-1")

    def test_loads(self):
        cfg = load()
        assert cfg.topic == "test-topic"
        assert cfg.ordinal == 2
        assert cfg.replica_count == 4
        assert cfg.group_id == "millpond-test-topic-events"
        assert cfg.iceberg_namespace == "millpond"
        assert cfg.iceberg_table == "events"
        assert cfg.iceberg_catalog_uri == "https://catalog.example/v1"
        assert cfg.iceberg_warehouse == "production"
        assert cfg.s3_access_key_id == "AKIA0000"
        assert cfg.s3_region == "us-east-1"

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

    def test_optional_iceberg_fields_default_none(self):
        cfg = load()
        assert cfg.iceberg_table_location is None
        assert cfg.iceberg_catalog_token is None
        assert cfg.s3_endpoint is None

    def test_optional_iceberg_fields_can_be_set(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_TABLE_LOCATION", "s3://bucket/lake/events")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "bearer123")
        monkeypatch.setenv("MILLPOND_S3_ENDPOINT", "minio.local:9000")
        cfg = load()
        assert cfg.iceberg_table_location == "s3://bucket/lake/events"
        assert cfg.iceberg_catalog_token == "bearer123"
        assert cfg.s3_endpoint == "minio.local:9000"

    def test_ordinal_exceeds_replica_count(self, monkeypatch):
        monkeypatch.setenv("POD_NAME", "millpond-events-5")
        with pytest.raises(RuntimeError, match="Ordinal 5 >= REPLICA_COUNT 4"):
            load()

    def test_missing_required(self, monkeypatch):
        monkeypatch.delenv("KAFKA_TOPIC")
        with pytest.raises(RuntimeError, match="KAFKA_TOPIC"):
            load()

    def test_missing_iceberg_namespace(self, monkeypatch):
        # Per P2: namespace is required, no default.
        monkeypatch.delenv("ICEBERG_NAMESPACE")
        with pytest.raises(RuntimeError, match="ICEBERG_NAMESPACE"):
            load()

    def test_missing_iceberg_catalog_uri(self, monkeypatch):
        monkeypatch.delenv("ICEBERG_CATALOG_URI")
        with pytest.raises(RuntimeError, match="ICEBERG_CATALOG_URI"):
            load()

    def test_missing_s3_access_key(self, monkeypatch):
        monkeypatch.delenv("MILLPOND_S3_ACCESS_KEY_ID")
        with pytest.raises(RuntimeError, match="MILLPOND_S3_ACCESS_KEY_ID"):
            load()

    def test_unsafe_table_name_rejected(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_TABLE", "events; DROP TABLE x")
        with pytest.raises(RuntimeError, match="unsafe characters"):
            load()

    def test_table_name_with_sql_injection(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_TABLE", "x--")
        with pytest.raises(RuntimeError, match="unsafe characters"):
            load()

    def test_valid_table_names(self, monkeypatch):
        for name in ["events", "my_table", "_private", "Events123"]:
            monkeypatch.setenv("ICEBERG_TABLE", name)
            cfg = load()
            assert cfg.iceberg_table == name

    def test_unsafe_namespace_rejected(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_NAMESPACE", "evil; DROP NAMESPACE")
        with pytest.raises(RuntimeError, match="unsafe characters"):
            load()

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
