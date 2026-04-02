import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import OFFSET_STORED

from confluent_kafka import OFFSET_STORED

from millpond.consumer import (
    _base_kafka_config,
    _maybe_attach_oauth_cb,
    _on_stats,
    compute_assignment,
    create,
    discover_partition_count,
)
from millpond.config import Config


class TestComputeAssignment:
    def test_even_distribution(self):
        assert compute_assignment(8, 2, 0) == [0, 2, 4, 6]
        assert compute_assignment(8, 2, 1) == [1, 3, 5, 7]

    def test_single_replica(self):
        assert compute_assignment(4, 1, 0) == [0, 1, 2, 3]

    def test_more_replicas_than_partitions(self):
        assert compute_assignment(2, 4, 0) == [0]
        assert compute_assignment(2, 4, 1) == [1]
        assert compute_assignment(2, 4, 2) == []
        assert compute_assignment(2, 4, 3) == []

    def test_uneven_distribution(self):
        # 10 partitions, 3 replicas
        assert compute_assignment(10, 3, 0) == [0, 3, 6, 9]
        assert compute_assignment(10, 3, 1) == [1, 4, 7]
        assert compute_assignment(10, 3, 2) == [2, 5, 8]


class TestOnStats:
    """librdkafka stats callback should update Prometheus gauges."""

    STATS_BLOB = json.dumps(
        {
            "replyq": 3,
            "msg_cnt": 42,
            "msg_size": 10240,
            "brokers": {
                "broker1:9092/1": {
                    "nodeid": 1,
                    "name": "broker1:9092/1",
                    "rtt": {"avg": 5000, "p99": 12000},  # microseconds
                },
                "broker2:9092/2": {
                    "nodeid": 2,
                    "name": "broker2:9092/2",
                    "rtt": {"avg": 3000, "p99": 8000},
                },
            },
        }
    )

    @patch("millpond.consumer.metrics")
    def test_queue_depth_set(self, mock_metrics):
        _on_stats(self.STATS_BLOB)
        mock_metrics.rdkafka_replyq.set.assert_called_once_with(3)
        mock_metrics.rdkafka_msg_cnt.set.assert_called_once_with(42)
        mock_metrics.rdkafka_msg_size.set.assert_called_once_with(10240)

    @patch("millpond.consumer.metrics")
    def test_broker_rtt_set(self, mock_metrics):
        _on_stats(self.STATS_BLOB)
        # Should be called once per broker with rtt in seconds
        calls = mock_metrics.rdkafka_broker_rtt_avg.labels.call_args_list
        assert len(calls) == 2
        brokers_seen = {c.kwargs["broker"] for c in calls}
        assert brokers_seen == {"broker1:9092/1", "broker2:9092/2"}

    @patch("millpond.consumer.metrics")
    def test_rtt_converted_to_seconds(self, mock_metrics):
        # Use distinct mocks per broker so we can verify per-broker values
        broker_avg_mocks = {}
        broker_p99_mocks = {}

        def avg_labels(broker):
            if broker not in broker_avg_mocks:
                broker_avg_mocks[broker] = MagicMock()
            return broker_avg_mocks[broker]

        def p99_labels(broker):
            if broker not in broker_p99_mocks:
                broker_p99_mocks[broker] = MagicMock()
            return broker_p99_mocks[broker]

        mock_metrics.rdkafka_broker_rtt_avg.labels = avg_labels
        mock_metrics.rdkafka_broker_rtt_p99.labels = p99_labels

        _on_stats(self.STATS_BLOB)

        broker_avg_mocks["broker1:9092/1"].set.assert_called_once_with(0.005)
        broker_avg_mocks["broker2:9092/2"].set.assert_called_once_with(0.003)
        broker_p99_mocks["broker1:9092/1"].set.assert_called_once_with(0.012)
        broker_p99_mocks["broker2:9092/2"].set.assert_called_once_with(0.008)

    @patch("millpond.consumer.metrics")
    def test_negative_rtt_skipped(self, mock_metrics):
        """librdkafka reports rtt=-1 when no samples exist; should not set gauge."""
        blob = json.dumps(
            {
                "replyq": 0,
                "msg_cnt": 0,
                "msg_size": 0,
                "brokers": {
                    "bootstrap:9092/bootstrap": {
                        "name": "bootstrap:9092/bootstrap",
                        "rtt": {"avg": -1, "p99": -1},
                    }
                },
            }
        )
        _on_stats(blob)
        mock_metrics.rdkafka_broker_rtt_avg.labels.assert_not_called()
        mock_metrics.rdkafka_broker_rtt_p99.labels.assert_not_called()

    @patch("millpond.consumer.metrics")
    def test_malformed_json_ignored(self, mock_metrics):
        _on_stats("not json")
        # Should not raise, gauges should not be called
        mock_metrics.rdkafka_replyq.set.assert_not_called()


def _make_cfg(**overrides) -> Config:
    defaults = dict(
        bootstrap_servers="localhost:9092",
        topic="test-topic",
        group_id="test-group",
        replica_count=4,
        ordinal=1,
        ducklake_table="events",
        ducklake_data_path="s3://bucket/data",
        ducklake_connection=":memory:",
        rds_host="localhost",
        rds_port="5432",
        rds_database="ducklake",
        rds_username="ducklake",
        rds_password="pass",
        flush_size=100,
        flush_interval_ms=1000,
        partition_by=None,
        fetch_min_bytes=1,
        fetch_max_wait_ms=500,
        consume_batch_size=1000,
        stats_interval_ms=5000,
        kafka_config_overrides=(("security.protocol", "SSL"),),
    )
    defaults.update(overrides)
    return Config(**defaults)


class TestBaseKafkaConfig:
    def test_includes_bootstrap_servers(self):
        cfg = _make_cfg()
        base = _base_kafka_config(cfg)
        assert base["bootstrap.servers"] == "localhost:9092"

    def test_includes_client_id(self):
        cfg = _make_cfg(ordinal=3)
        base = _base_kafka_config(cfg)
        assert base["client.id"] == "millpond-test-topic-events-3"

    def test_includes_overrides(self):
        cfg = _make_cfg(kafka_config_overrides=(("security.protocol", "SSL"), ("sasl.mechanisms", "PLAIN")))
        base = _base_kafka_config(cfg)
        assert base["security.protocol"] == "SSL"
        assert base["sasl.mechanisms"] == "PLAIN"

    def test_empty_overrides(self):
        cfg = _make_cfg(kafka_config_overrides=())
        base = _base_kafka_config(cfg)
        assert "security.protocol" not in base


class TestMaybeAttachOauthCb:
    def test_noop_for_ssl_config(self):
        config = {"bootstrap.servers": "localhost:9092", "security.protocol": "SSL"}
        result = _maybe_attach_oauth_cb(config)
        assert "oauth_cb" not in result

    def test_noop_when_no_sasl(self):
        config = {"bootstrap.servers": "localhost:9092"}
        result = _maybe_attach_oauth_cb(config)
        assert "oauth_cb" not in result

    @patch.dict("os.environ", {"AWS_REGION": "us-east-1"})
    def test_attaches_callback_for_oauthbearer(self):
        mock_provider = MagicMock()
        mock_provider.generate_auth_token.return_value = ("fake-token", 1700000000000)
        with patch.dict("sys.modules", {"aws_msk_iam_sasl_signer": MagicMock(MSKAuthTokenProvider=mock_provider)}):
            config = {"bootstrap.servers": "localhost:9092", "sasl.mechanisms": "OAUTHBEARER"}
            result = _maybe_attach_oauth_cb(config)
            assert "oauth_cb" in result
            token, expiry = result["oauth_cb"]("")
            assert token == "fake-token"
            assert expiry == 1700000000.0

    def test_singular_mechanism_ignored_with_warning(self, caplog):
        config = {"bootstrap.servers": "localhost:9092", "sasl.mechanism": "OAUTHBEARER"}
        result = _maybe_attach_oauth_cb(config)
        assert "oauth_cb" not in result
        assert "sasl.mechanisms (plural)" in caplog.text

    def test_raises_when_signer_not_installed(self):
        with patch.dict("sys.modules", {"aws_msk_iam_sasl_signer": None}):
            config = {"bootstrap.servers": "localhost:9092", "sasl.mechanisms": "OAUTHBEARER"}
            with pytest.raises(RuntimeError, match="aws-msk-iam-sasl-signer-python is required"):
                _maybe_attach_oauth_cb(config)

    @patch.dict("os.environ", {"AWS_REGION": "eu-central-1"})
    def test_uses_aws_region_env(self):
        mock_provider = MagicMock()
        mock_provider.generate_auth_token.return_value = ("token", 1000)
        with patch.dict("sys.modules", {"aws_msk_iam_sasl_signer": MagicMock(MSKAuthTokenProvider=mock_provider)}):
            config = {"sasl.mechanisms": "OAUTHBEARER"}
            result = _maybe_attach_oauth_cb(config)
            result["oauth_cb"]("")
            mock_provider.generate_auth_token.assert_called_with("eu-central-1")

    @patch.dict("os.environ", {}, clear=True)
    def test_raises_when_region_not_set(self):
        with patch.dict("sys.modules", {"aws_msk_iam_sasl_signer": MagicMock()}):
            config = {"sasl.mechanisms": "OAUTHBEARER"}
            with pytest.raises(RuntimeError, match="AWS_REGION or AWS_DEFAULT_REGION must be set"):
                _maybe_attach_oauth_cb(config)


class TestDiscoverPartitionCount:
    @patch("millpond.consumer.AdminClient")
    def test_returns_partition_count(self, mock_admin_cls):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_topic = MagicMock()
        mock_topic.error = None
        mock_topic.partitions = {0: None, 1: None, 2: None}
        mock_admin.list_topics.return_value.topics = {"test-topic": mock_topic}

        cfg = _make_cfg()
        count = discover_partition_count(cfg)
        assert count == 3

    @patch("millpond.consumer.AdminClient")
    def test_passes_overrides_to_admin_client(self, mock_admin_cls):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_topic = MagicMock()
        mock_topic.error = None
        mock_topic.partitions = {0: None}
        mock_admin.list_topics.return_value.topics = {"test-topic": mock_topic}

        cfg = _make_cfg(kafka_config_overrides=(("security.protocol", "SSL"),))
        discover_partition_count(cfg)

        admin_config = mock_admin_cls.call_args[0][0]
        assert admin_config["security.protocol"] == "SSL"
        assert admin_config["bootstrap.servers"] == "localhost:9092"

    @patch("millpond.consumer.AdminClient")
    def test_topic_not_found(self, mock_admin_cls):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.return_value.topics = {}

        cfg = _make_cfg()
        with pytest.raises(RuntimeError, match="not found"):
            discover_partition_count(cfg)

    @patch("millpond.consumer.AdminClient")
    def test_topic_error(self, mock_admin_cls):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_topic = MagicMock()
        mock_topic.error = "LEADER_NOT_AVAILABLE"
        mock_admin.list_topics.return_value.topics = {"test-topic": mock_topic}

        cfg = _make_cfg()
        with pytest.raises(RuntimeError, match="error"):
            discover_partition_count(cfg)


class TestCreate:
    @patch("millpond.consumer.Consumer")
    @patch("millpond.consumer.discover_partition_count", return_value=8)
    def test_assigns_correct_partitions(self, mock_discover, mock_consumer_cls):
        cfg = _make_cfg(ordinal=1, replica_count=4)
        create(cfg)

        assign_call = mock_consumer_cls.return_value.assign
        assign_call.assert_called_once()
        partitions = assign_call.call_args[0][0]
        assert [tp.partition for tp in partitions] == [1, 5]

    @patch("millpond.consumer.Consumer")
    @patch("millpond.consumer.discover_partition_count", return_value=8)
    def test_passes_overrides_to_consumer(self, mock_discover, mock_consumer_cls):
        cfg = _make_cfg(kafka_config_overrides=(("security.protocol", "SSL"),))
        create(cfg)

        consumer_config = mock_consumer_cls.call_args[0][0]
        assert consumer_config["security.protocol"] == "SSL"

    @patch("millpond.consumer.Consumer")
    @patch("millpond.consumer.discover_partition_count", return_value=8)
    def test_sets_client_id(self, mock_discover, mock_consumer_cls):
        cfg = _make_cfg(ordinal=2, replica_count=4)
        create(cfg)

        consumer_config = mock_consumer_cls.call_args[0][0]
        assert consumer_config["client.id"] == "millpond-test-topic-events-2"

    @patch("millpond.consumer.Consumer")
    @patch("millpond.consumer.discover_partition_count", return_value=8)
    def test_sets_queued_max_messages_kbytes(self, mock_discover, mock_consumer_cls):
        cfg = _make_cfg()
        create(cfg)

        consumer_config = mock_consumer_cls.call_args[0][0]
        assert consumer_config["queued.max.messages.kbytes"] == 16384

    @patch("millpond.consumer.Consumer")
    @patch("millpond.consumer.discover_partition_count", return_value=8)
    def test_assign_uses_stored_offsets(self, mock_discover, mock_consumer_cls):
        """Verify assign() explicitly uses OFFSET_STORED for all partitions.

        STORED means: resume from the committed offset in __consumer_offsets,
        falling back to auto.offset.reset (earliest) if none exists. This is
        critical for replica count changes — a new pod must resume from the
        offset committed by whichever pod previously owned that partition.
        """
        cfg = _make_cfg(ordinal=0, replica_count=2)
        create(cfg)

        assign_call = mock_consumer_cls.return_value.assign
        partitions = assign_call.call_args[0][0]
        for tp in partitions:
            assert tp.offset == OFFSET_STORED, (
                f"Partition {tp.partition} has offset {tp.offset}, expected OFFSET_STORED ({OFFSET_STORED}). "
                "assign() must use OFFSET_STORED to resume from committed offsets."
            )

    @patch("millpond.consumer.Consumer")
    @patch("millpond.consumer.discover_partition_count", return_value=8)
    def test_auto_offset_reset_is_earliest(self, mock_discover, mock_consumer_cls):
        """Verify auto.offset.reset=earliest for partitions with no committed offset."""
        cfg = _make_cfg()
        create(cfg)

        consumer_config = mock_consumer_cls.call_args[0][0]
        assert consumer_config["auto.offset.reset"] == "earliest"

    @patch("millpond.consumer.Consumer")
    @patch("millpond.consumer.discover_partition_count", return_value=2)
    def test_no_partitions_raises(self, mock_discover, mock_consumer_cls):
        cfg = _make_cfg(ordinal=3, replica_count=4)
        with pytest.raises(RuntimeError, match="No partitions assigned"):
            create(cfg)
