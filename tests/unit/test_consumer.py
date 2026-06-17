import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import OFFSET_INVALID, OFFSET_STORED, KafkaException, TopicPartition

from millpond.config import Config
from millpond.consumer import (
    _base_kafka_config,
    _maybe_attach_oauth_cb,
    _on_stats,
    _recover_stale_committed_offsets,
    _seed_startup_gauges,
    compute_assignment,
    create,
    discover_partition_count,
)


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
        partition_by=None,
        flush_size=100,
        flush_interval_ms=1000,
        fetch_min_bytes=1,
        fetch_max_wait_ms=500,
        consume_batch_size=1000,
        stats_interval_ms=5000,
        auto_offset_reset="earliest",
        broker_source="",
        filter_keep_field=None,
        filter_drop_field=None,
        filter_values=None,
        sort_by=None,
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
    def test_auto_offset_reset_defaults_earliest(self, mock_discover, mock_consumer_cls):
        """Verify auto.offset.reset is plumbed through from cfg.auto_offset_reset (default earliest)."""
        cfg = _make_cfg()
        create(cfg)

        consumer_config = mock_consumer_cls.call_args[0][0]
        assert consumer_config["auto.offset.reset"] == "earliest"

    @patch("millpond.consumer.Consumer")
    @patch("millpond.consumer.discover_partition_count", return_value=8)
    def test_auto_offset_reset_latest_is_plumbed(self, mock_discover, mock_consumer_cls):
        """NRT consumers set auto_offset_reset=latest so a fresh group skips the retention backlog."""
        cfg = _make_cfg(auto_offset_reset="latest")
        create(cfg)

        consumer_config = mock_consumer_cls.call_args[0][0]
        assert consumer_config["auto.offset.reset"] == "latest"

    @patch("millpond.consumer.Consumer")
    @patch("millpond.consumer.discover_partition_count", return_value=2)
    def test_no_partitions_raises(self, mock_discover, mock_consumer_cls):
        cfg = _make_cfg(ordinal=3, replica_count=4)
        with pytest.raises(RuntimeError, match="No partitions assigned"):
            create(cfg)


class _FakeAdminContext:
    """Patches AdminClient inside millpond.consumer so list_offsets returns
    the watermarks the test wants. Use via the helper below; AdminClient is
    constructed by the SUT, not the test, so we can't pass a Mock directly."""

    def __init__(self, watermarks, raise_on_dispatch=False, raise_on_partition=None):
        self.watermarks = watermarks
        self.raise_on_dispatch = raise_on_dispatch
        self.raise_on_partition = raise_on_partition or set()

    def __enter__(self):
        self._patcher = patch("millpond.consumer.AdminClient")
        admin_cls = self._patcher.start()
        admin_inst = admin_cls.return_value

        def list_offsets(spec_dict, request_timeout=None):
            if self.raise_on_dispatch:
                raise KafkaException("dispatch failed")
            # spec_dict is {TopicPartition: OffsetSpec}. We return per-tp
            # futures whose .result().offset gives the watermark value.
            from millpond.consumer import OffsetSpec

            futures = {}
            for tp, spec in spec_dict.items():
                fut = MagicMock()
                if tp.partition in self.raise_on_partition:
                    fut.result.side_effect = KafkaException("partition error")
                else:
                    info = MagicMock()
                    low, high = self.watermarks[tp.partition]
                    info.offset = low if isinstance(spec, OffsetSpec.earliest().__class__) else high
                    fut.result.return_value = info
                futures[tp] = fut
            return futures

        admin_inst.list_offsets.side_effect = list_offsets
        return admin_inst

    def __exit__(self, *a):
        self._patcher.stop()


class TestRecoverStaleCommittedOffsets:
    """Recovery runs BEFORE assign() — queries committed offsets via the
    consumer, watermarks via AdminClient.list_offsets (batched), then
    commits the auto.offset.reset target for any partition whose committed
    offset has aged out of retention."""

    def _make_consumer(self, committed_offsets, *, errors=None):
        """Build a Mock consumer responding to .committed() and .commit().

        committed_offsets: dict[int, int]   partition -> committed offset
                                             (OFFSET_INVALID for never-committed)
        errors:            dict[int, str]   partition -> error string for
                                             that partition's committed entry

        Note: real TopicPartition.error is a read-only C attribute, so the
        fake uses SimpleNamespace with the four fields the SUT reads
        (.topic, .partition, .offset, .error). Functionally identical for
        our recovery code's purposes.
        """
        from types import SimpleNamespace

        errors = errors or {}
        consumer = MagicMock()

        def fake_committed(tps, timeout=None):
            return [
                SimpleNamespace(
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=committed_offsets.get(tp.partition, OFFSET_INVALID),
                    error=errors.get(tp.partition),
                )
                for tp in tps
            ]

        consumer.committed.side_effect = fake_committed
        return consumer

    def test_no_committed_offsets_makes_no_commit(self):
        """Fresh consumer group (OFFSET_INVALID everywhere) — recovery is a no-op.
        auto.offset.reset handles fresh subscriptions correctly on first fetch."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(
            committed_offsets={0: OFFSET_INVALID, 1: OFFSET_INVALID},
        )
        with _FakeAdminContext(watermarks={0: (100, 200), 1: (100, 200)}):
            _recover_stale_committed_offsets(consumer, cfg, [0, 1])
        consumer.commit.assert_not_called()

    def test_in_retention_offsets_make_no_commit(self):
        """All committed offsets are within [low, high] — nothing to recover."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(committed_offsets={0: 150, 1: 180})
        with _FakeAdminContext(watermarks={0: (100, 200), 1: (100, 200)}):
            _recover_stale_committed_offsets(consumer, cfg, [0, 1])
        consumer.commit.assert_not_called()

    def test_stale_offsets_commit_high_for_latest_policy(self):
        """Committed below low + auto.offset.reset=latest → commit the high watermark."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(committed_offsets={0: 50, 1: 50})  # below low=100 for both
        with _FakeAdminContext(watermarks={0: (100, 200), 1: (100, 250)}):
            _recover_stale_committed_offsets(consumer, cfg, [0, 1])

        consumer.commit.assert_called_once()
        committed = consumer.commit.call_args[1]["offsets"]
        committed_by_partition = {tp.partition: tp.offset for tp in committed}
        assert committed_by_partition == {0: 200, 1: 250}
        assert consumer.commit.call_args[1]["asynchronous"] is False

    def test_stale_offsets_commit_low_for_earliest_policy(self):
        """Committed below low + auto.offset.reset=earliest → commit the low
        watermark. "low" is itself a record we haven't read — but it's the
        first available, matching what auto.offset.reset=earliest would seek to."""
        cfg = _make_cfg(auto_offset_reset="earliest")
        consumer = self._make_consumer(committed_offsets={0: 50})
        with _FakeAdminContext(watermarks={0: (100, 200)}):
            _recover_stale_committed_offsets(consumer, cfg, [0])

        consumer.commit.assert_called_once()
        committed = consumer.commit.call_args[1]["offsets"]
        assert committed[0].offset == 100

    def test_mixed_partitions_only_stale_recovered(self):
        """Stale + in-retention + never-committed → only the stale partition
        is committed; the others are left to auto.offset.reset / current consumer
        position. A partial sweep must not blow away healthy commits."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(
            committed_offsets={
                0: 50,  # stale
                1: 150,  # in retention
                2: OFFSET_INVALID,  # never committed
            },
        )
        with _FakeAdminContext(watermarks={0: (100, 200), 1: (100, 200), 2: (100, 200)}):
            _recover_stale_committed_offsets(consumer, cfg, [0, 1, 2])

        consumer.commit.assert_called_once()
        committed = consumer.commit.call_args[1]["offsets"]
        partitions = [tp.partition for tp in committed]
        assert partitions == [0], f"Expected only partition 0 to be recovered, got {partitions}"
        assert committed[0].offset == 200

    def test_committed_query_failure_skips_recovery(self):
        """KafkaException on .committed() bails out without raising — the
        auto.offset.reset fallback in librdkafka still works.

        Recovery is an optimization. If it can't run, fail open."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = MagicMock()
        consumer.committed.side_effect = KafkaException("broker unreachable")

        _recover_stale_committed_offsets(consumer, cfg, [0, 1])
        consumer.commit.assert_not_called()

    def test_watermark_dispatch_failure_skips_recovery(self):
        """KafkaException on AdminClient.list_offsets dispatch bails out.
        Recovery is best-effort; the auto.offset.reset path still kicks in."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(committed_offsets={0: 50, 1: 50})
        with _FakeAdminContext(watermarks={}, raise_on_dispatch=True):
            _recover_stale_committed_offsets(consumer, cfg, [0, 1])
        consumer.commit.assert_not_called()

    def test_per_partition_watermark_failure_skips_that_partition(self):
        """A per-partition future raising mid-batch logs and continues;
        recovery still commits for partitions whose futures succeeded."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(committed_offsets={0: 50, 1: 50})
        with _FakeAdminContext(
            watermarks={0: (100, 200), 1: (100, 200)},
            raise_on_partition={0},
        ):
            _recover_stale_committed_offsets(consumer, cfg, [0, 1])

        consumer.commit.assert_called_once()
        committed = consumer.commit.call_args[1]["offsets"]
        partitions = [tp.partition for tp in committed]
        assert partitions == [1], f"Expected only partition 1 (watermark succeeded), got {partitions}"

    def test_commit_failure_does_not_raise(self):
        """If the final .commit() fails, log warning and return — librdkafka's
        auto.offset.reset still triggers on first fetch. Don't crash the pod."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(committed_offsets={0: 50})
        consumer.commit.side_effect = KafkaException("group coordinator unavailable")
        with _FakeAdminContext(watermarks={0: (100, 200)}):
            # Should not raise
            _recover_stale_committed_offsets(consumer, cfg, [0])

    def test_committed_equal_to_low_is_in_retention(self):
        """Boundary: committed == low is the earliest *available* record. Not stale."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(committed_offsets={0: 100})
        with _FakeAdminContext(watermarks={0: (100, 200)}):
            _recover_stale_committed_offsets(consumer, cfg, [0])
        consumer.commit.assert_not_called()

    def test_empty_partition_low_equals_high(self):
        """Empty partition: low == high == 0. A committed value of 0 is
        "in retention" (committed == low). No commit fires."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(committed_offsets={0: 0})
        with _FakeAdminContext(watermarks={0: (0, 0)}):
            _recover_stale_committed_offsets(consumer, cfg, [0])
        consumer.commit.assert_not_called()

    def test_empty_partitions_list_is_noop(self):
        """If the caller passes [] (e.g. ordinal owns no partitions, which
        actually raises in create() — but defensive), recovery does nothing.
        Don't hit the broker, don't log a confusing "Recovering 0" line."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = MagicMock()
        # AdminClient must not even be constructed in this path.
        with patch("millpond.consumer.AdminClient") as admin_cls:
            _recover_stale_committed_offsets(consumer, cfg, [])
        consumer.committed.assert_not_called()
        consumer.commit.assert_not_called()
        admin_cls.assert_not_called()

    def test_committed_partition_error_skipped(self):
        """consumer.committed() returns per-partition entries with an `error`
        attribute. If set, `.offset` may be garbage from librdkafka — must not
        feed it into the watermark check. Skip with a warning instead."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(
            # Without the error skip, partition 0's offset 50 would look "stale"
            # against low=100 and trigger a spurious commit. The error must
            # short-circuit before that.
            committed_offsets={0: 50, 1: 50},
            errors={0: "UNKNOWN_TOPIC_OR_PARTITION"},
        )
        with _FakeAdminContext(watermarks={0: (100, 200), 1: (100, 200)}):
            _recover_stale_committed_offsets(consumer, cfg, [0, 1])

        consumer.commit.assert_called_once()
        committed = consumer.commit.call_args[1]["offsets"]
        partitions = [tp.partition for tp in committed]
        assert partitions == [1], f"Expected only partition 1 (no error), got {partitions}"

    @patch("millpond.consumer._seed_startup_gauges")
    def test_seeding_called_after_successful_recovery(self, mock_seed):
        """Recovery committed successfully → seeding gets the to_recover list as
        `recovered`, so the seeder uses the recovery target (not the pre-recovery
        stale committed value) when computing the gauge."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(committed_offsets={0: 50})  # stale
        with _FakeAdminContext(watermarks={0: (100, 200)}):
            _recover_stale_committed_offsets(consumer, cfg, [0])

        mock_seed.assert_called_once()
        _cfg, _committed, watermarks, recovered = mock_seed.call_args[0]
        assert watermarks == {0: (100, 200)}
        assert [tp.partition for tp in recovered] == [0]
        assert recovered[0].offset == 200  # high (latest policy)

    @patch("millpond.consumer._seed_startup_gauges")
    def test_seeding_called_with_empty_recovered_when_commit_fails(self, mock_seed):
        """commit() raised → recovery couldn't persist. Seed with recovered=[]
        so the seeder falls back to the auto.offset.reset target rather than
        falsely claiming the partition is "at the recovery position".

        librdkafka's STORED fallback will land at that same position on first
        fetch, so the gauge stays correct without us having to pretend recovery
        succeeded."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(committed_offsets={0: 50})  # stale
        consumer.commit.side_effect = KafkaException("group coordinator unavailable")
        with _FakeAdminContext(watermarks={0: (100, 200)}):
            _recover_stale_committed_offsets(consumer, cfg, [0])

        mock_seed.assert_called_once()
        _cfg, _committed, _watermarks, recovered = mock_seed.call_args[0]
        assert recovered == []

    @patch("millpond.consumer._seed_startup_gauges")
    def test_seeding_called_when_no_recovery_needed(self, mock_seed):
        """All offsets healthy → recovery is a no-op, but seeding still runs.
        This is the steady-state case where a quiet partition's gauge would
        otherwise be stuck at the previous pod's last reported lag."""
        cfg = _make_cfg(auto_offset_reset="latest")
        consumer = self._make_consumer(committed_offsets={0: 150})  # in retention
        with _FakeAdminContext(watermarks={0: (100, 200)}):
            _recover_stale_committed_offsets(consumer, cfg, [0])

        consumer.commit.assert_not_called()
        mock_seed.assert_called_once()
        _cfg, _committed, _watermarks, recovered = mock_seed.call_args[0]
        assert recovered == []


def _make_committed(committed_offsets: dict, errors: dict | None = None) -> list:
    """Build the `committed` list the seeder expects: SimpleNamespace entries
    with topic/partition/offset/error. Mirrors what consumer.committed()
    returns at runtime. Real TopicPartition.error is C-only and read-only,
    so the test fake uses SimpleNamespace."""
    from types import SimpleNamespace

    errors = errors or {}
    return [
        SimpleNamespace(
            topic="test-topic",
            partition=p,
            offset=offset,
            error=errors.get(p),
        )
        for p, offset in committed_offsets.items()
    ]


class TestSeedStartupGauges:
    """Direct tests for _seed_startup_gauges. Verifies the gauge values written
    for each per-partition state: recovered, in-retention, fresh, errored, and
    stale-but-not-recovered. The integration-level wiring (does the recovery
    function call this?) is covered by TestRecoverStaleCommittedOffsets."""

    @patch("millpond.consumer.metrics")
    def test_recovered_partition_uses_target_offset_latest(self, mock_metrics):
        """auto.offset.reset=latest → recovery target = high → gauge lag is 0
        and last_committed_offset is high. After recovery, the consumer is
        caught up by definition."""
        cfg = _make_cfg(auto_offset_reset="latest")
        committed = _make_committed({0: 50})  # the pre-recovery stale value
        watermarks = {0: (100, 200)}
        recovered = [TopicPartition("test-topic", 0, 200)]

        _seed_startup_gauges(cfg, committed, watermarks, recovered)

        mock_metrics.consumer_lag.labels.assert_called_once_with(partition="0")
        mock_metrics.consumer_lag.labels.return_value.set.assert_called_once_with(0)
        mock_metrics.last_committed_offset.labels.return_value.set.assert_called_once_with(200)

    @patch("millpond.consumer.metrics")
    def test_recovered_partition_uses_target_offset_earliest(self, mock_metrics):
        """auto.offset.reset=earliest → recovery target = low → gauge lag is
        high-low. The consumer has the full in-retention backlog ahead of it."""
        cfg = _make_cfg(auto_offset_reset="earliest")
        committed = _make_committed({0: 50})
        watermarks = {0: (100, 200)}
        recovered = [TopicPartition("test-topic", 0, 100)]

        _seed_startup_gauges(cfg, committed, watermarks, recovered)

        mock_metrics.consumer_lag.labels.return_value.set.assert_called_once_with(100)
        mock_metrics.last_committed_offset.labels.return_value.set.assert_called_once_with(100)

    @patch("millpond.consumer.metrics")
    def test_in_retention_committed_uses_committed_offset(self, mock_metrics):
        """committed value within [low, high] → use committed offset directly.
        This is steady-state. Lag = high - committed."""
        cfg = _make_cfg(auto_offset_reset="latest")
        committed = _make_committed({0: 150})
        watermarks = {0: (100, 200)}

        _seed_startup_gauges(cfg, committed, watermarks, recovered=[])

        mock_metrics.consumer_lag.labels.return_value.set.assert_called_once_with(50)
        mock_metrics.last_committed_offset.labels.return_value.set.assert_called_once_with(150)

    @patch("millpond.consumer.metrics")
    def test_fresh_partition_uses_high_for_latest(self, mock_metrics):
        """OFFSET_INVALID (never committed) + auto.offset.reset=latest →
        position=high. librdkafka will seek to high on first fetch, so the
        gauge starts at lag=0 matching where consumption will resume."""
        cfg = _make_cfg(auto_offset_reset="latest")
        committed = _make_committed({0: OFFSET_INVALID})
        watermarks = {0: (100, 200)}

        _seed_startup_gauges(cfg, committed, watermarks, recovered=[])

        mock_metrics.consumer_lag.labels.return_value.set.assert_called_once_with(0)
        mock_metrics.last_committed_offset.labels.return_value.set.assert_called_once_with(200)

    @patch("millpond.consumer.metrics")
    def test_fresh_partition_uses_low_for_earliest(self, mock_metrics):
        """OFFSET_INVALID + auto.offset.reset=earliest → position=low. The
        consumer will replay the full retention window, so lag starts at
        high - low."""
        cfg = _make_cfg(auto_offset_reset="earliest")
        committed = _make_committed({0: OFFSET_INVALID})
        watermarks = {0: (100, 200)}

        _seed_startup_gauges(cfg, committed, watermarks, recovered=[])

        mock_metrics.consumer_lag.labels.return_value.set.assert_called_once_with(100)
        mock_metrics.last_committed_offset.labels.return_value.set.assert_called_once_with(100)

    @patch("millpond.consumer.metrics")
    def test_errored_committed_uses_auto_offset_reset_target(self, mock_metrics):
        """tp.error set → tp.offset is unreliable. Use the auto.offset.reset
        target, same as fresh. Without this the seeder would publish a gauge
        built from a garbage offset."""
        cfg = _make_cfg(auto_offset_reset="latest")
        committed = _make_committed({0: 999_999}, errors={0: "UNKNOWN_TOPIC_OR_PARTITION"})
        watermarks = {0: (100, 200)}

        _seed_startup_gauges(cfg, committed, watermarks, recovered=[])

        mock_metrics.consumer_lag.labels.return_value.set.assert_called_once_with(0)
        mock_metrics.last_committed_offset.labels.return_value.set.assert_called_once_with(200)

    @patch("millpond.consumer.metrics")
    def test_stale_not_recovered_uses_auto_offset_reset_target(self, mock_metrics):
        """Committed is stale (below low) AND recovered=[] (commit failed
        upstream) → fall through to the auto.offset.reset target. That's where
        librdkafka's STORED fallback will land on first fetch, so the seeded
        gauge matches what the next delivery will confirm."""
        cfg = _make_cfg(auto_offset_reset="latest")
        committed = _make_committed({0: 50})  # below low
        watermarks = {0: (100, 200)}

        _seed_startup_gauges(cfg, committed, watermarks, recovered=[])

        mock_metrics.consumer_lag.labels.return_value.set.assert_called_once_with(0)
        mock_metrics.last_committed_offset.labels.return_value.set.assert_called_once_with(200)

    @patch("millpond.consumer.metrics")
    def test_committed_equal_to_low_is_in_retention(self, mock_metrics):
        """Boundary: committed == low is still in retention (the earliest
        available record). Match the recovery code's `tp.offset >= low` check
        so the two stay consistent — a partition not flagged for recovery
        must not be seeded as if it were."""
        cfg = _make_cfg(auto_offset_reset="latest")
        committed = _make_committed({0: 100})
        watermarks = {0: (100, 200)}

        _seed_startup_gauges(cfg, committed, watermarks, recovered=[])

        mock_metrics.consumer_lag.labels.return_value.set.assert_called_once_with(100)
        mock_metrics.last_committed_offset.labels.return_value.set.assert_called_once_with(100)

    @patch("millpond.consumer.metrics")
    def test_partition_without_watermark_skipped(self, mock_metrics):
        """Watermark query failed for a partition → it's absent from `watermarks`.
        Skip rather than guess: setting consumer_lag without a real `high` would
        publish a wrong value, and overwriting last_committed_offset with the
        raw committed value (without a verified `high`) is meaningless to the
        dashboard's lag panel."""
        cfg = _make_cfg(auto_offset_reset="latest")
        committed = _make_committed({0: 150, 1: 150})
        watermarks = {0: (100, 200)}  # partition 1 missing

        _seed_startup_gauges(cfg, committed, watermarks, recovered=[])

        # Only partition 0 should be seeded
        assert mock_metrics.consumer_lag.labels.call_count == 1
        mock_metrics.consumer_lag.labels.assert_called_with(partition="0")

    @patch("millpond.consumer.metrics")
    def test_empty_watermarks_no_gauge_calls(self, mock_metrics):
        """No partitions in `watermarks` → no gauges set, no log spam.
        Defensive against the all-watermark-queries-failed case."""
        cfg = _make_cfg(auto_offset_reset="latest")

        _seed_startup_gauges(cfg, committed=[], watermarks={}, recovered=[])

        mock_metrics.consumer_lag.labels.assert_not_called()
        mock_metrics.last_committed_offset.labels.assert_not_called()

    @patch("millpond.consumer.metrics")
    def test_multiple_partitions_each_state(self, mock_metrics):
        """Mixed bag in a single call: recovered, in-retention, fresh, and
        errored. Each gets its own per-partition gauge with the right value.
        Verifies the recovered-list lookup doesn't bleed across partitions."""
        cfg = _make_cfg(auto_offset_reset="latest")
        committed = _make_committed(
            {
                0: 50,  # stale, will be recovered
                1: 150,  # in retention
                2: OFFSET_INVALID,  # fresh
                3: 999_999,  # errored
            },
            errors={3: "UNKNOWN_TOPIC_OR_PARTITION"},
        )
        watermarks = {0: (100, 200), 1: (100, 200), 2: (100, 200), 3: (100, 200)}
        recovered = [TopicPartition("test-topic", 0, 200)]

        _seed_startup_gauges(cfg, committed, watermarks, recovered)

        # Collect per-partition .set() calls. Each .labels(partition=X) returns
        # the same mock, so we inspect call_args_list on the chained .set.
        lag_calls_by_partition = {}
        offset_calls_by_partition = {}
        for call in mock_metrics.consumer_lag.labels.call_args_list:
            lag_calls_by_partition[call.kwargs["partition"]] = None
        for call in mock_metrics.last_committed_offset.labels.call_args_list:
            offset_calls_by_partition[call.kwargs["partition"]] = None

        assert set(lag_calls_by_partition) == {"0", "1", "2", "3"}
        assert set(offset_calls_by_partition) == {"0", "1", "2", "3"}
