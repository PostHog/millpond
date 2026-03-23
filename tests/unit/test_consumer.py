import json
from unittest.mock import MagicMock, patch

from millpond.consumer import _on_stats, compute_assignment


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
