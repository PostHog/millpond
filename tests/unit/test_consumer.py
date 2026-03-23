import json
from unittest.mock import patch

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
        _on_stats(self.STATS_BLOB)
        # Collect all set() calls via the shared mock — labels() returns the same
        # mock regardless of args, so we check the values passed to set() directly.
        avg_values = [c.args[0] for c in mock_metrics.rdkafka_broker_rtt_avg.labels().set.call_args_list]
        p99_values = [c.args[0] for c in mock_metrics.rdkafka_broker_rtt_p99.labels().set.call_args_list]
        assert sorted(avg_values) == [0.003, 0.005]
        assert sorted(p99_values) == [0.008, 0.012]

    @patch("millpond.consumer.metrics")
    def test_malformed_json_ignored(self, mock_metrics):
        _on_stats("not json")
        # Should not raise, gauges should not be called
        mock_metrics.rdkafka_replyq.set.assert_not_called()
