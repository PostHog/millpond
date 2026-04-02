from unittest.mock import patch

from millpond.backpressure import MIN_BATCH_SIZE, compute_batch_size, init


class TestComputeBatchSize:
    def setup_method(self):
        init(1000)

    @patch("millpond.backpressure.metrics")
    def test_empty_buffer_returns_max(self, mock_metrics):
        assert compute_batch_size(0, 100_000_000) == 1000

    @patch("millpond.backpressure.metrics")
    def test_half_full_returns_half_max(self, mock_metrics):
        assert compute_batch_size(50_000_000, 100_000_000) == 500

    @patch("millpond.backpressure.metrics")
    def test_75_percent_returns_quarter_max(self, mock_metrics):
        assert compute_batch_size(75_000_000, 100_000_000) == 250

    @patch("millpond.backpressure.metrics")
    def test_full_buffer_returns_min(self, mock_metrics):
        result = compute_batch_size(100_000_000, 100_000_000)
        assert result == MIN_BATCH_SIZE

    @patch("millpond.backpressure.metrics")
    def test_over_full_returns_min(self, mock_metrics):
        result = compute_batch_size(150_000_000, 100_000_000)
        assert result == MIN_BATCH_SIZE

    @patch("millpond.backpressure.metrics")
    def test_near_empty_returns_near_max(self, mock_metrics):
        result = compute_batch_size(1_000_000, 100_000_000)
        assert result == 990

    @patch("millpond.backpressure.metrics")
    def test_zero_flush_size_returns_max(self, mock_metrics):
        assert compute_batch_size(50_000_000, 0) == 1000

    @patch("millpond.backpressure.metrics")
    def test_negative_flush_size_returns_max(self, mock_metrics):
        assert compute_batch_size(50_000_000, -1) == 1000

    @patch("millpond.backpressure.metrics")
    def test_never_below_min(self, mock_metrics):
        # Even with massive overfill
        result = compute_batch_size(999_999_999, 100)
        assert result == MIN_BATCH_SIZE

    @patch("millpond.backpressure.metrics")
    def test_respects_custom_max(self, mock_metrics):
        init(500)
        assert compute_batch_size(0, 100_000_000) == 500
        assert compute_batch_size(50_000_000, 100_000_000) == 250

    @patch("millpond.backpressure.metrics")
    def test_max_below_min_clamped(self, mock_metrics):
        init(5)
        assert compute_batch_size(0, 100_000_000) == MIN_BATCH_SIZE

    @patch("millpond.backpressure.metrics")
    def test_sets_fullness_metric(self, mock_metrics):
        compute_batch_size(25_000_000, 100_000_000)
        mock_metrics.buffer_fullness.set.assert_called_with(0.25)

    @patch("millpond.backpressure.metrics")
    def test_sets_batch_size_metric(self, mock_metrics):
        result = compute_batch_size(25_000_000, 100_000_000)
        mock_metrics.consume_batch_size_current.set.assert_called_with(result)

    @patch("millpond.backpressure.metrics")
    def test_fullness_rounds_to_3_decimals(self, mock_metrics):
        compute_batch_size(33_333_333, 100_000_000)
        mock_metrics.buffer_fullness.set.assert_called_with(0.333)

    @patch("millpond.backpressure.metrics")
    def test_linear_scaling(self, mock_metrics):
        """Verify the response is linear across the range."""
        init(1000)
        results = []
        for pct in range(0, 101, 10):
            pending = int(100_000_000 * pct / 100)
            results.append(compute_batch_size(pending, 100_000_000))

        # Should be monotonically decreasing
        for i in range(len(results) - 1):
            assert results[i] >= results[i + 1], f"Not monotonic at {i}: {results}"

        # First should be max, last should be min
        assert results[0] == 1000
        assert results[-1] == MIN_BATCH_SIZE
