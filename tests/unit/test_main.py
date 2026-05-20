from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pytest
from confluent_kafka import KafkaException
from pyiceberg.exceptions import (
    CommitFailedException,
    CommitStateUnknownException,
    ServerError,
    ServiceUnavailableError,
)

from millpond.main import _convert_batch, _flush, _write_with_retry


def _make_sink() -> MagicMock:
    """Mock Sink — just needs `write`, `reset_caches`, and `close`."""
    return MagicMock(spec=["write", "reset_caches", "close"])


# Realistic failure modes the broad `except Exception` in _write_with_retry
# must catch. If somebody narrows that handler later, these parametrizations
# fail in CI before the regression reaches production. Includes:
#   - OSError: S3 / network
#   - duckdb.Error: DuckLake local execution
#   - CommitFailedException: Iceberg optimistic-concurrency conflict
#   - CommitStateUnknownException: REST 5xx after commit submission
#   - ServerError / ServiceUnavailableError: catalog 5xx, transient
#   - KafkaException: broker disconnect during a write (rare but possible
#     if the same Kafka client surfaces an error mid-flush)
#   - RuntimeError: catch-all for backend-internal surprises
_RETRYABLE_EXCEPTIONS = [
    OSError("S3 timeout"),
    duckdb.Error("serialization conflict"),
    CommitFailedException("optimistic-concurrency clash"),
    CommitStateUnknownException("REST 5xx after submit"),
    ServerError("catalog 500"),
    ServiceUnavailableError("catalog 503"),
    KafkaException("broker disconnect"),
    RuntimeError("unexpected"),
]


class TestWriteWithRetry:
    def test_succeeds_first_try(self):
        sink = _make_sink()
        table = pa.table({"a": [1]})
        _write_with_retry(sink, table)
        assert sink.write.call_count == 1
        sink.reset_caches.assert_not_called()

    @pytest.mark.parametrize("exc", _RETRYABLE_EXCEPTIONS)
    def test_retries_on_failure(self, exc):
        sink = _make_sink()
        sink.write.side_effect = [exc, None]
        table = pa.table({"a": [1]})
        with patch("millpond.main.time") as mock_time:
            _write_with_retry(sink, table)
        assert sink.write.call_count == 2
        mock_time.sleep.assert_called_once_with(1.0)

    @pytest.mark.parametrize(
        "exc_cls",
        [
            OSError,
            duckdb.Error,
            CommitFailedException,
            CommitStateUnknownException,
            ServerError,
            ServiceUnavailableError,
            KafkaException,
            RuntimeError,
        ],
    )
    def test_raises_after_max_retries(self, exc_cls):
        sink = _make_sink()
        sink.write.side_effect = exc_cls("persistent failure")
        table = pa.table({"a": [1]})
        with patch("millpond.main.time"), pytest.raises(exc_cls):
            _write_with_retry(sink, table)
        assert sink.write.call_count == 3

    def test_exponential_backoff(self):
        sink = _make_sink()
        sink.write.side_effect = [OSError(), OSError(), None]
        table = pa.table({"a": [1]})
        with patch("millpond.main.time") as mock_time:
            _write_with_retry(sink, table)
        calls = [c.args[0] for c in mock_time.sleep.call_args_list]
        assert calls == [1.0, 2.0]

    @pytest.mark.parametrize("exc", _RETRYABLE_EXCEPTIONS)
    def test_resets_caches_on_retry(self, exc):
        sink = _make_sink()
        sink.write.side_effect = [exc, None]
        table = pa.table({"a": [1]})
        with patch("millpond.main.time"):
            _write_with_retry(sink, table)
        sink.reset_caches.assert_called_once()


class TestFlushErrorDistinction:
    """Offset commit failures must be distinguishable from write failures in metrics and logs."""

    def _make_flush_args(self):
        sink = _make_sink()
        cfg = MagicMock()
        cfg.table_label = "test_table"
        kafka = MagicMock()
        table = pa.table({"a": [1, 2]})
        offsets = {("topic", 0): 42}
        return sink, cfg, kafka, table, offsets

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    def test_commit_failure_raises_after_retries(self, mock_metrics, mock_server, mock_time):
        mock_time.monotonic.return_value = 0.0
        sink, cfg, kafka, table, offsets = self._make_flush_args()
        kafka.commit.side_effect = KafkaException("broker unavailable")

        with pytest.raises(KafkaException):
            _flush(sink, cfg, kafka, table, 100, 2, offsets, 1.0)

        assert kafka.commit.call_count == 3
        # Each failed attempt increments the offset_commit error counter
        commit_calls = [
            c for c in mock_metrics.errors_total.labels.call_args_list if c.kwargs.get("type") == "offset_commit"
        ]
        assert len(commit_calls) == 3

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    def test_commit_succeeds_after_retry(self, mock_metrics, mock_server, mock_time):
        mock_time.monotonic.return_value = 0.0
        sink, cfg, kafka, table, offsets = self._make_flush_args()
        kafka.commit.side_effect = [KafkaException("transient"), None]

        # Should not raise — commit succeeds on second attempt
        _flush(sink, cfg, kafka, table, 100, 2, offsets, 1.0)
        assert kafka.commit.call_count == 2

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    def test_commit_retry_exponential_backoff(self, mock_metrics, mock_server, mock_time):
        mock_time.monotonic.return_value = 0.0
        sink, cfg, kafka, table, offsets = self._make_flush_args()
        kafka.commit.side_effect = [KafkaException("fail"), KafkaException("fail"), None]

        _flush(sink, cfg, kafka, table, 100, 2, offsets, 1.0)
        delays = [c.args[0] for c in mock_time.sleep.call_args_list]
        assert delays == [0.5, 1.0]

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    def test_write_failure_does_not_increment_offset_commit_error(self, mock_metrics, mock_server, mock_time):
        sink, cfg, kafka, table, offsets = self._make_flush_args()
        sink.write.side_effect = OSError("S3 timeout")

        with pytest.raises(OSError):
            _flush(sink, cfg, kafka, table, 100, 2, offsets, 1.0)

        # offset_commit error should NOT have been incremented
        commit_calls = [
            c for c in mock_metrics.errors_total.labels.call_args_list if c.kwargs.get("type") == "offset_commit"
        ]
        assert len(commit_calls) == 0

    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    def test_successful_flush_records_write_metrics(self, mock_metrics, mock_server):
        sink, cfg, kafka, table, offsets = self._make_flush_args()

        _flush(sink, cfg, kafka, table, 100, 2, offsets, 1.0, trigger="size")

        mock_metrics.records_written_total.inc.assert_called_once_with(2)
        mock_metrics.batches_flushed_total.labels.assert_called_once_with(trigger="size")
        mock_metrics.batches_flushed_total.labels.return_value.inc.assert_called_once()


class TestArrowConversionTiming:
    """Arrow conversion time should be tracked via a histogram metric."""

    @patch("millpond.main.metrics")
    @patch("millpond.main.arrow_converter")
    def test_conversion_time_observed(self, mock_converter, mock_metrics):
        """convert() duration should be observed on the histogram."""
        mock_converter.convert.return_value = pa.table({"a": [1]})

        table = _convert_batch([b'{"a": 1}'])
        assert table is not None
        mock_metrics.arrow_conversion_seconds.observe.assert_called_once()
        observed = mock_metrics.arrow_conversion_seconds.observe.call_args[0][0]
        assert isinstance(observed, float)
        assert observed >= 0

    @patch("millpond.main.metrics")
    @patch("millpond.main.arrow_converter")
    def test_conversion_time_not_observed_when_none(self, mock_converter, mock_metrics):
        """If convert() returns None, no timing should be observed."""
        mock_converter.convert.return_value = None

        table = _convert_batch([b'garbage'])
        assert table is None
        mock_metrics.arrow_conversion_seconds.observe.assert_not_called()
