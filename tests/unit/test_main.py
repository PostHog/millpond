from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from confluent_kafka import KafkaException

from millpond.main import _convert_batch, _flush, _write_with_retry


class TestWriteWithRetry:
    def test_succeeds_first_try(self):
        db = MagicMock()
        schema_mgr = MagicMock()
        table = pa.table({"a": [1]})
        with patch("millpond.main.ducklake") as mock_dl:
            _write_with_retry(db, "test", table, schema_mgr)
            assert mock_dl.write.call_count == 1

    def test_retries_on_failure(self):
        db = MagicMock()
        schema_mgr = MagicMock()
        table = pa.table({"a": [1]})
        with patch("millpond.main.ducklake") as mock_dl, patch("millpond.main.time") as mock_time:
            mock_dl.write.side_effect = [OSError("S3 timeout"), None]
            _write_with_retry(db, "test", table, schema_mgr)
            assert mock_dl.write.call_count == 2
            mock_time.sleep.assert_called_once_with(1.0)

    def test_raises_after_max_retries(self):
        db = MagicMock()
        schema_mgr = MagicMock()
        table = pa.table({"a": [1]})
        with patch("millpond.main.ducklake") as mock_dl, patch("millpond.main.time"):
            mock_dl.write.side_effect = OSError("persistent failure")
            with pytest.raises(OSError):
                _write_with_retry(db, "test", table, schema_mgr)
            assert mock_dl.write.call_count == 3

    def test_exponential_backoff(self):
        db = MagicMock()
        schema_mgr = MagicMock()
        table = pa.table({"a": [1]})
        with patch("millpond.main.ducklake") as mock_dl, patch("millpond.main.time") as mock_time:
            mock_dl.write.side_effect = [OSError(), OSError(), None]
            _write_with_retry(db, "test", table, schema_mgr)
            calls = [c.args[0] for c in mock_time.sleep.call_args_list]
            assert calls == [1.0, 2.0]

    def test_invalidates_schema_on_retry(self):
        db = MagicMock()
        schema_mgr = MagicMock()
        table = pa.table({"a": [1]})
        with patch("millpond.main.ducklake") as mock_dl, patch("millpond.main.time"):
            mock_dl.write.side_effect = [OSError("schema conflict"), None]
            _write_with_retry(db, "test", table, schema_mgr)
            schema_mgr.invalidate.assert_called_once()


class TestFlushErrorDistinction:
    """Offset commit failures must be distinguishable from write failures in metrics and logs."""

    def _make_flush_args(self):
        db = MagicMock()
        cfg = MagicMock()
        cfg.ducklake_table = "test_table"
        kafka = MagicMock()
        table = pa.table({"a": [1, 2]})
        offsets = {("topic", 0): 42}
        schema_mgr = MagicMock()
        return db, cfg, kafka, table, offsets, schema_mgr

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    @patch("millpond.main.ducklake")
    def test_commit_failure_raises_after_retries(self, mock_dl, mock_metrics, mock_server, mock_time):
        mock_time.monotonic.return_value = 0.0
        db, cfg, kafka, table, offsets, schema_mgr = self._make_flush_args()
        kafka.commit.side_effect = KafkaException("broker unavailable")

        with pytest.raises(KafkaException):
            _flush(db, cfg, kafka, table, 100, 2, offsets, 1.0, schema_mgr)

        assert kafka.commit.call_count == 3
        # Each failed attempt increments the offset_commit error counter
        commit_calls = [
            c for c in mock_metrics.errors_total.labels.call_args_list if c.kwargs.get("type") == "offset_commit"
        ]
        assert len(commit_calls) == 3

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    @patch("millpond.main.ducklake")
    def test_commit_succeeds_after_retry(self, mock_dl, mock_metrics, mock_server, mock_time):
        mock_time.monotonic.return_value = 0.0
        db, cfg, kafka, table, offsets, schema_mgr = self._make_flush_args()
        kafka.commit.side_effect = [KafkaException("transient"), None]

        # Should not raise — commit succeeds on second attempt
        _flush(db, cfg, kafka, table, 100, 2, offsets, 1.0, schema_mgr)
        assert kafka.commit.call_count == 2

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    @patch("millpond.main.ducklake")
    def test_commit_retry_exponential_backoff(self, mock_dl, mock_metrics, mock_server, mock_time):
        mock_time.monotonic.return_value = 0.0
        db, cfg, kafka, table, offsets, schema_mgr = self._make_flush_args()
        kafka.commit.side_effect = [KafkaException("fail"), KafkaException("fail"), None]

        _flush(db, cfg, kafka, table, 100, 2, offsets, 1.0, schema_mgr)
        delays = [c.args[0] for c in mock_time.sleep.call_args_list]
        assert delays == [0.5, 1.0]

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    @patch("millpond.main.ducklake")
    def test_write_failure_does_not_increment_offset_commit_error(self, mock_dl, mock_metrics, mock_server, mock_time):
        db, cfg, kafka, table, offsets, schema_mgr = self._make_flush_args()
        mock_dl.write.side_effect = OSError("S3 timeout")

        with pytest.raises(OSError):
            _flush(db, cfg, kafka, table, 100, 2, offsets, 1.0, schema_mgr)

        # offset_commit error should NOT have been incremented
        commit_calls = [
            c for c in mock_metrics.errors_total.labels.call_args_list if c.kwargs.get("type") == "offset_commit"
        ]
        assert len(commit_calls) == 0

    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    @patch("millpond.main.ducklake")
    def test_successful_flush_records_write_metrics(self, mock_dl, mock_metrics, mock_server):
        db, cfg, kafka, table, offsets, schema_mgr = self._make_flush_args()

        _flush(db, cfg, kafka, table, 100, 2, offsets, 1.0, schema_mgr, trigger="size")

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
