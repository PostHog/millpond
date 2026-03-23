from unittest.mock import MagicMock, patch

import pyarrow as pa
from confluent_kafka import KafkaException

from millpond.main import _flush, _write_with_retry


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
            try:
                _write_with_retry(db, "test", table, schema_mgr)
                assert False, "Should have raised"
            except OSError:
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

    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    @patch("millpond.main.ducklake")
    def test_commit_failure_increments_offset_commit_error(self, mock_dl, mock_metrics, mock_server):
        db, cfg, kafka, table, offsets, schema_mgr = self._make_flush_args()
        kafka.commit.side_effect = KafkaException("broker unavailable")

        try:
            _flush(db, cfg, kafka, table, 100, 2, offsets, 1.0, schema_mgr)
            assert False, "Should have raised"
        except KafkaException:
            pass

        mock_metrics.errors_total.labels.assert_called_with(type="offset_commit")
        mock_metrics.errors_total.labels(type="offset_commit").inc.assert_called_once()

    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    @patch("millpond.main.ducklake")
    def test_write_failure_does_not_increment_offset_commit_error(self, mock_dl, mock_metrics, mock_server):
        db, cfg, kafka, table, offsets, schema_mgr = self._make_flush_args()
        mock_dl.write.side_effect = OSError("S3 timeout")

        try:
            _flush(db, cfg, kafka, table, 100, 2, offsets, 1.0, schema_mgr)
            assert False, "Should have raised"
        except OSError:
            pass

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

        _flush(db, cfg, kafka, table, 100, 2, offsets, 1.0, schema_mgr)

        mock_metrics.records_written_total.inc.assert_called_once_with(2)
        mock_metrics.batches_flushed_total.inc.assert_called_once()
