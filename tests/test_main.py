from unittest.mock import MagicMock, patch

import pyarrow as pa

from millpond.main import _write_with_retry


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
