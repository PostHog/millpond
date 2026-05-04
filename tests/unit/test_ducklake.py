from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pytest

from millpond.ducklake import (
    _ensure_table,
    _escape_libpq,
    _sanitize_setting_value,
    _table_exists,
    _tables_ensured,
    _validate_partition_expr,
    reset_table_cache,
)


class TestSanitizeSettingValue:
    def test_plain_value(self):
        assert _sanitize_setting_value("us-east-1") == "us-east-1"

    def test_sql_injection_rejected(self):
        with pytest.raises(ValueError, match="Illegal character"):
            _sanitize_setting_value("us-east-1'; DROP TABLE x; --")

    def test_single_quote_rejected(self):
        with pytest.raises(ValueError, match="Illegal character"):
            _sanitize_setting_value("it's")

    def test_normal_s3_values(self):
        assert _sanitize_setting_value("minioadmin") == "minioadmin"
        assert _sanitize_setting_value("false") == "false"
        assert _sanitize_setting_value("path") == "path"
        assert _sanitize_setting_value("minio:9000") == "minio:9000"

    def test_url_style_values(self):
        assert _sanitize_setting_value("s3.amazonaws.com") == "s3.amazonaws.com"

    def test_access_key_with_slashes(self):
        assert _sanitize_setting_value("ABC/def123+key") == "ABC/def123+key"

    def test_base64_padding(self):
        assert _sanitize_setting_value("abc123==") == "abc123=="

    def test_empty_rejected(self):
        with pytest.raises(ValueError, match="Illegal character"):
            _sanitize_setting_value("")


class TestValidatePartitionExpr:
    def test_simple_column(self):
        assert _validate_partition_expr("region") == "region"

    def test_temporal_functions(self):
        assert _validate_partition_expr("year(ts),month(ts),day(ts),hour(ts)") == "year(ts),month(ts),day(ts),hour(ts)"

    def test_mixed(self):
        assert _validate_partition_expr("team_id,year(timestamp)") == "team_id,year(timestamp)"

    def test_spaces_around_commas(self):
        assert _validate_partition_expr("year(ts), month(ts)") == "year(ts), month(ts)"

    def test_sql_injection_rejected(self):
        with pytest.raises(ValueError, match="unsafe"):
            _validate_partition_expr("year(ts); DROP TABLE x")

    def test_comment_injection_rejected(self):
        with pytest.raises(ValueError, match="unsafe"):
            _validate_partition_expr("year(ts) -- comment")

    def test_quoted_string_rejected(self):
        with pytest.raises(ValueError, match="unsafe"):
            _validate_partition_expr("'malicious'")


class TestEscapeLibpq:
    """libpq connstring grammar: backslash escapes, NOT SQL-style doubled quotes."""

    def test_plain_value(self):
        assert _escape_libpq("ducklake") == "'ducklake'"

    def test_single_quote(self):
        assert _escape_libpq("pass'word") == "'pass\\'word'"

    def test_backslash(self):
        assert _escape_libpq("pass\\word") == "'pass\\\\word'"

    def test_both(self):
        assert _escape_libpq("it's\\complex") == "'it\\'s\\\\complex'"

    def test_none(self):
        assert _escape_libpq(None) == "''"


class TestResetTableCache:
    def test_reset_clears_ensured_tables(self):
        _tables_ensured.add("test_table")
        assert "test_table" in _tables_ensured
        reset_table_cache()
        assert len(_tables_ensured) == 0


@pytest.fixture(autouse=True)
def _clear_table_cache():
    """Ensure table cache is clean before and after each test."""
    reset_table_cache()
    yield
    reset_table_cache()


def _sample_batch() -> pa.Table:
    return pa.table({"col_a": [1], "col_b": ["x"]})


class TestTableExists:
    def test_returns_true_when_table_found(self):
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (1,)
        assert _table_exists(conn, "events") is True

    def test_returns_false_when_no_table(self):
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = None
        assert _table_exists(conn, "events") is False


class TestEnsureTable:
    def test_skips_if_cached(self):
        conn = MagicMock()
        _tables_ensured.add("events")
        _ensure_table(conn, "events", _sample_batch())
        conn.execute.assert_not_called()

    @patch("millpond.ducklake._table_exists", return_value=True)
    def test_skips_create_if_table_exists(self, mock_exists):
        conn = MagicMock()
        _ensure_table(conn, "events", _sample_batch())
        conn.execute.assert_not_called()
        assert "events" in _tables_ensured

    @patch("millpond.ducklake._table_exists", return_value=False)
    def test_creates_table_and_caches(self, mock_exists):
        conn = MagicMock()
        _ensure_table(conn, "events", _sample_batch())
        # Should have called register, execute (CREATE), unregister
        assert conn.execute.call_count >= 1
        create_sql = conn.execute.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in create_sql
        assert "events" in _tables_ensured

    @patch("millpond.ducklake._table_exists", return_value=False)
    def test_creates_table_with_partitioning(self, mock_exists):
        conn = MagicMock()
        _ensure_table(conn, "events", _sample_batch(), partition_by="team_id,year(ts)")
        # Find the ALTER PARTITIONED BY call
        alter_calls = [
            call for call in conn.execute.call_args_list if "PARTITIONED BY" in str(call)
        ]
        assert len(alter_calls) == 1
        assert "team_id,year(ts)" in str(alter_calls[0])

    @patch("millpond.ducklake._table_exists")
    def test_concurrent_create_recovers(self, mock_exists):
        """If CREATE fails but another pod created the table, continue."""
        # First call: table doesn't exist. Second call (after error): table exists.
        mock_exists.side_effect = [False, True]
        conn = MagicMock()
        conn.register = MagicMock()
        conn.unregister = MagicMock()
        conn.execute.side_effect = duckdb.Error("serialization conflict")
        _ensure_table(conn, "events", _sample_batch())
        assert "events" in _tables_ensured

    @patch("millpond.ducklake._table_exists")
    def test_create_fails_and_table_still_missing_raises(self, mock_exists):
        """If CREATE fails and table doesn't exist, raise."""
        mock_exists.return_value = False
        conn = MagicMock()
        conn.register = MagicMock()
        conn.unregister = MagicMock()
        conn.execute.side_effect = duckdb.Error("connection lost")
        with pytest.raises(RuntimeError, match="Failed to create table"):
            _ensure_table(conn, "events", _sample_batch())

    @patch("millpond.ducklake._table_exists")
    def test_concurrent_partition_alter_recovers(self, mock_exists):
        """If ALTER PARTITIONED BY fails but table exists, continue."""
        mock_exists.side_effect = [False, True]
        conn = MagicMock()
        conn.register = MagicMock()
        conn.unregister = MagicMock()

        def execute_side_effect(sql, *args):
            if "PARTITIONED BY" in sql:
                raise duckdb.Error("serialization conflict")

        conn.execute.side_effect = execute_side_effect
        _ensure_table(conn, "events", _sample_batch(), partition_by="team_id")
        assert "events" in _tables_ensured
