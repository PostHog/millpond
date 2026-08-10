from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pytest

from millpond.ducklake import (
    _ensure_table,
    _escape_libpq,
    _sanitize_setting_value,
    _table_exists,
    _validate_partition_expr,
    build_insert_select_sql,
    check_variant_column_collision,
    variant_column_name,
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


def _sample_batch() -> pa.Table:
    return pa.table({"col_a": [1], "col_b": ["x"]})


@pytest.fixture
def cache() -> set[str]:
    """A fresh caller-owned ensure cache, one per test."""
    return set()


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
    def test_skips_if_cached(self, cache):
        conn = MagicMock()
        cache.add("events")
        _ensure_table(conn, "events", _sample_batch(), cache)
        conn.execute.assert_not_called()

    @patch("millpond.ducklake._table_exists", return_value=True)
    def test_skips_create_if_table_exists(self, mock_exists, cache):
        conn = MagicMock()
        _ensure_table(conn, "events", _sample_batch(), cache)
        conn.execute.assert_not_called()
        assert "events" in cache

    @patch("millpond.ducklake._table_exists", return_value=False)
    def test_creates_table_and_caches(self, mock_exists, cache):
        conn = MagicMock()
        _ensure_table(conn, "events", _sample_batch(), cache)
        # Should have called register, execute (CREATE), unregister
        assert conn.execute.call_count >= 1
        create_sql = conn.execute.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in create_sql
        assert "events" in cache

    @patch("millpond.ducklake._table_exists", return_value=False)
    def test_creates_table_with_partitioning(self, mock_exists, cache):
        conn = MagicMock()
        _ensure_table(conn, "events", _sample_batch(), cache, partition_by="team_id,year(ts)")
        # Find the ALTER PARTITIONED BY call
        alter_calls = [call for call in conn.execute.call_args_list if "PARTITIONED BY" in str(call)]
        assert len(alter_calls) == 1
        assert "team_id,year(ts)" in str(alter_calls[0])

    @patch("millpond.ducklake._table_exists")
    def test_concurrent_create_recovers(self, mock_exists, cache):
        """If CREATE fails but another pod created the table, continue."""
        # First call: table doesn't exist. Second call (after error): table exists.
        mock_exists.side_effect = [False, True]
        conn = MagicMock()
        conn.register = MagicMock()
        conn.unregister = MagicMock()
        conn.execute.side_effect = duckdb.Error("serialization conflict")
        _ensure_table(conn, "events", _sample_batch(), cache)
        assert "events" in cache

    @patch("millpond.ducklake._table_exists")
    def test_create_fails_and_table_still_missing_raises(self, mock_exists, cache):
        """If CREATE fails and table doesn't exist, raise."""
        mock_exists.return_value = False
        conn = MagicMock()
        conn.register = MagicMock()
        conn.unregister = MagicMock()
        conn.execute.side_effect = duckdb.Error("connection lost")
        with pytest.raises(RuntimeError, match="Failed to create table"):
            _ensure_table(conn, "events", _sample_batch(), cache)

    @patch("millpond.ducklake._table_exists")
    def test_concurrent_partition_alter_recovers(self, mock_exists, cache):
        """If ALTER PARTITIONED BY fails but table exists, continue."""
        mock_exists.side_effect = [False, True]
        conn = MagicMock()
        conn.register = MagicMock()
        conn.unregister = MagicMock()

        def execute_side_effect(sql, *args):
            if "PARTITIONED BY" in sql:
                raise duckdb.Error("serialization conflict")

        conn.execute.side_effect = execute_side_effect
        _ensure_table(conn, "events", _sample_batch(), cache, partition_by="team_id")
        assert "events" in cache


class TestVariantColumnName:
    def test_suffix(self):
        assert variant_column_name("properties") == "properties_variant"

    def test_person_properties(self):
        assert variant_column_name("person_properties") == "person_properties_variant"


class TestBuildInsertSelectSql:
    def test_no_variant_columns_uses_star(self):
        # Opt-in dual-write must not rewrite every writer's INSERT.
        sql = build_insert_select_sql(["event", "team_id"], None)
        assert sql == "*, NOW() AS _inserted_at"

    def test_empty_variant_tuple_uses_star(self):
        sql = build_insert_select_sql(["event"], ())
        assert sql == "*, NOW() AS _inserted_at"

    def test_configured_source_absent_from_batch_uses_star(self):
        sql = build_insert_select_sql(["event"], ("properties",))
        assert "properties_variant" not in sql
        assert sql == "*, NOW() AS _inserted_at"

    def test_dual_writes_configured_source(self):
        sql = build_insert_select_sql(
            ["event", "properties", "team_id"],
            ("properties",),
        )
        assert '"properties"' in sql
        assert 'try_cast(try_cast("properties" AS JSON) AS VARIANT) AS "properties_variant"' in sql
        assert sql.endswith("NOW() AS _inserted_at")
        # Original column still present (dual-write, not replace).
        assert sql.index('"properties"') < sql.index("properties_variant")
        # Not the star form when dual-write is active.
        assert not sql.startswith("*")

    def test_multiple_sources(self):
        sql = build_insert_select_sql(
            ["properties", "person_properties"],
            ("properties", "person_properties"),
        )
        assert 'AS "properties_variant"' in sql
        assert 'AS "person_properties_variant"' in sql

    def test_escapes_embedded_quotes_in_identifiers(self):
        sql = build_insert_select_sql(['weir"d', "properties"], ("properties",))
        assert '"weir""d"' in sql


class TestCheckVariantColumnCollision:
    def test_no_config_ok(self):
        batch = pa.table({"properties": ["{}"], "properties_variant": ["x"]})
        check_variant_column_collision(batch.schema, None)  # no raise

    def test_collision_raises(self):
        batch = pa.table({"properties": ["{}"], "properties_variant": ["x"]})
        with pytest.raises(ValueError, match="properties_variant"):
            check_variant_column_collision(batch.schema, ("properties",))

    def test_no_collision_when_source_absent(self):
        # Batch has the derived name but not the source — dual-write is skipped
        # for that source, so no collision to report.
        batch = pa.table({"event": ["x"], "properties_variant": ["y"]})
        check_variant_column_collision(batch.schema, ("properties",))

    def test_clean_batch_ok(self):
        batch = pa.table({"properties": ["{}"], "event": ["x"]})
        check_variant_column_collision(batch.schema, ("properties",))
