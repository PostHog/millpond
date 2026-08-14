from unittest.mock import MagicMock, patch

import duckdb
import orjson
import pyarrow as pa
import pytest

from millpond.ducklake import (
    _apply_variant_shred_settings,
    _ensure_table,
    _escape_libpq,
    _sanitize_setting_value,
    _table_exists,
    _validate_partition_expr,
    build_insert_select_sql,
    drop_variant_companion_columns,
    sanitize_variant_sources,
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
        assert 'try_cast(try_cast("properties" AS JSON) AS VARIANT)' in sql
        assert 'AS "properties_variant"' in sql
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

    def test_case_insensitive_source_match(self):
        # DuckDB resolves identifiers case-insensitively — a Properties batch
        # key feeds the same column as configured properties, so it must
        # dual-write. Alias uses the configured source's casing.
        sql = build_insert_select_sql(["Properties", "event"], ("properties",))
        assert 'try_cast(try_cast("Properties" AS JSON) AS VARIANT)' in sql
        assert 'AS "properties_variant"' in sql

    def test_case_variant_duplicates_project_companion_once(self):
        # Two batch keys that casefold to the same source must not emit two
        # companion aliases (duplicate alias would fail the INSERT).
        sql = build_insert_select_sql(["Properties", "properties"], ("properties",))
        assert sql.count('AS "properties_variant"') == 1


class TestDropVariantCompanionColumns:
    def test_no_config_unchanged(self):
        batch = pa.table({"properties": ["{}"], "properties_variant": ["x"]})
        out = drop_variant_companion_columns(batch, None)
        assert out.schema.names == ["properties", "properties_variant"]

    def test_drops_companion_keeps_source(self):
        batch = pa.table({"properties": ["{}"], "properties_variant": ["x"], "event": ["e"]})
        out = drop_variant_companion_columns(batch, ("properties",))
        assert out.schema.names == ["properties", "event"]
        assert out.column("properties").to_pylist() == ["{}"]

    def test_drops_orphan_companion_without_source(self):
        # Companion alone would otherwise evolve() as VARCHAR — strip it.
        batch = pa.table({"event": ["x"], "properties_variant": ["y"]})
        out = drop_variant_companion_columns(batch, ("properties",))
        assert out.schema.names == ["event"]

    def test_clean_batch_unchanged(self):
        batch = pa.table({"properties": ["{}"], "event": ["x"]})
        out = drop_variant_companion_columns(batch, ("properties",))
        assert out.schema.names == ["properties", "event"]

    def test_drops_case_variant_companion(self):
        # DuckDB identifiers are case-insensitive: PROPERTIES_VARIANT would
        # land in (and poison) the same catalog column as properties_variant.
        batch = pa.table({"properties": ["{}"], "PROPERTIES_VARIANT": ["x"], "Properties_Variant": ["y"]})
        out = drop_variant_companion_columns(batch, ("properties",))
        assert out.schema.names == ["properties"]


class TestSanitizeVariantSourcesKeepsEveryKey:
    def test_no_poison_digits_skips_parse(self):
        raw = '{"$browser": "Chrome", "width": 1920}'
        batch = pa.table({"properties": [raw]})
        out, src = sanitize_variant_sources(batch, ("properties",))
        assert src == {}
        assert out.schema.names == ["properties"]

    def test_poison_int_does_not_drop_siblings(self):
        poison = 9223372036854775999
        raw = orjson.dumps({"$n": poison, "width": 1, "custom": "x"}).decode()
        batch = pa.table({"properties": [raw]})
        out, src = sanitize_variant_sources(batch, ("properties",))
        assert "properties" in src
        rewritten = orjson.loads(out.column(src["properties"]).to_pylist()[0])
        assert rewritten == {"$n": str(poison), "width": 1, "custom": "x"}
        assert out.column("properties").to_pylist() == [raw]


class TestApplyVariantShredSettings:
    def test_unknown_setting_is_not_fatal(self):
        # Stock DuckDB 1.5.2 has no variant_shred_key_prefix. connect() must
        # still succeed so official wheels start; the warning is the signal.
        conn = duckdb.connect()
        _apply_variant_shred_settings(conn, "$", ("utm_source",))

    def test_unset_is_noop(self):
        conn = duckdb.connect()
        _apply_variant_shred_settings(conn, None, None)
        _apply_variant_shred_settings(conn, "", ())
