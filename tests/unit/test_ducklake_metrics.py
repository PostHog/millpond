"""Unit tests for tools/ducklake_metrics.py.

Coverage:
  * YAML loader: name/type validation, missing-key errors, user override.
  * parse_interval: m-only suffix, 1m floor.
  * _run_query end-to-end against an in-memory DuckDB so the
    column-name-resolve, gauge clear-and-set, and error-trap paths are
    exercised without standing up a real lake.
"""

from __future__ import annotations

import duckdb
import ducklake_metrics as dm
import pytest
from prometheus_client import CollectorRegistry

# ---------------------------------------------------------------------------
# parse_interval
# ---------------------------------------------------------------------------


class TestParseInterval:
    def test_one_minute(self):
        assert dm.parse_interval("1m") == 60

    def test_arbitrary_minutes(self):
        assert dm.parse_interval("5m") == 300
        assert dm.parse_interval("60m") == 3600

    def test_seconds_rejected(self):
        with pytest.raises(ValueError, match="whole minutes"):
            dm.parse_interval("30s")

    def test_zero_rejected(self):
        with pytest.raises(ValueError, match=">= 1m"):
            dm.parse_interval("0m")

    def test_naked_number_rejected(self):
        with pytest.raises(ValueError, match="whole minutes"):
            dm.parse_interval("60")

    def test_hours_rejected(self):
        # only 'm' suffix supported (D9 in the plan)
        with pytest.raises(ValueError, match="whole minutes"):
            dm.parse_interval("1h")


# ---------------------------------------------------------------------------
# YAML loader
# ---------------------------------------------------------------------------


class TestLoadQueries:
    def test_builtin_only(self):
        queries = dm.load_queries(None, set())
        names = [q.name for q in queries]
        assert "ducklake_pending_deletes" in names

    def test_disable_drops_builtin(self):
        queries = dm.load_queries(None, {"ducklake_pending_deletes"})
        assert all(q.name != "ducklake_pending_deletes" for q in queries)

    def test_user_yaml_extends(self, tmp_path):
        p = tmp_path / "user.yaml"
        p.write_text(
            "queries:\n"
            "  - name: my_custom\n"
            "    help: Custom counter\n"
            "    interval: 2m\n"
            "    values: [n]\n"
            "    sql: SELECT 1 AS n\n"
        )
        queries = dm.load_queries(str(p), set())
        names = [q.name for q in queries]
        assert "my_custom" in names
        assert "ducklake_pending_deletes" in names

    def test_user_yaml_overrides_builtin(self, tmp_path):
        p = tmp_path / "user.yaml"
        p.write_text(
            "queries:\n"
            "  - name: ducklake_pending_deletes\n"
            "    help: overridden\n"
            "    interval: 5m\n"
            "    values: [total]\n"
            "    sql: SELECT 0 AS total\n"
        )
        queries = dm.load_queries(str(p), set())
        q = next(q for q in queries if q.name == "ducklake_pending_deletes")
        assert q.help == "overridden"
        assert q.interval_seconds == 300

    def test_missing_required_key(self):
        with pytest.raises(ValueError, match="missing required key"):
            dm._query_from_dict({"name": "x", "help": "h", "interval": "1m"}, "test")

    def test_bad_name_rejected(self):
        with pytest.raises(ValueError, match="must match"):
            dm._query_from_dict(
                {"name": "1bad-name", "help": "h", "interval": "1m", "sql": "SELECT 1"},
                "test",
            )

    def test_top_level_must_have_queries_key(self):
        with pytest.raises(ValueError, match="'queries' key"):
            dm._load_yaml_doc("foo: bar\n", "test")

    def test_queries_must_be_list(self):
        with pytest.raises(ValueError, match="must be a list"):
            dm._load_yaml_doc("queries: 'not a list'\n", "test")

    def test_empty_queries_list(self):
        assert dm._load_yaml_doc("queries:\n", "test") == []


# ---------------------------------------------------------------------------
# _run_query end-to-end (in-memory duckdb, isolated registry)
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    c = duckdb.connect()
    yield c
    c.close()


@pytest.fixture
def registry():
    return CollectorRegistry()


def _gauge_value(reg: CollectorRegistry, metric: str, labels: dict | None = None) -> float | None:
    return reg.get_sample_value(metric, labels or {})


class TestRunQuery:
    def test_label_less_three_values(self, conn, registry):
        # Mirrors the b4 query shape: one row, three values, no labels.
        conn.execute("CREATE TABLE q (path TEXT)")
        conn.execute("INSERT INTO q VALUES ('a'), ('a'), ('b')")
        q = dm.Query(
            name="t_pending",
            help="t",
            sql=(
                "SELECT COUNT(*) AS total, COUNT(DISTINCT path) AS unique_paths, "
                "COUNT(*) - COUNT(DISTINCT path) AS dup_rows FROM q"
            ),
            interval_seconds=60,
            labels=[],
            values=["total", "unique_paths", "dup_rows"],
        )
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)

        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "t_pending_total") == 3
        assert _gauge_value(registry, "t_pending_unique_paths") == 2
        assert _gauge_value(registry, "t_pending_dup_rows") == 1
        # Self-metrics: success path advances duration + last_success, leaves errors at 0.
        assert _gauge_value(registry, "ducklake_metrics_query_duration_seconds", {"query": "t_pending"}) is not None
        assert _gauge_value(registry, "ducklake_metrics_query_last_success_timestamp", {"query": "t_pending"}) > 0
        assert (
            _gauge_value(registry, "ducklake_metrics_query_errors_total", {"query": "t_pending"}) is None
            or _gauge_value(registry, "ducklake_metrics_query_errors_total", {"query": "t_pending"}) == 0
        )

    def test_with_labels_clears_stale_combinations(self, conn, registry):
        # Two ticks: first has bands (a, b), second only (a). The (b)
        # series must disappear after clear() — that's what the
        # plan's "Gauge.clear() then re-populate" line guards against.
        q = dm.Query(
            name="t_per_band",
            help="t",
            sql="SELECT band, n FROM bands",
            interval_seconds=60,
            labels=["band"],
            values=["n"],
        )
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)

        conn.execute("CREATE TABLE bands (band TEXT, n BIGINT)")
        conn.execute("INSERT INTO bands VALUES ('a', 10), ('b', 20)")
        dm._run_query(conn, q, gauges[q.name], sm)
        assert _gauge_value(registry, "t_per_band_n", {"band": "a"}) == 10
        assert _gauge_value(registry, "t_per_band_n", {"band": "b"}) == 20

        conn.execute("DELETE FROM bands WHERE band = 'b'")
        dm._run_query(conn, q, gauges[q.name], sm)
        assert _gauge_value(registry, "t_per_band_n", {"band": "a"}) == 10
        assert _gauge_value(registry, "t_per_band_n", {"band": "b"}) is None

    def test_sql_error_increments_counter(self, conn, registry):
        q = dm.Query(
            name="t_broken",
            help="t",
            sql="SELECT * FROM table_that_does_not_exist",
            interval_seconds=60,
            labels=[],
            values=["n"],
        )
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)

        # Must not raise — daemon stays up across catalog flap.
        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "ducklake_metrics_query_errors_total", {"query": "t_broken"}) == 1
        # last_success not set on failure.
        assert (
            _gauge_value(registry, "ducklake_metrics_query_last_success_timestamp", {"query": "t_broken"}) is None
        )

    def test_column_name_mismatch_raises_through_error_path(self, conn, registry):
        # SQL columns must include every name listed in labels+values.
        # Mismatch is caught and logged via the error-counter path, not
        # re-raised — same pathway as a SQL exception.
        q = dm.Query(
            name="t_mismatch",
            help="t",
            sql="SELECT 1 AS wrong_name",
            interval_seconds=60,
            labels=[],
            values=["n"],
        )
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)

        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "ducklake_metrics_query_errors_total", {"query": "t_mismatch"}) == 1


# ---------------------------------------------------------------------------
# Built-in: ducklake_pending_deletes — validate the SQL shape against an
# in-memory stand-in for the metadata schema.
# ---------------------------------------------------------------------------


class TestBuiltinPendingDeletes:
    def test_against_stub_schema(self, conn, registry):
        # Re-create just enough of __ducklake_metadata_lake to run the query.
        conn.execute("CREATE SCHEMA __ducklake_metadata_lake")
        conn.execute(
            "CREATE TABLE __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion (path TEXT)"
        )
        conn.execute(
            "INSERT INTO __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion "
            "VALUES ('s3://b/x'), ('s3://b/x'), ('s3://b/y'), ('s3://b/z')"
        )

        q = next(
            q for q in dm.load_queries(None, set()) if q.name == "ducklake_pending_deletes"
        )
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)
        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "ducklake_pending_deletes_total") == 4
        assert _gauge_value(registry, "ducklake_pending_deletes_unique_paths") == 3
        assert _gauge_value(registry, "ducklake_pending_deletes_dup_rows") == 1
