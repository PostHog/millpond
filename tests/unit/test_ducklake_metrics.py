"""Unit tests for tools/ducklake_metrics.py.

Coverage:
  * YAML loader: name/type validation, missing-key errors, user override.
  * parse_interval: m-only suffix, 1m floor.
  * _run_query end-to-end against an in-memory DuckDB so the
    column-name-resolve, gauge clear-and-set, and error-trap paths are
    exercised without standing up a real lake.
"""

from __future__ import annotations

import threading

import duckdb
import ducklake_metrics as dm
import pytest
from prometheus_client import CollectorRegistry

# ---------------------------------------------------------------------------
# interval_mins validation
# ---------------------------------------------------------------------------


class TestValidateIntervalMins:
    def test_one_minute(self):
        assert dm._validate_interval_mins(1) == 60

    def test_arbitrary_minutes(self):
        assert dm._validate_interval_mins(5) == 300
        assert dm._validate_interval_mins(60) == 3600

    def test_string_rejected(self):
        # The whole point of the rename: the unit is in the field name,
        # so values are integers — no "1m" / "30s" suffix parsing.
        with pytest.raises(ValueError, match="must be an integer"):
            dm._validate_interval_mins("1m")

    def test_zero_rejected(self):
        with pytest.raises(ValueError, match=">= 1"):
            dm._validate_interval_mins(0)

    def test_negative_rejected(self):
        with pytest.raises(ValueError, match=">= 1"):
            dm._validate_interval_mins(-1)

    def test_float_rejected(self):
        with pytest.raises(ValueError, match="must be an integer"):
            dm._validate_interval_mins(1.5)

    def test_bool_rejected(self):
        # bool is technically an int subclass but isn't a sensible
        # interval; reject explicitly.
        with pytest.raises(ValueError, match="must be an integer"):
            dm._validate_interval_mins(True)


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
            "    interval_mins: 2\n"
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
            "    interval_mins: 5\n"
            "    values: [total]\n"
            "    sql: SELECT 0 AS total\n"
        )
        queries = dm.load_queries(str(p), set())
        q = next(q for q in queries if q.name == "ducklake_pending_deletes")
        assert q.help == "overridden"
        assert q.interval_seconds == 300

    def test_missing_required_key(self):
        with pytest.raises(ValueError, match="missing required key"):
            dm._query_from_dict({"name": "x", "help": "h", "interval_mins": 1}, "test")

    def test_missing_interval_mins_specifically(self):
        with pytest.raises(ValueError, match="'interval_mins'"):
            dm._query_from_dict({"name": "x", "help": "h", "sql": "SELECT 1"}, "test")

    def test_bad_name_rejected(self):
        with pytest.raises(ValueError, match="must match"):
            dm._query_from_dict(
                {"name": "1bad-name", "help": "h", "interval_mins": 1, "sql": "SELECT 1"},
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

    def test_returns_true_on_success(self, conn, registry):
        # Outer scheduler distinguishes by return value; verify the contract.
        conn.execute("CREATE TABLE t (n BIGINT)")
        conn.execute("INSERT INTO t VALUES (5)")
        q = dm.Query(name="t_ok", help="t", sql="SELECT n FROM t", interval_seconds=60, labels=[], values=["n"])
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)
        assert dm._run_query(conn, q, gauges[q.name], sm) is True

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
        assert dm._run_query(conn, q, gauges[q.name], sm) is False

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

        q = next(q for q in dm.load_queries(None, set()) if q.name == "ducklake_pending_deletes")
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)
        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "ducklake_pending_deletes_total") == 4
        assert _gauge_value(registry, "ducklake_pending_deletes_unique_paths") == 3
        assert _gauge_value(registry, "ducklake_pending_deletes_dup_rows") == 1


# ---------------------------------------------------------------------------
# Built-ins: b2/b3/b5/b6 against stub catalog tables.
# Stub schemas mirror just the columns each query reads — the real DuckLake
# catalog has wider tables; column drift on the read columns would make
# these tests fail loudly, which is the point.
# ---------------------------------------------------------------------------


def _stub_catalog(conn):
    conn.execute("CREATE SCHEMA __ducklake_metadata_lake")
    conn.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_data_file ("
        "data_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, "
        "path VARCHAR, file_size_bytes BIGINT, partition_id BIGINT)"
    )
    conn.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_file_partition_value ("
        "data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_snapshot ("
        "snapshot_id BIGINT, snapshot_time VARCHAR, schema_version BIGINT)"
    )


def _builtin(name):
    return next(q for q in dm.load_queries(None, set()) if q.name == name)


class TestBuiltinFilesPerBand:
    def test_buckets_live_files_only(self, conn, registry):
        _stub_catalog(conn)
        # Live files (end_snapshot IS NULL) span four bands; one expired
        # row must be filtered out.
        conn.execute(
            "INSERT INTO __ducklake_metadata_lake.ducklake_data_file VALUES "
            "(1, 1, 0, NULL, 'a', 500000, 100),"        # lt1mib
            "(2, 1, 0, NULL, 'b', 1500000, 100),"       # 1to5mib
            "(3, 1, 0, NULL, 'c', 8000000, 200),"       # 5to10mib
            "(4, 1, 0, NULL, 'd', 50000000, 200),"      # 32to64mib
            "(5, 1, 0, NULL, 'e', 200000000, 300),"     # gt128mib
            "(6, 1, 0, 5,    'f', 100, 300)"            # expired — must NOT be counted
        )
        q = _builtin("ducklake_files_per_band")
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)
        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "ducklake_files_per_band_count", {"band": "lt1mib"}) == 1
        assert _gauge_value(registry, "ducklake_files_per_band_count", {"band": "1to5mib"}) == 1
        assert _gauge_value(registry, "ducklake_files_per_band_count", {"band": "5to10mib"}) == 1
        assert _gauge_value(registry, "ducklake_files_per_band_count", {"band": "32to64mib"}) == 1
        assert _gauge_value(registry, "ducklake_files_per_band_count", {"band": "gt128mib"}) == 1
        assert _gauge_value(registry, "ducklake_files_per_band_bytes", {"band": "lt1mib"}) == 500000
        assert _gauge_value(registry, "ducklake_files_per_band_bytes", {"band": "gt128mib"}) == 200000000

    def test_band_boundaries_inclusive_lower_exclusive_upper(self, conn, registry):
        # Exactly 1 MiB (1048576) must land in 1to5mib, not lt1mib —
        # matches DuckLake's own bin semantics (min inclusive, max
        # exclusive) so this metric's bands compose with maintenance.py's
        # tiered compaction.
        _stub_catalog(conn)
        conn.execute(
            "INSERT INTO __ducklake_metadata_lake.ducklake_data_file VALUES "
            "(1, 1, 0, NULL, 'a', 1048576, 100),"
            "(2, 1, 0, NULL, 'b', 1048575, 100)"
        )
        q = _builtin("ducklake_files_per_band")
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)
        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "ducklake_files_per_band_count", {"band": "lt1mib"}) == 1
        assert _gauge_value(registry, "ducklake_files_per_band_count", {"band": "1to5mib"}) == 1


class TestBuiltinCompactionCandidates:
    def test_per_tier_plus_total(self, conn, registry):
        _stub_catalog(conn)
        conn.execute(
            "INSERT INTO __ducklake_metadata_lake.ducklake_data_file VALUES "
            "(1, 1, 0, NULL, 'a', 500000, 100),"        # tier1
            "(2, 1, 0, NULL, 'b', 1500000, 100),"       # tier2
            "(3, 1, 0, NULL, 'c', 8000000, 200),"       # tier2
            "(4, 1, 0, NULL, 'd', 50000000, 200),"      # tier3
            "(5, 1, 0, NULL, 'e', 200000000, 300),"     # large
            "(6, 1, 0, 5,    'f', 100, 300)"            # expired
        )
        q = _builtin("ducklake_compaction_candidates")
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)
        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "ducklake_compaction_candidates_count", {"tier": "tier1"}) == 1
        assert _gauge_value(registry, "ducklake_compaction_candidates_count", {"tier": "tier2"}) == 2
        assert _gauge_value(registry, "ducklake_compaction_candidates_count", {"tier": "tier3"}) == 1
        assert _gauge_value(registry, "ducklake_compaction_candidates_count", {"tier": "large"}) == 1
        assert _gauge_value(registry, "ducklake_compaction_candidates_count", {"tier": "total"}) == 5


class TestBuiltinSnapshots:
    def test_count_and_ages(self, conn, registry):
        _stub_catalog(conn)
        # snapshot_time is VARCHAR in the real catalog; the query CASTs to
        # TIMESTAMPTZ. Use literal offsets so the cast is unambiguous.
        conn.execute(
            "INSERT INTO __ducklake_metadata_lake.ducklake_snapshot VALUES "
            "(0, '2026-05-01 12:00:00+00', 0),"
            "(1, '2026-05-07 12:00:00+00', 1)"
        )
        q = _builtin("ducklake_snapshots")
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)
        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "ducklake_snapshots_count") == 2
        oldest = _gauge_value(registry, "ducklake_snapshots_oldest_seconds_ago")
        newest = _gauge_value(registry, "ducklake_snapshots_newest_seconds_ago")
        # oldest >= newest, both positive (now() > snapshot times in the past).
        assert oldest >= newest > 0

    def test_empty_table_returns_zeros_not_errors(self, conn, registry):
        _stub_catalog(conn)
        q = _builtin("ducklake_snapshots")
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)
        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "ducklake_snapshots_count") == 0
        assert _gauge_value(registry, "ducklake_snapshots_oldest_seconds_ago") == 0
        assert _gauge_value(registry, "ducklake_snapshots_newest_seconds_ago") == 0


class TestBuiltinFilesPerPartitionTop20:
    def test_groups_by_composite_value(self, conn, registry):
        _stub_catalog(conn)
        conn.execute(
            "INSERT INTO __ducklake_metadata_lake.ducklake_data_file VALUES "
            "(1, 1, 0, NULL, 'a', 100, 100),"
            "(2, 1, 0, NULL, 'b', 100, 100),"
            "(3, 1, 0, NULL, 'c', 100, 200),"
            "(4, 1, 0, NULL, 'd', 100, 200),"
            "(5, 1, 0, NULL, 'e', 100, 300),"
            "(6, 1, 0, NULL, 'no_part', 100, NULL)"  # live file with no partition_value rows
        )
        conn.execute(
            "INSERT INTO __ducklake_metadata_lake.ducklake_file_partition_value VALUES "
            "(1, 1, 0, '2026-05-01'),"
            "(2, 1, 0, '2026-05-01'),"
            "(3, 1, 0, '2026-05-02'),"
            "(4, 1, 0, '2026-05-02'),"
            "(5, 1, 0, '2026-05-03')"
        )
        q = _builtin("ducklake_files_per_partition_top20")
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)
        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "ducklake_files_per_partition_top20_count", {"partition": "2026-05-01"}) == 2
        assert _gauge_value(registry, "ducklake_files_per_partition_top20_count", {"partition": "2026-05-02"}) == 2
        assert _gauge_value(registry, "ducklake_files_per_partition_top20_count", {"partition": "2026-05-03"}) == 1
        # Live file without any partition_value rows surfaces as '<none>'.
        assert _gauge_value(registry, "ducklake_files_per_partition_top20_count", {"partition": "<none>"}) == 1

    def test_composite_partition_keys_joined_with_slash(self, conn, registry):
        _stub_catalog(conn)
        conn.execute(
            "INSERT INTO __ducklake_metadata_lake.ducklake_data_file VALUES "
            "(1, 1, 0, NULL, 'a', 100, 100)"
        )
        # Two-column partition: index 0 = year, index 1 = day. The query
        # joins them in key-index order with '/'.
        conn.execute(
            "INSERT INTO __ducklake_metadata_lake.ducklake_file_partition_value VALUES "
            "(1, 1, 1, '05-01'),"
            "(1, 1, 0, '2026')"
        )
        q = _builtin("ducklake_files_per_partition_top20")
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)
        dm._run_query(conn, q, gauges[q.name], sm)

        assert _gauge_value(registry, "ducklake_files_per_partition_top20_count", {"partition": "2026/05-01"}) == 1


# ---------------------------------------------------------------------------
# Reconnect path: backoff + scheduler-driven escalation.
# ---------------------------------------------------------------------------


class TestSchedulerReconnect:
    """The scheduler escalates to _ReconnectNeeded only on sustained failure."""

    def _build_one_query(self, registry):
        # interval_seconds=0 fires every iteration with no sleep, so a
        # threshold-many run-failures completes in microseconds.
        q = dm.Query(name="q", help="h", sql="SELECT 1 AS n", interval_seconds=0, labels=[], values=["n"])
        gauges = dm._build_query_gauges([q], registry=registry)
        sm = dm._build_self_metrics(registry=registry)
        return q, gauges, sm

    def test_threshold_consecutive_failures_raises(self, registry, monkeypatch):
        q, gauges, sm = self._build_one_query(registry)
        monkeypatch.setattr(dm, "_run_query", lambda *a, **kw: False)
        stop = threading.Event()
        with pytest.raises(dm._ReconnectNeeded):
            dm._scheduler_loop(None, [q], gauges, sm, stop)

    def test_success_resets_counter(self, registry, monkeypatch):
        # Pattern: repeatedly fail (THRESHOLD-1) times then succeed once;
        # success resets the streak so the loop runs forever (here: until
        # we set stop manually). Without the reset the second batch of
        # failures would trip the threshold.
        q, gauges, sm = self._build_one_query(registry)
        stop = threading.Event()
        results: list[bool] = []
        for _ in range(3):
            results += [False] * (dm.CONSECUTIVE_FAILURE_THRESHOLD - 1)
            results.append(True)
        it = iter(results)

        def fake_run(*_a, **_kw):
            try:
                return next(it)
            except StopIteration:
                stop.set()
                return True

        monkeypatch.setattr(dm, "_run_query", fake_run)
        # Must not raise: each run of (THRESHOLD-1) failures is followed
        # by a success that resets the counter back to 0.
        dm._scheduler_loop(None, [q], gauges, sm, stop)


class TestConnectWithBackoff:
    """maintenance.connect() retries with exponential backoff until success or stop."""

    def test_eventually_returns_connection(self, registry, monkeypatch):
        sm = dm._build_self_metrics(registry=registry)
        # Fail twice, then succeed.
        sentinel = object()
        attempts = {"n": 0}

        def fake_connect():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("transient")
            return sentinel

        monkeypatch.setattr(dm.maintenance, "connect", fake_connect)
        # Speed test up: Event.wait returns immediately if stop is already
        # set, but here we don't want that. Instead patch the constants so
        # delays are in ms, not seconds.
        monkeypatch.setattr(dm, "_BACKOFF_INITIAL_SECONDS", 0.001)
        monkeypatch.setattr(dm, "_BACKOFF_MAX_SECONDS", 0.001)

        stop = threading.Event()
        result = dm._connect_with_backoff(stop, sm)
        assert result is sentinel
        assert attempts["n"] == 3
        # up was held at 0 throughout; the caller flips it to 1 once it
        # owns the returned connection.
        assert _gauge_value(registry, "ducklake_metrics_up") == 0

    def test_returns_none_when_stop_set_during_backoff(self, registry, monkeypatch):
        sm = dm._build_self_metrics(registry=registry)
        stop = threading.Event()

        def fake_connect():
            # Set stop on the first failure so the next stop.wait() exits.
            stop.set()
            raise RuntimeError("never recovers")

        monkeypatch.setattr(dm.maintenance, "connect", fake_connect)
        monkeypatch.setattr(dm, "_BACKOFF_INITIAL_SECONDS", 0.001)

        assert dm._connect_with_backoff(stop, sm) is None
