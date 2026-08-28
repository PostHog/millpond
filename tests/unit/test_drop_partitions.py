"""Unit tests for the drop-partitions op in tools/ducklake_maintenance.py.

Tier A: pure helpers (tuple parsing, cutoff validation, composition,
enumeration classification, SQL shape).
Tier B: scripted fake-libpq flow tests (txn order, skip classification,
retry/lock behavior, cursor guard).

Tier C (real catalog) is the validation protocol in DROP_PARTITIONS_PLAN.md —
deliberately out of scope here.
"""

import contextlib
import json
import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import ducklake_maintenance as dm
import psycopg
import pytest

# ---------------------------------------------------------------------------
# Fixtures / builders
# ---------------------------------------------------------------------------


def _spec_nrt():
    """events_nrt live spec: (team_id, year, month, day, hour)."""
    return dm._PartitionSpec(
        partition_id=0,
        keys=[
            dm._PartitionKey(0, 19, "identity", "team_id"),
            dm._PartitionKey(1, 20, "year", "_inserted_at"),
            dm._PartitionKey(2, 20, "month", "_inserted_at"),
            dm._PartitionKey(3, 20, "day", "_inserted_at"),
            dm._PartitionKey(4, 20, "hour", "_inserted_at"),
        ],
    )


def _spec_daily():
    """duckling-style spec: (year, month, day) — no hour key."""
    return dm._PartitionSpec(
        partition_id=3,
        keys=[
            dm._PartitionKey(0, 7, "year", "created_at"),
            dm._PartitionKey(1, 7, "month", "created_at"),
            dm._PartitionKey(2, 7, "day", "created_at"),
        ],
    )


def _erow(fid, values, begin, partition_id=0, rc=10, nbytes=1000, fpv_rows=5, fpv_keys=5):
    """One enumeration row in _enumeration_sql column order."""
    return (fid, *values, fpv_rows, fpv_keys, rc, nbytes, begin, partition_id)


CUTOFF = datetime(2026, 8, 14, 0, 0, tzinfo=UTC)
FLOOR = 1000


class FakePG:
    """Scripted psycopg double. script: list of (needle, rows | exception |
    callable(sql, params) -> rows). First matching needle wins, so order the
    script specific-before-general."""

    def __init__(self, script):
        self._script = list(script)
        self.executed: list[tuple[str, tuple | None]] = []
        self.closed = False
        self._rows = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        for needle, outcome in self._script:
            if needle in sql:
                rows = outcome(sql, params) if callable(outcome) else outcome
                if isinstance(rows, BaseException):
                    raise rows
                self._rows = rows
                return self
        raise AssertionError(f"unscripted SQL: {sql[:240]}")

    def cursor(self):
        # A cursor is NOT the connection: closing it must not close us
        # (the txn helper closes its cursor between attempts).
        parent = self

        class _Cur:
            def execute(self, sql, params=None):
                parent.execute(sql, params)
                return self

            def fetchall(self):
                return parent.fetchall()

            def fetchone(self):
                return parent.fetchone()

            def close(self):
                pass

        return _Cur()

    def transaction(self):
        @contextlib.contextmanager
        def _txn():
            yield

        return _txn()

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _std_txn_script(baseline=(41, 7, 900, 5000), updated=None, stats_rows=None):
    """The statements every drop-leaf attempt runs, in execution order."""
    if updated is None:
        updated = [(11, 10, 1000), (12, 10, 1000)]
    if stats_rows is None:
        stats_rows = [(1_000_000,)]
    return [
        ("pg_advisory_unlock_all", []),
        ("pg_try_advisory_lock", [(True,)]),
        ("SET LOCAL", []),
        ("SHOW transaction_isolation", [("read committed",)]),
        ("ducklake_metadata", [("1.1-dev1",)]),
        ("ducklake_delete_file", [(0,)]),
        ("ORDER BY snapshot_id DESC", [baseline]),
        ("INSERT INTO public.ducklake_snapshot (", []),
        ("UPDATE public.ducklake_data_file", updated),
        (SEL_SKIPPED, []),
        (F3_RECHECK, []),
        ("NOT (begin_snapshot <", [(0,)]),  # F2 rewind verification (guarded mode)
        ("UPDATE public.ducklake_table_stats", stats_rows),
        ("INSERT INTO public.ducklake_snapshot_changes", []),
    ]


INS_SNAP = "INSERT INTO public.ducklake_snapshot ("
SEL_SKIPPED = "AS eligible FROM public.ducklake_data_file"
F3_RECHECK = "AND end_snapshot IS NULL AND (begin_snapshot"


def _swap(script, needle, entry):
    """Replace the script entry whose needle matches (positional indexing is
    too fragile across fixture edits)."""
    for i, (n, _) in enumerate(script):
        if n == needle:
            script[i] = entry
            return script
    raise AssertionError(f"needle {needle!r} not in script")


# ---------------------------------------------------------------------------
# Tier A — pure helpers
# ---------------------------------------------------------------------------


class TestPartitionTuple:
    def test_parse_nrt_tuple(self):
        out = dm._parse_partition_tuple(_spec_nrt(), "team_id=2,year=2026,month=8,day=1,hour=13")
        assert out == {0: "2", 1: "2026", 2: "8", 3: "1", 4: "13"}

    def test_parse_daily_tuple_no_hour(self):
        out = dm._parse_partition_tuple(_spec_daily(), "year=2026,month=8,day=1")
        assert out == {0: "2026", 1: "8", 2: "1"}

    def test_missing_key_rejected(self):
        with pytest.raises(dm._DropAbort, match="missing keys"):
            dm._parse_partition_tuple(_spec_nrt(), "team_id=2,year=2026,month=8,day=1")

    def test_unknown_key_rejected(self):
        with pytest.raises(dm._DropAbort, match="not in spec"):
            dm._parse_partition_tuple(_spec_nrt(), "team_id=2,year=2026,month=8,day=1,hour=3,foo=1")

    def test_duplicate_key_rejected(self):
        with pytest.raises(dm._DropAbort, match="repeats key"):
            dm._parse_partition_tuple(_spec_nrt(), "team_id=2,team_id=3,year=2026,month=8,day=1,hour=3")

    def test_non_integer_time_rejected(self):
        with pytest.raises(dm._DropAbort, match="must be an integer"):
            dm._parse_partition_tuple(_spec_nrt(), "team_id=2,year=2026,month=8.5,day=1,hour=3")

    def test_identity_value_charset(self):
        with pytest.raises(dm._DropAbort, match="illegal value"):
            dm._parse_partition_tuple(_spec_nrt(), "team_id=2 2,year=2026,month=8,day=1,hour=3")

    def test_malformed_component(self):
        with pytest.raises(dm._DropAbort, match="malformed"):
            dm._parse_partition_tuple(_spec_nrt(), "team_id=2,year=2026,month,day=1,hour=3")

    def test_canonical_round_trip(self):
        spec = _spec_nrt()
        tm = dm._parse_partition_tuple(spec, "team_id=2,year=2026,month=8,day=1,hour=13")
        s = dm._canonical_leaf_str(spec, tm)
        assert s == "team_id=2,year=2026,month=8,day=1,hour=13"
        assert dm._parse_partition_tuple(spec, s) == tm


class TestComposePartTs:
    def test_hour_grain(self):
        ts = dm._compose_part_ts(_spec_nrt(), {0: "2", 1: "2026", 2: "8", 3: "1", 4: "13"})
        assert ts == datetime(2026, 8, 1, 13, tzinfo=UTC)

    def test_day_grain_defaults_midnight(self):
        ts = dm._compose_part_ts(_spec_daily(), {0: "2026", 1: "8", 2: "1"})
        assert ts == datetime(2026, 8, 1, 0, tzinfo=UTC)

    @pytest.mark.parametrize(
        "values",
        [
            {0: "2", 1: "2026", 2: "13", 3: "1", 4: "0"},  # month 13
            {0: "2", 1: "2026", 2: "2", 3: "31", 4: "0"},  # Feb 31
            {0: "2", 1: "2026", 2: "x", 3: "1", 4: "0"},  # non-numeric
            {0: "2", 1: "2026", 2: "8", 3: None, 4: "0"},  # missing day
        ],
    )
    def test_invalid_returns_none(self, values):
        assert dm._compose_part_ts(_spec_nrt(), values) is None


class TestCutoff:
    def _args(self, **kw):
        ns = SimpleNamespace(cutoff="", retention_days=None, force_young_cutoff=False)
        for k, v in kw.items():
            setattr(ns, k, v)
        return ns

    def test_retention_days_aligned(self):
        cutoff = dm._resolve_cutoff(self._args(retention_days=14), "hour")
        assert cutoff.minute == cutoff.second == cutoff.microsecond == 0
        assert cutoff.tzinfo is not None

    def test_both_rejected(self):
        with pytest.raises(dm._DropAbort, match="exactly one"):
            dm._resolve_cutoff(self._args(cutoff="2026-08-01T00", retention_days=14), "hour")

    def test_neither_rejected(self):
        with pytest.raises(dm._DropAbort, match="one of"):
            dm._resolve_cutoff(self._args(), "hour")

    def test_hour_grain_rejects_minutes(self):
        with pytest.raises(dm._DropAbort, match="aligned"):
            dm._resolve_cutoff(self._args(cutoff="2026-08-01T10:30"), "hour")

    def test_day_grain_rejects_hour(self):
        with pytest.raises(dm._DropAbort, match="aligned"):
            dm._resolve_cutoff(self._args(cutoff="2026-08-01T10:00"), "day")

    def test_young_cutoff_refused(self):
        young = (datetime.now(UTC) - timedelta(days=2)).replace(minute=0, second=0, microsecond=0)
        with pytest.raises(dm._DropAbort, match="--force-young-cutoff"):
            dm._resolve_cutoff(self._args(cutoff=young.isoformat()), "hour")

    def test_young_cutoff_forced(self):
        young = (datetime.now(UTC) - timedelta(days=2)).replace(minute=0, second=0, microsecond=0)
        assert dm._resolve_cutoff(self._args(cutoff=young.isoformat(), force_young_cutoff=True), "hour") == young

    def test_naive_iso_treated_as_utc(self):
        cutoff = dm._resolve_cutoff(self._args(cutoff="2026-08-01T13"), "hour")
        assert cutoff == datetime(2026, 8, 1, 13, tzinfo=UTC)


class TestSqlShape:
    def test_enumeration_two_table_params(self):
        sql = dm._enumeration_sql(_spec_nrt())
        assert sql.count("%s") == 2

    def test_leaf_selection_param_order(self):
        spec = _spec_nrt()
        sql = dm._leaf_selection_sql(spec)
        # table_id, partition_id, floor, key-count, then one value per key
        assert sql.count("%s") == 4 + len(spec.keys)
        assert "df.partition_id = %s" in sql
        assert "df.begin_snapshot <= %s" in sql
        assert "df.end_snapshot IS NULL" in sql
        for k in spec.keys:
            assert f"f{k.index}.partition_key_index = {k.index}" in sql

    def test_leaf_selection_params_match(self):
        spec = _spec_nrt()
        tm = dm._parse_partition_tuple(spec, "team_id=2,year=2026,month=8,day=1,hour=13")
        pg = FakePG([("FROM public.ducklake_data_file", [])])
        dm._select_leaf_files(pg, 5, spec, tm, floor_snap=100, max_files=10)
        _, params = pg.executed[0]
        assert params == (5, 0, 100, 5, "2", "2026", "8", "1", "13")


class TestClassifyEnumeration:
    def test_groups_and_sorts_oldest_first(self):
        spec = _spec_nrt()
        rows = [
            _erow(1, ("2", "2026", "8", "1", "13"), begin=10),
            _erow(2, ("2", "2026", "8", "1", "13"), begin=11),
            _erow(3, ("2", "2026", "8", "1", "12"), begin=10),
        ]
        leaves, diag = dm._classify_enumeration(rows, spec, CUTOFF, FLOOR, 10_000, 0)
        assert [lf.partition for lf in leaves] == [
            "team_id=2,year=2026,month=8,day=1,hour=12",
            "team_id=2,year=2026,month=8,day=1,hour=13",
        ]
        assert len(leaves[1].files) == 2
        assert leaves[1].rows == 20
        assert leaves[1].max_begin_snapshot == 11
        assert leaves[1].values["team_id"] == "2"
        assert diag == {"rot_files": 0, "rot_samples": [], "floored_files": 0}

    def test_above_cutoff_excluded(self):
        rows = [_erow(1, ("2", "2026", "8", "14", "0"), begin=10)]  # == cutoff, not below
        leaves, _ = dm._classify_enumeration(rows, _spec_nrt(), CUTOFF, FLOOR, 10_000, 0)
        assert leaves == []

    def test_floor_excludes_stragglers(self):
        rows = [
            _erow(1, ("2", "2026", "8", "1", "13"), begin=10),
            _erow(2, ("2", "2026", "8", "1", "13"), begin=FLOOR + 1),
        ]
        leaves, diag = dm._classify_enumeration(rows, _spec_nrt(), CUTOFF, FLOOR, 10_000, 0)
        assert [f.data_file_id for f in leaves[0].files] == [1]
        assert diag["floored_files"] == 1

    @pytest.mark.parametrize(
        "row",
        [
            _erow(9, ("2", "2026", "8", "1", "13"), begin=1, fpv_rows=6),  # duplicate index
            _erow(9, ("2", "2026", "8", None, "13"), begin=1),  # missing day key
            _erow(9, ("2", "2026", "8", "1", "13"), begin=1, fpv_keys=6),  # extra index
            _erow(9, ("2", "2026", "x", "1", "13"), begin=1),  # non-numeric month
        ],
        ids=["dup", "missing", "extra", "nonnumeric"],
    )
    def test_rot_aborts_at_zero_threshold(self, row):
        with pytest.raises(dm._DropAbort, match="fpv-rot"):
            dm._classify_enumeration([row], _spec_nrt(), CUTOFF, FLOOR, 10_000, 0)

    def test_rot_within_threshold_is_excluded_and_counted(self):
        rows = [
            _erow(1, ("2", "2026", "8", "1", "13"), begin=10),
            _erow(9, ("2", "2026", "8", None, "13"), begin=1),
        ]
        leaves, diag = dm._classify_enumeration(rows, _spec_nrt(), CUTOFF, FLOOR, 10_000, 1)
        assert [f.data_file_id for f in leaves[0].files] == [1]
        assert diag["rot_files"] == 1 and diag["rot_samples"] == [9]

    def test_old_spec_in_window_aborts(self):
        rows = [_erow(7, ("2", "2026", "8", "1", "13"), begin=1, partition_id=4)]
        with pytest.raises(dm._DropAbort, match="NON-LIVE partition spec"):
            dm._classify_enumeration(rows, _spec_nrt(), CUTOFF, FLOOR, 10_000, 0)

    def test_old_spec_uncomposable_aborts(self):
        rows = [_erow(7, (None, None, None, None, None), begin=1, partition_id=4, fpv_rows=5, fpv_keys=5)]
        with pytest.raises(dm._DropAbort, match="NON-LIVE partition spec"):
            dm._classify_enumeration(rows, _spec_nrt(), CUTOFF, FLOOR, 10_000, 0)

    def test_old_spec_provably_above_cutoff_ignored(self):
        rows = [_erow(7, ("2", "2027", "8", "1", "13"), begin=1, partition_id=4)]
        leaves, _ = dm._classify_enumeration(rows, _spec_nrt(), CUTOFF, FLOOR, 10_000, 0)
        assert leaves == []

    def test_oversized_leaf_marked(self):
        rows = [_erow(i, ("2", "2026", "8", "1", "13"), begin=1) for i in range(5)]
        leaves, _ = dm._classify_enumeration(rows, _spec_nrt(), CUTOFF, FLOOR, max_files=4, max_rot=0)
        assert leaves[0].oversized is True


class TestManifest:
    def test_load_rejects_wrong_tool(self, tmp_path):
        p = tmp_path / "m.json"
        p.write_text(json.dumps({"tool": "something-else"}))
        with pytest.raises(dm._DropAbort, match="not a list-droppable-partitions"):
            dm._load_manifest(str(p))

    def test_load_rejects_missing_file(self, tmp_path):
        with pytest.raises(dm._DropAbort, match="cannot read manifest"):
            dm._load_manifest(str(tmp_path / "nope.json"))

    def test_round_trip_values(self, tmp_path):
        spec = _spec_nrt()
        rows = [_erow(1, ("2", "2026", "8", "1", "13"), begin=10)]
        leaves, _ = dm._classify_enumeration(rows, spec, CUTOFF, FLOOR, 10_000, 0)
        manifest = {
            "tool": "list-droppable-partitions",
            "manifest_version": 1,
            "table": "main.events_nrt",
            "cutoff": CUTOFF.isoformat(),
            "leaves": [{"partition": leaves[0].partition, "values": leaves[0].values, "file_ids": [1]}],
        }
        p = tmp_path / "m.json"
        p.write_text(json.dumps(manifest))
        loaded = dm._load_manifest(str(p))
        tm = dm._tuple_from_values(spec, loaded["leaves"][0]["values"])
        assert tm == {0: "2", 1: "2026", 2: "8", 3: "1", 4: "13"}


class TestRetryClassification:
    @pytest.mark.parametrize("sqlstate", ["23505", "40001", "55P03", "57014", "40P01"])
    def test_retryable_sqlstates(self, sqlstate):
        exc = psycopg.Error("x")
        exc.sqlstate = sqlstate
        assert dm._is_retryable_pg_error(exc) is True

    def test_fatal_sqlstate(self):
        exc = psycopg.Error("x")
        exc.sqlstate = "22023"
        assert dm._is_retryable_pg_error(exc) is False

    def test_no_sqlstate(self):
        assert dm._is_retryable_pg_error(RuntimeError("x")) is False


# ---------------------------------------------------------------------------
# Tier B — scripted fake-libpq flows
# ---------------------------------------------------------------------------


class TestResolve:
    def test_resolve_table_exactly_one(self):
        pg = FakePG([("ducklake_table", [])])
        with pytest.raises(dm._DropAbort, match="exactly 1"):
            dm._resolve_table_id(pg, "main.events_nrt")

    def test_resolve_table_ok(self):
        pg = FakePG([("ducklake_table", [(5,)])])
        table_id, s, t = dm._resolve_table_id(pg, "main.events_nrt")
        assert (table_id, s, t) == (5, "main", "events_nrt")

    def test_resolve_table_refuses_bare_id(self):
        with pytest.raises(dm._DropAbort, match="schema.table"):
            dm._resolve_table_id(FakePG([]), "5")

    def _spec_script(self, info_rows, col_rows):
        return [("ducklake_partition_info", info_rows), ("ducklake_partition_column", col_rows)]

    def test_live_spec_none(self):
        pg = FakePG(self._spec_script([], []))
        with pytest.raises(dm._DropAbort, match="no live partition spec"):
            dm._resolve_live_spec(pg, 5)

    def test_live_spec_two_aborts(self):
        pg = FakePG(self._spec_script([(0,), (1,)], []))
        with pytest.raises(dm._DropAbort, match="corruption"):
            dm._resolve_live_spec(pg, 5)

    def test_unsupported_transform(self):
        cols = [
            (0, 1, "year", "_inserted_at"),
            (1, 1, "month", "_inserted_at"),
            (2, 1, "day", "_inserted_at"),
            (3, 2, "bucket[16]", "team_id"),
        ]
        pg = FakePG(self._spec_script([(0,)], cols))
        with pytest.raises(dm._DropAbort, match="unsupported partition transform"):
            dm._resolve_live_spec(pg, 5)

    def test_missing_year_aborts(self):
        cols = [(0, 1, "month", "c"), (1, 1, "day", "c")]
        pg = FakePG(self._spec_script([(0,)], cols))
        with pytest.raises(dm._DropAbort, match="no 'year' time transform"):
            dm._resolve_live_spec(pg, 5)

    def test_ambiguous_address_aborts(self):
        # identity column literally named 'year' collides with the year transform
        cols = [
            (0, 2, "identity", "year"),
            (1, 1, "year", "c"),
            (2, 1, "month", "c"),
            (3, 1, "day", "c"),
        ]
        pg = FakePG(self._spec_script([(0,)], cols))
        with pytest.raises(dm._DropAbort, match="ambiguous"):
            dm._resolve_live_spec(pg, 5)


class TestCursorGuard:
    FLOOR_T = datetime(2026, 8, 2, tzinfo=UTC)

    def test_min_cursor(self):
        rows = [
            ("d1", "i1", 100, datetime(2026, 8, 3, tzinfo=UTC)),
            ("d1", "i2", 90, datetime(2026, 8, 3, tzinfo=UTC)),
        ]
        pg = FakePG([("viaduck.viaduck_state v", rows)])
        g = dm._cursor_guard(pg, "viaduck", "viaduck_state", self.FLOOR_T, False)
        assert g.snapshot_id == 90 and g.cursor_rows == 2 and not g.overridden

    def test_missing_table_fails_closed(self):
        pg = FakePG([("viaduck.viaduck_state v", psycopg.errors.UndefinedTable("nope"))])
        with pytest.raises(dm._DropAbort, match="does not exist"):
            dm._cursor_guard(pg, "viaduck", "viaduck_state", self.FLOOR_T, False)

    def test_empty_table_fails_closed(self):
        pg = FakePG([("viaduck.viaduck_state v", [])])
        with pytest.raises(dm._DropAbort, match="no cursor rows"):
            dm._cursor_guard(pg, "viaduck", "viaduck_state", self.FLOOR_T, False)

    def test_lag_aborts(self):
        rows = [("d1", "i1", 100, datetime(2026, 7, 1, tzinfo=UTC))]  # way behind the floor
        pg = FakePG([("viaduck.viaduck_state v", rows)])
        with pytest.raises(dm._DropAbort, match="actual loss condition"):
            dm._cursor_guard(pg, "viaduck", "viaduck_state", self.FLOOR_T, False)

    def test_lag_null_snapshot_time_aborts(self):
        rows = [("d1", "i1", 100, None)]  # cursor points at an expired snapshot
        pg = FakePG([("viaduck.viaduck_state v", rows)])
        with pytest.raises(dm._DropAbort, match="actual loss condition"):
            dm._cursor_guard(pg, "viaduck", "viaduck_state", self.FLOOR_T, False)

    def test_override_returns_no_guard_and_logs(self, caplog):
        rows = [("d1", "i1", 100, datetime(2026, 7, 1, tzinfo=UTC))]
        pg = FakePG([("viaduck.viaduck_state v", rows)])
        with caplog.at_level(logging.WARNING, logger="maintenance"):
            g = dm._cursor_guard(pg, "viaduck", "viaduck_state", self.FLOOR_T, True)
        assert g.snapshot_id == 100 and g.overridden
        assert "WOULD HAVE ABORTED" in caplog.text

    def test_state_identifiers_validated(self):
        with pytest.raises(dm._DropAbort, match="safe identifier"):
            dm._cursor_guard(FakePG([]), "viaduck; DROP TABLE x", "viaduck_state", self.FLOOR_T, False)


class TestDropLeafTxn:
    GUARD_SQL = "viaduck.viaduck_state"

    def _files(self):
        return [
            dm._LeafFile(11, 10, 1000, begin_snapshot=10),
            dm._LeafFile(12, 10, 1000, begin_snapshot=11),
        ]

    def test_txn_order_and_f1_bump(self):
        pg = FakePG(_std_txn_script())
        out = dm._drop_leaf_txn(pg, 5, self._files(), self.GUARD_SQL, "m", True)
        assert out["snapshot_id"] == 42
        assert out["files"] == 2 and out["rows"] == 20 and out["bytes"] == 2000
        sqls = [s for s, _ in pg.executed]

        def idx(needle):
            return next(i for i, s in enumerate(sqls) if needle in s)

        assert idx("ducklake_metadata") < idx("ORDER BY snapshot_id DESC")
        assert idx("ducklake_delete_file") < idx(INS_SNAP)
        assert idx(INS_SNAP) < idx("UPDATE public.ducklake_data_file")
        assert idx("UPDATE public.ducklake_data_file") < idx("UPDATE public.ducklake_table_stats")
        assert idx("UPDATE public.ducklake_table_stats") < idx("INSERT INTO public.ducklake_snapshot_changes")
        # F1: next_file_id = baseline + 1 (baseline 5000 -> 5001)
        snap_insert = next(p for s, p in pg.executed if INS_SNAP in s)
        assert snap_insert == (42, 7, 900, 5001)
        # token + author (author is a SQL literal; params carry id/token/message)
        changes = next((s, p) for s, p in pg.executed if "ducklake_snapshot_changes" in s)
        assert "'drop-partitions'" in changes[0]
        assert changes[1][0] == 42 and changes[1][1] == "deleted_from_table:5" and changes[1][2] == "m"
        # the cursor guard is a SELF-FRESHENING subquery in the UPDATE's WHERE
        # (a frozen scalar goes stale across retries — M2/H1); RETURNING drives stats
        file_update = next((s, p) for s, p in pg.executed if "UPDATE public.ducklake_data_file" in s)
        assert "begin_snapshot < (SELECT min(last_snapshot_id) FROM viaduck.viaduck_state)" in file_update[0]
        assert file_update[1] == (42, [11, 12])
        # dv-check is table-scoped (ducklake_delete_file_table_idx)
        dv_check = next(p for s, p in pg.executed if "ducklake_delete_file" in s)
        assert dv_check == (5, [11, 12])

    def test_no_guard_predicate_when_overridden(self):
        pg = FakePG(_std_txn_script())
        dm._drop_leaf_txn(pg, 5, self._files(), None, "m", False)
        file_update = next((s, p) for s, p in pg.executed if "UPDATE public.ducklake_data_file" in s)
        assert "last_snapshot_id" not in file_update[0]

    def test_empty_catalog_aborts(self):
        script = _swap(_std_txn_script(), "ORDER BY snapshot_id DESC", ("ORDER BY snapshot_id DESC", []))
        with pytest.raises(dm._DropAbort, match="empty catalog"):
            dm._drop_leaf_txn(FakePG(script), 5, self._files(), self.GUARD_SQL, "m", False)

    def test_version_pin_aborts(self):
        script = _swap(_std_txn_script(), "ducklake_metadata", ("ducklake_metadata", [("9.9-wrong",)]))
        with pytest.raises(dm._DropAbort, match="version pin"):
            dm._drop_leaf_txn(FakePG(script), 5, self._files(), self.GUARD_SQL, "m", False)

    def test_live_delete_vector_aborts(self):
        script = _swap(_std_txn_script(), "ducklake_delete_file", ("ducklake_delete_file", [(2,)]))
        with pytest.raises(dm._DropAbort, match="LIVE delete vector"):
            dm._drop_leaf_txn(FakePG(script), 5, self._files(), self.GUARD_SQL, "m", False)

    def test_wrong_isolation_aborts(self):
        script = _swap(
            _std_txn_script(), "SHOW transaction_isolation", ("SHOW transaction_isolation", [("repeatable read",)])
        )
        with pytest.raises(dm._DropAbort, match="READ COMMITTED"):
            dm._drop_leaf_txn(FakePG(script), 5, self._files(), self.GUARD_SQL, "m", True)

    def test_stats_assertion_aborts_with_diagnostics(self):
        script = _std_txn_script(stats_rows=[])
        script.append(("SELECT record_count, file_size_bytes FROM public.ducklake_table_stats", [(5, 10)]))
        with pytest.raises(dm._DropAbort, match="stats assertion failed"):
            dm._drop_leaf_txn(FakePG(script), 5, self._files(), self.GUARD_SQL, "m", False)

    def test_skipped_ended_concurrently_ok(self):
        # file 12 ended by a racing compaction: skipped, tolerated, counted
        script = _swap(_std_txn_script(updated=[(11, 10, 1000)]), SEL_SKIPPED, (SEL_SKIPPED, [(12, 77, 11, False)]))
        out = dm._drop_leaf_txn(FakePG(script), 5, self._files(), self.GUARD_SQL, "m", False)
        assert out["files"] == 1 and out["ended_concurrently"] == 1

    def test_skipped_still_live_eligible_aborts(self, monkeypatch):
        monkeypatch.setattr(dm.time, "sleep", lambda _: None)
        # live+eligible on the classification read AND still there on recheck:
        # a genuine update miss -> abort
        script = _swap(_std_txn_script(updated=[(11, 10, 1000)]), SEL_SKIPPED, (SEL_SKIPPED, [(12, None, 11, True)]))
        script = _swap(script, F3_RECHECK, (F3_RECHECK, [(12,)]))
        with pytest.raises(dm._DropAbort, match="tool bug"):
            dm._drop_leaf_txn(FakePG(script), 5, self._files(), self.GUARD_SQL, "m", False)

    def test_skipped_transient_eligibility_skips(self, monkeypatch):
        # live+eligible on the fresher classification snapshot but GONE on
        # recheck: a cursor ADVANCED mid-txn; the UPDATE-time skip was correct
        # -> whole-leaf skip (rollback), not a spurious tool-bug abort (F3)
        monkeypatch.setattr(dm.time, "sleep", lambda _: None)
        script = _swap(_std_txn_script(updated=[(11, 10, 1000)]), SEL_SKIPPED, (SEL_SKIPPED, [(12, None, 11, True)]))
        # recheck needle defaults to [] in _std_txn_script -> transient path
        with pytest.raises(dm._LeafSkip, match="transient eligibility"):
            dm._drop_leaf_txn(FakePG(script), 5, self._files(), self.GUARD_SQL, "m", False)

    def test_rewind_verification_skips_leaf(self):
        # files we ended read as guard-INELIGIBLE on a fresh statement: the
        # cursor rewound mid-drop (flush failure) -> roll the leaf back (F2)
        script = _swap(_std_txn_script(), "NOT (begin_snapshot <", ("NOT (begin_snapshot <", [(1,)]))
        with pytest.raises(dm._LeafSkip, match="cursor rewound"):
            dm._drop_leaf_txn(FakePG(script), 5, self._files(), self.GUARD_SQL, "m", False)

    def test_rewind_check_absent_when_unguarded(self):
        pg = FakePG(_std_txn_script())
        dm._drop_leaf_txn(pg, 5, self._files(), None, "m", False)
        assert not any("NOT (begin_snapshot" in s for s, _ in pg.executed)

    def test_skipped_still_live_ineligible_skips_leaf(self):
        script = _swap(_std_txn_script(updated=[(11, 10, 1000)]), SEL_SKIPPED, (SEL_SKIPPED, [(12, None, 999, False)]))
        with pytest.raises(dm._LeafSkip, match="guard-ineligible"):
            dm._drop_leaf_txn(FakePG(script), 5, self._files(), self.GUARD_SQL, "m", False)

    def test_leaf_vanished_skips(self):
        script = _std_txn_script(updated=[])
        with pytest.raises(dm._LeafSkip, match="vanished"):
            dm._drop_leaf_txn(FakePG(script), 5, self._files(), self.GUARD_SQL, "m", False)


class TestRetryLoop:
    def _files(self):
        return [dm._LeafFile(11, 10, 1000, begin_snapshot=10)]

    def _run(self, pg, metrics=None):
        metrics = metrics if metrics is not None else {"retries": 0}
        holder = {"conn": pg}
        return (
            dm._drop_leaf_with_retries(holder, 5, self._files(), "viaduck.viaduck_state", "m", False, metrics),
            metrics,
        )

    def test_retry_then_success(self, monkeypatch):
        monkeypatch.setattr(dm.time, "sleep", lambda _: None)
        attempts = {"n": 0}

        def flaky_snapshot_insert(sql, params):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise psycopg.errors.UniqueViolation("duplicate key value violates unique constraint")
            return []

        script = _swap(_std_txn_script(), INS_SNAP, (INS_SNAP, flaky_snapshot_insert))
        pg = FakePG(script)
        out, metrics = self._run(pg)
        assert out["attempts"] == 2 and metrics["retries"] == 1
        # each attempt re-reads the baseline (no stale state across retries)
        baseline_reads = [s for s, _ in pg.executed if "ORDER BY snapshot_id DESC" in s]
        assert len(baseline_reads) == 2

    def test_deadlock_retries(self, monkeypatch):
        # 40P01: engine FlushDrop locks ascending; so do we (ORDER BY), but a
        # three-way interleave can still deadlock — full rollback is idempotent.
        monkeypatch.setattr(dm.time, "sleep", lambda _: None)
        attempts = {"n": 0}

        def deadlocked(sql, params):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise psycopg.errors.DeadlockDetected("deadlock detected")
            return []

        script = _swap(_std_txn_script(), INS_SNAP, (INS_SNAP, deadlocked))
        out, metrics = self._run(FakePG(script))
        assert out["attempts"] == 2 and metrics["retries"] == 1

    def test_fatal_sqlstate_propagates(self):
        def boom(sql, params):
            raise psycopg.errors.UndefinedColumn("nope")  # 42703 — not retryable, not OperationalError

        script = _swap(_std_txn_script(), INS_SNAP, (INS_SNAP, boom))
        with pytest.raises(psycopg.Error, match="nope"):
            self._run(FakePG(script))

    def test_attempt_cap(self, monkeypatch):
        monkeypatch.setattr(dm.time, "sleep", lambda _: None)
        monkeypatch.setattr(dm, "_DROP_MAX_ATTEMPTS", 3)

        def always_conflict(sql, params):
            raise psycopg.errors.UniqueViolation("dup")

        script = _swap(_std_txn_script(), INS_SNAP, (INS_SNAP, always_conflict))
        with pytest.raises(dm._DropAbort, match="exceeded 3 attempts"):
            self._run(FakePG(script))

    def test_leaf_skip_not_retried(self, monkeypatch):
        monkeypatch.setattr(dm.time, "sleep", lambda _: None)
        script = _std_txn_script(updated=[])  # vanished leaf -> _LeafSkip
        out_metrics = {"retries": 0}
        with pytest.raises(dm._LeafSkip):
            self._run(FakePG(script), out_metrics)
        assert out_metrics["retries"] == 0

    def test_lock_contention_retries_then_succeeds(self, monkeypatch):
        monkeypatch.setattr(dm.time, "sleep", lambda _: None)
        lock_attempts = {"n": 0}

        def contended(sql, params):
            lock_attempts["n"] += 1
            return [(lock_attempts["n"] > 1,)]

        script = _swap(_std_txn_script(), "pg_try_advisory_lock", ("pg_try_advisory_lock", contended))
        out, metrics = self._run(FakePG(script))
        assert out["attempts"] == 2 and metrics["retries"] == 1

    def test_lock_contention_has_own_budget(self, monkeypatch):
        monkeypatch.setattr(dm.time, "sleep", lambda _: None)
        monkeypatch.setattr(dm, "_DROP_LOCK_ATTEMPTS", 2)
        script = _swap(_std_txn_script(), "pg_try_advisory_lock", ("pg_try_advisory_lock", [(False,)]))
        with pytest.raises(dm._DropAbort, match="advisory lock contended"):
            self._run(FakePG(script))

    def test_lock_released_per_attempt(self, monkeypatch):
        monkeypatch.setattr(dm.time, "sleep", lambda _: None)
        pg = FakePG(_std_txn_script())
        self._run(pg)
        unlocks = [s for s, _ in pg.executed if "pg_advisory_unlock_all" in s]
        assert len(unlocks) >= 2  # acquire-path reset + finally release


class TestFloorSnapshot:
    def test_primary_path(self):
        pg = FakePG([("SELECT max(snapshot_id)", [(900,)])])
        assert dm._floor_snapshot(pg, CUTOFF) == 900

    def test_expiry_horizon_fallback(self):
        # 3-day expire eats all rows older than cutoff+48h: fall back to
        # min(surviving) - 1 rather than a degenerate -1-that-drops-nothing.
        pg = FakePG([("SELECT max(snapshot_id)", [(None,)]), ("SELECT min(snapshot_id)", [(500,)])])
        assert dm._floor_snapshot(pg, CUTOFF) == 499

    def test_empty_catalog_aborts(self):
        pg = FakePG([("SELECT max(snapshot_id)", [(None,)]), ("SELECT min(snapshot_id)", [(None,)])])
        with pytest.raises(dm._DropAbort, match="empty catalog"):
            dm._floor_snapshot(pg, CUTOFF)


class TestUnageableFiles:
    def test_count_zero_passes(self):
        pg = FakePG([("SELECT count(*)", [(0,)])])
        dm._assert_no_unageable_files(pg, 5)

    def test_aborts_with_count_and_samples(self):
        pg = FakePG([("SELECT count(*)", [(2,)]), ("LIMIT 5", [(77,), (78,)])])
        with pytest.raises(dm._DropAbort, match="NO partition values"):
            dm._assert_no_unageable_files(pg, 5)


def _driver_args(**kw):
    ns = SimpleNamespace(
        table="main.events_nrt",
        partition="",
        manifest="",
        cutoff="",
        retention_days=None,
        force_young_cutoff=False,
        execute=False,
        max_files=10_000,
        no_viaduck_guard=False,
        viaduck_state_schema="viaduck",
        viaduck_state_table="viaduck_state",
        pace_ms=0,
        max_seconds=3300,
        campaign="",
    )
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


def _gauges():
    from prometheus_client import CollectorRegistry, Gauge

    reg = CollectorRegistry()
    names = [
        "files_dropped",
        "leaves_dropped",
        "leaves_skipped",
        "retries",
        "aborts",
        "skipped_files",
        "excluded_rot",
        "last_success",
    ]
    return {k: Gauge(f"test_drop_{k}", "", ["table"], registry=reg) for k in names}


def _driver_script(selection_rows, cursor_rows=None):
    """Full driver-path script: resolve table/spec, no unageable files, floor
    via the primary path, cursor guard, then per-leaf selection."""
    if cursor_rows is None:
        # fresh timestamp: the lag abort compares against now-derived floors
        cursor_rows = [("d1", "i1", 500, datetime.now(UTC))]
    spec = _spec_nrt()
    col_rows = [(k.index, k.column_id, k.transform, k.column_name) for k in spec.keys]
    return [
        ("FROM public.ducklake_table t", [(5,)]),
        ("FROM public.ducklake_partition_info", [(0,)]),
        ("FROM public.ducklake_partition_column", col_rows),
        ("AND NOT EXISTS", [(0,)]),  # unageable count
        ("IS DISTINCT FROM", [(0,)]),  # old-spec leaf check (partition mode)
        ("SELECT max(snapshot_id)", [(FLOOR,)]),
        ("viaduck.viaduck_state v", cursor_rows),
        ("df.partition_id = %s", selection_rows),
    ]


class TestListDriver:
    """Smoke for the list op's driver path (round-2 F1: the signature drift
    crashed it before any SQL ran)."""

    def test_smoke_writes_manifest(self, monkeypatch, tmp_path):
        out = tmp_path / "manifest.json"
        args = SimpleNamespace(
            table="main.events_nrt",
            cutoff="2026-08-01T00",
            retention_days=None,
            max_files=10_000,
            max_rot=0,
            force_young_cutoff=False,
            out=str(out),
            with_file_ids=True,
        )
        spec = _spec_nrt()
        script = [
            ("FROM public.ducklake_table t", [(5,)]),
            ("FROM public.ducklake_partition_info", [(0,)]),
            (
                "FROM public.ducklake_partition_column",
                [(k.index, k.column_id, k.transform, k.column_name) for k in spec.keys],
            ),
            ("AND NOT EXISTS", [(0,)]),
            ("SELECT max(snapshot_id)", [(FLOOR,)]),
            ("GROUP BY fpv.data_file_id", [_erow(11, ("2", "2026", "7", "30", "13"), begin=10)]),
        ]
        pg = FakePG(script)
        monkeypatch.setattr(dm, "_pg_direct_connect", lambda: pg)
        dm.list_droppable_partitions(args, _gauges())
        manifest = json.loads(out.read_text())
        assert manifest["table_id"] == 5 and manifest["partition_id"] == 0
        assert [lf["partition"] for lf in manifest["leaves"]] == ["team_id=2,year=2026,month=7,day=30,hour=13"]
        assert manifest["leaves"][0]["file_ids"] == [11]
        assert manifest["diagnostics"]["rot_files"] == 0


class TestDropPartitionsDriver:
    """Orchestration through drop_partitions() with a scripted libpq double."""

    def _manifest(self, tmp_path, leaves, cutoff=CUTOFF):
        manifest = {
            "tool": "list-droppable-partitions",
            "manifest_version": 1,
            "table": "main.events_nrt",
            "table_id": 5,
            "partition_id": 0,
            "cutoff": cutoff.isoformat(),
            "leaves": leaves,
        }
        p = tmp_path / "m.json"
        p.write_text(json.dumps(manifest))
        return str(p)

    def _run(self, monkeypatch, args, script):
        pg = FakePG(script)
        monkeypatch.setattr(dm, "_pg_direct_connect", lambda: pg)
        dm.drop_partitions(args, _gauges())
        return pg

    def test_dry_run_writes_nothing(self, monkeypatch, tmp_path):
        args = _driver_args(partition="team_id=2,year=2026,month=8,day=1,hour=13", retention_days=14)
        pg = self._run(monkeypatch, args, _driver_script([(11, 10, 1000, 10)]))
        writes = [s for s, _ in pg.executed if "INSERT INTO public.ducklake_snapshot (" in s or "UPDATE public." in s]
        assert writes == []
        assert not any("pg_try_advisory_lock" in s for s, _ in pg.executed)  # dry-run never locks

    def test_guard_skip_writes_nothing(self, monkeypatch, tmp_path):
        # cursor guard at 5; the member's begin_snapshot 10 >= 5 -> whole-leaf skip
        args = _driver_args(partition="team_id=2,year=2026,month=8,day=1,hour=13", retention_days=14)
        cursor_rows = [("d1", "i1", 5, datetime.now(UTC))]  # tiny cursor id, fresh timestamp (no lag)
        pg = self._run(monkeypatch, args, _driver_script([(11, 10, 1000, 10)], cursor_rows))
        writes = [s for s, _ in pg.executed if INS_SNAP in s]
        assert writes == []

    def test_execute_drops_one_leaf(self, monkeypatch):
        args = _driver_args(partition="team_id=2,year=2026,month=8,day=1,hour=13", retention_days=14, execute=True)
        script = _driver_script([(11, 10, 1000, 10)]) + _std_txn_script()
        pg = self._run(monkeypatch, args, script)
        assert any(INS_SNAP in s for s, _ in pg.executed)

    def test_manifest_table_mismatch_aborts(self, monkeypatch, tmp_path):
        path = self._manifest(tmp_path, [])
        import json as _json

        m = _json.loads(open(path).read())
        m["table"] = "main.other"
        open(path, "w").write(_json.dumps(m))
        args = _driver_args(manifest=path)
        with pytest.raises(dm._DropAbort, match="!= --table"):
            self._run(monkeypatch, args, _driver_script([]))

    def test_manifest_cutoff_arg_refused(self, monkeypatch, tmp_path):
        path = self._manifest(tmp_path, [])
        args = _driver_args(manifest=path, cutoff="2026-08-01T00")
        with pytest.raises(dm._DropAbort, match="refused"):
            self._run(monkeypatch, args, _driver_script([]))

    def test_manifest_id_crosscheck_aborts(self, monkeypatch, tmp_path):
        path = self._manifest(tmp_path, [])
        import json as _json

        m = _json.loads(open(path).read())
        m["partition_id"] = 9  # live spec is 0
        open(path, "w").write(_json.dumps(m))
        args = _driver_args(manifest=path)
        with pytest.raises(dm._DropAbort, match="table_id/partition_id"):
            self._run(monkeypatch, args, _driver_script([]))

    def test_manifest_oversized_leaf_skips_not_aborts(self, monkeypatch, tmp_path):
        leaf = {
            "partition": "team_id=2,year=2026,month=8,day=1,hour=13",
            "values": {"team_id": "2", "year": "2026", "month": "8", "day": "1", "hour": "13"},
            "file_ids": [11, 12],
        }
        path = self._manifest(tmp_path, [leaf])
        # selection returns 3 rows against max_files=2 -> oversized -> skip
        args = _driver_args(manifest=path, max_files=2)
        script = _driver_script([(11, 10, 1000, 10), (12, 10, 1000, 11), (13, 10, 1000, 12)])
        pg = self._run(monkeypatch, args, script)  # must NOT raise
        assert not any(INS_SNAP in s for s, _ in pg.executed)

    def test_partition_mode_oversized_aborts(self, monkeypatch):
        args = _driver_args(partition="team_id=2,year=2026,month=8,day=1,hour=13", retention_days=14, max_files=2)
        script = _driver_script([(11, 10, 1000, 10), (12, 10, 1000, 11), (13, 10, 1000, 12)])
        with pytest.raises(dm._DropAbort, match="pathological leaf"):
            self._run(monkeypatch, args, script)

    def test_dry_run_leaves_no_metric_footprint(self, monkeypatch):
        # A manual dry-run must not zero the cron's counters nor refresh the
        # last-success staleness signal (round-2 ops HIGH).
        gauges = _gauges()
        args = _driver_args(partition="team_id=2,year=2026,month=8,day=1,hour=13", retention_days=14)
        pg = FakePG(_driver_script([(11, 10, 1000, 10)]))
        monkeypatch.setattr(dm, "_pg_direct_connect", lambda: pg)
        dm.drop_partitions(args, gauges)
        for g in gauges.values():
            assert list(g.collect()) == [] or all(not m.samples for m in g.collect())
