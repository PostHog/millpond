"""Unit tests for millpond/hoglake.py — the HoglakeSink.

Everything here runs against a mocked pyhoglake client layer; the real
server round-trips live in tests/integration/test_hoglake_integration.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pyarrow as pa
import pytest
from pyhoglake import (
    AlreadyExistsError,
    Column,
    CommitConflictError,
    ExpiredError,
    HoglakeError,
    IncarnationChangedError,
    MalformedResponseError,
    NotFoundError,
    UnsupportedTypeError,
    ValidationError,
)

from millpond import hoglake

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cfg(**overrides) -> MagicMock:
    cfg = MagicMock()
    cfg.destination = "hoglake"
    cfg.hoglake_url = "http://localhost:28080"
    cfg.hoglake_catalog = "millpond"
    cfg.hoglake_namespace = "analytics"
    cfg.hoglake_table = "events"
    cfg.hoglake_data_path = None
    cfg.hoglake_s3_endpoint = "http://localhost:29000"
    cfg.hoglake_s3_access_key = "ak"
    cfg.hoglake_s3_secret_key = "sk"
    cfg.hoglake_s3_region = "us-east-1"
    cfg.hoglake_partition_by = None
    cfg.sort_by = None
    cfg.ordinal = 0
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _col(name, type_, field_id, ordinal, **kw) -> Column:
    return Column(name=name, type=type_, field_id=field_id, ordinal=ordinal, **kw)


def _wire_dynamic_alter(table):
    """Make the mock table's alter() behave like the real client: apply
    add_column/promote_column to the mock's live columns and return a
    TableInfo-shaped object, exactly as pyhoglake's Table.alter does."""

    def do_alter(ops_list):
        cols = list(table.columns)
        for op in ops_list:
            if op.op == "add_column":
                c = op.body["column"]
                cols.append(_col(c["name"], c["type"], 100 + len(cols), len(cols) + 1))
            elif op.op == "promote_column":
                cols = [
                    _col(x.name, op.body["to"], x.field_id, x.ordinal) if x.name == op.body["name"] else x for x in cols
                ]
        table.columns = tuple(cols)
        info = MagicMock()
        info.columns = tuple(cols)
        table.info.return_value = info
        return info

    table.alter.side_effect = do_alter


def _mock_stack(columns):
    """(client, catalog, ns, table) MagicMocks wired the way pyhoglake
    resolves them. `columns` is the live table schema."""
    client = MagicMock()
    catalog = MagicMock()
    ns = MagicMock()
    table = MagicMock()
    table.columns = tuple(columns)
    info = MagicMock()
    info.columns = tuple(columns)
    table.info.return_value = info
    _wire_dynamic_alter(table)
    client.catalog.return_value = catalog
    catalog.namespace.return_value = ns
    ns.table.return_value = table
    append_result = MagicMock()
    append_result.snapshot_id = 7
    table.append.return_value = append_result
    return client, catalog, ns, table


_EVENTS_COLUMNS = [
    _col("uuid", "string", 1, 1),
    _col("event", "string", 2, 2),
    _col("team_id", "long", 3, 3),
    _col("properties", "string", 4, 4),
    _col("_inserted_at", "timestamptz", 5, 5),
]


def _sink(cfg=None, columns=_EVENTS_COLUMNS):
    cfg = cfg or _cfg()
    client, catalog, ns, table = _mock_stack(columns)
    with patch("millpond.hoglake.HoglakeClient", return_value=client):
        s = hoglake.HoglakeSink(cfg)
    return s, client, catalog, ns, table


def _batch(**cols) -> pa.Table:
    return pa.table(cols) if cols else pa.table({"uuid": ["a"], "event": ["e"], "team_id": [1], "properties": ["{}"]})


# ---------------------------------------------------------------------------
# Constructor guards
# ---------------------------------------------------------------------------


class TestHoglakeSinkInit:
    @pytest.mark.parametrize(
        "missing_field",
        [
            "hoglake_url",
            "hoglake_catalog",
            "hoglake_namespace",
            "hoglake_table",
            "hoglake_s3_access_key",
            "hoglake_s3_secret_key",
        ],
    )
    def test_missing_required_field_raises_runtimeerror(self, missing_field):
        cfg = _cfg(**{missing_field: None})
        with pytest.raises(RuntimeError, match=missing_field):
            with patch("millpond.hoglake.HoglakeClient"):
                hoglake.HoglakeSink(cfg)

    def test_runtimeerror_survives_python_optimize(self):
        # `python -O` strips asserts; verify the guard is an explicit raise
        # by checking the constructor source doesn't use `assert`.
        import inspect

        src = inspect.getsource(hoglake.HoglakeSink.__init__)
        assert "assert " not in src

    def test_client_constructed_with_s3_config(self):
        cfg = _cfg()
        with patch("millpond.hoglake.HoglakeClient") as mock_client:
            hoglake.HoglakeSink(cfg)
        _, kwargs = mock_client.call_args
        s3 = kwargs["s3"]
        assert mock_client.call_args.args[0] == "http://localhost:28080"
        assert s3.access_key == "ak"
        assert s3.secret_key == "sk"
        assert s3.endpoint_override == "http://localhost:29000"
        assert s3.region == "us-east-1"


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------


class TestIsRetryable:
    @pytest.mark.parametrize(
        "exc",
        [
            CommitConflictError("conflict", status_code=409),  # OCC — retry on a fresh baseline
            NotFoundError("gone", status_code=404),  # table dropped; re-ensure after reset
            IncarnationChangedError("recreated"),  # re-resolve the live incarnation
            httpx.ConnectError("refused"),
            httpx.ReadTimeout("slow"),
            HoglakeError("boom", status_code=503),  # commit admission backpressure
            HoglakeError("boom", status_code=500),
            OSError("S3 flake"),  # pyarrow S3 upload failures
            RuntimeError("unknown"),  # unknown → assume transient
        ],
    )
    def test_retryable(self, exc):
        assert hoglake.is_retryable(exc) is True

    @pytest.mark.parametrize(
        "exc",
        [
            ValidationError("bad", status_code=422),
            UnsupportedTypeError("no mapping"),
            AlreadyExistsError("exists", status_code=409),
            ExpiredError("expired", status_code=410),
            MalformedResponseError("garbage"),
        ],
    )
    def test_not_retryable(self, exc):
        assert hoglake.is_retryable(exc) is False


# ---------------------------------------------------------------------------
# Schema mapping — every type millpond's converter can produce
# ---------------------------------------------------------------------------


class TestTableSchemaForBatch:
    def test_appends_inserted_at_timestamptz(self):
        out = hoglake.table_schema_for_batch(pa.schema([("uuid", pa.string())]))
        assert out.names == ["uuid", "_inserted_at"]
        assert out.field("_inserted_at").type == pa.timestamp("us", tz="UTC")

    @pytest.mark.parametrize(
        ("arrow_type", "hoglake_type"),
        [
            (pa.string(), "string"),  # VARCHAR columns (incl. flattened JSON)
            (pa.large_string(), "string"),
            (pa.int64(), "long"),  # _normalize_numeric_types pins ints to int64
            (pa.float64(), "double"),  # ... and floats to float64
            (pa.bool_(), "boolean"),
            (pa.timestamp("us", tz="UTC"), "timestamptz"),  # MILLPOND_TYPED_COLUMNS timestamptz
        ],
    )
    def test_millpond_types_map(self, arrow_type, hoglake_type):
        from pyhoglake.types import schema_to_column_defs

        out = hoglake.table_schema_for_batch(pa.schema([("c", arrow_type)]))
        defs = schema_to_column_defs(out)
        assert defs[0]["type"] == hoglake_type


# ---------------------------------------------------------------------------
# write(): reserved collisions and column hygiene
# ---------------------------------------------------------------------------


class TestReservedCollision:
    @pytest.mark.parametrize("name", ["_inserted_at", "year", "month", "day", "hour"])
    def test_reserved_column_raises_before_any_client_call(self, name):
        s, client, *_ = _sink()
        with pytest.raises(ValueError, match="Hoglake-reserved"):
            s.write(pa.table({name: ["x"], "uuid": ["a"]}))
        client.catalog.assert_not_called()


class TestColumnHygiene:
    @patch("millpond.hoglake.metrics")
    def test_unsafe_field_name_dropped_with_metric(self, mock_metrics):
        s, *_, table = _sink()
        s.write(pa.table({"uuid": ["a"], "bad-name": ["x"]}))
        appended = table.append.call_args.args[0]
        assert "bad-name" not in appended.column_names
        mock_metrics.records_skipped_total.labels.assert_any_call(reason="unsafe_field_name")

    @patch("millpond.hoglake.metrics")
    def test_hog_prefixed_field_dropped_with_metric(self, mock_metrics):
        # `_hog*` is server-reserved (422 at DDL and append); a payload key
        # with that prefix must not be able to wedge the partition forever.
        s, *_, table = _sink()
        s.write(pa.table({"uuid": ["a"], "_hog_row_id": [5]}))
        appended = table.append.call_args.args[0]
        assert "_hog_row_id" not in appended.column_names
        mock_metrics.records_skipped_total.labels.assert_any_call(reason="unsafe_field_name")


# ---------------------------------------------------------------------------
# write(): bootstrap
# ---------------------------------------------------------------------------


class TestBootstrap:
    def test_missing_catalog_without_data_path_is_clear_error(self):
        s, client, *_ = _sink()
        client.catalog.side_effect = NotFoundError("no catalog", status_code=404)
        with pytest.raises(RuntimeError, match="HOGLAKE_DATA_PATH"):
            s.write(_batch())

    def test_missing_catalog_created_when_data_path_set(self):
        s, client, catalog, ns, table = _sink(_cfg(hoglake_data_path="s3://bucket/millpond/"))
        client.catalog.side_effect = [NotFoundError("no catalog", status_code=404)]
        created = MagicMock()
        created.namespace.return_value = ns
        client.create_catalog.return_value = created
        s.write(_batch())
        client.create_catalog.assert_called_once_with("millpond", "s3://bucket/millpond/")

    def test_concurrent_catalog_creation_tolerated(self):
        s, client, catalog, ns, table = _sink(_cfg(hoglake_data_path="s3://bucket/millpond/"))
        client.catalog.side_effect = [NotFoundError("no catalog", status_code=404), catalog]
        client.create_catalog.side_effect = AlreadyExistsError("exists", status_code=409)
        assert s.write(_batch()) == 1

    def test_missing_namespace_created(self):
        s, client, catalog, ns, table = _sink()
        catalog.namespace.side_effect = NotFoundError("no ns", status_code=404)
        catalog.create_namespace.return_value = ns
        s.write(_batch())
        catalog.create_namespace.assert_called_once_with("analytics")

    def test_concurrent_namespace_creation_tolerated(self):
        s, client, catalog, ns, table = _sink()
        catalog.namespace.side_effect = [NotFoundError("no ns", status_code=404), ns]
        catalog.create_namespace.side_effect = AlreadyExistsError("exists", status_code=409)
        assert s.write(_batch()) == 1

    def test_missing_table_created_from_batch_schema(self):
        s, client, catalog, ns, table = _sink()
        ns.table.side_effect = NotFoundError("no table", status_code=404)
        ns.create_table.return_value = table
        s.write(_batch())
        name, schema = ns.create_table.call_args.args
        assert name == "events"
        assert schema.names == ["uuid", "event", "team_id", "properties", "_inserted_at"]
        assert schema.field("_inserted_at").type == pa.timestamp("us", tz="UTC")

    def test_concurrent_table_creation_tolerated(self):
        s, client, catalog, ns, table = _sink()
        ns.table.side_effect = [NotFoundError("no table", status_code=404), table]
        ns.create_table.side_effect = AlreadyExistsError("exists", status_code=409)
        assert s.write(_batch()) == 1

    def test_second_write_uses_cached_table(self):
        s, client, catalog, ns, table = _sink()
        s.write(_batch())
        s.write(_batch())
        assert client.catalog.call_count == 1
        assert ns.table.call_count == 1

    def test_reset_caches_forces_reresolve(self):
        s, client, catalog, ns, table = _sink()
        s.write(_batch())
        s.reset_caches()
        s.write(_batch())
        assert client.catalog.call_count == 2

    def test_close_closes_client(self):
        s, client, *_ = _sink()
        s.close()
        client.close.assert_called_once()


class TestBootstrapSpecs:
    def _create_flow(self, cfg, created_columns=_EVENTS_COLUMNS):
        s, client, catalog, ns, table = _sink(cfg, columns=created_columns)
        ns.table.side_effect = NotFoundError("no table", status_code=404)
        ns.create_table.return_value = table
        return s, ns, table

    def test_partition_spec_set_with_resolved_field_ids(self):
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None), ("_inserted_at", "month", None)))
        s, ns, table = self._create_flow(cfg)
        s.write(_batch())
        ops_sent = table.alter.call_args.args[0]
        spec_op = next(o for o in ops_sent if o.op == "set_partition_spec")
        fields = spec_op.body["fields"]
        assert fields[0]["source_field_id"] == 3  # team_id
        assert fields[0]["transform"] == "identity"
        assert fields[1]["source_field_id"] == 5  # _inserted_at
        assert fields[1]["transform"] == "month"

    def test_bucket_param_carried(self):
        cfg = _cfg(hoglake_partition_by=(("team_id", "bucket", 16),))
        s, ns, table = self._create_flow(cfg)
        s.write(_batch())
        spec_op = next(o for o in table.alter.call_args.args[0] if o.op == "set_partition_spec")
        assert spec_op.body["fields"][0]["transform_param"] == 16

    def test_partition_column_missing_from_schema_raises(self):
        cfg = _cfg(hoglake_partition_by=(("nope", "identity", None),))
        s, ns, table = self._create_flow(cfg)
        with pytest.raises(RuntimeError, match="nope"):
            s.write(_batch())

    def test_sort_order_declared_from_sort_by(self):
        cfg = _cfg(sort_by=("team_id", "uuid"))
        s, ns, table = self._create_flow(cfg)
        s.write(_batch())
        ops_sent = table.alter.call_args.args[0]
        sort_op = next(o for o in ops_sent if o.op == "set_sort_order")
        fields = sort_op.body["sort_fields"]
        # asc + nulls_last mirrors main._apply_sort (ascending, at_end)
        assert fields == [
            {"source_field_id": 3, "direction": "asc", "null_order": "nulls_last"},
            {"source_field_id": 1, "direction": "asc", "null_order": "nulls_last"},
        ]

    def test_missing_sort_field_skips_sort_spec_nonfatally(self):
        cfg = _cfg(sort_by=("not_there",))
        s, ns, table = self._create_flow(cfg)
        s.write(_batch())  # no raise
        ops_sent = table.alter.call_args.args[0] if table.alter.call_args else []
        assert not any(o.op == "set_sort_order" for o in ops_sent)

    def test_no_specs_means_no_alter(self):
        s, ns, table = self._create_flow(_cfg())
        s.write(_batch())
        table.alter.assert_not_called()


# ---------------------------------------------------------------------------
# write(): steady state + evolution
# ---------------------------------------------------------------------------


class TestSteadyStateWrite:
    def test_returns_record_count(self):
        s, *_, table = _sink()
        out = s.write(_batch())
        assert out == 1
        table.append.assert_called_once()

    def test_inserted_at_stamped_once_per_flush(self):
        s, *_, table = _sink()
        s.write(pa.table({"uuid": ["a", "b", "c"], "event": ["e", "e", "e"], "team_id": [1, 2, 3]}))
        appended = table.append.call_args.args[0]
        col = appended.column("_inserted_at")
        assert col.type == pa.timestamp("us", tz="UTC")
        vals = col.to_pylist()
        assert len(set(vals)) == 1 and vals[0] is not None

    def test_missing_table_columns_null_filled(self):
        # `properties` absent from this batch — upstream removed a column;
        # mirror DuckLake's INSERT BY NAME null-fill.
        s, *_, table = _sink()
        s.write(pa.table({"uuid": ["a"], "event": ["e"], "team_id": [1]}))
        appended = table.append.call_args.args[0]
        assert appended.column("properties").null_count == 1
        assert appended.column("properties").type == pa.string()


class TestEvolution:
    @patch("millpond.hoglake.metrics")
    def test_new_column_added_via_alter(self, mock_metrics):
        s, *_, table = _sink()
        s.write(pa.table({"uuid": ["a"], "new_col": ["x"]}))
        add_ops = [o for c in table.alter.call_args_list for o in c.args[0] if o.op == "add_column"]
        assert len(add_ops) == 1
        assert add_ops[0].body["column"]["name"] == "new_col"
        assert add_ops[0].body["column"]["type"] == "string"
        mock_metrics.schema_columns_added_total.inc.assert_called_once()

    @patch("millpond.hoglake.metrics")
    def test_add_failure_drops_column_and_continues(self, mock_metrics):
        s, *_, table = _sink()
        table.alter.side_effect = ValidationError("nope", status_code=422)
        s.write(pa.table({"uuid": ["a"], "new_col": ["x"]}))
        appended = table.append.call_args.args[0]
        assert "new_col" not in appended.column_names
        mock_metrics.errors_total.labels.assert_any_call(type="schema")

    @patch("millpond.hoglake.metrics")
    def test_conflict_on_add_reresolves_and_proceeds(self, mock_metrics):
        # Another writer added the column concurrently: the 409 must not
        # fail the flush; re-resolve shows the column present.
        cols_after = _EVENTS_COLUMNS + [_col("new_col", "string", 6, 6)]
        s, client, catalog, ns, table = _sink()
        table.alter.side_effect = CommitConflictError("concurrent DDL", status_code=409)
        info_after = MagicMock()
        info_after.columns = tuple(cols_after)
        table.info.return_value = info_after
        s.write(pa.table({"uuid": ["a"], "new_col": ["x"]}))
        appended = table.append.call_args.args[0]
        assert "new_col" in appended.column_names

    @patch("millpond.hoglake.metrics")
    def test_int_promoted_to_long(self, mock_metrics):
        cols = [_col("uuid", "string", 1, 1), _col("count", "int", 2, 2), _col("_inserted_at", "timestamptz", 3, 3)]
        s, *_, table = _sink(columns=cols)
        s.write(pa.table({"uuid": ["a"], "count": [2**40]}))
        promote_ops = [o for c in table.alter.call_args_list for o in c.args[0] if o.op == "promote_column"]
        assert promote_ops and promote_ops[0].body == {"name": "count", "to": "long"}
        mock_metrics.schema_columns_widened_total.inc.assert_called_once()

    @patch("millpond.hoglake.metrics")
    def test_float_promoted_to_double(self, mock_metrics):
        cols = [_col("uuid", "string", 1, 1), _col("ratio", "float", 2, 2), _col("_inserted_at", "timestamptz", 3, 3)]
        s, *_, table = _sink(columns=cols)
        s.write(pa.table({"uuid": ["a"], "ratio": [0.5]}))
        promote_ops = [o for c in table.alter.call_args_list for o in c.args[0] if o.op == "promote_column"]
        assert promote_ops and promote_ops[0].body == {"name": "ratio", "to": "double"}

    @patch("millpond.hoglake.metrics")
    def test_unpromotable_mismatch_left_to_append_cast(self, mock_metrics):
        # Table says long, batch says string (all-null inference wobble).
        # No ALTER is issued; the append-side cast handles it (nulls cast
        # cleanly; real garbage fails loudly — same posture as DuckLake's
        # INSERT-side cast).
        s, *_, table = _sink()
        s.write(pa.table({"uuid": ["a"], "team_id": pa.array([None], type=pa.string())}))
        assert table.alter.call_count == 0
        table.append.assert_called_once()


class TestWriteFailurePropagation:
    def test_append_failure_raises(self):
        # At-least-once: a failed write must surface to main.py's retry
        # loop; offsets only commit after write() returns.
        s, *_, table = _sink()
        table.append.side_effect = CommitConflictError("conflict", status_code=409)
        with pytest.raises(CommitConflictError):
            s.write(_batch())

    def test_incarnation_change_raises(self):
        s, *_, table = _sink()
        table.append.side_effect = IncarnationChangedError("recreated")
        with pytest.raises(IncarnationChangedError):
            s.write(_batch())
