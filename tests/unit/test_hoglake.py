"""Unit tests for millpond/hoglake.py — the HoglakeSink.

Everything here runs against a mocked pyhoglake client layer; the real
server round-trips live in tests/integration/test_hoglake_integration.py.
"""

from __future__ import annotations

from dataclasses import dataclass
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
    PartitionField,
    PartitionSpec,
    UnsupportedTypeError,
    ValidationError,
)
from pyhoglake.models import SortField, SortSpec

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
    cfg.hoglake_max_retry_count = 8
    cfg.hoglake_request_timeout_s = 30.0
    cfg.sort_by = None
    cfg.ordinal = 0
    cfg.table_label = "events"
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _col(name, type_, field_id, ordinal, **kw) -> Column:
    return Column(name=name, type=type_, field_id=field_id, ordinal=ordinal, **kw)


@dataclass
class _FakeInfo:
    """The TableInfo surface the sink reads. A MagicMock is wrong here:
    `info.partition_spec` on a MagicMock is a truthy Mock, so a test
    could never tell an unpartitioned table from a partitioned one — and
    telling those apart is the whole point of the reconciliation path."""

    columns: tuple
    partition_spec: PartitionSpec | None = None
    sort_spec: SortSpec | None = None
    namespace: str = "analytics"
    name: str = "events"


def _wire_dynamic_alter(table, state: _FakeInfo):
    """Make the mock table's alter() behave like the real server: apply
    the ops to the live table state and return the post-alter info,
    exactly as pyhoglake's Table.alter does (one atomic DDL commit)."""

    def do_alter(ops_list):
        cols = list(state.columns)
        for op in ops_list:
            if op.op == "add_column":
                c = op.body["column"]
                cols.append(_col(c["name"], c["type"], 100 + len(cols), len(cols) + 1))
            elif op.op == "promote_column":
                cols = [
                    _col(x.name, op.body["to"], x.field_id, x.ordinal) if x.name == op.body["name"] else x for x in cols
                ]
            elif op.op == "set_partition_spec":
                fields = tuple(
                    PartitionField(f["source_field_id"], f["transform"], f.get("transform_param"))
                    for f in op.body["fields"]
                )
                state.partition_spec = PartitionSpec(spec_id=1, fields=fields) if fields else None
            elif op.op == "set_sort_order":
                fields = tuple(
                    SortField(f["source_field_id"], f["direction"], f["null_order"]) for f in op.body["sort_fields"]
                )
                state.sort_spec = SortSpec(sort_id=1, fields=fields) if fields else None
        state.columns = tuple(cols)
        table.columns = state.columns
        return state

    table.alter.side_effect = do_alter


_WRITTEN: list[pa.Table] = []


@pytest.fixture(autouse=True)
def _capture_parquet(monkeypatch):
    """Capture what the sink serializes instead of writing it to disk.

    The sink no longer hands pyarrow tables to `Table.append`: it writes
    local parquet, uploads it, and registers the upload in a separate
    idempotent commit. The batch that lands in the lake is therefore the
    one that reaches `pq.write_table`, which is what these tests assert
    on."""
    _WRITTEN.clear()
    monkeypatch.setattr(hoglake.pq, "write_table", lambda table, path, **kw: _WRITTEN.append(table))
    return _WRITTEN


def _published(_table=None) -> pa.Table:
    """The (single) batch the last flush serialized for upload."""
    assert _WRITTEN, "nothing was written"
    return _WRITTEN[-1]


def _wire_prepared_commit(table, catalog):
    """Mock the prepared-append handshake: prepare_append_files returns a
    commit request naming one file per partition group, and
    Catalog.commit_prepared publishes it."""

    def prepare(files, *, idempotency_key, **kwargs):
        return {
            "idempotency_key": idempotency_key,
            "read_snapshot": 41,
            "appends": [
                {
                    "namespace": "analytics",
                    "table": "events",
                    "expected_table_uuid": "0e0b6c8e-0000-0000-0000-000000000001",
                    "files": [
                        {"path": f"s3://bucket/lake/{idempotency_key}/{i}.parquet", "partition_values": values}
                        for i, (_path, values) in enumerate(files)
                    ],
                }
            ],
        }

    table.prepare_append_files.side_effect = prepare
    catalog.commit_prepared.return_value = MagicMock(snapshot_id=7, schema_version=1)


def _mock_stack(columns, partition_spec=None, sort_spec=None):
    """(client, catalog, ns, table) MagicMocks wired the way pyhoglake
    resolves them. `columns` is the live table schema."""
    client = MagicMock()
    catalog = MagicMock()
    ns = MagicMock()
    table = MagicMock()
    state = _FakeInfo(columns=tuple(columns), partition_spec=partition_spec, sort_spec=sort_spec)
    table.columns = state.columns
    table.info.return_value = state
    table.state = state
    _wire_dynamic_alter(table, state)
    client.catalog.return_value = catalog
    catalog.namespace.return_value = ns
    ns.table.return_value = table
    ns.create_table.return_value = table
    _wire_prepared_commit(table, catalog)
    return client, catalog, ns, table


_EVENTS_COLUMNS = [
    _col("uuid", "string", 1, 1),
    _col("event", "string", 2, 2),
    _col("team_id", "long", 3, 3),
    _col("properties", "string", 4, 4),
    _col("_inserted_at", "timestamptz", 5, 5),
]


def _sink(cfg=None, columns=_EVENTS_COLUMNS, partition_spec=None, sort_spec=None):
    cfg = cfg or _cfg()
    client, catalog, ns, table = _mock_stack(columns, partition_spec, sort_spec)
    with patch("millpond.hoglake.HoglakeClient", return_value=client):
        s = hoglake.HoglakeSink(cfg)
    return s, client, catalog, ns, table


def _spec(*fields) -> PartitionSpec:
    """PartitionSpec from (column_name, transform[, param]) triples,
    resolved against _EVENTS_COLUMNS field ids."""
    fid = {c.name: c.field_id for c in _EVENTS_COLUMNS}
    return PartitionSpec(
        spec_id=1,
        fields=tuple(PartitionField(fid[f[0]], f[1], f[2] if len(f) > 2 else None) for f in fields),
    )


def _sort(*names) -> SortSpec:
    fid = {c.name: c.field_id for c in _EVENTS_COLUMNS}
    return SortSpec(sort_id=1, fields=tuple(SortField(fid[n], "asc", "nulls_last") for n in names))


def _batch(**cols) -> pa.Table:
    return pa.table(cols) if cols else pa.table({"uuid": ["a"], "event": ["e"], "team_id": [1], "properties": ["{}"]})


def _rows(n: int) -> pa.Table:
    """An n-row events batch."""
    return pa.table({"uuid": [f"u{i}" for i in range(n)], "team_id": list(range(n))})


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
    """Every exception here is built in the SHAPE pyhoglake actually
    produces. The client raises server errors through `_raise`, which
    always passes `status_code=` — a statusless instance is only ever
    the client-side variant, and the two classify differently, so a test
    that constructs the convenient one proves nothing about production."""

    @pytest.mark.parametrize(
        "exc",
        [
            CommitConflictError("commit conflict", status_code=409),  # OCC — retry on a fresh baseline
            NotFoundError("not found", status_code=404),  # table dropped; re-ensure after reset
            # BOTH incarnation shapes. The server-raised one carries 409
            # plus the recreation marker (client.py maps it off the
            # CommitConflictError body); the client's own pre-flight
            # re-resolve raises it with no status at all.
            IncarnationChangedError(
                "table x was recreated: expected uuid ...",
                status_code=409,
                detail="the table was recreated",
            ),
            IncarnationChangedError("was recreated"),
            httpx.ConnectError("refused"),
            httpx.ReadTimeout("slow"),
            HoglakeError("commit_queue_timeout", status_code=503),  # admission backpressure
            HoglakeError("internal_error", status_code=500),
            HoglakeError("too many requests", status_code=429),
            HoglakeError("request timeout", status_code=408),
            HoglakeError("client-side failure"),  # no status: never reached the server
            OSError("S3 flake"),  # pyarrow S3 upload failures
            RuntimeError("unknown"),  # unknown → assume transient
        ],
    )
    def test_retryable(self, exc):
        assert hoglake.is_retryable(exc) is True

    @pytest.mark.parametrize(
        "exc",
        [
            ValidationError("validation", status_code=422),
            UnsupportedTypeError("no mapping"),
            AlreadyExistsError("already_exists", status_code=409),
            ExpiredError("expired", status_code=410),
            MalformedResponseError("TableInfo: missing field 'columns'"),
            HoglakeError("bad_request", status_code=400),
        ],
    )
    def test_not_retryable(self, exc):
        assert hoglake.is_retryable(exc) is False

    def test_sink_exposes_the_classifier_to_the_retry_loop(self):
        # main._write_with_retry duck-types `is_retryable` off the sink;
        # an unwired classifier is a classifier that never runs.
        s, *_ = _sink()
        assert s.is_retryable(ValidationError("validation", status_code=422)) is False
        assert s.is_retryable(CommitConflictError("conflict", status_code=409)) is True


class TestRetryBudgetAndBackpressure:
    def test_budget_comes_from_config(self):
        s, *_ = _sink(_cfg(hoglake_max_retry_count=7))
        attempts, base = s.write_retry_budget()
        assert attempts == 7
        assert base > 0

    def test_client_built_with_the_configured_timeout(self):
        cfg = _cfg(hoglake_request_timeout_s=12.5)
        with patch("millpond.hoglake.HoglakeClient") as mock_client:
            hoglake.HoglakeSink(cfg)
        assert mock_client.call_args.kwargs["timeout"] == 12.5

    def test_retry_after_header_is_captured_and_consumed_once(self):
        # pyhoglake drops Retry-After entirely (it maps status codes and
        # nothing else), so the sink reads it off the response itself.
        s, *_ = _sink()
        s._note_response(httpx.Response(503, headers={"Retry-After": "1"}))
        assert s.retry_after_hint() == 1.0
        # One-shot: a hint from a past failure must not govern the next one.
        assert s.retry_after_hint() is None

    def test_retry_after_ignored_on_a_success(self):
        s, *_ = _sink()
        s._note_response(httpx.Response(200, headers={"Retry-After": "90"}))
        assert s.retry_after_hint() is None

    def test_unparseable_retry_after_is_ignored(self):
        s, *_ = _sink()
        s._note_response(httpx.Response(503, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}))
        assert s.retry_after_hint() is None

    def test_no_hint_without_a_header(self):
        s, *_ = _sink()
        s._note_response(httpx.Response(503))
        assert s.retry_after_hint() is None


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
    def test_inserted_at_collision_raises_before_any_table_work(self):
        s, client, catalog, ns, table = _sink()
        with pytest.raises(ValueError, match="Hoglake-reserved"):
            s.write(pa.table({"_inserted_at": ["x"], "uuid": ["a"]}))
        ns.table.assert_not_called()
        table.prepare_append_files.assert_not_called()

    @pytest.mark.parametrize("name", ["year", "month", "day", "hour"])
    def test_hive_names_are_ordinary_columns_for_hoglake(self, name):
        # year/month/day/hour are load-bearing for DuckLake only (Hive
        # directory keys). Hoglake partitions by Iceberg-semantics
        # transforms and materializes no derived columns, so a payload
        # key with one of those names is an ordinary column — it must
        # land, not crash-loop the partition on a fatal ValueError.
        assert name not in hoglake.RESERVED_COLUMNS
        s, client, catalog, ns, table = _sink()
        assert s.write(pa.table({name: ["x"], "uuid": ["a"]})) == 1
        appended = _published(table)
        assert name in appended.column_names


class TestColumnHygiene:
    @patch("millpond.hoglake.metrics")
    def test_unsafe_field_name_dropped_with_metric(self, mock_metrics):
        s, *_, table = _sink()
        s.write(pa.table({"uuid": ["a"], "bad-name": ["x"]}))
        appended = _published(table)
        assert "bad-name" not in appended.column_names
        mock_metrics.records_skipped_total.labels.assert_any_call(reason="unsafe_field_name")

    @patch("millpond.hoglake.metrics")
    def test_overlong_field_name_dropped_with_metric(self, mock_metrics):
        # The server caps column names at 128 chars
        # (^[A-Za-z_][A-Za-z0-9_-]{0,127}$). Steady state would degrade
        # (the add_column 422s and the column is dropped), but bootstrap
        # ships the whole schema in one create_table — an over-long name
        # there wedges the table forever. Drop it at the same gate as the
        # other unwritable names so both paths degrade identically.
        long_name = "x" * 129
        s, *_, table = _sink()
        assert s.write(pa.table({"uuid": ["a"], long_name: ["x"]})) == 1
        appended = _published(table)
        assert long_name not in appended.column_names
        mock_metrics.records_skipped_total.labels.assert_any_call(reason="unsafe_field_name")

    def test_name_at_the_length_limit_is_kept(self):
        s, *_, table = _sink()
        name = "x" * 128
        s.write(pa.table({"uuid": ["a"], name: ["x"]}))
        assert name in _published(table).column_names

    @patch("millpond.hoglake.metrics")
    def test_hog_prefixed_field_dropped_with_metric(self, mock_metrics):
        # `_hog*` is server-reserved (422 at DDL and append); a payload key
        # with that prefix must not be able to wedge the partition forever.
        s, *_, table = _sink()
        s.write(pa.table({"uuid": ["a"], "_hog_row_id": [5]}))
        appended = _published(table)
        assert "_hog_row_id" not in appended.column_names
        mock_metrics.records_skipped_total.labels.assert_any_call(reason="unsafe_field_name")


# ---------------------------------------------------------------------------
# write(): bootstrap
# ---------------------------------------------------------------------------


class TestStartupResolution:
    """The catalog is resolved in __init__, so a bad URL, bad credentials
    or an absent catalog is a STARTUP failure — which is what config.py
    and the README have always claimed. Resolving it lazily on the first
    flush meant the pod passed its probes, joined the consumer group,
    accumulated lag, and only then crash-looped."""

    def _construct(self, client, cfg=None):
        with patch("millpond.hoglake.HoglakeClient", return_value=client):
            return hoglake.HoglakeSink(cfg or _cfg())

    def test_catalog_resolved_at_construction(self):
        client, *_ = _mock_stack(_EVENTS_COLUMNS)
        self._construct(client)
        client.catalog.assert_called_once_with("millpond")

    def test_missing_catalog_without_data_path_refuses_at_construction(self):
        client, *_ = _mock_stack(_EVENTS_COLUMNS)
        client.catalog.side_effect = NotFoundError("not found", status_code=404)
        with pytest.raises(RuntimeError, match="HOGLAKE_DATA_PATH"):
            self._construct(client)

    def test_unreachable_control_plane_names_the_url(self):
        client, *_ = _mock_stack(_EVENTS_COLUMNS)
        client.catalog.side_effect = httpx.ConnectError("connection refused")
        with pytest.raises(RuntimeError, match="http://localhost:28080"):
            self._construct(client)

    def test_first_write_does_not_re_resolve_the_catalog(self):
        s, client, catalog, ns, table = _sink()
        s.write(_batch())
        client.catalog.assert_called_once()


class TestBootstrap:
    def test_missing_catalog_created_when_data_path_set(self):
        client, catalog, ns, table = _mock_stack(_EVENTS_COLUMNS)
        client.catalog.side_effect = [NotFoundError("no catalog", status_code=404)]
        created = MagicMock()
        created.namespace.return_value = ns
        client.create_catalog.return_value = created
        with patch("millpond.hoglake.HoglakeClient", return_value=client):
            s = hoglake.HoglakeSink(_cfg(hoglake_data_path="s3://bucket/millpond/"))
        client.create_catalog.assert_called_once_with("millpond", "s3://bucket/millpond/")
        assert s.write(_batch()) == 1

    def test_concurrent_catalog_creation_tolerated(self):
        client, catalog, ns, table = _mock_stack(_EVENTS_COLUMNS)
        client.catalog.side_effect = [NotFoundError("no catalog", status_code=404), catalog]
        client.create_catalog.side_effect = AlreadyExistsError("exists", status_code=409)
        with patch("millpond.hoglake.HoglakeClient", return_value=client):
            s = hoglake.HoglakeSink(_cfg(hoglake_data_path="s3://bucket/millpond/"))
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
        assert ns.table.call_count == 1

    def test_reset_caches_forces_reresolve(self):
        s, client, catalog, ns, table = _sink()
        s.write(_batch())
        s.reset_caches()
        s.write(_batch())
        assert ns.table.call_count == 2

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


class TestSpecDeclarationIsAtomicOrRecoverable:
    """Creation is two round trips — create_table, then one alter that
    declares the partition spec and sort order. The window between them
    is the hazard: if the alter fails for anything other than a
    concurrent-DDL 409, the table EXISTS and is UNPARTITIONED, and the
    next attempt used to take the 'table already exists' path and return
    happily. The result was a permanently unpartitioned table, written
    to forever, with no error after the first one."""

    def _create_flow(self, cfg, **stack):
        s, client, catalog, ns, table = _sink(cfg, **stack)
        ns.table.side_effect = NotFoundError("not found", status_code=404)
        return s, ns, table

    def test_alter_failure_is_fatal_and_leaves_no_cached_table(self):
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, ns, table = self._create_flow(cfg)
        table.alter.side_effect = ValidationError("validation", status_code=422)
        with pytest.raises(HoglakeError):
            s.write(_batch())
        assert s._table is None  # nothing cached: the next attempt re-checks

    def test_a_bad_spec_keeps_failing_on_every_later_attempt(self):
        # The config typo case (month(team_id), bucket(float_col, 16)):
        # the server 422s the alter. Attempt 2 finds the table present
        # and unpartitioned — it must re-declare and fail again, not
        # silently accept an unpartitioned table.
        cfg = _cfg(hoglake_partition_by=(("team_id", "month", None),))
        s, client, catalog, ns, table = _sink(cfg)
        ns.table.side_effect = [NotFoundError("not found", status_code=404), table, table]
        table.alter.side_effect = ValidationError(
            "transform 'month' requires a date or timestamp column; 'team_id' is 'long'",
            status_code=422,
        )
        for _ in range(3):
            with pytest.raises(HoglakeError):
                s.write(_batch())
            s.reset_caches()
        assert table.alter.call_count == 3
        table.prepare_append_files.assert_not_called()

    def test_post_condition_catches_a_spec_the_server_did_not_apply(self):
        # Belt and braces: if the alter reports success but the live spec
        # is not what config asked for, that is still a table millpond
        # must not write to.
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, ns, table = self._create_flow(cfg)
        table.alter.side_effect = lambda ops_list: _FakeInfo(columns=tuple(_EVENTS_COLUMNS))
        with pytest.raises(RuntimeError, match="partition spec"):
            s.write(_batch())


class TestExistingTableReconciliation:
    """A spec is declared at CREATE. On every later resolve the live spec
    was never looked at, so changing HOGLAKE_PARTITION_BY on a deployed
    pipeline was a silent no-op — the pod logged nothing and kept writing
    under the old layout."""

    def test_matching_spec_is_accepted(self):
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),), sort_by=("uuid",))
        s, client, catalog, ns, table = _sink(
            cfg, partition_spec=_spec(("team_id", "identity")), sort_spec=_sort("uuid")
        )
        assert s.write(_batch()) == 1
        table.alter.assert_not_called()  # nothing to reconcile

    def test_changed_partition_config_fails_loudly(self):
        cfg = _cfg(hoglake_partition_by=(("team_id", "bucket", 16),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        with pytest.raises(RuntimeError, match="HOGLAKE_PARTITION_BY"):
            s.write(_batch())

    def test_changed_transform_param_fails_loudly(self):
        cfg = _cfg(hoglake_partition_by=(("team_id", "bucket", 32),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "bucket", 16)))
        with pytest.raises(RuntimeError, match="HOGLAKE_PARTITION_BY"):
            s.write(_batch())

    def test_partitioned_table_with_no_configured_spec_fails_loudly(self):
        # The MILLPOND_DESTINATION flip: DUCKLAKE_PARTITION_BY is nulled
        # for hoglake, so the pod's config says 'unpartitioned' about a
        # table that is partitioned. Refuse rather than write on under a
        # layout nobody declared.
        s, client, catalog, ns, table = _sink(_cfg(), partition_spec=_spec(("team_id", "identity")))
        with pytest.raises(RuntimeError, match="HOGLAKE_PARTITION_BY"):
            s.write(_batch())

    def test_unpartitioned_table_with_configured_spec_is_declared(self):
        # The recovery half of the create-then-alter window, and the
        # reason the mismatch check cannot simply raise here: the table
        # exists, the spec does not, config says it should. Declare it.
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg)
        assert s.write(_batch()) == 1
        spec_op = next(o for o in table.alter.call_args.args[0] if o.op == "set_partition_spec")
        assert spec_op.body["fields"][0]["source_field_id"] == 3

    def test_changed_sort_config_fails_loudly(self):
        cfg = _cfg(sort_by=("uuid",))
        s, client, catalog, ns, table = _sink(cfg, sort_spec=_sort("team_id"))
        with pytest.raises(RuntimeError, match="MILLPOND_SORT_BY"):
            s.write(_batch())

    def test_missing_sort_field_still_degrades_without_reconciling(self):
        # A sort field absent from the table schema already degrades
        # (warn, write unsorted). That must not turn into a hard failure
        # via the new reconciliation path.
        cfg = _cfg(sort_by=("not_there",))
        s, client, catalog, ns, table = _sink(cfg, sort_spec=_sort("team_id"))
        assert s.write(_batch()) == 1


# ---------------------------------------------------------------------------
# write(): steady state + evolution
# ---------------------------------------------------------------------------


class TestSteadyStateWrite:
    def test_returns_record_count(self):
        s, *_, table = _sink()
        out = s.write(_batch())
        assert out == 1
        assert table.prepare_append_files.call_count == 1

    def test_inserted_at_stamped_once_per_flush(self):
        s, *_, table = _sink()
        s.write(pa.table({"uuid": ["a", "b", "c"], "event": ["e", "e", "e"], "team_id": [1, 2, 3]}))
        appended = _published(table)
        col = appended.column("_inserted_at")
        assert col.type == pa.timestamp("us", tz="UTC")
        vals = col.to_pylist()
        assert len(set(vals)) == 1 and vals[0] is not None

    def test_missing_table_columns_null_filled(self):
        # `properties` absent from this batch — upstream removed a column;
        # mirror DuckLake's INSERT BY NAME null-fill.
        s, *_, table = _sink()
        s.write(pa.table({"uuid": ["a"], "event": ["e"], "team_id": [1]}))
        appended = _published(table)
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
        appended = _published(table)
        assert "new_col" not in appended.column_names
        mock_metrics.errors_total.labels.assert_any_call(type="schema")

    @patch("millpond.hoglake.metrics")
    def test_conflict_on_add_reresolves_and_proceeds(self, mock_metrics):
        # Another writer added the column concurrently: the 409 must not
        # fail the flush; re-resolve shows the column present.
        cols_after = _EVENTS_COLUMNS + [_col("new_col", "string", 6, 6)]
        s, client, catalog, ns, table = _sink()
        table.alter.side_effect = CommitConflictError("concurrent DDL", status_code=409)
        table.info.return_value = _FakeInfo(columns=tuple(cols_after))
        s.write(pa.table({"uuid": ["a"], "new_col": ["x"]}))
        appended = _published(table)
        assert "new_col" in appended.column_names

    @patch("millpond.hoglake.metrics")
    def test_several_new_columns_share_one_alter(self, mock_metrics):
        # /alter applies its op list in order, atomically, as ONE DDL
        # commit. A column per commit multiplies the catalog's
        # commit-lock traffic by the width of the schema drift, and each
        # one is a separate chance to lose the concurrent-DDL race.
        s, *_, table = _sink()
        s.write(pa.table({"uuid": ["a"], "c1": ["x"], "c2": [1], "c3": [1.5]}))
        add_calls = [c for c in table.alter.call_args_list if any(o.op == "add_column" for o in c.args[0])]
        assert len(add_calls) == 1
        assert [o.body["column"]["name"] for o in add_calls[0].args[0]] == ["c1", "c2", "c3"]
        # One counter bump of 3, not three bumps of 1 — same total.
        mock_metrics.schema_columns_added_total.inc.assert_called_once_with(3)

    @patch("millpond.hoglake.metrics")
    def test_batched_alter_failure_degrades_per_column(self, mock_metrics):
        # The batch is an optimization, never a semantics change: if one
        # column in the batch is unacceptable the whole alter fails
        # (it is one transaction), so fall back to the per-column loop
        # and keep the columns that CAN land.
        s, *_, table = _sink()
        applied = table.alter.side_effect  # the helper that mutates the mock's live columns

        def alter(ops_list):
            if len(ops_list) > 1:
                raise ValidationError("validation", status_code=422)
            if ops_list[0].body.get("column", {}).get("name") == "bad_col":
                raise ValidationError("validation", status_code=422)
            return applied(ops_list)

        table.alter.side_effect = alter
        s.write(pa.table({"uuid": ["a"], "good_col": ["x"], "bad_col": ["y"]}))
        appended = _published(table)
        assert "good_col" in appended.column_names
        assert "bad_col" not in appended.column_names
        mock_metrics.errors_total.labels.assert_any_call(type="schema")

    @patch("millpond.hoglake.metrics")
    def test_unmappable_type_never_reaches_the_batch(self, mock_metrics):
        # A column with no hoglake type mapping is dropped before the
        # alter is built — it must not take the whole batched alter down
        # with it.
        s, *_, table = _sink()
        unmappable = pa.table({"dur": pa.array([1], type=pa.duration("s"))})
        s.write(pa.table({"uuid": ["a"], "ok_col": ["x"]}).append_column("dur", unmappable.column("dur")))
        add_ops = [o for c in table.alter.call_args_list for o in c.args[0] if o.op == "add_column"]
        assert [o.body["column"]["name"] for o in add_ops] == ["ok_col"]
        assert "dur" not in _published(table).column_names

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
        assert table.prepare_append_files.call_count == 1


class TestFilesWrittenMetric:
    @patch("millpond.hoglake.metrics")
    def test_files_per_flush_counted(self, mock_metrics):
        # Partitioned fanout registers one parquet per partition tuple in
        # one commit; the file count is the compaction-debt feed rate,
        # and it is counted off the registration that was PUBLISHED — so
        # a replayed commit cannot inflate it.
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        s.write(pa.table({"uuid": ["a", "b", "c"], "team_id": [1, 2, 3]}))
        mock_metrics.hoglake_files_written_total.inc.assert_called_once_with(3)
        assert len(_WRITTEN) == 3  # one parquet per tuple


class TestIdempotentPublication:
    """A commit whose response is lost is indistinguishable from one that
    never happened. The key that makes the retry a REPLAY is derived from
    the Kafka offset range, and the uploaded registration is held across
    retries so the replay is the same request byte for byte."""

    OFFSETS = (("events", 0, 41), ("events", 1, 17))

    def test_key_is_derived_from_the_offsets(self):
        s, *_ = _sink()
        first = s._flush_key(self.OFFSETS)
        assert first == s._flush_key(self.OFFSETS)
        # Order-independent: the same range described differently is the
        # same flush.
        assert first == s._flush_key(tuple(reversed(self.OFFSETS)))
        # A different range is a different publication.
        assert first != s._flush_key((("events", 0, 42), ("events", 1, 17)))
        # And a different table in the same catalog is a different one
        # too: receipts are scoped per CATALOG, not per table.
        other, *_ = _sink(_cfg(hoglake_table="other"))
        assert first != other._flush_key(self.OFFSETS)

    def test_key_is_random_without_offsets(self):
        # No identity to recognize a retry by: honest at-least-once
        # rather than a key that could collide across flushes.
        s, *_ = _sink()
        assert s._flush_key(None) != s._flush_key(None)

    def test_commit_carries_the_derived_key(self):
        s, client, catalog, ns, table = _sink()
        s.write(_batch(), kafka_offsets=self.OFFSETS)
        payload = catalog.commit_prepared.call_args.args[0]
        assert payload["idempotency_key"] == s._flush_key(self.OFFSETS)
        assert payload["author"] == "millpond/events/0"

    def test_payload_is_a_blind_append(self):
        # prepare_append_files pins read_snapshot to the catalog head,
        # and the server then 409s the commit if ANY DDL touched this
        # table since — which for millpond means "another pod added a
        # column". A frozen payload can never clear that conflict.
        # Appends never conflict with appends, so the field comes off.
        s, client, catalog, ns, table = _sink()
        s.write(_batch(), kafka_offsets=self.OFFSETS)
        assert "read_snapshot" not in catalog.commit_prepared.call_args.args[0]

    def test_retry_replays_the_same_payload_without_re_uploading(self):
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = [httpx.ReadTimeout("response lost"), MagicMock()]
        with pytest.raises(httpx.ReadTimeout):
            s.write(_rows(3), kafka_offsets=self.OFFSETS)
        # The retry path invalidates caches first, exactly as main.py does.
        s.reset_caches()
        assert s.write(_rows(3), kafka_offsets=self.OFFSETS) == 3
        assert table.prepare_append_files.call_count == 1  # no second upload
        first, second = (c.args[0] for c in catalog.commit_prepared.call_args_list)
        assert first == second  # byte-identical replay

    def test_reset_caches_keeps_the_prepared_payload(self):
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = [httpx.ConnectError("reset"), MagicMock()]
        with pytest.raises(httpx.ConnectError):
            s.write(_batch(), kafka_offsets=self.OFFSETS)
        s.reset_caches()
        assert s._prepared is not None

    def test_a_different_flush_prepares_again(self):
        s, client, catalog, ns, table = _sink()
        s.write(_batch(), kafka_offsets=self.OFFSETS)
        s.write(_batch(), kafka_offsets=(("events", 0, 99),))
        assert table.prepare_append_files.call_count == 2
        keys = {c.args[0]["idempotency_key"] for c in catalog.commit_prepared.call_args_list}
        assert len(keys) == 2

    @patch("millpond.hoglake.metrics")
    def test_key_reused_with_a_different_payload_is_already_published(self, mock_metrics):
        # The crash-restart case: the pod died after the commit applied
        # and before the offsets committed, so Kafka replayed the range
        # and the flush was rebuilt with fresh file names. The receipt
        # says this range is already in the lake. Publishing the rebuilt
        # copy would duplicate it; failing forever would wedge the
        # partition on rows that are already there.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = ValidationError(
            "idempotency_key reused with a different request", status_code=422
        )
        assert s.write(_rows(5), kafka_offsets=self.OFFSETS) == 5
        mock_metrics.hoglake_commit_replays_total.labels.assert_called_once_with(outcome="already_published")
        # The upload we just made is unreferenced and nothing will
        # reclaim it — say so in a metric rather than in nothing.
        mock_metrics.hoglake_orphaned_files_total.inc.assert_called_once_with(1)
        assert s._prepared is None

    def test_other_validation_errors_are_not_swallowed(self):
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = ValidationError("path outside the catalog data path", status_code=422)
        with pytest.raises(ValidationError):
            s.write(_batch(), kafka_offsets=self.OFFSETS)

    def test_zero_row_batch_never_reaches_the_commit(self):
        # A commit must register at least one file with at least one row;
        # a zero-row flush would be a 422 the retry loop could not clear.
        s, client, catalog, ns, table = _sink()
        assert s.write(_batch().slice(0, 0), kafka_offsets=self.OFFSETS) == 0
        catalog.commit_prepared.assert_not_called()


class TestConcurrentAddDuringAppend:
    @patch("millpond.hoglake.metrics")
    def test_align_refusal_refreshes_and_reappends_once(self, mock_metrics):
        """Race found by the live integration suite: another writer's
        add_column lands between this sink's alignment and append()'s
        pre-flight resolve, so pyhoglake's strict _align_table refuses
        with "data is missing table columns". The sink must refresh the
        live schema, null-fill the new column, and re-append ONCE."""
        cols_after = _EVENTS_COLUMNS + [_col("other_writer_col", "string", 6, 6)]
        s, client, catalog, ns, table = _sink()
        prepared = table.prepare_append_files.side_effect
        calls = {"n": 0}

        def prepare(files, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ValidationError("data is missing table columns: ['other_writer_col']", status_code=None)
            return prepared(files, **kwargs)

        table.prepare_append_files.side_effect = prepare
        table.info.return_value = _FakeInfo(columns=tuple(cols_after))
        assert s.write(_batch()) == 1
        assert table.prepare_append_files.call_count == 2
        retried = _published(table)
        assert "other_writer_col" in retried.column_names
        assert retried.column("other_writer_col").null_count == retried.num_rows

    @patch("millpond.hoglake.metrics")
    def test_other_validation_errors_still_raise(self, mock_metrics):
        s, *_, table = _sink()
        table.prepare_append_files.side_effect = ValidationError("prepared file must contain rows", status_code=None)
        with pytest.raises(ValidationError):
            s.write(_batch())
        assert table.prepare_append_files.call_count == 1

    @patch("millpond.hoglake.metrics")
    def test_persistent_align_refusal_raises_after_one_retry(self, mock_metrics):
        s, *_, table = _sink()
        table.prepare_append_files.side_effect = ValidationError(
            "data is missing table columns: ['x']", status_code=None
        )
        with pytest.raises(ValidationError):
            s.write(_batch())
        assert table.prepare_append_files.call_count == 2


class TestWriteFailurePropagation:
    def test_append_failure_raises(self):
        # At-least-once: a failed write must surface to main.py's retry
        # loop; offsets only commit after write() returns.
        s, *_, table = _sink()
        table.prepare_append_files.side_effect = CommitConflictError("conflict", status_code=409)
        with pytest.raises(CommitConflictError):
            s.write(_batch())

    def test_incarnation_change_raises(self):
        s, *_, table = _sink()
        table.prepare_append_files.side_effect = IncarnationChangedError("recreated")
        with pytest.raises(IncarnationChangedError):
            s.write(_batch())
