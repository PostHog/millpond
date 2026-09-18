"""Unit tests for millpond/hoglake.py — the HoglakeSink.

Everything here runs against a mocked pyhoglake client layer; the real
server round-trips live in tests/integration/test_hoglake_integration.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
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


TABLE_UUID = "0e0b6c8e-0000-0000-0000-000000000001"


@dataclass
class _FakeInfo:
    """The TableInfo surface the sink reads. A MagicMock is wrong here:
    `info.partition_spec` on a MagicMock is a truthy Mock, so a test
    could never tell an unpartitioned table from a partitioned one — and
    telling those apart is the whole point of the reconciliation path.
    Same for `table_uuid`: the incarnation is half of the idempotency
    key, and a Mock would compare unequal to itself across calls."""

    columns: tuple
    partition_spec: PartitionSpec | None = None
    sort_spec: SortSpec | None = None
    namespace: str = "analytics"
    name: str = "events"
    table_uuid: str = TABLE_UUID


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


# Captured before the autouse fixture below replaces it: one test needs
# the REAL serializer, because everything else in this file asserts on
# the in-memory table that reaches `pq.write_table` and therefore never
# exercises the cast or the file it produces.
_REAL_WRITE_TABLE = pq.write_table

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

    def prepare(files, *, idempotency_key, expected_table_uuid=None, **kwargs):
        # pyhoglake pins the guard to the incarnation the CALLER names
        # (`expected = expected_table_uuid or self.table_uuid`) and then
        # fast-fails, BEFORE the first upload, when the name now binds to
        # a different table (`Table._check_incarnation`). `table_uuid` on
        # the mock stands for what the name resolves to now, exactly as
        # `self._info.table_uuid` does after a refresh.
        expected = expected_table_uuid or table.table_uuid
        if expected != table.table_uuid:
            raise IncarnationChangedError(
                f"table was recreated: expected table_uuid {expected}, name now resolves to {table.table_uuid}"
            )
        return {
            "idempotency_key": idempotency_key,
            "read_snapshot": 41,
            "appends": [
                {
                    "namespace": "analytics",
                    "table": "events",
                    "expected_table_uuid": expected,
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
    table.table_uuid = state.table_uuid
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
            Exception("unknown"),  # genuinely unknown → assume transient
            # The one sink-raised stop a rebuilt flush really does clear.
            hoglake.HoglakeSinkError("partition spec changed", retryable=True),
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
            # This sink's OWN refusals. Every one of them is a statement
            # about the config or the code; eight attempts and 91 seconds
            # of backoff cannot make any of them true.
            hoglake.HoglakeSinkError("live partition spec does not match HOGLAKE_PARTITION_BY"),
            ValueError("columns collide with Hoglake-reserved metadata column names"),
            KeyError('Field "col_w1_0" does not exist in schema'),
            TypeError("not a schema"),
        ],
    )
    def test_not_retryable(self, exc):
        assert hoglake.is_retryable(exc) is False

    def test_sink_refusals_are_the_sinks_own_type(self):
        """Every safety stop in this module must be classifiable. A plain
        RuntimeError from here would be read as "unknown, assume
        transient" — which is how the loudest stops became the slowest."""
        import inspect
        import re

        src = inspect.getsource(hoglake)
        stray = re.findall(r"raise RuntimeError", src)
        assert not stray, "sink-raised refusals must be HoglakeSinkError so is_retryable can judge them"

    @pytest.mark.parametrize(
        ("raiser", "kwargs"),
        [
            ("spec mismatch", {"hoglake_partition_by": (("team_id", "bucket", 16),)}),
        ],
    )
    def test_reconciliation_refusal_is_permanent(self, raiser, kwargs):
        # End to end through write(): the refusal main.py sees must be
        # classified permanent, so the pod crashes with the message
        # instead of after the ladder.
        s, client, catalog, ns, table = _sink(_cfg(**kwargs), partition_spec=_spec(("team_id", "identity")))
        with pytest.raises(RuntimeError) as caught:
            s.write(_batch())
        assert hoglake.is_retryable(caught.value) is False

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

    def test_the_hook_is_installed_on_a_real_client(self):
        """Every other test in this class calls `_note_response` by hand,
        and the sink they build has a MagicMock client — on which the
        install (into `client._http.event_hooks`, a private attribute,
        behind a bare except) succeeds no matter what it does. So the
        one thing that can actually break — pyhoglake restructuring its
        transport — was the one thing nothing checked."""
        from pyhoglake import HoglakeClient

        real = HoglakeClient("http://127.0.0.1:28080")
        real.catalog = MagicMock(return_value=MagicMock())
        try:
            with patch("millpond.hoglake.HoglakeClient", return_value=real):
                s = hoglake.HoglakeSink(_cfg())
            assert s._note_response in real._http.event_hooks["response"]
            # And it works end to end through the transport's own hook
            # list, not just by being in it.
            for hook in real._http.event_hooks["response"]:
                hook(httpx.Response(503, headers={"Retry-After": "2"}))
            assert s.retry_after_hint() == 2.0
        finally:
            real.close()


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

    def test_post_condition_covers_the_sort_order_too(self):
        # The sort order is advisory for writers and BINDING for hoglake
        # compaction: a table that silently carries a different one gets
        # every file this pod writes re-sorted. The declaration is
        # checked, not assumed — for both halves of the alter.
        cfg = _cfg(sort_by=("team_id",))
        s, ns, table = self._create_flow(cfg)
        table.alter.side_effect = lambda ops_list: _FakeInfo(columns=tuple(_EVENTS_COLUMNS), sort_spec=_sort("uuid"))
        with pytest.raises(RuntimeError, match="sort order"):
            s.write(_batch())

    def test_post_condition_covers_a_sort_order_the_server_dropped(self):
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),), sort_by=("team_id",))
        s, ns, table = self._create_flow(cfg)
        # Partition applied, sort silently absent.
        table.alter.side_effect = lambda ops_list: _FakeInfo(
            columns=tuple(_EVENTS_COLUMNS), partition_spec=_spec(("team_id", "identity"))
        )
        with pytest.raises(RuntimeError, match="sort order"):
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


class TestRealSerialization:
    """Every other test here reads the in-memory table handed to
    `pq.write_table`, which the autouse fixture replaces with a no-op —
    so the cast and the file it produces were never unit-exercised at
    all. This one writes real parquet and reads it back."""

    def test_the_file_written_is_the_table_cast_to_the_destination(self, monkeypatch):
        monkeypatch.setattr(hoglake.pq, "write_table", _REAL_WRITE_TABLE)
        s, client, catalog, ns, table = _sink()
        captured: dict[str, list] = {}
        prepared = table.prepare_append_files.side_effect

        def prepare(files, **kwargs):
            captured["files"] = [pq.read_table(path) for path, _ in files]
            return prepared(files, **kwargs)

        table.prepare_append_files.side_effect = prepare
        # team_id arrives as int32; the live column is `long`.
        s.write(pa.table({"uuid": ["a"], "team_id": pa.array([5], type=pa.int32())}))
        written = captured["files"][0]
        # Column ORDER is the destination's, not the batch's: the
        # prepared path compares schemas position by position.
        assert written.schema.names == [c.name for c in _EVENTS_COLUMNS]
        assert written.column("team_id").type == pa.int64()
        assert written.column("team_id").to_pylist() == [5]
        assert written.column("_inserted_at").type == pa.timestamp("us", tz="UTC")
        assert written.column("_inserted_at").null_count == 0
        assert written.column("properties").null_count == 1  # absent upstream, null-filled

    def test_a_partitioned_flush_writes_one_real_file_per_tuple(self, monkeypatch):
        monkeypatch.setattr(hoglake.pq, "write_table", _REAL_WRITE_TABLE)
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        captured: dict[str, list] = {}
        prepared = table.prepare_append_files.side_effect

        def prepare(files, **kwargs):
            captured["files"] = [(values, pq.read_table(path)) for path, values in files]
            return prepared(files, **kwargs)

        table.prepare_append_files.side_effect = prepare
        s.write(pa.table({"uuid": ["a", "b", "c"], "team_id": [3, 1, 3]}))
        assert [values for values, _ in captured["files"]] == [("3",), ("1",)]
        assert [t.column("uuid").to_pylist() for _, t in captured["files"]] == [["a", "c"], ["b"]]


class TestPartitionGrouping:
    """The prepared path puts row-to-partition correctness on the client:
    the server validates the SHAPE of what it is told and never opens a
    data file, so a wrong value here mis-prunes reads of that file
    forever."""

    def _info(self, partition_spec):
        return _FakeInfo(columns=tuple(_EVENTS_COLUMNS), partition_spec=partition_spec)

    def test_groups_follow_first_occurrence_in_the_batch(self):
        # File registration order IS row-id assignment order on the
        # server (rows, then offset), and the batch arrives in the order
        # MILLPOND_SORT_BY put it in. Grouping that reorders — by sort
        # order, by hash order — silently breaks the correspondence
        # between a table's row ids and its declared sort.
        data = pa.table({"uuid": ["a", "b", "c", "d"], "team_id": [3, 1, 3, 2]})
        groups = hoglake._partition_groups(data, self._info(_spec(("team_id", "identity"))))
        assert [values for values, _ in groups] == [("3",), ("1",), ("2",)]
        assert [part.column("uuid").to_pylist() for _, part in groups] == [["a", "c"], ["b"], ["d"]]

    def test_first_occurrence_order_survives_arrows_hash_order(self):
        """The ordering is NOT free, and a handful of groups does not
        prove it: pyarrow's `group_by` emits in hash order, which happens
        to coincide with first-occurrence order for a few keys and stops
        coinciding as soon as there are more. Eight partition values is
        already enough — arrow returns them 0,1,2,3,4,5,7,6 — so this is
        the size at which dropping the explicit ordering shows up."""
        teams = list(range(8))
        rows = 64
        data = pa.table(
            {
                "uuid": [f"u{i}" for i in range(rows)],
                "team_id": [teams[i % len(teams)] for i in range(rows)],
            }
        )
        groups = hoglake._partition_groups(data, self._info(_spec(("team_id", "identity"))))
        assert [values[0] for values, _ in groups] == [str(t) for t in teams]

    def test_every_row_lands_in_exactly_one_group(self):
        data = pa.table({"uuid": [f"u{i}" for i in range(9)], "team_id": [1, 2, 3, 1, 2, 3, 1, 2, 3]})
        groups = hoglake._partition_groups(data, self._info(_spec(("team_id", "identity"))))
        assert sum(part.num_rows for _, part in groups) == 9
        assert (
            sorted(u for _, part in groups for u in part.column("uuid").to_pylist()) == data.column("uuid").to_pylist()
        )

    def test_a_null_source_value_forms_its_own_group(self):
        data = pa.table({"uuid": ["a", "b", "c"], "team_id": pa.array([1, None, 1], type=pa.int64())})
        groups = hoglake._partition_groups(data, self._info(_spec(("team_id", "identity"))))
        assert [values for values, _ in groups] == [("1",), (None,)]

    def test_an_unpartitioned_table_is_one_group(self):
        data = pa.table({"uuid": ["a", "b"], "team_id": [1, 2]})
        groups = hoglake._partition_groups(data, self._info(None))
        assert len(groups) == 1 and groups[0][0] is None

    def test_a_spec_referencing_a_dead_field_id_is_a_permanent_refusal(self):
        data = pa.table({"uuid": ["a"], "team_id": [1]})
        spec = PartitionSpec(spec_id=1, fields=(PartitionField(9999, "identity", None),))
        with pytest.raises(RuntimeError) as caught:
            hoglake._partition_groups(data, self._info(spec))
        assert hoglake.is_retryable(caught.value) is False


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
    never happened. The key that makes the retry a REPLAY names the
    destination INCARNATION and the complete Kafka offset range, and the
    uploaded registration is held across retries so the replay is the
    same request byte for byte."""

    OFFSETS = (("events", 0, 30, 41), ("events", 1, 9, 17))

    def _key(self, sink, offsets, table_uuid=TABLE_UUID):
        return sink._flush_key(table_uuid, offsets)

    def test_key_is_derived_from_the_offsets(self):
        s, *_ = _sink()
        first = self._key(s, self.OFFSETS)
        assert first == self._key(s, self.OFFSETS)
        # Order-independent: the same range described differently is the
        # same flush.
        assert first == self._key(s, tuple(reversed(self.OFFSETS)))
        # A different range is a different publication...
        assert first != self._key(s, (("events", 0, 30, 42), ("events", 1, 9, 17)))
        # ...including one that differs only in where it STARTED. A key
        # naming the high offset alone says "everything up to 41", which
        # a rewound partition re-flushing [0, 41] then collides with.
        assert first != self._key(s, (("events", 0, 0, 41), ("events", 1, 9, 17)))
        # And a different table in the same catalog is a different one
        # too: receipts are scoped per CATALOG, not per table.
        other, *_ = _sink(_cfg(hoglake_table="other"))
        assert first != self._key(other, self.OFFSETS)

    def test_key_names_the_namespace(self):
        # Two pipelines with the same table name in different namespaces
        # of one catalog share a receipt space.
        s, *_ = _sink()
        other, *_ = _sink(_cfg(hoglake_namespace="other_ns"))
        assert self._key(s, self.OFFSETS) != self._key(other, self.OFFSETS)

    def test_key_names_the_table_incarnation(self):
        # Receipts survive a table drop — hoglake has no cascade from the
        # table to its receipts. Without the incarnation in the key, a
        # dropped-and-recreated table answers a flush from its
        # PREDECESSOR's receipt and millpond advances offsets over rows
        # that are in a table which no longer exists.
        s, *_ = _sink()
        recreated = "99999999-0000-0000-0000-000000000009"
        assert self._key(s, self.OFFSETS) != self._key(s, self.OFFSETS, recreated)

    def test_key_keeps_each_offset_with_its_partition(self):
        # Partition 0's offsets 0-5 and partition 1's offsets 0-5 are
        # different rows — and at the head of a fresh topic, two
        # partitions carrying the same range is the ordinary case, not a
        # contrived one. A key that names the range without the partition
        # it belongs to calls them the same flush.
        s, *_ = _sink()
        a = self._key(s, (("events", 0, 0, 5),))
        b = self._key(s, (("events", 1, 0, 5),))
        assert a != b
        # And the same two ranges held by opposite partitions.
        c = self._key(s, (("events", 0, 17, 17), ("events", 1, 41, 41)))
        d = self._key(s, (("events", 0, 41, 41), ("events", 1, 17, 17)))
        assert c != d

    @patch("millpond.hoglake.metrics")
    def test_a_stale_handle_refuses_rather_than_publishing_across_incarnations(self, mock_metrics):
        # `_ensure_table` caches its resolved handle for the pod's life,
        # and that handle is a NAME, not an incarnation. A drop+recreate
        # underneath it used to be invisible to every guard at once:
        # `_prepare`'s own `table.info()` rebases pyhoglake's pinned
        # `_info` onto the new incarnation before `prepare_append_files`
        # reads `self.table_uuid` off it, so the client's pre-flight, the
        # server's `expected_table_uuid` and
        # `_check_destination_still_ours` all compared fresh against
        # fresh and passed. The commit landed on a table this pod had
        # never reconciled, under a key naming the dead one.
        #
        # Unlike `test_a_recreated_table_does_not_answer_from_the_old_receipt`
        # and `test_a_reused_key_against_a_recreated_table_is_not_accepted`,
        # the sink here holds a STALE handle — which is the only state in
        # which the window is open.
        s, client, catalog, ns, table = _sink()
        offsets = (("events", 0, 2, 3),)
        assert s.write(_rows(2), kafka_offsets=(("events", 0, 0, 1),)) == 2

        reborn = "deadbeef-0000-0000-0000-000000000000"
        table.info.return_value = _FakeInfo(columns=tuple(_EVENTS_COLUMNS), table_uuid=reborn)
        table.table_uuid = reborn

        with pytest.raises(IncarnationChangedError):
            s.write(_rows(2), kafka_offsets=offsets)
        assert catalog.commit_prepared.call_count == 1  # nothing published across the seam
        assert s._prepared is None
        # The pre-flight refuses before the first upload, so there is no
        # orphan to count and counting one would send an operator
        # sweeping for an object that does not exist.
        mock_metrics.hoglake_orphaned_files_total.inc.assert_not_called()

        # Retryable: main.py resets caches, the next attempt re-resolves
        # — which is what finally puts the recreated table through
        # `_reconcile_specs` — and the key then names the live one.
        s.reset_caches()
        assert s.write(_rows(2), kafka_offsets=offsets) == 2
        published = catalog.commit_prepared.call_args.args[0]["idempotency_key"]
        assert published == self._key(s, offsets, reborn)
        assert published != self._key(s, offsets, TABLE_UUID)

    def test_key_is_random_without_offsets(self):
        # No identity to recognize a retry by: honest at-least-once
        # rather than a key that could collide across flushes.
        s, *_ = _sink()
        assert self._key(s, None) != self._key(s, None)

    def test_commit_carries_the_derived_key(self):
        s, client, catalog, ns, table = _sink()
        s.write(_batch(), kafka_offsets=self.OFFSETS)
        payload = catalog.commit_prepared.call_args.args[0]
        assert payload["idempotency_key"] == self._key(s, self.OFFSETS)
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

    @patch("millpond.hoglake.metrics")
    def test_a_resolved_replay_is_visible_to_operators(self, mock_metrics):
        # A commit re-sent after an uncertain outcome that comes back 200
        # is the mechanism WORKING. Previously it was indistinguishable
        # from a first publish, so the only replay an operator could see
        # was the already-published one.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = [httpx.ReadTimeout("response lost"), MagicMock()]
        with pytest.raises(httpx.ReadTimeout):
            s.write(_rows(3), kafka_offsets=self.OFFSETS)
        mock_metrics.hoglake_commit_replays_total.labels.assert_not_called()
        s.reset_caches()
        assert s.write(_rows(3), kafka_offsets=self.OFFSETS) == 3
        mock_metrics.hoglake_commit_replays_total.labels.assert_called_once_with(outcome="replayed")

    @patch("millpond.hoglake.metrics")
    def test_a_first_publish_is_not_a_replay(self, mock_metrics):
        s, client, catalog, ns, table = _sink()
        s.write(_rows(3), kafka_offsets=self.OFFSETS)
        mock_metrics.hoglake_commit_replays_total.labels.assert_not_called()

    def test_reset_caches_keeps_a_sendable_prepared_payload(self):
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = [httpx.ConnectError("reset"), MagicMock()]
        with pytest.raises(httpx.ConnectError):
            s.write(_batch(), kafka_offsets=self.OFFSETS)
        s.reset_caches()
        assert s._prepared is not None

    def test_a_different_flush_prepares_again(self):
        s, client, catalog, ns, table = _sink()
        s.write(_batch(), kafka_offsets=self.OFFSETS)
        s.write(_batch(), kafka_offsets=(("events", 0, 42, 99),))
        assert table.prepare_append_files.call_count == 2
        keys = {c.args[0]["idempotency_key"] for c in catalog.commit_prepared.call_args_list}
        assert len(keys) == 2

    @patch("millpond.hoglake.metrics")
    def test_a_held_payload_is_never_replayed_for_a_different_flush(self, mock_metrics):
        # The cached payload belongs to ONE offset range. Replaying it
        # for the next flush would publish the previous flush's rows
        # under this flush's offsets and drop these rows on the floor.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = [httpx.ReadTimeout("lost"), MagicMock()]
        with pytest.raises(httpx.ReadTimeout):
            s.write(_rows(3), kafka_offsets=self.OFFSETS)
        moved_on = (("events", 0, 42, 99),)
        assert s.write(_rows(7), kafka_offsets=moved_on) == 7
        assert table.prepare_append_files.call_count == 2  # rebuilt, not replayed
        sent = catalog.commit_prepared.call_args.args[0]
        assert sent["idempotency_key"] == self._key(s, moved_on)
        # The abandoned upload is an orphan and is counted as one.
        mock_metrics.hoglake_orphaned_files_total.inc.assert_called_once_with(1)

    @patch("millpond.hoglake.metrics")
    def test_key_reused_with_a_different_payload_publishes_nothing(self, mock_metrics):
        # The crash-restart case: the pod died after the commit applied
        # and before the offsets committed, so Kafka replayed the range
        # and the flush was rebuilt with a fresh `_inserted_at` stamp and
        # fresh file names. The key names this table incarnation and this
        # complete offset range, and the server writes a receipt only in
        # the transaction that publishes — so the rows are in the lake.
        # Publishing the rebuilt copy would duplicate them; failing
        # forever would wedge the partition on rows already there.
        #
        # The exception is built in the shape the SERVER produces:
        # ApiError{error, detail} maps to message="validation" and the
        # sentence in `detail`. A matcher reading `.message` alone sees
        # "validation" and nothing else.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = ValidationError(
            "validation",
            status_code=422,
            detail="idempotency_key reused with a different request",
        )
        # ZERO, not 5: this process published nothing. Reporting the
        # batch size here is how a writer came to claim 8 rows for a
        # range that had 3 in the lake.
        assert s.write(_rows(5), kafka_offsets=self.OFFSETS) == 0
        mock_metrics.hoglake_commit_replays_total.labels.assert_called_once_with(outcome="already_published")
        # The upload we just made is unreferenced and nothing will
        # reclaim it — say so in a metric rather than in nothing.
        mock_metrics.hoglake_orphaned_files_total.inc.assert_called_once_with(1)
        assert s._prepared is None

    def test_a_reused_key_against_a_recreated_table_is_not_accepted(self):
        # A receipt from the PREVIOUS incarnation must never stand in for
        # a publication to this one. (The key names the incarnation, so
        # this is belt and braces on the same invariant.)
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = ValidationError(
            "validation", status_code=422, detail="idempotency_key reused with a different request"
        )
        infos = [_FakeInfo(columns=tuple(_EVENTS_COLUMNS)), _FakeInfo(columns=tuple(_EVENTS_COLUMNS))]
        infos.append(_FakeInfo(columns=tuple(_EVENTS_COLUMNS), table_uuid="deadbeef-0000-0000-0000-000000000000"))
        table.info.side_effect = infos
        with pytest.raises(IncarnationChangedError):
            s.write(_rows(5), kafka_offsets=self.OFFSETS)
        assert s._prepared is None

    @pytest.mark.parametrize(
        "detail",
        [
            "idempotency_key must be a UUID",
            "idempotency_key is required for prepared commits",
        ],
    )
    def test_other_idempotency_errors_are_not_treated_as_published(self, detail):
        # The marker is the whole sentence the server uses for a reuse,
        # not the word "idempotency": every other 422 mentioning the key
        # is a request that was REFUSED, with nothing in the lake.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = ValidationError("validation", status_code=422, detail=detail)
        with pytest.raises(ValidationError):
            s.write(_batch(), kafka_offsets=self.OFFSETS)

    def test_other_validation_errors_are_not_swallowed(self):
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = ValidationError(
            "validation", status_code=422, detail="path outside the catalog data path"
        )
        with pytest.raises(ValidationError):
            s.write(_batch(), kafka_offsets=self.OFFSETS)


class TestRefusedCommitsDropThePayload:
    """A refusal the server ANSWERED is one transaction that wrote
    nothing, and it means the same thing to every identical resend.

    Holding the payload across one of those made `reset_caches()` inert
    (the replay short-circuits before the table is re-resolved), so a
    409 repeated for the whole retry budget with ONE prepare behind it,
    and the pod crashed having orphaned an upload per cycle."""

    OFFSETS = (("events", 0, 30, 41),)

    @pytest.mark.parametrize(
        "exc",
        [
            IncarnationChangedError("table was recreated", status_code=409, detail="the table was recreated"),
            CommitConflictError("commit_conflict", status_code=409, detail="removal queue collision"),
            ValidationError("validation", status_code=422, detail="path outside the catalog data path"),
        ],
    )
    @patch("millpond.hoglake.metrics")
    def test_an_answered_refusal_drops_the_payload_and_re_resolves(self, mock_metrics, exc):
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = exc
        with pytest.raises(type(exc)):
            s.write(_rows(2), kafka_offsets=self.OFFSETS)
        assert s._prepared is None
        mock_metrics.hoglake_orphaned_files_total.inc.assert_called_once_with(1)
        # The next attempt (main.py resets caches first) rebuilds rather
        # than re-sending a request the server already judged.
        catalog.commit_prepared.side_effect = None
        s.reset_caches()
        assert s.write(_rows(2), kafka_offsets=self.OFFSETS) == 2
        assert table.prepare_append_files.call_count == 2
        assert ns.table.call_count == 2  # the table WAS re-resolved

    def test_transport_uncertainty_keeps_the_payload(self):
        # The other half of the rule: no response means no verdict, so
        # the same request must go again.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = httpx.ReadTimeout("no response")
        with pytest.raises(httpx.ReadTimeout):
            s.write(_rows(2), kafka_offsets=self.OFFSETS)
        assert s._prepared is not None

    def test_server_side_5xx_keeps_the_payload(self):
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = HoglakeError("commit_queue_timeout", status_code=503)
        with pytest.raises(HoglakeError):
            s.write(_rows(2), kafka_offsets=self.OFFSETS)
        assert s._prepared is not None

    @patch("millpond.hoglake.metrics")
    def test_files_written_counts_only_what_the_commit_registered(self, mock_metrics):
        # The counter is the compaction-debt feed rate. A refused commit
        # registers nothing, so counting before the commit lands reports
        # debt the catalog does not have (and hides an orphan).
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = ValidationError("validation", status_code=422, detail="nope")
        with pytest.raises(ValidationError):
            s.write(_rows(2), kafka_offsets=self.OFFSETS)
        mock_metrics.hoglake_files_written_total.inc.assert_not_called()

    @patch("millpond.hoglake.metrics")
    def test_a_prepare_that_fails_mid_upload_is_counted(self, mock_metrics):
        # Nothing on the server reclaims a client upload, so an
        # uncounted orphan path is storage nobody can find.
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        table.prepare_append_files.side_effect = OSError("S3 reset midway")
        with pytest.raises(OSError):
            s.write(pa.table({"uuid": ["a", "b", "c"], "team_id": [1, 2, 3]}), kafka_offsets=self.OFFSETS)
        mock_metrics.hoglake_orphaned_files_total.inc.assert_called_once_with(3)

    @patch("millpond.hoglake.metrics")
    def test_a_prepare_refused_before_its_upload_is_not_counted(self, mock_metrics):
        # pyhoglake validates file i and THEN uploads file i: a
        # validation refusal on a single-file flush uploaded nothing.
        s, client, catalog, ns, table = _sink()
        table.prepare_append_files.side_effect = ValidationError("prepared file must contain rows", status_code=None)
        with pytest.raises(ValidationError):
            s.write(_rows(2), kafka_offsets=self.OFFSETS)
        mock_metrics.hoglake_orphaned_files_total.inc.assert_not_called()

    @patch("millpond.hoglake.metrics")
    def test_close_counts_an_unpublished_payload(self, mock_metrics):
        # SIGTERM between prepare and commit.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = httpx.ReadTimeout("no response")
        with pytest.raises(httpx.ReadTimeout):
            s.write(_rows(2), kafka_offsets=self.OFFSETS)
        s.close()
        mock_metrics.hoglake_orphaned_files_total.inc.assert_called_once_with(1)


class TestSpecChangeUnderAPreparedPayload:
    """A registered file carries the partition VALUES the client computed
    and the spec_id the table has when the commit lands. The server never
    opens the file, so a same-arity re-spec between prepare and commit
    stamps identity values as bucket values — silent, permanent
    mis-pruning of every future scan."""

    OFFSETS = (("events", 0, 30, 41),)

    @patch("millpond.hoglake.metrics")
    def test_a_same_arity_spec_change_refuses_the_commit(self, mock_metrics):
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        prepared = table.prepare_append_files.side_effect

        def prepare(files, **kwargs):
            # The re-spec lands while the upload is in flight.
            table.info.return_value = _FakeInfo(
                columns=tuple(_EVENTS_COLUMNS), partition_spec=_spec(("team_id", "bucket", 16))
            )
            return prepared(files, **kwargs)

        table.prepare_append_files.side_effect = prepare
        with pytest.raises(RuntimeError, match="partition spec"):
            s.write(pa.table({"uuid": ["a"], "team_id": [1]}), kafka_offsets=self.OFFSETS)
        catalog.commit_prepared.assert_not_called()
        assert s._prepared is None
        mock_metrics.hoglake_orphaned_files_total.inc.assert_called_once_with(1)

    def test_the_refusal_is_retryable(self):
        # Unlike the sink's other stops: a REBUILT flush computes its
        # values under the new spec and publishes cleanly.
        err = hoglake.HoglakeSinkError("partition spec ... changed", retryable=True)
        assert hoglake.is_retryable(err) is True

    def test_an_unchanged_spec_commits(self):
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        assert s.write(pa.table({"uuid": ["a"], "team_id": [1]}), kafka_offsets=self.OFFSETS) == 1

    def test_zero_row_batch_never_reaches_the_commit(self):
        # A commit must register at least one file with at least one row;
        # a zero-row flush would be a 422 the retry loop could not clear.
        s, client, catalog, ns, table = _sink()
        assert s.write(_batch().slice(0, 0), kafka_offsets=self.OFFSETS) == 0
        catalog.commit_prepared.assert_not_called()


class TestConcurrentAddDuringAppend:
    """The concurrent-`add_column` race, in both halves.

    `_evolve_and_align` null-fills against the columns it resolved; a
    beat later `_prepare` adopts a FRESH `table.info()`. Another writer's
    add_column in that window puts a name in the target schema the batch
    does not carry. The alignment must survive that on its own, and the
    self-heal behind it must match the refusals pyhoglake ACTUALLY
    raises.
    """

    def test_column_added_between_align_and_prepare_is_null_filled(self):
        # The live-suite failure (`KeyError: Field "col_w1_0" does not
        # exist in schema`): pa.Table.select raises KeyError, not a
        # ValidationError, so the self-heal never fired for its own
        # motivating case — and KeyError classifies as retryable, so the
        # pod burned its whole budget and then crashed.
        cols_after = _EVENTS_COLUMNS + [_col("other_writer_col", "string", 6, 6)]
        s, client, catalog, ns, table = _sink()
        before = _FakeInfo(columns=tuple(_EVENTS_COLUMNS))
        after = _FakeInfo(columns=tuple(cols_after))
        # _ensure_table resolves against the old schema; _prepare adopts
        # the new one. The window is one round trip wide in production.
        table.info.side_effect = [before, after, after, after]
        assert s.write(_batch()) == 1
        published = _published(table)
        assert "other_writer_col" in published.column_names
        assert published.column("other_writer_col").null_count == published.num_rows
        assert table.prepare_append_files.call_count == 1  # no self-heal round needed

    @patch("millpond.hoglake.metrics")
    def test_align_refusal_refreshes_and_reappends_once(self, mock_metrics):
        """The self-heal behind the null-fill, on the message pyhoglake
        really raises: `prepare_append_files` compares the parquet's
        schema (field IDs included) against the destination and refuses
        with "prepared Parquet schema/field IDs differ from destination"
        (client.py:901). The sink must refresh the live schema,
        null-fill, and prepare ONCE more."""
        cols_after = _EVENTS_COLUMNS + [_col("other_writer_col", "string", 6, 6)]
        s, client, catalog, ns, table = _sink()
        prepared = table.prepare_append_files.side_effect
        calls = {"n": 0}

        def prepare(files, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ValidationError("prepared Parquet schema/field IDs differ from destination", status_code=None)
            return prepared(files, **kwargs)

        table.prepare_append_files.side_effect = prepare
        table.info.return_value = _FakeInfo(columns=tuple(cols_after))
        assert s.write(_batch()) == 1
        assert table.prepare_append_files.call_count == 2
        retried = _published(table)
        assert "other_writer_col" in retried.column_names
        assert retried.column("other_writer_col").null_count == retried.num_rows

    @patch("millpond.hoglake.metrics")
    def test_variant_path_column_refusal_also_self_heals(self, mock_metrics):
        # The other real refusal string (parquet_schema.py:173).
        s, client, catalog, ns, table = _sink()
        prepared = table.prepare_append_files.side_effect
        calls = {"n": 0}

        def prepare(files, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ValidationError("prepared Parquet columns differ from destination", status_code=None)
            return prepared(files, **kwargs)

        table.prepare_append_files.side_effect = prepare
        assert s.write(_batch()) == 1
        assert table.prepare_append_files.call_count == 2

    @patch("millpond.hoglake.metrics")
    def test_partition_arity_refusal_is_not_an_alignment_refusal(self, mock_metrics):
        # "prepared file partition arity differs from destination" is a
        # spec problem, not a column problem: re-aligning cannot fix it,
        # so it must not consume the one self-heal.
        s, *_, table = _sink()
        table.prepare_append_files.side_effect = ValidationError(
            "prepared file partition arity differs from destination", status_code=None
        )
        with pytest.raises(ValidationError):
            s.write(_batch())
        assert table.prepare_append_files.call_count == 1

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
            "prepared Parquet schema/field IDs differ from destination", status_code=None
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
