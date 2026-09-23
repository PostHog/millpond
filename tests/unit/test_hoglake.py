"""Unit tests for millpond/hoglake.py — the HoglakeSink.

Everything here runs against a mocked pyhoglake client layer; the real
server round-trips live in tests/integration/test_hoglake_integration.py.
"""

from __future__ import annotations

import logging
import re
import uuid
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
from millpond.arrow_converter import coerce_typed_columns

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
    # A real string, because the startup credential probe derives the
    # object-store prefix from it: a MagicMock data_path would let a
    # probe that never formed a usable path still look healthy.
    catalog.data_path = "s3://bucket/lake/"
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


# The shape pyhoglake really builds: `{data_path}/data/{ns}/{table}/
# {idempotency_key}/{uuid4}-{index}.parquet`. The uuid4 in every name is
# why the prefix is not sweepable — a retry under the SAME key lands new
# names beside these.
_URI_BASE = "s3://bucket/lake/data/analytics/events/9a1f0f0e-0000-0000-0000-00000000dead"


def _stamped(exc: BaseException, count: int, uris: tuple[str, ...] | None = None) -> BaseException:
    """An exception as pyhoglake>=1.1.1 hands it back from
    `prepare_append_files`: `uploaded_files` is how many uploads CLOSED
    cleanly and `uploaded_uris` names exactly those, in order. A refusal
    raised before the first upload carries 0 / ()."""
    exc.uploaded_files = count
    exc.uploaded_uris = uris if uris is not None else tuple(f"{_URI_BASE}/u{i}-{i}.parquet" for i in range(count))
    assert len(exc.uploaded_uris) == count, "pyhoglake keeps these two consistent; so must the fake"
    return exc


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
        ],
    )
    def test_missing_required_field_raises_runtimeerror(self, missing_field):
        cfg = _cfg(**{missing_field: None})
        with pytest.raises(RuntimeError, match=missing_field):
            with patch("millpond.hoglake.HoglakeClient"):
                hoglake.HoglakeSink(cfg)

    def test_both_s3_keys_absent_is_accepted(self):
        # The Kubernetes shape: pyhoglake passes no key to pyarrow, so
        # the AWS SDK resolves the ServiceAccount's web-identity token.
        cfg = _cfg(hoglake_s3_access_key=None, hoglake_s3_secret_key=None)
        client, *_ = _mock_stack(_EVENTS_COLUMNS)
        with patch("millpond.hoglake.HoglakeClient", return_value=client) as mock_client:
            hoglake.HoglakeSink(cfg)
        s3 = mock_client.call_args.kwargs["s3"]
        assert s3.access_key is None
        assert s3.secret_key is None
        # Endpoint and region are independent of the credential source.
        assert s3.endpoint_override == "http://localhost:29000"
        assert s3.region == "us-east-1"

    @pytest.mark.parametrize("present", ["hoglake_s3_access_key", "hoglake_s3_secret_key"])
    def test_one_s3_key_without_the_other_raises_naming_both(self, present):
        # pyarrow refuses half a pair itself — `S3FileSystem(access_key=...)`
        # with no secret raises ValueError — so this guard is not about
        # preventing a silent fallback. It is about WHERE and HOW: here,
        # in the constructor, naming both config fields, rather than as
        # a pyarrow ValueError raised from inside pyhoglake that names
        # neither.
        cfg = _cfg(**{f: None for f in ("hoglake_s3_access_key", "hoglake_s3_secret_key") if f != present})
        with pytest.raises(hoglake.HoglakeSinkError) as excinfo:
            with patch("millpond.hoglake.HoglakeClient"):
                hoglake.HoglakeSink(cfg)
        message = str(excinfo.value)
        assert "hoglake_s3_access_key" in message
        assert "hoglake_s3_secret_key" in message

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


class TestStartupCredentialProbe:
    """One authenticated object-store call at construction.

    pyarrow resolves credentials lazily, so without this a role that
    cannot write the bucket shows up as an S3 403 on the FIRST FLUSH —
    after the pod passed its probes, took its partitions and built lag.
    The probe is a zero-byte PUT because that is the grant the sink
    actually needs; a LIST would prove a permission the IAM role is not
    even meant to carry, and (with `allow_not_found`) would read a
    missing bucket as an empty prefix."""

    def _fs(self, client):
        return client._filesystem.return_value

    def _stream(self, client):
        return self._fs(client).open_output_stream.return_value.__enter__.return_value

    def test_probe_puts_the_marker_object_under_the_data_path(self):
        _, client, *_ = _sink()
        key = self._fs(client).open_output_stream.call_args.args[0]
        # Bucket-relative, exactly as pyhoglake's own uploader passes it.
        assert key == "bucket/lake/_millpond/probe"

    def test_the_marker_key_is_the_same_with_or_without_a_trailing_slash(self):
        # The data path comes off the catalog row, and hoglake stores it
        # as the operator typed it. One marker per data path, not two.
        for data_path in ("s3://bucket/lake/", "s3://bucket/lake"):
            client, catalog, *_ = _mock_stack(_EVENTS_COLUMNS)
            catalog.data_path = data_path
            with patch("millpond.hoglake.HoglakeClient", return_value=client):
                hoglake.HoglakeSink(_cfg())
            assert self._fs(client).open_output_stream.call_args.args[0] == "bucket/lake/_millpond/probe"

    def test_the_marker_is_zero_bytes(self):
        # Nothing is written into the stream: the upload itself is the
        # whole question, and an empty object is the cheapest thing to
        # leave behind (overwritten on every boot, so at most one per
        # path).
        _, client, *_ = _sink()
        self._stream(client).write.assert_not_called()

    def test_the_stream_is_closed_by_the_context_manager(self):
        # pyarrow only sends the upload on close, so a probe that opened
        # the stream and dropped it would prove nothing — and would still
        # pass every other assertion here, because CPython's refcount
        # closes it a moment later. The `with` is the contract.
        _, client, *_ = _sink()
        assert self._fs(client).open_output_stream.return_value.__exit__.called

    def test_probe_runs_exactly_once_at_construction_and_never_on_write(self):
        s, client, _catalog, _ns, _table = _sink()
        fs = self._fs(client)
        assert fs.open_output_stream.call_count == 1
        s.write(_batch())
        s.write(_batch())
        assert fs.open_output_stream.call_count == 1

    def _refusing_sink(self, error_text, **cfg_overrides):
        client, *_ = _mock_stack(_EVENTS_COLUMNS)
        client._filesystem.return_value.open_output_stream.side_effect = OSError(error_text)
        with pytest.raises(hoglake.HoglakeSinkError) as excinfo:
            with patch("millpond.hoglake.HoglakeClient", return_value=client):
                hoglake.HoglakeSink(_cfg(**cfg_overrides))
        return excinfo.value

    def test_access_denied_names_the_marker_path_the_catalog_and_the_static_keys(self):
        e = self._refusing_sink("When creating key 'lake/_millpond/probe' in bucket 'bucket': AWS Error ACCESS_DENIED")
        message = str(e)
        assert "s3://bucket/lake/_millpond/probe" in message
        assert "'millpond'" in message  # the catalog
        assert "static HOGLAKE_S3_* keys" in message
        # The SDK's own text, verbatim: it is the only thing that
        # distinguishes one 403 from another.
        assert "AWS Error ACCESS_DENIED" in message
        # ... and the grant that would fix it, which is NOT ListBucket.
        assert "s3:PutObject" in message
        assert "s3:AbortMultipartUpload" in message
        assert "ListBucket" not in message

    def test_access_denied_names_the_default_credential_chain_without_keys(self):
        # The message has to say WHICH credential source was in use:
        # under IRSA the fix is a role/policy change, not a Secret.
        e = self._refusing_sink(
            "AWS Error ACCESS_DENIED",
            hoglake_s3_access_key=None,
            hoglake_s3_secret_key=None,
        )
        assert "the AWS default credential chain (IRSA in Kubernetes)" in str(e)

    def test_a_missing_bucket_is_named_as_such(self):
        # The case a LIST with allow_not_found could never catch: a typo
        # in HOGLAKE_DATA_PATH's bucket read back as an empty prefix,
        # and the catalog row has already frozen the bad path.
        e = self._refusing_sink("When creating key '...' in bucket 'bukcet': AWS Error NO_SUCH_BUCKET: NoSuchBucket")
        message = str(e)
        assert "bucket" in message.lower()
        assert "HOGLAKE_DATA_PATH" in message

    @pytest.mark.parametrize("sdk_text", ["AuthorizationHeaderMalformed", "PermanentRedirect"])
    def test_a_region_mismatch_points_at_the_region_settings(self, sdk_text):
        # Signed for the wrong region: the SDK says so in two different
        # ways depending on the operation, and neither says "region" in
        # a way an operator can act on.
        e = self._refusing_sink(f"AWS Error UNKNOWN: {sdk_text}")
        message = str(e)
        assert "HOGLAKE_S3_REGION" in message
        assert "AWS_REGION" in message

    @pytest.mark.parametrize("sdk_text", ["INVALID_ACCESS_KEY_ID", "SIGNATURE_DOES_NOT_MATCH"])
    def test_rejected_credentials_point_at_the_static_keys(self, sdk_text):
        # S3 refusing the key id or the signature is not a policy
        # problem: the material itself is wrong (stale Secret, wrong
        # account, truncated value), and under static keys that is the
        # only thing it can be.
        e = self._refusing_sink(f"AWS Error {sdk_text}: rejected")
        message = str(e)
        # The hint, not just the credential-source phrase the message
        # always carries — and it says why this branch is static-only.
        assert "the static HOGLAKE_S3_* keys are wrong" in message
        assert "only reachable with static keys" in message
        # Not the generic grant advice: the policy is not the problem.
        assert "bucket policy" not in message

    def test_probe_failure_is_permanent(self):
        # Not retryable: a missing grant does not heal by waiting, and
        # this fires at startup where there is no retry loop anyway.
        e = self._refusing_sink("AWS Error ACCESS_DENIED")
        assert e.retryable is False
        assert hoglake.is_retryable(e) is False

    def test_a_pyhoglake_without_a_filesystem_accessor_fails_loudly(self):
        # A security-relevant guard must not degrade to a warning on an
        # attribute rename: no probe means no proof, and no proof at
        # startup is the whole failure mode this exists to close.
        client, *_ = _mock_stack(_EVENTS_COLUMNS)
        del client._filesystem
        with pytest.raises(hoglake.HoglakeSinkError) as excinfo:
            with patch("millpond.hoglake.HoglakeClient", return_value=client):
                hoglake.HoglakeSink(_cfg())
        assert "no longer exposes the filesystem" in str(excinfo.value)
        assert excinfo.value.retryable is False

    @pytest.mark.parametrize(
        ("overrides", "expected"),
        [
            ({}, "static HOGLAKE_S3_* keys"),
            (
                {"hoglake_s3_access_key": None, "hoglake_s3_secret_key": None},
                "the AWS default credential chain (IRSA in Kubernetes)",
            ),
        ],
    )
    def test_startup_logs_the_credential_source_and_the_probe_outcome(self, overrides, expected, caplog):
        with caplog.at_level(logging.INFO, logger="millpond.hoglake"):
            _sink(_cfg(**overrides))
        messages = [r.getMessage() for r in caplog.records]
        # ONE line, carrying the source and the fact that the probe
        # passed against this path.
        matching = [m for m in messages if expected in m]
        assert len(matching) == 1
        assert "s3://bucket/lake/_millpond/probe" in matching[0]
        # The SOURCE, never the material: `ak`/`sk` are the fixture's
        # key values, and a log line is the easiest place to leak one.
        assert not re.search(r"\b(ak|sk)\b", " ".join(messages))


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
        real.catalog = MagicMock(return_value=MagicMock(data_path="s3://bucket/lake/"))
        # This client was built with no S3Config (the sink always passes
        # one), and the startup credential probe must not turn that into
        # a real object-store call from a unit test.
        real._filesystem = MagicMock()
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
            (pa.uuid(), "uuid"),  # MILLPOND_TYPED_COLUMNS uuid
        ],
    )
    def test_millpond_types_map(self, arrow_type, hoglake_type):
        from pyhoglake.types import schema_to_column_defs

        out = hoglake.table_schema_for_batch(pa.schema([("c", arrow_type)]))
        defs = schema_to_column_defs(out)
        assert defs[0]["type"] == hoglake_type


class TestUuidColumnWireForm:
    """What a `uuid`-pinned column actually looks like in the file this sink
    uploads."""

    RAW = "018f3c7e-6b2a-7c3d-9e4f-5a6b7c8d9e0f"

    def _uuid_columns(self):
        return [
            _col("uuid", "uuid", 1, 1),
            _col("event", "string", 2, 2),
            _col("team_id", "long", 3, 3),
            _col("properties", "string", 4, 4),
            _col("_inserted_at", "timestamptz", 5, 5),
        ]

    def _written(self, monkeypatch):
        monkeypatch.setattr(hoglake.pq, "write_table", _REAL_WRITE_TABLE)
        s, client, catalog, ns, table = _sink(columns=self._uuid_columns())
        captured: dict[str, list] = {}
        prepared = table.prepare_append_files.side_effect

        def prepare(files, **kwargs):
            captured["files"] = [pq.ParquetFile(path) for path, _ in files]
            return prepared(files, **kwargs)

        table.prepare_append_files.side_effect = prepare
        batch = coerce_typed_columns(
            pa.table({"uuid": [self.RAW], "event": ["e"], "team_id": [1]}),
            (("uuid", "uuid"),),
        )
        s.write(batch)
        return captured["files"][0]

    def test_coerced_column_aligns_to_the_live_uuid_column(self, monkeypatch):
        # No add_column, no promote: `pa.uuid()` already IS the live type,
        # and since pyhoglake 1.3.0 it is also the type the destination schema
        # names, so `_prepare`'s cast is a no-op rather than a downgrade.
        pf = self._written(monkeypatch)
        assert pf.schema.column(0).name == "uuid"
        assert pf.schema.column(0).physical_type == "FIXED_LEN_BYTE_ARRAY"
        assert pf.schema.column(0).length == 16
        assert pf.read().column("uuid").to_pylist() == [uuid.UUID(self.RAW)]

    def test_uploaded_parquet_carries_the_uuid_logical_annotation(self, monkeypatch):
        """The file carries the parquet `LogicalTypeAnnotation.uuidType()` an
        Iceberg reader binds a uuid column through — the Trino hoglake
        connector among them.

        It did not, until pyhoglake 1.3.0. `_prepare` casts the batch to
        `columns_to_arrow_schema(info.columns)`, and pyhoglake used to answer a
        `uuid` column with plain `pa.binary(16)` (`types.py`
        `coltype_to_arrow`), for which pyarrow stamps no logical type at all.
        Casting to `pa.uuid()` from the millpond side instead did produce the
        annotation and then had the file REFUSED, because
        `prepare_append_files` compared
        `parquet.schema_arrow.equals(columns_to_arrow_schema(...))` exactly —
        so the two spellings had to move together, which is what 1.3.0 did:
        `coltype_to_arrow("uuid")` returns `pa.uuid()` and append accepts
        either spelling. Nothing on this side changed; the pin did the work.
        """
        pf = self._written(monkeypatch)
        assert pf.schema.column(0).logical_type.type == "UUID"
        assert pf.schema_arrow.field("uuid").type == pa.uuid()


class TestUuidAgainstStringColumn:
    """B1: pinning `uuid` on a table whose live column is `string`.

    This is the state every existing table is in — `events_raw` in dev today —
    because the column was created from unpinned string batches. Before the
    degradation arm, `_evolve_and_align` found no ("string", "uuid") promotion,
    left the column alone, and `_prepare`'s `.cast(target)` raised
    `ArrowInvalid: Invalid UTF8 payload` on every flush: offsets never commit,
    the restart re-consumes the same batch, the partition is wedged for good.
    """

    RAW = "018f3c7e-6b2a-7c3d-9e4f-5a6b7c8d9e0f"

    def _batch(self):
        return coerce_typed_columns(
            pa.table({"uuid": [self.RAW], "event": ["e"], "team_id": [1]}),
            (("uuid", "uuid"),),
        )

    def test_degrades_to_text_instead_of_wedging(self):
        # _EVENTS_COLUMNS types `uuid` as string — the unpinned-table shape.
        s, client, catalog, ns, table = _sink()
        assert s.write(self._batch()) == 1
        written = _published()
        assert written.schema.field("uuid").type == pa.string()
        assert written.column("uuid").to_pylist() == [self.RAW]
        # No DDL was attempted: string cannot be promoted to uuid.
        assert table.alter.call_count == 0

    @patch("millpond.hoglake.metrics")
    def test_degradation_is_metricked_and_logged_once(self, mock_metrics, caplog):
        s, client, catalog, ns, table = _sink()
        with caplog.at_level(logging.WARNING, logger="millpond.hoglake"):
            s.write(self._batch())
            s.write(self._batch())
        mock_metrics.errors_total.labels.assert_any_call(type="schema")
        warnings = [r.message for r in caplog.records if "promotion" in r.message]
        assert len(warnings) == 1

    def test_nulls_survive_the_restringify(self):
        s, client, catalog, ns, table = _sink()
        batch = coerce_typed_columns(
            pa.table({"uuid": pa.array([self.RAW, None], pa.string()), "team_id": [1, 2]}),
            (("uuid", "uuid"),),
        )
        s.write(batch)
        assert _published().column("uuid").to_pylist() == [self.RAW, None]

    def test_live_uuid_column_is_not_restringified(self):
        # The control: when the table really is uuid-typed, nothing degrades.
        columns = [
            _col("uuid", "uuid", 1, 1),
            _col("event", "string", 2, 2),
            _col("team_id", "long", 3, 3),
            _col("properties", "string", 4, 4),
            _col("_inserted_at", "timestamptz", 5, 5),
        ]
        s, client, catalog, ns, table = _sink(columns=columns)
        s.write(self._batch())
        assert _published().schema.field("uuid").type == pa.uuid()


class TestStringAgainstUuidColumn:
    """B1's mirror: a live `uuid` column and a plain `string` batch column.

    Two ways to land here, both ordinary: an operator removes `<col>:uuid`
    from MILLPOND_TYPED_COLUMNS as a rollback, or one pod on a mixed fleet has
    not picked the pin up yet and writes to a table another pod created with
    it. Before the rewrite arm this fell to `_prepare`'s cast and raised
    `ArrowInvalid: Failed casting from string to fixed_size_binary[16]`, which
    `is_retryable` correctly calls permanent — so it crashed on attempt 1 with
    the offsets uncommitted, every time.
    """

    RAW = "018f3c7e-6b2a-7c3d-9e4f-5a6b7c8d9e0f"
    OTHER = "5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d"

    def _uuid_table(self):
        return [
            _col("uuid", "uuid", 1, 1),
            _col("event", "string", 2, 2),
            _col("team_id", "long", 3, 3),
            _col("properties", "string", 4, 4),
            _col("_inserted_at", "timestamptz", 5, 5),
        ]

    def test_unpinned_string_batch_is_parsed_instead_of_crashing(self):
        s, client, catalog, ns, table = _sink(columns=self._uuid_table())
        assert s.write(pa.table({"uuid": [self.RAW], "event": ["e"], "team_id": [1]})) == 1
        written = _published()
        assert written.schema.field("uuid").type == pa.uuid()
        assert written.column("uuid").to_pylist() == [uuid.UUID(self.RAW)]
        assert table.alter.call_count == 0

    @patch("millpond.hoglake.metrics")
    def test_unparseable_text_is_nulled_and_metricked(self, mock_metrics):
        s, client, catalog, ns, table = _sink(columns=self._uuid_table())
        s.write(pa.table({"uuid": [self.RAW, "not-a-uuid", None], "team_id": [1, 2, 3]}))
        assert _published().column("uuid").to_pylist() == [uuid.UUID(self.RAW), None, None]
        mock_metrics.errors_total.labels.assert_any_call(type="schema")
        mock_metrics.errors_total.labels.assert_any_call(type="column_coercion")

    def test_large_string_batch_column_is_parsed_too(self):
        s, client, catalog, ns, table = _sink(columns=self._uuid_table())
        s.write(pa.table({"uuid": pa.array([self.RAW], pa.large_string()), "team_id": [1]}))
        assert _published().column("uuid").to_pylist() == [uuid.UUID(self.RAW)]


class TestUuidRewriteShapes:
    """Shapes the rewrite has to survive that the single-column, single-chunk
    happy paths above do not exercise."""

    A = "018f3c7e-6b2a-7c3d-9e4f-5a6b7c8d9e0f"
    B = "5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d"
    C = "ffffffff-ffff-4fff-bfff-ffffffffffff"

    def _string_table(self):
        # `uuid` and `person_id` both string-typed live: the pre-pin shape of
        # the real events table.
        return [
            _col("uuid", "string", 1, 1),
            _col("person_id", "string", 2, 2),
            _col("team_id", "long", 3, 3),
            _col("_inserted_at", "timestamptz", 4, 4),
        ]

    def _uuid_table(self):
        return [
            _col("uuid", "uuid", 1, 1),
            _col("person_id", "uuid", 2, 2),
            _col("team_id", "long", 3, 3),
            _col("_inserted_at", "timestamptz", 4, 4),
        ]

    def _multichunk(self, values, type_):
        # What `pa.concat_tables` hands `_flush` when MILLPOND_SORT_BY is
        # unset: one chunk per consumed batch, never combined.
        half = len(values) // 2
        return pa.chunked_array([pa.array(values[:half], type_), pa.array(values[half:], type_)])

    def test_multichunk_two_columns_uuid_batch_to_string_table(self):
        s, client, catalog, ns, table = _sink(columns=self._string_table())
        text = [self.A, self.B, self.C, self.A]
        pins = (("uuid", "uuid"), ("person_id", "uuid"))
        # The production shape exactly: coercion runs per CONSUMED batch, and
        # `_flush`'s `pa.concat_tables` leaves one chunk per batch behind when
        # MILLPOND_SORT_BY is unset (a sort would combine them).
        batch = pa.concat_tables(
            [
                coerce_typed_columns(
                    pa.table(
                        {
                            "uuid": pa.array(text[i : i + 2], pa.string()),
                            "person_id": pa.array(list(reversed(text))[i : i + 2], pa.string()),
                            "team_id": pa.array([i, i + 1], pa.int64()),
                        }
                    ),
                    pins,
                )
                for i in (0, 2)
            ]
        )
        assert batch.column("uuid").num_chunks == 2
        s.write(batch)
        written = _published()
        # Every chunk and BOTH columns: a rewrite that stopped after the first
        # of either would pass a single-chunk single-column assertion.
        assert written.column("uuid").to_pylist() == text
        assert written.column("person_id").to_pylist() == list(reversed(text))

    def test_rewritten_column_is_the_uuid_extension_type(self):
        # NOT bare `fixed_size_binary(16)`: pyhoglake maps both to "uuid", so
        # the wrong one still appends — and then the column is the one thing
        # that cannot carry the parquet UUID annotation if pyhoglake ever
        # stops stripping it, and disagrees with what the coercer emits.
        s, client, catalog, ns, table = _sink(columns=self._uuid_table())
        batch = pa.table({"uuid": [self.A], "team_id": pa.array([1], pa.int64())})
        out = s._rewrite_column(batch, "uuid", "uuid", "string")
        assert out.schema.field("uuid").type == pa.uuid()
        assert out.column("uuid").combine_chunks().storage.to_pylist() == [uuid.UUID(self.A).bytes]

    @patch("millpond.hoglake.metrics")
    def test_both_directions_of_one_column_each_warn(self, mock_metrics, caplog):
        # The dedup key carries the DIRECTION: keyed on the name alone, a pod
        # that rolls the pin back after applying it would never report the
        # second mismatch.
        s, client, catalog, ns, table = _sink(columns=self._string_table())
        pinned = coerce_typed_columns(pa.table({"uuid": [self.A]}), (("uuid", "uuid"),))
        text = pa.table({"uuid": [self.A]})
        with caplog.at_level(logging.WARNING, logger="millpond.hoglake"):
            s._rewrite_column(pinned, "uuid", "string", "uuid")
            s._rewrite_column(pinned, "uuid", "string", "uuid")
            s._rewrite_column(text, "uuid", "uuid", "string")
        messages = [r.message for r in caplog.records if "promotion" in r.message]
        assert len(messages) == 2
        # And each renders its own direction the right way round.
        assert "hoglake has no string->uuid promotion" in messages[0]
        assert "A real 'uuid' column needs the table created that way" in messages[0]
        assert "hoglake has no uuid->string promotion" in messages[1]
        assert "A real 'string' column needs the table created that way" in messages[1]

    def test_multichunk_two_columns_string_batch_to_uuid_table(self):
        s, client, catalog, ns, table = _sink(columns=self._uuid_table())
        text = [self.A, self.B, self.C, self.A]
        s.write(
            pa.table(
                {
                    "uuid": self._multichunk(text, pa.string()),
                    "person_id": self._multichunk(list(reversed(text)), pa.string()),
                    "team_id": pa.array([1, 2, 3, 4], pa.int64()),
                }
            )
        )
        written = _published()
        assert written.column("uuid").to_pylist() == [uuid.UUID(v) for v in text]
        assert written.column("person_id").to_pylist() == [uuid.UUID(v) for v in reversed(text)]

    def test_bare_fixed_size_binary_batch_against_a_string_column(self):
        # pyhoglake maps a bare `fixed_size_binary(16)` to "uuid" too, and that
        # array has no `.storage` — reading one unguarded raised AttributeError,
        # which `is_retryable` calls transient, so it burned the whole ladder
        # before the crash.
        s, client, catalog, ns, table = _sink(columns=self._string_table())
        s.write(
            pa.table(
                {
                    "uuid": pa.array([uuid.UUID(self.A).bytes], pa.binary(16)),
                    "team_id": pa.array([1], pa.int64()),
                }
            )
        )
        assert _published().column("uuid").to_pylist() == [self.A]

    def test_unknown_column_name_raises_rather_than_rewriting_the_last_one(self):
        # `get_field_index` answers -1 for a miss and `set_column(-1, ...)`
        # would silently rewrite the LAST column.
        s, client, catalog, ns, table = _sink(columns=self._string_table())
        batch = pa.table({"uuid": [self.A], "team_id": [1]})
        with pytest.raises(ValueError, match="not in the batch schema"):
            s._rewrite_column(batch, "absent", "string", "uuid")


class TestArrowErrorsAreNotRetryable:
    """A pyarrow failure is deterministic on the batch in hand: the same cast
    of the same bytes fails identically on every attempt, so the retry ladder
    is pure delay before the crash the operator needs to see."""

    @pytest.mark.parametrize(
        "exc",
        [
            pa.ArrowInvalid("Invalid UTF8 payload"),
            pa.ArrowTypeError("no kernel"),
            pa.ArrowNotImplementedError("Sorting not supported for type extension<arrow.uuid>"),
        ],
    )
    def test_arrow_errors_are_permanent(self, exc):
        assert hoglake.is_retryable(exc) is False


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

    def test_a_retry_is_recognized_by_its_kafka_identity_not_by_a_re_derived_key(self):
        # The identity the payload was BUILT for is what makes a retry a
        # retry. Re-deriving the key here instead would consult the live
        # table incarnation — which the retry path deliberately has not
        # re-resolved — so a drop+recreate under a held payload would
        # read as "a different flush": the sink would abandon a
        # registration whose commit may well have landed, upload a second
        # copy of the same rows, and publish it under a name the server
        # has no receipt for.
        #
        # Replayed instead, the frozen payload carries its own
        # `expected_table_uuid`, and the commit is where that gets
        # judged.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = httpx.ReadTimeout("response lost")
        with pytest.raises(httpx.ReadTimeout):
            s.write(_rows(3), kafka_offsets=self.OFFSETS)
        s.reset_caches()
        reborn = "deadbeef-0000-0000-0000-000000000000"
        table.info.return_value = _FakeInfo(columns=tuple(_EVENTS_COLUMNS), table_uuid=reborn)
        table.table_uuid = reborn
        with pytest.raises(IncarnationChangedError):
            s.write(_rows(3), kafka_offsets=self.OFFSETS)
        assert table.prepare_append_files.call_count == 1  # replayed and judged, never rebuilt

    def test_the_commit_time_lookup_never_becomes_the_reconciled_cache(self):
        # `_live_table` resolves a bare handle for the pre-commit
        # incarnation/spec check, and must not cache it: `self._table`
        # means "resolved AND reconciled by `_ensure_table`", and a
        # handle fetched here has been through neither. Caching it would
        # let the next flush write under a layout this pod never checked
        # against config.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = [httpx.ReadTimeout("lost"), MagicMock(), MagicMock()]
        with pytest.raises(httpx.ReadTimeout):
            s.write(_rows(2), kafka_offsets=self.OFFSETS)
        # main.py's retry path: caches dropped, so the replay's
        # destination check is what resolves the handle.
        s.reset_caches()
        assert s.write(_rows(2), kafka_offsets=self.OFFSETS) == 2
        assert s._table is None
        # ...and the consequence that makes it matter: the next ordinary
        # flush still goes through `_ensure_table`'s resolve-and-reconcile.
        resolves = ns.table.call_count
        assert s.write(_rows(2), kafka_offsets=(("events", 0, 42, 43),)) == 2
        assert ns.table.call_count > resolves

    def test_an_empty_offset_tuple_is_not_an_identity(self):
        # `()` names no rows, so it cannot recognize anything — and
        # treating it as an identity is worse than having none: two
        # consecutive anonymous flushes then match each other, and the
        # second replays the first's payload and drops its own rows on
        # the floor. main.py cannot produce one today (it gates on
        # pending_records > 0), which is exactly why the guard needs a
        # test rather than a caller.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = [httpx.ReadTimeout("lost"), MagicMock()]
        with pytest.raises(httpx.ReadTimeout):
            s.write(_rows(3), kafka_offsets=())
        assert s.write(_rows(7), kafka_offsets=()) == 7
        assert table.prepare_append_files.call_count == 2  # rebuilt, never replayed

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

    @pytest.mark.parametrize(
        "exc",
        [
            CommitConflictError("commit_conflict", status_code=409, detail="idempotency_key reused"),
            HoglakeError("internal", status_code=500, detail="idempotency_key reused"),
        ],
    )
    @patch("millpond.hoglake.metrics")
    def test_the_marker_is_only_believed_on_a_422(self, mock_metrics, exc):
        # "these rows are already in the lake" is the one answer that
        # lets millpond advance Kafka offsets over rows it did not
        # write, so it is only ever read off the response the server
        # says it in: a 422. The marker is a SENTENCE in an error body,
        # and `_commit_prepared` catches the whole HoglakeError family —
        # so a 409 or a 5xx whose body happens to echo it (a conflict
        # report quoting the request, a proxy folding a 422 into a 500)
        # reaches this line too. Widening the guard to any HoglakeError
        # trades a retryable failure for silent data loss.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = exc
        with pytest.raises(type(exc)):
            s.write(_rows(5), kafka_offsets=self.OFFSETS)
        mock_metrics.hoglake_commit_replays_total.labels.assert_not_called()


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

    @pytest.mark.parametrize(
        "detail",
        [
            "prepared Parquet schema/field IDs differ from destination",
            "prepared file partition arity differs from destination",
            "prepared file must contain rows",
        ],
    )
    @pytest.mark.parametrize("rows", [1, 3])
    @patch("millpond.hoglake.metrics")
    def test_a_prepare_refusal_that_uploaded_nothing_counts_zero(self, mock_metrics, rows, detail):
        # A pre-upload refusal comes back stamped 0 / (), and the sink
        # books zero because it READ that — not because it reasoned
        # about where in pyhoglake's loop the refusal fires. The
        # reasoning is what this branch got wrong twice: an earlier
        # shape asserted N-1 for a 3-file fanout, which is N-1 phantom
        # orphans every time a concurrent add_column trips the refusal
        # and `write()` self-heals into a flush that SUCCEEDS.
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        table.prepare_append_files.side_effect = _stamped(ValidationError(detail, status_code=None), 0)
        with pytest.raises(ValidationError):
            s.write(
                pa.table({"uuid": [f"u{i}" for i in range(rows)], "team_id": list(range(rows))}),
                kafka_offsets=self.OFFSETS,
            )
        mock_metrics.hoglake_orphaned_files_total.inc.assert_not_called()

    @pytest.mark.parametrize(
        "exc",
        [
            HoglakeError("commit_queue_timeout", status_code=503),
            NotFoundError("table not found", status_code=404),
            IncarnationChangedError("table was recreated"),
            httpx.ReadTimeout("no response"),
            ValueError("badly formed hexadecimal UUID string"),
        ],
    )
    @patch("millpond.hoglake.metrics")
    def test_a_prepare_that_fails_before_the_first_byte_is_not_counted(self, mock_metrics, exc):
        # The catalog-side failures — the key's UUID parse, the
        # read-snapshot refresh, the incarnation pre-flight — all come
        # back stamped zero, so the retry ladder (8 attempts x an 8-way
        # fanout) cannot book 64 phantom orphans a flush.
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        table.prepare_append_files.side_effect = _stamped(exc, 0)
        with pytest.raises(type(exc)):
            s.write(pa.table({"uuid": ["a", "b", "c"], "team_id": [1, 2, 3]}), kafka_offsets=self.OFFSETS)
        mock_metrics.hoglake_orphaned_files_total.inc.assert_not_called()

    @patch("millpond.hoglake.metrics")
    def test_a_prepare_that_failed_mid_fanout_counts_exactly_what_landed(self, mock_metrics, caplog):
        # pyarrow's S3 upload failing partway through a fanout: two
        # objects closed cleanly, the third raised. pyhoglake reports
        # the two, so the sink books two — not the fanout width, not
        # nothing.
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        uris = (f"{_URI_BASE}/aaaa-0.parquet", f"{_URI_BASE}/bbbb-1.parquet")
        table.prepare_append_files.side_effect = _stamped(OSError("S3 reset midway"), 2, uris)
        with caplog.at_level(logging.WARNING, logger="millpond.hoglake"), pytest.raises(OSError):
            s.write(pa.table({"uuid": ["a", "b", "c"], "team_id": [1, 2, 3]}), kafka_offsets=self.OFFSETS)
        mock_metrics.hoglake_orphaned_files_total.inc.assert_called_once_with(2)
        assert "Orphaned 2 uploaded parquet file(s)" in caplog.text

    @patch("millpond.hoglake.metrics")
    def test_the_orphan_log_names_the_uris_and_retracts_the_prefix_sweep(self, mock_metrics, caplog):
        # Object names are `{uuid4}-{index}.parquet` under the
        # `{idempotency_key}/` prefix, and a retry under the SAME key
        # writes new names beside the old ones. So an operator who
        # sweeps the prefix after a later attempt succeeds deletes live,
        # committed files. The log names the objects, and says so.
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        uris = (f"{_URI_BASE}/aaaa-0.parquet", f"{_URI_BASE}/bbbb-1.parquet")
        table.prepare_append_files.side_effect = _stamped(OSError("S3 reset midway"), 2, uris)
        with caplog.at_level(logging.WARNING, logger="millpond.hoglake"), pytest.raises(OSError):
            s.write(pa.table({"uuid": ["a", "b", "c"], "team_id": [1, 2, 3]}), kafka_offsets=self.OFFSETS)
        for uri in uris:
            assert uri in caplog.text
        assert "never the prefix" in caplog.text
        # The old advice — a fanout-width upper bound and the prefix as
        # the thing to sweep — must be gone.
        assert "up to" not in caplog.text

    @patch("millpond.hoglake.metrics")
    def test_the_file_that_failed_is_uncounted_but_flagged_as_possibly_there(self, mock_metrics, caplog):
        # A close that fails can leave a TRUNCATED object behind, and
        # pyhoglake cannot name it. The count is therefore a lower bound
        # on what is in storage, and the log has to say so or the sweep
        # walks past a real object.
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        uris = (f"{_URI_BASE}/aaaa-0.parquet",)
        table.prepare_append_files.side_effect = _stamped(OSError("close failed"), 1, uris)
        with caplog.at_level(logging.WARNING, logger="millpond.hoglake"), pytest.raises(OSError):
            s.write(pa.table({"uuid": ["a", "b", "c"], "team_id": [1, 2, 3]}), kafka_offsets=self.OFFSETS)
        mock_metrics.hoglake_orphaned_files_total.inc.assert_called_once_with(1)
        assert "truncated" in caplog.text

    @patch("millpond.hoglake.metrics")
    def test_a_wide_fanout_logs_a_capped_list_and_says_what_it_omitted(self, mock_metrics, caplog):
        # One line per flush, not one line per team. The cap is a log
        # concern only: the metric still books every object.
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        uris = tuple(f"{_URI_BASE}/f{i:03d}-{i}.parquet" for i in range(25))
        table.prepare_append_files.side_effect = _stamped(OSError("S3 reset midway"), len(uris), uris)
        with caplog.at_level(logging.WARNING, logger="millpond.hoglake"), pytest.raises(OSError):
            s.write(pa.table({"uuid": ["a", "b", "c"], "team_id": [1, 2, 3]}), kafka_offsets=self.OFFSETS)
        mock_metrics.hoglake_orphaned_files_total.inc.assert_called_once_with(25)
        listed = [uri for uri in uris if uri in caplog.text]
        assert len(listed) == hoglake._ORPHAN_URIS_LOGGED
        assert listed == list(uris[: hoglake._ORPHAN_URIS_LOGGED])
        assert f"{len(uris) - hoglake._ORPHAN_URIS_LOGGED} more" in caplog.text

    @patch("millpond.hoglake.metrics")
    def test_an_older_client_without_the_attributes_counts_zero(self, mock_metrics, caplog):
        # The stamp arrived in pyhoglake 1.1.1 (well under the 1.3.0
        # floor), but it is best effort at
        # the source too: pyhoglake suppresses the AttributeError from an
        # exception type whose __slots__ refuse it. Either way the read
        # must degrade to zero, never to a second exception thrown over
        # the first.
        cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None),))
        s, client, catalog, ns, table = _sink(cfg, partition_spec=_spec(("team_id", "identity")))
        bare = OSError("S3 reset midway")
        assert not hasattr(bare, "uploaded_files")
        table.prepare_append_files.side_effect = bare
        with caplog.at_level(logging.WARNING, logger="millpond.hoglake"), pytest.raises(OSError) as caught:
            s.write(pa.table({"uuid": ["a", "b", "c"], "team_id": [1, 2, 3]}), kafka_offsets=self.OFFSETS)
        assert caught.value is bare  # the original error, not an AttributeError over it
        mock_metrics.hoglake_orphaned_files_total.inc.assert_not_called()

    @patch("millpond.hoglake.metrics")
    def test_a_discarded_payload_names_the_objects_it_orphans(self, mock_metrics, caplog):
        # Retracting the prefix sweep took away the only way an operator
        # could locate these, so the paths the payload already carries
        # have to reach the log.
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = CommitConflictError("commit_conflict", status_code=409)
        with caplog.at_level(logging.WARNING, logger="millpond.hoglake"), pytest.raises(CommitConflictError):
            s.write(_rows(2), kafka_offsets=self.OFFSETS)
        paths = [f["path"] for f in catalog.commit_prepared.call_args[0][0]["appends"][0]["files"]]
        assert paths
        for path in paths:
            assert path in caplog.text
        assert "never the prefix" in caplog.text

    @patch("millpond.hoglake.metrics")
    def test_an_already_published_flush_names_the_objects_it_orphans(self, mock_metrics, caplog):
        s, client, catalog, ns, table = _sink()
        catalog.commit_prepared.side_effect = ValidationError(
            "validation", status_code=422, detail="idempotency_key reused with a different request"
        )
        with caplog.at_level(logging.WARNING, logger="millpond.hoglake"):
            assert s.write(_rows(2), kafka_offsets=self.OFFSETS) == 0
        paths = [f["path"] for f in catalog.commit_prepared.call_args[0][0]["appends"][0]["files"]]
        assert paths
        for path in paths:
            assert path in caplog.text

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
        #
        # This test is also what retires the KeyError arm the self-heal
        # once carried: `_prepare` null-fills against `info.columns` and
        # then selects names from that same object, so the select can
        # only ever narrow. The column below arriving between the two
        # resolves is exactly the case that used to raise, and it is
        # null-filled without the self-heal being consulted at all.
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
