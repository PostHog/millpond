"""Integration tests for HoglakeSink against a REAL hoglake server.

Boots the throwaway stack in tests/hoglake_stack/docker-compose.yaml
(compose project `millpond-hog-it`: hoglake-server + postgres + minio,
high 127.0.0.1 ports only — never the hoglake dev stack's defaults) and
drives the actual sink against it. Skips cleanly when Docker or the
server image is unavailable. Tears the stack down (volumes included) at
session end.

Run explicitly:
    uv run pytest tests/integration/test_hoglake_integration.py -v
"""

from __future__ import annotations

import contextlib
import logging
import threading
import uuid
from dataclasses import dataclass, field
from unittest.mock import MagicMock

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import fs as pafs
from pyhoglake import IncarnationChangedError, NotFoundError, ValidationError

from millpond.hoglake import HoglakeSink, HoglakeSinkError, is_retryable, table_schema_for_batch
from millpond.main import _flush
from tests.hoglake_stack import stack

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session", autouse=True)
def _bind_metrics():
    """The real sink and main._flush hit the real metrics module; bind the
    common labels once so the raw labeled metrics don't refuse bare
    .inc()/.observe() calls (main() does this at startup in production)."""
    from millpond import metrics

    metrics.init("hog-it", broker_source="test")


CATALOG = "millpond-it"
BUCKET = "millpond-it"
DATA_PATH = f"s3://{BUCKET}/lake/"


@dataclass
class HogCfg:
    """The Config surface HoglakeSink (and main._flush) actually reads."""

    destination: str = "hoglake"
    hoglake_url: str = stack.SERVER_URL
    hoglake_catalog: str = CATALOG
    hoglake_namespace: str = "it"
    hoglake_table: str = "events"
    hoglake_data_path: str | None = DATA_PATH
    hoglake_s3_endpoint: str = stack.MINIO_URL
    hoglake_s3_access_key: str = stack.S3_ACCESS_KEY
    hoglake_s3_secret_key: str = stack.S3_SECRET_KEY
    hoglake_s3_region: str | None = None
    hoglake_partition_by: tuple[tuple[str, str, int | None], ...] | None = None
    hoglake_max_retry_count: int = 8
    hoglake_request_timeout_s: float = 45.0
    sort_by: tuple[str, ...] | None = None
    ordinal: int = 0
    table_label: str = field(default="events")


@pytest.fixture(scope="session")
def hog_stack():
    stack.ensure_available()
    stack.up()
    try:
        stack.make_bucket(BUCKET)
        yield stack
    finally:
        stack.down()


@pytest.fixture(scope="session")
def client(hog_stack):
    from pyhoglake import HoglakeClient, S3Config

    c = HoglakeClient(
        stack.SERVER_URL,
        s3=S3Config(
            access_key=stack.S3_ACCESS_KEY,
            secret_key=stack.S3_SECRET_KEY,
            endpoint_override=stack.MINIO_URL,
        ),
    )
    yield c
    c.close()


def _fresh(**overrides) -> HogCfg:
    cfg = HogCfg(**overrides)
    if "hoglake_table" not in overrides:
        cfg.hoglake_table = f"events_{uuid.uuid4().hex[:8]}"
    cfg.table_label = cfg.hoglake_table
    return cfg


def _batch(n=3, teams=(1,), extra_cols=None) -> pa.Table:
    rows = {
        "uuid": [f"u{i}" for i in range(n)],
        "event": ["pageview"] * n,
        "team_id": [teams[i % len(teams)] for i in range(n)],
        "properties": ['{"k": 1}'] * n,
    }
    if extra_cols:
        rows.update(extra_cols)
    return pa.table(rows)


def _table(client, cfg):
    return client.catalog(cfg.hoglake_catalog).namespace(cfg.hoglake_namespace).table(cfg.hoglake_table)


def _record_count(client, cfg) -> int:
    return sum(f.record_count for f in _table(client, cfg).files())


def _list_objects(data_path: str, cfg) -> list[str]:
    """Every object under this table's data prefix, bucket-relative."""
    s3 = pafs.S3FileSystem(
        access_key=stack.S3_ACCESS_KEY,
        secret_key=stack.S3_SECRET_KEY,
        endpoint_override=stack.MINIO_URL,
    )
    prefix = f"{data_path}data/{cfg.hoglake_namespace}/{cfg.hoglake_table}".removeprefix("s3://")
    selector = pafs.FileSelector(prefix, recursive=True, allow_not_found=True)
    return [f.path for f in s3.get_file_info(selector) if f.type == pafs.FileType.File]


def _minio() -> pafs.S3FileSystem:
    return pafs.S3FileSystem(
        access_key=stack.S3_ACCESS_KEY,
        secret_key=stack.S3_SECRET_KEY,
        endpoint_override=stack.MINIO_URL,
    )


def _object_exists(uri: str) -> bool:
    """Whether one object is present in the stack MinIO, by full s3:// URI."""
    return _minio().get_file_info(uri.removeprefix("s3://")).type == pafs.FileType.File


def _delete_object(uri: str) -> None:
    """Remove one object if it is there; a no-op if it is not."""
    with contextlib.suppress(FileNotFoundError, OSError):
        _minio().delete_file(uri.removeprefix("s3://"))


def _read_parquet(path: str) -> pa.Table:
    s3 = pafs.S3FileSystem(
        access_key=stack.S3_ACCESS_KEY,
        secret_key=stack.S3_SECRET_KEY,
        endpoint_override=stack.MINIO_URL,
    )
    with s3.open_input_file(path.removeprefix("s3://")) as f:
        return pq.read_table(f)


class TestBootstrap:
    def test_first_write_creates_catalog_namespace_table(self, hog_stack, client):
        cfg = _fresh(
            hoglake_catalog="millpond-it-boot",
            hoglake_data_path=f"s3://{BUCKET}/boot/",
            hoglake_namespace="boot",
        )
        sink = HoglakeSink(cfg)
        try:
            assert sink.write(_batch(5)) == 5
        finally:
            sink.close()
        catalog = client.catalog("millpond-it-boot")
        assert catalog.data_path == f"s3://{BUCKET}/boot/"
        table = catalog.namespace("boot").table(cfg.hoglake_table)
        types = {c.name: c.type for c in table.columns}
        assert types == {
            "uuid": "string",
            "event": "string",
            "team_id": "long",
            "properties": "string",
            "_inserted_at": "timestamptz",
        }

    def test_missing_catalog_without_data_path_is_a_startup_error(self, hog_stack):
        # Literally at startup now: the sink resolves its catalog in
        # __init__, so this never reaches a flush (and never builds lag
        # behind a pod that passes its probes).
        cfg = _fresh(hoglake_catalog="millpond-it-absent", hoglake_data_path=None)
        with pytest.raises(RuntimeError, match="HOGLAKE_DATA_PATH"):
            HoglakeSink(cfg)

    def test_unreachable_control_plane_is_a_startup_error(self, hog_stack):
        cfg = _fresh(hoglake_url="http://127.0.0.1:1")
        with pytest.raises(RuntimeError, match="cannot reach the hoglake control plane"):
            HoglakeSink(cfg)

    def test_object_store_credentials_are_probed_at_startup(self, hog_stack, monkeypatch):
        # MinIO authenticates every request, so a sink with no static
        # keys has nothing the AWS default chain can turn into a grant on
        # this bucket — the same shape as an IRSA role whose policy
        # misses the bucket. The point of the test is WHERE it fails:
        # in the constructor, before a single row is offered, rather than
        # as an S3 403 on the first flush behind a pod that is already
        # consuming. (This is as close as the stack gets to the IRSA
        # case; it cannot mint a web-identity token, so what is covered
        # is the probe firing and refusing, not the chain succeeding.)
        #
        # IMDS off: with no credentials anywhere else, the SDK otherwise
        # spends seconds knocking on 169.254.169.254 before giving up on
        # a laptop.
        monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
        cfg = _fresh(hoglake_s3_access_key=None, hoglake_s3_secret_key=None)
        with pytest.raises(HoglakeSinkError) as excinfo:
            HoglakeSink(cfg)
        message = str(excinfo.value)
        assert f"{DATA_PATH}_millpond/probe" in message
        assert "the AWS default credential chain (IRSA in Kubernetes)" in message
        # Permanent: no retry loop exists at startup, and a missing grant
        # is not something waiting fixes.
        assert excinfo.value.retryable is False

    def test_static_keys_pass_the_probe_and_leave_the_marker(self, hog_stack, caplog):
        # The other half of the pair: the probe must not refuse a sink
        # that can in fact write, and the proof that it RAN is the marker
        # object plus its one startup log line (a probe that silently
        # no-ops would pass this test's first half on its own).
        #
        # Delete the marker first: every other sink in this session has
        # already written it, so finding one afterwards would otherwise
        # prove nothing about THIS construction.
        marker = f"{DATA_PATH}_millpond/probe"
        _delete_object(marker)
        assert not _object_exists(marker)
        with caplog.at_level(logging.INFO, logger="millpond.hoglake"):
            sink = HoglakeSink(_fresh())
            sink.close()
        lines = [r.getMessage() for r in caplog.records if "object store auth source" in r.getMessage()]
        assert len(lines) == 1
        assert "static HOGLAKE_S3_* keys" in lines[0]
        assert f"{DATA_PATH}_millpond/probe" in lines[0]
        assert _object_exists(marker)

    def test_a_bucket_typo_is_refused_at_construction(self, hog_stack):
        # The failure the old LIST probe could not see: pyarrow's
        # get_file_info maps a NoSuchBucket 404 onto an empty listing, so
        # a typo'd bucket passed and then failed on the first flush —
        # with the bad data path already frozen into the catalog row.
        #
        # This leaves a poisoned catalog row behind in the stack's
        # Postgres (hoglake has no delete-catalog route, which is the
        # very hazard under test). Harmless here: the stack is torn down
        # with its volumes at session end.
        cfg = _fresh(
            hoglake_catalog="millpond-it-typo",
            hoglake_data_path="s3://millpond-it-typpo/lake/",
        )
        with pytest.raises(HoglakeSinkError) as excinfo:
            HoglakeSink(cfg)
        message = str(excinfo.value)
        assert "millpond-it-typpo" in message
        assert excinfo.value.retryable is False

    def test_partition_spec_and_sort_order_declared(self, hog_stack, client):
        cfg = _fresh(
            hoglake_partition_by=(("team_id", "identity", None), ("_inserted_at", "month", None)),
            sort_by=("team_id", "uuid"),
        )
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(6, teams=(101, 202)))
        finally:
            sink.close()
        info = _table(client, cfg).info()
        fid = {c.name: c.field_id for c in info.columns}
        assert info.partition_spec is not None
        assert [(f.source_field_id, f.transform) for f in info.partition_spec.fields] == [
            (fid["team_id"], "identity"),
            (fid["_inserted_at"], "month"),
        ]
        assert info.sort_spec is not None
        assert [(f.source_field_id, f.direction, f.null_order) for f in info.sort_spec.fields] == [
            (fid["team_id"], "asc", "nulls_last"),
            (fid["uuid"], "asc", "nulls_last"),
        ]

    def test_partitioned_append_fans_out_one_file_per_tuple(self, hog_stack, client):
        cfg = _fresh(hoglake_partition_by=(("team_id", "identity", None),))
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(9, teams=(1, 2, 3)))
        finally:
            sink.close()
        files = _table(client, cfg).files()
        assert len(files) == 3
        assert sum(f.record_count for f in files) == 9
        assert sorted(f.partition_values[0] for f in files) == ["1", "2", "3"]

    def test_changing_the_partition_spec_fails_loudly(self, hog_stack, client):
        # A deployed pipeline whose HOGLAKE_PARTITION_BY changes used to
        # keep writing under the old layout with nothing in the logs.
        cfg = _fresh(hoglake_partition_by=(("team_id", "identity", None),))
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(3))
        finally:
            sink.close()
        changed = _fresh(
            hoglake_table=cfg.hoglake_table,
            hoglake_partition_by=(("team_id", "bucket", 8),),
        )
        sink2 = HoglakeSink(changed)
        try:
            with pytest.raises(RuntimeError, match="HOGLAKE_PARTITION_BY"):
                sink2.write(_batch(3))
        finally:
            sink2.close()
        assert _record_count(client, cfg) == 3  # nothing written under the wrong layout

    def test_spec_declaration_recovers_on_an_existing_unpartitioned_table(self, hog_stack, client):
        # The create-then-alter window: the table exists, its spec does
        # not. The next bootstrap must declare it, not shrug.
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(2))
        finally:
            sink.close()
        partitioned = _fresh(
            hoglake_table=cfg.hoglake_table,
            hoglake_partition_by=(("team_id", "identity", None),),
        )
        sink2 = HoglakeSink(partitioned)
        try:
            sink2.write(_batch(4, teams=(7, 8)))
        finally:
            sink2.close()
        info = _table(client, cfg).info()
        assert info.partition_spec is not None
        assert [f.transform for f in info.partition_spec.fields] == ["identity"]

    def test_a_typo_in_the_spec_keeps_failing(self, hog_stack, client):
        # month() on a bigint is a 422 from the server. The failure must
        # repeat on every attempt rather than leaving a permanently
        # unpartitioned table behind after the first one.
        cfg = _fresh(hoglake_partition_by=(("team_id", "month", None),))
        sink = HoglakeSink(cfg)
        try:
            for _ in range(2):
                # A 422 the server returns for the alter, surfaced as the
                # sink's own refusal — and a PERMANENT one, so main.py's
                # retry loop crashes the pod with the message instead of
                # spending eight attempts on a config typo.
                with pytest.raises(ValidationError) as caught:
                    sink.write(_batch(2))
                assert is_retryable(caught.value) is False
                sink.reset_caches()
        finally:
            sink.close()
        # The table exists (create succeeded, the alter did not) and has
        # no rows: the flush never got past the declaration.
        assert _record_count(client, cfg) == 0

    def test_concurrent_create_tolerated(self, hog_stack, client):
        cfg = _fresh()
        sinks = [HoglakeSink(_fresh(hoglake_table=cfg.hoglake_table)) for _ in range(2)]
        errors: list[BaseException] = []

        def go(s):
            try:
                s.write(_batch(4))
            except BaseException as e:  # noqa: BLE001 - collected for assertion
                errors.append(e)

        threads = [threading.Thread(target=go, args=(s,)) for s in sinks]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for s in sinks:
            s.close()
        assert not errors, f"concurrent first-writes failed: {errors!r}"
        assert _record_count(client, cfg) == 8


class TestAppendVisibility:
    def test_rows_queryable_with_stats(self, hog_stack, client):
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(5))
            sink.write(_batch(7))
        finally:
            sink.close()
        table = _table(client, cfg)
        files = table.files()
        assert sorted(f.record_count for f in files) == [5, 7]
        # Stats shipped with the commit (never deferred): the server
        # marks them provided/loaded, not pending.
        assert all(f.stats_state != "pending" for f in files)
        # Scan planning sees the same files.
        scan = table.scan_plan()
        assert sorted(s.data_file.record_count for s in scan) == [5, 7]

    def test_parquet_contents_round_trip(self, hog_stack, client):
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(3, teams=(42,)))
        finally:
            sink.close()
        files = _table(client, cfg).files()
        assert len(files) == 1
        data = _read_parquet(files[0].path)
        assert data.num_rows == 3
        assert data.column("team_id").to_pylist() == [42, 42, 42]
        assert data.column("properties").to_pylist() == ['{"k": 1}'] * 3
        assert data.column("_inserted_at").null_count == 0


class TestSchemaEvolution:
    def test_new_column_added_and_old_column_null_filled(self, hog_stack, client):
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(2))
            # New upstream column appears; `event` disappears upstream.
            batch2 = pa.table(
                {
                    "uuid": ["x", "y"],
                    "team_id": [7, 7],
                    "properties": ["{}", "{}"],
                    "new_col": ["a", "b"],
                }
            )
            sink.write(batch2)
        finally:
            sink.close()
        table = _table(client, cfg)
        types = {c.name: c.type for c in table.columns}
        assert types["new_col"] == "string"
        files = sorted(table.files(), key=lambda f: f.data_file_id)
        assert [f.record_count for f in files] == [2, 2]
        second = _read_parquet(files[1].path)
        assert second.column("new_col").to_pylist() == ["a", "b"]
        assert second.column("event").null_count == 2  # null-filled

    def test_concurrent_writers_evolving_and_appending(self, hog_stack, client):
        cfg = _fresh()
        settle = HoglakeSink(_fresh(hoglake_table=cfg.hoglake_table))
        settle.write(_batch(1))  # settle creation before the race
        settle.close()

        errors: list[BaseException] = []

        def writer(idx: int):
            s = HoglakeSink(_fresh(hoglake_table=cfg.hoglake_table, ordinal=idx))
            try:
                for round_ in range(3):
                    extra = {f"col_w{idx}_{round_}": [f"v{idx}"] * 2}
                    s.write(_batch(2, teams=(idx,), extra_cols=extra))
            except BaseException as e:  # noqa: BLE001 - collected for assertion
                errors.append(e)
            finally:
                s.close()

        threads = [threading.Thread(target=writer, args=(i,)) for i in (1, 2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, f"concurrent evolve/append failed: {errors!r}"
        assert _record_count(client, cfg) == 1 + 2 * 3 * 2
        names = {c.name for c in _table(client, cfg).columns}
        for idx in (1, 2):
            for round_ in range(3):
                assert f"col_w{idx}_{round_}" in names


class TestRestartAndReset:
    def test_reset_caches_reresolves(self, hog_stack, client):
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(2))
            sink.reset_caches()
            sink.write(_batch(3))
        finally:
            sink.close()
        assert _record_count(client, cfg) == 5

    def test_new_sink_instance_resumes_existing_table(self, hog_stack, client):
        cfg = _fresh()
        s1 = HoglakeSink(cfg)
        try:
            s1.write(_batch(2))
        finally:
            s1.close()
        s2 = HoglakeSink(_fresh(hoglake_table=cfg.hoglake_table))
        try:
            s2.write(_batch(4))
        finally:
            s2.close()
        assert _record_count(client, cfg) == 6

    def test_external_drop_recreate_recovers_after_reset(self, hog_stack, client):
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(2))
            _table(client, cfg).drop()
            # The cached handle points at a dead incarnation: either the
            # client's pre-flight re-resolve or the server's
            # expected_table_uuid guard refuses it. Both are retryable —
            # reset_caches adopts the live table on the next attempt.
            with pytest.raises((IncarnationChangedError, NotFoundError)) as caught:
                sink.write(_batch(2))
            assert is_retryable(caught.value) is True
            sink.reset_caches()
            assert sink.write(_batch(3)) == 3
        finally:
            sink.close()
        assert _record_count(client, cfg) == 3

    def test_external_drop_recreate_is_refused_not_published(self, hog_stack, client):
        # The RECREATE case, which the drop-only test above cannot reach.
        # Once the name resolves again, every check that was supposed to
        # catch this passed: `_prepare`'s own `table.info()` adopts the
        # new incarnation into the pyhoglake handle, and a defaulted
        # `expected_table_uuid` is then read off that same refreshed
        # value — so the client pre-flight, the server's guard and
        # `_check_destination_still_ours` all compared fresh against
        # fresh. The commit landed on a table this sink had never
        # reconciled, and the only thing still naming the dead one was
        # the idempotency key.
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            assert sink.write(_batch(2), kafka_offsets=(("events", 0, 0, 1),)) == 2
            dead = _table(client, cfg).table_uuid
            _table(client, cfg).drop()
            ns = client.catalog(cfg.hoglake_catalog).namespace(cfg.hoglake_namespace)
            reborn = ns.create_table(cfg.hoglake_table, table_schema_for_batch(_batch(1).schema))
            assert reborn.table_uuid != dead

            with pytest.raises(IncarnationChangedError) as caught:
                sink.write(_batch(2), kafka_offsets=(("events", 0, 2, 3),))
            assert is_retryable(caught.value) is True
            assert _record_count(client, cfg) == 0, "the flush published across incarnations"

            # Retryable, so main.py resets and re-resolves — which is
            # also what puts the recreated table through _reconcile_specs.
            sink.reset_caches()
            assert sink.write(_batch(3), kafka_offsets=(("events", 0, 2, 4),)) == 3
        finally:
            sink.close()
        assert _record_count(client, cfg) == 3


class TestLostCommitResponse:
    """THE duplicate-publication hazard, against the real server.

    A commit the server APPLIED whose response never reaches the client
    is indistinguishable, at the client, from a commit that never
    happened: both surface as a timeout. millpond retries; the retry
    mints fresh parquet paths (uuid4) and commits again; hoglake permits
    a path to be registered twice (there is no unique index on it, by
    design); the rows publish a second time and the offsets advance over
    both. No log, no metric, no way to know afterwards.

    The fix is the commit's idempotency key, derived from the Kafka
    offset range being flushed, plus a prepared payload the sink holds
    across retries — so the retry is byte-identically the SAME request
    and the server answers it from its receipt without writing.
    """

    def _drop_next_commit_response(self, sink):
        """Let the commit reach the server and apply, then destroy the
        response on its way back — exactly what a connection reset or a
        gateway timeout does."""
        http = sink._client._http
        real = http.request
        state = {"dropped": False}

        def request(method, url, **kwargs):
            response = real(method, url, **kwargs)
            if not state["dropped"] and method == "POST" and "/commit" in str(url):
                state["dropped"] = True
                raise httpx.ReadTimeout("response lost in transit", request=response.request)
            return response

        http.request = request
        return state

    def _flush_once(self, sink, cfg, batch, offsets):
        kafka = MagicMock()
        _flush(sink, cfg, kafka, batch, batch.nbytes, batch.num_rows, offsets, 1.0)
        return kafka

    def test_lost_response_publishes_the_rows_exactly_once(self, hog_stack, client):
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        offsets = {("events", 0): (36, 41)}
        batch = _batch(6, teams=(1, 2))
        try:
            sink.write(_batch(1))  # bootstrap while healthy
            state = self._drop_next_commit_response(sink)
            kafka = self._flush_once(sink, cfg, batch, offsets)
        finally:
            sink.close()
        assert state["dropped"], "the harness never dropped a commit response"
        # The flush as a whole SUCCEEDED (the retry resolved it), so the
        # offsets committed — which is the dangerous half: had the retry
        # published a second copy, the offsets would have advanced over
        # duplicated rows with nothing to show for it.
        kafka.commit.assert_called_once()
        assert _record_count(client, cfg) == 1 + 6

    def test_lost_response_on_a_partitioned_table_too(self, hog_stack, client):
        # The fanout path registers one file per partition tuple in one
        # commit, so a replay has to reproduce the whole file SET.
        cfg = _fresh(hoglake_partition_by=(("team_id", "identity", None),))
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(2, teams=(1,)))
            state = self._drop_next_commit_response(sink)
            self._flush_once(sink, cfg, _batch(9, teams=(1, 2, 3)), {("events", 0): (69, 77)})
        finally:
            sink.close()
        assert state["dropped"]
        assert _record_count(client, cfg) == 2 + 9
        # And exactly one file per tuple from the retried flush — not two.
        files = _table(client, cfg).files()
        assert len(files) == 1 + 3

    def test_replay_does_not_re_upload(self, hog_stack, client):
        # A retry must replay the identical registration, not build a new
        # one: re-uploading orphans the first set, and a payload that
        # differs by so much as a file name is a 422 against the receipt.
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(1))
            self._drop_next_commit_response(sink)
            self._flush_once(sink, cfg, _batch(4), {("events", 0): (9, 12)})
            registered = {f.path for f in _table(client, cfg).files()}
        finally:
            sink.close()
        objects = {o for o in _list_objects(DATA_PATH, cfg) if o.endswith(".parquet")}
        # Guard the subset assertion: an empty listing (a wrong prefix, a
        # renamed layout) satisfies `<=` against anything and proves
        # nothing.
        assert len(objects) == len(registered) == 2  # the bootstrap write plus the retried flush
        # Every object under the table's data path is a registered file:
        # no orphan from a second upload.
        assert objects <= {p.removeprefix("s3://") for p in registered}


class TestNewProcessReplay:
    """What the key does and does not buy ACROSS a process boundary.

    In-process the guarantee is exact: the payload is held, the retry is
    byte-identical, the receipt resolves it. Across a restart neither of
    those holds — the rebuilt flush stamps a fresh `_inserted_at` and
    uploads under fresh uuid4 names — so all the key can do is recognize
    a repeated BOUNDARY and refuse to publish twice over it.

    And the boundary is not reproducible. It is whatever the size and
    time triggers happened to cut at: a size trigger accumulates per poll
    batch, the time trigger is wall-clock, the filter's allowlist is
    mutable, and every partition in the flush has to coincide. So the
    pipeline is at-least-once across process boundaries, with the key
    suppressing the duplicate in the case where the boundary does repeat.

    Both halves are pinned here, because a claim that only holds in the
    lucky case is worse than no claim: it is the one an operator reasons
    with at 3am.
    """

    def _flush_as_new_process(self, cfg, batch, offsets):
        """A fresh sink over the same table — the restart, minus the
        process boundary. Nothing survives but the catalog."""
        sink = HoglakeSink(_fresh(hoglake_table=cfg.hoglake_table))
        try:
            kafka = MagicMock()
            _flush(sink, cfg, kafka, batch, batch.nbytes, batch.num_rows, offsets, 1.0)
            return kafka
        finally:
            sink.close()

    def test_the_same_boundary_from_a_new_process_publishes_once(self, hog_stack, client):
        # The pod died after the commit applied and before the offsets
        # committed; Kafka replayed the identical range and the flush was
        # rebuilt from scratch.
        cfg = _fresh()
        batch = _batch(5)
        offsets = {("events", 0): (12, 16)}
        self._flush_as_new_process(cfg, batch, offsets)
        assert _record_count(client, cfg) == 5
        kafka = self._flush_as_new_process(cfg, batch, offsets)
        assert _record_count(client, cfg) == 5, "the replayed boundary published a second copy"
        # The offsets still commit: the rows ARE in the lake, just not
        # because of this process.
        kafka.commit.assert_called_once()

    def test_the_reused_key_branch_reports_zero_rows_written(self, hog_stack, client):
        """The reused-key branch, taken against the real server rather
        than a fabricated exception.

        This is the branch a unit fixture got backwards: hoglake answers
        `{error: "validation", detail: "idempotency_key reused with a
        different request"}`, so the sentence is in `detail` and the
        message is the bare code. It is exercised here end to end, and
        the thing asserted is the number that used to be wrong — a
        writer reporting its whole batch for a range it did not publish
        is how 8 reported rows became 3 in the lake.
        """
        cfg = _fresh()
        batch = _batch(5)
        offsets = {("events", 0): (12, 16)}
        self._flush_as_new_process(cfg, batch, offsets)
        assert _record_count(client, cfg) == 5

        sink = HoglakeSink(_fresh(hoglake_table=cfg.hoglake_table))
        try:
            written = sink.write(batch, kafka_offsets=(("events", 0, 12, 16),))
        finally:
            sink.close()
        assert written == 0, "reported rows this process did not publish"
        assert _record_count(client, cfg) == 5

    def test_a_shifted_boundary_from_a_new_process_duplicates(self, hog_stack, client):
        # The honest half. The restart re-consumed the same records but
        # its flush cut at a different offset, so this is a different
        # publication by every name anyone has — and the rows land twice.
        # AT-LEAST-ONCE, which is what the docs now say.
        cfg = _fresh()
        batch = _batch(5)
        self._flush_as_new_process(cfg, batch, {("events", 0): (12, 16)})
        assert _record_count(client, cfg) == 5
        self._flush_as_new_process(cfg, batch, {("events", 0): (12, 19)})
        assert _record_count(client, cfg) == 10

    def test_a_recreated_table_does_not_answer_from_the_old_receipt(self, hog_stack, client):
        # Receipts are per catalog and survive a table drop with no
        # cascade. Without the incarnation in the key, this sequence
        # reported the second flush as already published and advanced
        # over rows that were in a dropped table: 5 rows written, 0 in
        # the lake, offsets committed.
        cfg = _fresh()
        batch = _batch(5)
        offsets = {("events", 0): (12, 16)}
        self._flush_as_new_process(cfg, batch, offsets)
        assert _record_count(client, cfg) == 5
        _table(client, cfg).drop()
        self._flush_as_new_process(cfg, batch, offsets)
        assert _record_count(client, cfg) == 5, "the recreated table answered from its predecessor's receipt"


class TestSpecChangeUnderAPreparedPayload:
    """A registered file carries the partition VALUES the client computed
    and is stamped with whatever spec_id the table has when the commit
    lands. The server never opens the file, so if the spec changes to
    another of the SAME ARITY between prepare and commit, the file is
    registered as (say) bucketed while carrying identity values — and
    every scan prunes it wrongly, forever, with nothing anywhere saying
    so."""

    def test_a_same_arity_respec_mid_flight_refuses_the_commit(self, hog_stack, client, monkeypatch):
        from pyhoglake import ops

        from millpond.hoglake import HoglakeSink as Sink

        cfg = _fresh(hoglake_partition_by=(("team_id", "identity", None),))
        sink = Sink(cfg)
        original = Sink._prepare

        def prepare_then_respec(self, table, batch, key):
            payload = original(self, table, batch, key)
            # Another operator re-specs the table while the upload is in
            # flight. Same arity, so the server's commit-side validation
            # (which checks arity and nothing else) would accept it.
            live = _table(client, cfg)
            fid = {c.name: c.field_id for c in live.info().columns}["team_id"]
            live.alter([ops.set_partition_spec([ops.partition_field(fid, "bucket", 8)])])
            return payload

        try:
            sink.write(_batch(3, teams=(1,)))
            assert _record_count(client, cfg) == 3
            monkeypatch.setattr(Sink, "_prepare", prepare_then_respec)
            with pytest.raises(RuntimeError, match="partition spec") as caught:
                sink.write(_batch(3, teams=(2,)))
            # Retryable: a REBUILT flush computes its values under the new
            # spec and publishes cleanly, unlike the sink's other stops.
            assert is_retryable(caught.value) is True
        finally:
            monkeypatch.undo()
            sink.close()
        # Nothing was registered under the wrong spec.
        assert _record_count(client, cfg) == 3
        # And the rebuild, under the live spec, lands.
        sink2 = HoglakeSink(_fresh(hoglake_table=cfg.hoglake_table, hoglake_partition_by=(("team_id", "bucket", 8),)))
        try:
            assert sink2.write(_batch(3, teams=(2,))) == 3
        finally:
            sink2.close()
        assert _record_count(client, cfg) == 6


class _FailNthUpload:
    """The real S3 filesystem, with the Nth `open_output_stream` refused.

    Wrapping the filesystem rather than mocking pyhoglake is the point:
    every upload before the Nth is a real object really in MinIO, so the
    count pyhoglake stamps can be checked against the bucket instead of
    against another fake."""

    def __init__(self, real, fail_on: int):
        self._real = real
        self._fail_on = fail_on
        self.calls = 0

    def open_output_stream(self, path, *a, **kw):
        self.calls += 1
        if self.calls == self._fail_on:
            raise OSError(f"injected object-store failure on upload {self.calls}")
        return self._real.open_output_stream(path, *a, **kw)

    def __getattr__(self, name):
        return getattr(self._real, name)


class TestPrepareOrphanAccounting:
    """What a failed `prepare_append_files` leaves behind, measured
    against the bucket rather than deduced.

    millpond used to reason about where in pyhoglake's upload loop a
    given failure could fire, and book a number from that reasoning. It
    was wrong twice. pyhoglake >=1.1.1 reports the truth on the
    exception itself; these tests pin that contract against the real
    library, the real server and real objects, because a unit test that
    stamps the attributes itself can only prove millpond reads what a
    fake wrote."""

    def test_a_mid_fanout_failure_counts_and_names_the_objects_that_landed(self, hog_stack, client, caplog):
        cfg = _fresh(hoglake_partition_by=(("team_id", "identity", None),))
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(3, teams=(1,)))  # bootstrap, so the table exists
            before = {o for o in _list_objects(DATA_PATH, cfg) if o.endswith(".parquet")}
            real = sink._client._filesystem()
            sink._client._fs = _FailNthUpload(real, fail_on=3)
            with caplog.at_level(logging.WARNING, logger="millpond.hoglake"), pytest.raises(OSError) as caught:
                sink.write(_batch(9, teams=(1, 2, 3)))
        finally:
            sink._client._fs = real
            sink.close()

        # pyhoglake's own accounting: two uploads closed cleanly, the
        # third raised and is in neither number.
        assert caught.value.uploaded_files == 2
        assert len(caught.value.uploaded_uris) == 2

        # ...and those two really are in the bucket. This is the whole
        # claim: the count is provable, not inferred.
        after = {o for o in _list_objects(DATA_PATH, cfg) if o.endswith(".parquet")}
        landed = after - before
        assert {u.removeprefix("s3://") for u in caught.value.uploaded_uris} <= landed

        # Nothing was registered, so every one of them is an orphan.
        assert _record_count(client, cfg) == 3
        assert "Orphaned 2 uploaded parquet file(s)" in caplog.text
        for uri in caught.value.uploaded_uris:
            assert uri in caplog.text
        # The retracted advice: the shared prefix is not the sweep unit.
        assert "never the prefix" in caplog.text
        assert "truncated" in caplog.text

    def test_a_pre_upload_refusal_really_does_carry_zero(self, hog_stack, client):
        # The other half of the contract, and the one millpond used to
        # assert from first principles: a refusal raised before the first
        # upload reports 0 / (). Driven through a real drop+recreate
        # under the sink's cached handle, so the real client pre-flight
        # raises it.
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(2))
            before = {o for o in _list_objects(DATA_PATH, cfg) if o.endswith(".parquet")}
            ns = client.catalog(cfg.hoglake_catalog).namespace(cfg.hoglake_namespace)
            ns.table(cfg.hoglake_table).drop()
            ns.create_table(cfg.hoglake_table, table_schema_for_batch(_batch(1).schema))
            with pytest.raises(IncarnationChangedError) as caught:
                sink.write(_batch(2))
        finally:
            sink.close()
        assert getattr(caught.value, "uploaded_files", None) == 0
        assert getattr(caught.value, "uploaded_uris", None) == ()
        # And the bucket agrees — no object was written for the refusal.
        after = {o for o in _list_objects(DATA_PATH, cfg) if o.endswith(".parquet")}
        assert after == before


class TestAtLeastOnce:
    def test_server_outage_no_offset_advance_then_clean_retry(self, hog_stack, client):
        """The at-least-once sequencing against a real outage: stop the
        server, drive main._flush (real sink, mock kafka) — the write
        raises through the retry budget and offsets are never committed;
        restart the server, flush again — rows land exactly once and the
        offset commit fires."""
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        kafka = MagicMock()
        offsets = {("topic", 0): (7, 10)}
        batch = _batch(4)
        try:
            sink.write(_batch(1))  # bootstrap while healthy

            stack.compose("stop", "hoglake-server")
            try:
                # Connection refused, through the full retry budget: a
                # transport failure, never a verdict on the request.
                with pytest.raises(httpx.HTTPError):
                    _flush(sink, cfg, kafka, batch, batch.nbytes, 4, offsets, 1.0)
                kafka.commit.assert_not_called()
            finally:
                stack.compose("start", "hoglake-server")
                stack.wait_http_ok(f"{stack.SERVER_URL}/healthz")

            # Clean retry after recovery: same batch, offsets commit once.
            sink.reset_caches()
            _flush(sink, cfg, kafka, batch, batch.nbytes, 4, offsets, 1.0)
            kafka.commit.assert_called_once()
            committed = kafka.commit.call_args.kwargs["offsets"]
            assert committed[0].offset == 11  # +1 next-to-fetch
        finally:
            sink.close()
        assert _record_count(client, cfg) == 5  # 1 bootstrap + 4, no duplicates

    def test_write_survives_transient_500s_via_retry_path(self, hog_stack, client):
        # Sanity: a healthy server + the retry helper writes exactly once.
        from millpond.main import _write_with_retry

        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            written = _write_with_retry(sink, _batch(2))
        finally:
            sink.close()
        assert written == 2
        assert _record_count(client, cfg) == 2


class TestColumnHygieneLive:
    def test_hog_prefixed_and_unsafe_columns_dropped_not_fatal(self, hog_stack, client):
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            n = sink.write(
                pa.table(
                    {
                        "uuid": ["a"],
                        "_hog_row_id": [9],
                        "bad-name": ["x"],
                        "team_id": [1],
                    }
                )
            )
        finally:
            sink.close()
        assert n == 1
        names = {c.name for c in _table(client, cfg).columns}
        assert "_hog_row_id" not in names
        assert "bad-name" not in names
        assert _record_count(client, cfg) == 1


class TestUuidColumnLive:
    """`MILLPOND_TYPED_COLUMNS=uuid:uuid` end to end against the real server.

    The coercer turns the ClickHouse-events UUID strings into `pa.uuid()`;
    this pins that the server types the column `uuid` (not `string`, not
    `binary`) and that the parquet the sink uploads carries the UUID logical
    annotation an Iceberg reader binds the column through.
    """

    CANONICAL = "018f3c7e-6b2a-7c3d-9e4f-5a6b7c8d9e0f"
    OTHER = "5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d"

    def _pinned_batch(self) -> pa.Table:
        from millpond.arrow_converter import coerce_typed_columns

        raw = pa.table(
            {
                "uuid": pa.array([self.CANONICAL, self.OTHER, None], pa.string()),
                "person_id": pa.array([None, None, None], pa.string()),
                "event": ["pageview"] * 3,
                "team_id": [1, 1, 1],
            }
        )
        batch = coerce_typed_columns(raw, (("uuid", "uuid"), ("person_id", "uuid")))
        assert batch.schema.field("uuid").type == pa.uuid()
        # An all-null pinned column must still arrive typed, or the table is
        # created with a string `person_id` that no later flush can narrow.
        assert batch.schema.field("person_id").type == pa.uuid()
        return batch

    def test_server_types_the_column_uuid(self, hog_stack, client):
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            assert sink.write(self._pinned_batch()) == 3
        finally:
            sink.close()
        types = {c.name: c.type for c in _table(client, cfg).columns}
        assert types["uuid"] == "uuid"
        assert types["person_id"] == "uuid"
        assert types["event"] == "string"
        assert _record_count(client, cfg) == 3

    def test_uploaded_parquet_is_16_raw_bytes_without_the_uuid_annotation(self, hog_stack, client):
        """The bytes are right; the parquet `LogicalTypeAnnotation.uuidType()`
        is absent, and pyhoglake is what forbids it.

        `_prepare` casts the batch to `columns_to_arrow_schema(info.columns)`,
        and pyhoglake answers a `uuid` column with plain `pa.binary(16)`
        (`types.py` `coltype_to_arrow`), for which pyarrow stamps no logical
        type. Casting to `pa.uuid()` instead DOES stamp it — and then
        `prepare_append_files` refuses the file outright:
        `parquet.schema_arrow.equals(columns_to_arrow_schema(...))` is an exact
        compare, so the extension-typed column comes back as "prepared Parquet
        schema/field IDs differ from destination" (observed against this
        server). So the annotation needs pyhoglake to move both sides; this
        test is the canary for that landing.
        """
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(self._pinned_batch())
        finally:
            sink.close()
        paths = _list_objects(DATA_PATH, cfg)
        assert len(paths) == 1
        s3 = _minio()
        with s3.open_input_file(paths[0]) as f:
            pf = pq.ParquetFile(f)
            by_name = {pf.schema.column(i).name: pf.schema.column(i) for i in range(pf.metadata.num_columns)}
            for name in ("uuid", "person_id"):
                assert by_name[name].physical_type == "FIXED_LEN_BYTE_ARRAY"
                assert by_name[name].length == 16
                assert by_name[name].logical_type.type == "NONE"
            table = pf.read()
        assert table.column("uuid").to_pylist() == [
            uuid.UUID(self.CANONICAL).bytes,
            uuid.UUID(self.OTHER).bytes,
            None,
        ]

    def test_second_flush_appends_without_schema_drift(self, hog_stack, client):
        # The coerced type must reconcile against the live `uuid` column, not
        # try to add or widen it every flush.
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(self._pinned_batch())
            sink.write(self._pinned_batch())
        finally:
            sink.close()
        types = {c.name: c.type for c in _table(client, cfg).columns}
        assert types["uuid"] == "uuid"
        assert _record_count(client, cfg) == 6


class TestUuidPinOnAStringColumnLive:
    """B1 against the real server: the pin arriving at a table whose column is
    already `string` — the state every existing table is in, `events_raw` in
    dev included, because the column was created from unpinned batches.

    Before the degradation arm this was a permanent wedge, not a degraded
    flush: `cast(pa.uuid() -> string)` reinterprets the 16 bytes as UTF-8 and
    raises `ArrowInvalid: Invalid UTF8 payload` on every attempt, so the
    offsets never commit and the restart re-consumes the same batch forever.
    """

    CANONICAL = "018f3c7e-6b2a-7c3d-9e4f-5a6b7c8d9e0f"
    OTHER = "5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d"

    def _pinned(self, values):
        from millpond.arrow_converter import coerce_typed_columns

        batch = coerce_typed_columns(
            pa.table(
                {
                    "uuid": pa.array(values, pa.string()),
                    "event": ["pageview"] * len(values),
                    "team_id": [1] * len(values),
                }
            ),
            (("uuid", "uuid"),),
        )
        assert batch.schema.field("uuid").type == pa.uuid()
        return batch

    def test_pin_on_an_existing_string_column_degrades_to_text(self, hog_stack, client):
        cfg = _fresh()

        # 1. Unpinned flush creates the table with `uuid` as a string column —
        #    exactly how every table that predates the pin was created.
        sink = HoglakeSink(cfg)
        try:
            assert sink.write(_batch(2)) == 2
        finally:
            sink.close()
        assert {c.name: c.type for c in _table(client, cfg).columns}["uuid"] == "string"

        # 2. Now the operator adds `uuid:uuid`. The flush must land, not wedge.
        sink = HoglakeSink(cfg)
        try:
            assert sink.write(self._pinned([self.CANONICAL, None, self.OTHER])) == 3
        finally:
            sink.close()

        # The column is still `string` — hoglake has no string->uuid promotion,
        # so no DDL was attempted — and the rows are canonical UUID text.
        assert {c.name: c.type for c in _table(client, cfg).columns}["uuid"] == "string"
        assert _record_count(client, cfg) == 5
        written = sorted(
            v for path in _list_objects(DATA_PATH, cfg) for v in _read_parquet(path).column("uuid").to_pylist() if v
        )
        assert self.CANONICAL in written
        assert self.OTHER in written

    def test_repeated_flushes_keep_landing(self, hog_stack, client):
        # The wedge was permanent, so "it worked once" is not the property
        # under test — every subsequent flush has to land too.
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(_batch(1))
            sink.write(self._pinned([self.CANONICAL]))
            sink.write(self._pinned([self.OTHER]))
        finally:
            sink.close()
        assert _record_count(client, cfg) == 3


class TestUuidPinRollbackLive:
    """B1's mirror against the real server: a live `uuid` column and an
    UNPINNED string batch.

    The rollback direction, and the mixed-fleet one — a pod that has not taken
    the new config writes to a table another pod created with the pin. Before
    the rewrite arm this raised `ArrowInvalid: Failed casting from string to
    fixed_size_binary[16]: widths must match`, which the retry path correctly
    calls permanent, so the flush died on attempt 1 with offsets uncommitted.
    """

    CANONICAL = "018f3c7e-6b2a-7c3d-9e4f-5a6b7c8d9e0f"
    OTHER = "5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d"

    def _pinned(self, values):
        from millpond.arrow_converter import coerce_typed_columns

        return coerce_typed_columns(
            pa.table(
                {
                    "uuid": pa.array(values, pa.string()),
                    "event": ["pageview"] * len(values),
                    "team_id": [1] * len(values),
                }
            ),
            (("uuid", "uuid"),),
        )

    def _unpinned(self, values):
        return pa.table(
            {
                "uuid": pa.array(values, pa.string()),
                "event": ["pageview"] * len(values),
                "team_id": [1] * len(values),
            }
        )

    def test_unpinned_batch_lands_on_a_uuid_column(self, hog_stack, client):
        cfg = _fresh()

        # 1. A pinned flush creates the table with a real `uuid` column.
        sink = HoglakeSink(cfg)
        try:
            assert sink.write(self._pinned([self.CANONICAL])) == 1
        finally:
            sink.close()
        assert {c.name: c.type for c in _table(client, cfg).columns}["uuid"] == "uuid"

        # 2. The pin comes back off (or a stale pod flushes). Must land, and
        #    keep landing — the wedge was permanent, not a one-off.
        sink = HoglakeSink(cfg)
        try:
            assert sink.write(self._unpinned([self.OTHER, None])) == 2
            assert sink.write(self._unpinned([self.CANONICAL])) == 1
        finally:
            sink.close()

        assert {c.name: c.type for c in _table(client, cfg).columns}["uuid"] == "uuid"
        assert _record_count(client, cfg) == 4
        written = sorted(
            v for path in _list_objects(DATA_PATH, cfg) for v in _read_parquet(path).column("uuid").to_pylist() if v
        )
        assert written == sorted(
            [
                uuid.UUID(self.CANONICAL).bytes,
                uuid.UUID(self.CANONICAL).bytes,
                uuid.UUID(self.OTHER).bytes,
            ]
        )

    def test_non_uuid_text_is_nulled_not_fatal(self, hog_stack, client):
        cfg = _fresh()
        sink = HoglakeSink(cfg)
        try:
            sink.write(self._pinned([self.CANONICAL]))
            assert sink.write(self._unpinned([self.OTHER, "not-a-uuid"])) == 2
        finally:
            sink.close()
        assert _record_count(client, cfg) == 3
        values = [v for path in _list_objects(DATA_PATH, cfg) for v in _read_parquet(path).column("uuid").to_pylist()]
        assert None in values
        assert uuid.UUID(self.OTHER).bytes in values
