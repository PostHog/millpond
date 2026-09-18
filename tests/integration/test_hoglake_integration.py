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

from millpond.hoglake import HoglakeSink, is_retryable
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
