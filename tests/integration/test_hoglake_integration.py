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

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import fs as pafs

from millpond.hoglake import HoglakeSink
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
    sort_by: tuple[str, ...] | None = None
    ordinal: int = 0
    table_label: str = field(default="events")


@pytest.fixture(scope="session")
def hog_stack():
    if not stack.docker_available():
        pytest.skip("Docker is not available")
    if not stack.ensure_image():
        pytest.skip(f"hoglake server image unavailable: {stack.server_image()}")
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

    def test_missing_catalog_without_data_path_is_startup_shaped_error(self, hog_stack):
        cfg = _fresh(hoglake_catalog="millpond-it-absent", hoglake_data_path=None)
        sink = HoglakeSink(cfg)
        try:
            with pytest.raises(RuntimeError, match="HOGLAKE_DATA_PATH"):
                sink.write(_batch())
        finally:
            sink.close()

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
            with pytest.raises(Exception):  # noqa: B017 - incarnation/404, backend-typed
                sink.write(_batch(2))
            sink.reset_caches()
            assert sink.write(_batch(3)) == 3
        finally:
            sink.close()
        assert _record_count(client, cfg) == 3


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
        offsets = {("topic", 0): 10}
        batch = _batch(4)
        try:
            sink.write(_batch(1))  # bootstrap while healthy

            stack.compose("stop", "hoglake-server")
            try:
                with pytest.raises(Exception):
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
