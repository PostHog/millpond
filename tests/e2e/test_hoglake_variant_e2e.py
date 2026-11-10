"""E2E: LZ4-framed events_json-shaped messages from a local Kafka broker,
through main.py, into a PRE-CREATED hoglake table whose seven JSON columns
are declared `variant` (D1, D2), then compacted by the server in the
declared layout.

The text path is the arrow_converter's as deployed: the producer sends
`properties` and friends as JSON strings inside the event (how
ClickHouse's events_json topic carries them), the converter keeps them as
string columns, and the sink retypes them to `pa.json_()` for pyhoglake to
encode. Nothing in millpond's config names a variant column.

Same throwaway stack as tests/e2e/test_hoglake_e2e.py (compose project
`millpond-hog-it`, e2e profile, high 127.0.0.1 ports), its own catalog,
bucket, topic and consumer group. Run explicitly:
    uv run pytest tests/e2e/test_hoglake_variant_e2e.py -v -s
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import time
import uuid as uuidlib
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import fs as pafs

from tests.e2e.test_hoglake_e2e import _metric_samples, _wait_for_kafka
from tests.hoglake_stack import stack
from tests.hoglake_stack.variant_table import PROPERTIES_SV, VARIANT_NAMES, precreate

pytestmark = pytest.mark.e2e

# Unique per run: a rerun against a stack a killed run left up must not
# consume the previous run's topic (or its committed offsets).
RUN = uuidlib.uuid4().hex[:8]
TOPIC = f"hog-e2e-whales-{RUN}"
PARTITIONS = 2
CATALOG = "millpond-e2e-variant"
BUCKET = "millpond-e2e-variant"
NAMESPACE = "e2e"
TABLE = f"events_whales_{RUN}"
HTTP_PORT = 28001
# Six size-triggered flushes' worth into one (team, month) group, so the
# server's default HOGLAKE_COMPACTION_MIN_INPUT_FILES=5 has a group to take;
# FLUSH_SIZE below is tuned to that.
N_EVENTS = 1200
TEAM = 7

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def e2e_stack():
    stack.ensure_available()
    stack.up(profile="e2e")
    try:
        running = stack.running_server_image()
        assert running == stack.server_image(), (
            f"the hoglake-server container is running {running!r}, not the pinned {stack.server_image()!r}"
        )
        logging.getLogger(__name__).warning("hoglake server image under test: %s", running)
        stack.make_bucket(BUCKET)
        _wait_for_kafka()
        yield stack
    finally:
        stack.down()


def _create_topic() -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": stack.KAFKA_BOOTSTRAP})
    fut = admin.create_topics([NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)])[TOPIC]
    try:
        fut.result(timeout=30)
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise


def _wait_for_group_coordinator(timeout_s: float = 60.0) -> None:
    """The first committed-offsets fetch on a fresh single-node broker races
    the creation of __consumer_offsets: millpond's startup recovery then
    gets NOT_COORDINATOR and the consumer sits with no partitions for the
    whole run. Poll the coordinator for THIS module's group (the default
    `millpond-hoglake-<topic>-<table>`) until it answers, as the other
    hoglake e2e does for its own."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": stack.KAFKA_BOOTSTRAP,
            "group.id": f"millpond-hoglake-{TOPIC}-{TABLE}",
            "enable.auto.commit": False,
        }
    )
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            try:
                consumer.committed([TopicPartition(TOPIC, 0)], timeout=5)
                return
            except Exception:
                time.sleep(2)
        raise TimeoutError("Kafka group coordinator not ready")
    finally:
        consumer.close()


def _lz4_frame(payload: bytes) -> bytes:
    """`payload` as one LZ4 frame, the way the WarpStream pipelines write
    message values; pyarrow's `lz4` codec is the frame format the
    converter's `_decode_value` recognises by its magic."""
    sink = pa.BufferOutputStream()
    with pa.CompressedOutputStream(sink, "lz4") as out:
        out.write(payload)
    framed = sink.getvalue().to_pybytes()
    assert framed[:4] == b"\x04\x22\x4d\x18", "not an LZ4 frame"
    return framed


def _event(i: int) -> dict:
    """One events_json-shaped record: the JSON columns are STRINGS holding
    JSON text. Declared keys, undeclared keys, a JSON null under a
    declared key, a declared int64 sent as text, SQL NULL (absent) and
    invalid text, so the encoder's every outcome is exercised."""
    k = i % 6
    if k == 4:
        properties = None
    elif k == 5:
        properties = "not json {"
    else:
        properties = json.dumps(
            {
                "$browser": ["Chrome", "Safari", "Firefox", "Edge"][k],
                "$current_url": f"https://ex.am/ple/{i}",
                "$lib": "web" if k else None,
                "custom": {"depth": [1, 2, {"x": k}]},
                "n": i,
            }
        )
    record = {
        "uuid": str(uuidlib.uuid5(uuidlib.NAMESPACE_URL, f"whale-{i}")),
        "event": "pageview" if i % 2 == 0 else "$autocapture",
        "team_id": TEAM,
        "distinct_id": f"user-{i % 50}",
        "timestamp": f"2026-09-17 12:{(i // 60) % 60:02d}:{i % 60:02d}.123",
        "person_properties": json.dumps({"email": f"p{i}@ex.am", "age": "thirty"}),
    }
    if properties is not None:
        record["properties"] = properties
    if i % 2 == 0:
        record["group0_properties"] = json.dumps({"name": "acme"})
    # A MIXED-TYPE variant-bound column from the very first poll: a bare
    # bool in some records, JSON text in others. The converter can only
    # make the bool JSON (`true`) if the sink already knows the column is
    # variant (`json_text_columns`, read-only resolve before the first
    # write); str()-ed it would be `True`, an invalid_json null.
    if i % 10 == 0:
        record["group1_properties"] = True
    elif i % 10 == 5:
        record["group1_properties"] = json.dumps({"name": "g1"})
    return record


def _produce_events(n: int) -> list[dict]:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": stack.KAFKA_BOOTSTRAP})
    records = []
    for i in range(n):
        record = _event(i)
        records.append(record)
        producer.produce(TOPIC, value=_lz4_frame(json.dumps(record).encode()), partition=i % PARTITIONS)
        if i % 100 == 0:
            producer.poll(0)
    remaining = producer.flush(30)
    assert remaining == 0, f"{remaining} events unflushed"
    return records


def _millpond_env() -> dict[str, str]:
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("MILLPOND_", "HOGLAKE_", "DUCKLAKE_", "KAFKA_", "FLUSH_", "DUCKDB_"))
    }
    env.update(
        {
            "MILLPOND_DESTINATION": "hoglake",
            "KAFKA_BOOTSTRAP_SERVERS": stack.KAFKA_BOOTSTRAP,
            "KAFKA_TOPIC": TOPIC,
            "REPLICA_COUNT": "1",
            "POD_NAME": "millpond-hog-e2e-variant-0",
            "HOGLAKE_URL": stack.SERVER_URL,
            "HOGLAKE_CATALOG": CATALOG,
            "HOGLAKE_NAMESPACE": NAMESPACE,
            "HOGLAKE_TABLE": TABLE,
            "HOGLAKE_S3_ENDPOINT": stack.MINIO_URL,
            "HOGLAKE_S3_ACCESS_KEY": stack.S3_ACCESS_KEY,
            "HOGLAKE_S3_SECRET_KEY": stack.S3_SECRET_KEY,
            # The live table's layout, which the sink reconciles against.
            "HOGLAKE_PARTITION_BY": "team_id,month(timestamp)",
            "MILLPOND_SORT_BY": "timestamp",
            "MILLPOND_TYPED_COLUMNS": "timestamp:timestamptz",
            # Shipped stats: the stack runs no hydrator and a `pending`
            # file is not a compaction input.
            "HOGLAKE_DEFERRED_STATS": "false",
            "MILLPOND_HOGLAKE_VARIANT_ON_INVALID": "null",
            "MILLPOND_HTTP_PORT": str(HTTP_PORT),
            "MILLPOND_LOG_FORMAT": "text",
            # Six flushes of 200 rows into the one (team, month) group: the
            # size gate is checked per poll batch, so the batch size bounds
            # the rows per flush from below and FLUSH_SIZE (well under 200
            # of these records) makes every batch a flush. A short interval
            # drains whatever a last poll left over.
            "CONSUME_BATCH_SIZE": "200",
            "FLUSH_SIZE": "16384",
            "FLUSH_INTERVAL_MS": "2000",
            "FETCH_MIN_BYTES": "1",
        }
    )
    return env


def _client():
    from pyhoglake import HoglakeClient, S3Config

    return HoglakeClient(
        stack.SERVER_URL,
        s3=S3Config(
            access_key=stack.S3_ACCESS_KEY,
            secret_key=stack.S3_SECRET_KEY,
            endpoint_override=stack.MINIO_URL,
        ),
    )


def _table(client):
    return client.catalog(CATALOG).namespace(NAMESPACE).table(TABLE)


def _count(client) -> int:
    return sum(f.record_count for f in _table(client).files())


def _read_parquet(path: str) -> pa.Table:
    s3 = pafs.S3FileSystem(
        access_key=stack.S3_ACCESS_KEY,
        secret_key=stack.S3_SECRET_KEY,
        endpoint_override=stack.MINIO_URL,
    )
    with s3.open_input_file(path.removeprefix("s3://")) as f:
        return pq.read_table(f)


def _labelled_samples(metrics_text: str) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    """{(sample name, sorted (label, value) pairs without the common
    pipeline/broker_source labels): value}, so a test can assert on ONE
    series' value rather than on the metric's name being in the text."""
    from prometheus_client.parser import text_string_to_metric_families

    out: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
    for family in text_string_to_metric_families(metrics_text):
        for sample in family.samples:
            labels = tuple(sorted((k, v) for k, v in sample.labels.items() if k not in ("pipeline", "broker_source")))
            out[sample.name, labels] = sample.value
    return out


def _server_metric(name: str) -> float:
    from prometheus_client.parser import text_string_to_metric_families

    text = httpx.get("http://127.0.0.1:29090/metrics", timeout=10).text
    return sum(
        sample.value
        for family in text_string_to_metric_families(text)
        for sample in family.samples
        if sample.name in (name, f"{name}_total")
    )


@pytest.fixture(scope="module")
def pipeline(e2e_stack, tmp_path_factory):
    """Pre-create the table, produce LZ4-framed events, run millpond's real
    entry point as a subprocess until every row has landed, SIGTERM it."""
    client = _client()
    table = precreate(client, CATALOG, NAMESPACE, TABLE, data_path=f"s3://{BUCKET}/lake/")
    column = {c.name: c for c in table.columns}
    _create_topic()
    _wait_for_group_coordinator()
    records = _produce_events(N_EVENTS)

    log_path = tmp_path_factory.mktemp("millpond") / "millpond.log"
    proc = subprocess.Popen(
        [sys.executable, "-m", "millpond.main"],
        cwd=str(REPO_ROOT),
        env=_millpond_env(),
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
    )
    metrics_text = ""
    try:
        deadline = time.monotonic() + 180
        count = -1
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                pytest.fail(f"millpond exited early ({proc.returncode}); log:\n{log_path.read_text()[-6000:]}")
            count = _count(client)
            if count >= N_EVENTS:
                break
            time.sleep(2)
        try:
            metrics_text = httpx.get(f"http://127.0.0.1:{HTTP_PORT}/metrics", timeout=5).text
        except Exception:
            pass
        # The files the pipeline wrote, read beside the metrics and before
        # the server compacts them: every event is one (team, month), so
        # files == flushes == encode observations.
        files_landed = len(_table(client).files())
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        yield client, table, column, records, log_path, proc, count, metrics_text, files_landed
    finally:
        if proc.poll() is None:
            proc.kill()
        client.close()


def _expected(records: list[dict], name: str) -> dict[str, object]:
    from pyhoglake import variant

    out: dict[str, object] = {}
    for record in records:
        text = record.get(name)
        if text is None:
            out[record["uuid"]] = None
            continue
        if not isinstance(text, str):
            # A bare value the producer sent: the converter's JSON text
            # of it decodes back to the value itself.
            out[record["uuid"]] = text
            continue
        try:
            value = json.loads(text)
        except ValueError:
            out[record["uuid"]] = None
            continue
        out[record["uuid"]] = variant.VARIANT_NULL if value is None else value
    return out


def _decoded(client, column) -> tuple[pa.Table, dict[str, dict[str, object]]]:
    from pyhoglake import variant

    decoded: dict[str, dict[str, object]] = {name: {} for name in VARIANT_NAMES}
    tables = []
    for f in _table(client).files():
        back = _read_parquet(f.path)
        tables.append(back)
        uuids = back.column("uuid").to_pylist()
        for name in VARIANT_NAMES:
            decoded[name].update(zip(uuids, variant.to_python(back.column(name), column[name]), strict=True))
    return pa.concat_tables(tables, promote_options="permissive"), decoded


class TestHoglakeVariantE2E:
    def test_every_event_lands_once_and_the_pod_exits_cleanly(self, pipeline):
        client, _table_, _column, _records, log_path, proc, count, _metrics, _files = pipeline
        assert count == N_EVENTS, f"expected {N_EVENTS} rows, saw {count}; log:\n{log_path.read_text()[-6000:]}"
        assert _count(client) == N_EVENTS
        assert proc.returncode == 0

    def test_the_table_is_still_the_setup_scripts(self, pipeline):
        client, *_, files_landed = pipeline
        info = _table(client).info()
        types = {c.name: c.type for c in info.columns}
        assert all(types[name] == "variant" for name in VARIANT_NAMES)
        assert types["timestamp"] == "timestamptz"
        assert {c.name: c.type_params for c in info.columns}["properties"] == {"shredding": PROPERTIES_SV}
        # The converter also produced `distinct_id`, which the table lacked:
        # ordinary evolution, as a string.
        assert types["distinct_id"] == "string"
        files = _table(client).files()
        assert {f.stats_state for f in files} == {"provided"}
        # `month(timestamp)` on the wire is the month ordinal since 1970-01: 2026-09 is 680.
        assert {tuple(f.partition_values) for f in files} == {(str(TEAM), "680")}
        # The count the fixture captured before compaction (which the
        # compaction test below changes), so this holds in any order.
        assert files_landed >= 5, "fewer files than compaction needs; lower FLUSH_SIZE"

    def test_variant_values_round_trip_and_declared_leaves_are_typed(self, pipeline):
        client, _table_, column, records, *_ = pipeline
        back, decoded = _decoded(client, column)
        for name in VARIANT_NAMES:
            assert decoded[name] == _expected(records, name), name
        # The first poll's bare bools in group1_properties landed as the
        # Variant true, not as `True` text nulled as invalid: the sink
        # knew the column was variant before the first conversion.
        bools = [r["uuid"] for r in records if r.get("group1_properties") is True]
        assert bools and all(decoded["group1_properties"][u] is True for u in bools)
        props = back.column("properties").combine_chunks()
        typed = props.field("typed_value")
        assert set(typed.type.names) == {f["name"] for f in PROPERTIES_SV["fields"]}
        with_browser = sum(1 for r in records if r.get("properties", "x").startswith("{"))
        assert typed.field("$browser").field("typed_value").null_count == N_EVENTS - with_browser
        assert with_browser > 0

    def test_the_variant_metrics_are_served(self, pipeline):
        *_, records, _log, _proc, _count, metrics_text, files_landed = pipeline
        samples = _metric_samples(metrics_text)
        labelled = _labelled_samples(metrics_text)
        assert samples["millpond_variant_rows_total"] == N_EVENTS * len(VARIANT_NAMES)
        invalid = sum(1 for i in range(N_EVENTS) if i % 6 == 5)
        assert samples["millpond_variant_invalid_total"] == invalid
        assert labelled["millpond_variant_invalid_total", (("reason", "invalid_json"),)] == invalid
        # One observation per prepare and no rebuilds, so exactly the
        # flushes that landed — which is the file count, every event being
        # one (team, month) group. Not N_EVENTS / CONSUME_BATCH_SIZE: the
        # size gate fires at FLUSH_SIZE (~30 records) on whatever a poll
        # returned, and a partial poll is a flush of its own.
        assert files_landed >= 5
        assert samples["millpond_variant_encode_seconds_count"] == files_landed
        # A SAMPLE with a value, per declared path — not the HELP line.
        with_browser = sum(1 for r in records if r.get("properties", "x").startswith("{"))
        typed = labelled[
            "millpond_variant_values_total",
            (("column", "properties"), ("outcome", "typed"), ("path", "$.$browser")),
        ]
        assert typed == with_browser > 0
        fallback = labelled[
            "millpond_variant_values_total",
            (("column", "person_properties"), ("outcome", "fallback"), ("path", "$.age")),
        ]
        assert fallback == N_EVENTS
        # The codec: native for the process, and nothing routed to Python
        # or redone there; every label set exported, at zero.
        assert labelled["millpond_variant_codec_info", (("backend", "native"),)] == 1
        for reason in ("unavailable", "plan", "not_utf8"):
            key = (("op", "encode"), ("reason", reason))
            assert labelled["millpond_variant_codec_routed_to_python_total", key] == 0, reason
        for reason in ("mismatch", "type", "panic", "shadow_mismatch"):
            assert labelled["millpond_variant_codec_fallback_total", (("reason", reason),)] == 0, reason

    def test_the_server_compacts_millponds_files_in_the_declared_layout(self, pipeline):
        client, _table_, column, *_ = pipeline
        before_files = {f.path for f in _table(client).files()}
        _before, decoded_before = _decoded(client, column)
        mismatch_before = _server_metric("hoglake_compaction_variant_layout_mismatch_total")
        catalog = client.catalog(CATALOG)
        compacted = 0
        for _ in range(5):
            result = catalog._client._request("POST", catalog._path("/maintenance/compact"), params={"batch": 10})
            compacted += result["groups_compacted"]
            if result["groups_compacted"] == 0:
                break
        assert compacted >= 1
        files = _table(client).files()
        assert len(files) < len(before_files)
        assert _server_metric("hoglake_compaction_variant_layout_mismatch_total") == mismatch_before
        _after, decoded_after = _decoded(client, column)
        assert decoded_after == decoded_before
        assert _count(client) == N_EVENTS
