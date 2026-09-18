"""E2E test: full pipeline from a local Kafka broker through main.py to a
real hoglake server.

Mirrors tests/e2e/test_e2e.py's harness shape with destination=hoglake:
the throwaway stack (compose project `millpond-hog-it`, e2e profile adds
a single-node KRaft Kafka; every port a high 127.0.0.1 bind) is booted,
events are produced to Kafka, and millpond's real entry point runs as a
host subprocess consuming them into the hoglake catalog. End-state row
counts must match the produced events exactly.

Run explicitly:
    uv run pytest tests/e2e/test_hoglake_e2e.py -v -s
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import uuid as uuidlib
from pathlib import Path

import httpx
import pytest

from tests.hoglake_stack import stack

pytestmark = pytest.mark.e2e

TOPIC = "hog-e2e-events"
PARTITIONS = 4
CATALOG = "millpond-e2e"
BUCKET = "millpond-e2e"
NAMESPACE = "e2e"
TABLE = "events"
HTTP_PORT = 28000
N_EVENTS = 600
TEAMS = (101, 202)

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def e2e_stack():
    if not stack.docker_available():
        pytest.skip("Docker is not available")
    if not stack.ensure_image():
        pytest.skip(f"hoglake server image unavailable: {stack.server_image()}")
    stack.up(profile="e2e")
    try:
        stack.make_bucket(BUCKET)
        _wait_for_kafka()
        yield stack
    finally:
        stack.down()


def _wait_for_kafka(timeout_s: float = 90.0) -> None:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": stack.KAFKA_BOOTSTRAP})
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            admin.list_topics(timeout=5)
            return
        except Exception:
            time.sleep(2)
    raise TimeoutError("Kafka broker not reachable")


def _create_topic() -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": stack.KAFKA_BOOTSTRAP})
    fut = admin.create_topics([NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)])[TOPIC]
    try:
        fut.result(timeout=30)
    except Exception as e:  # already exists on rerun within a session
        if "already exists" not in str(e).lower():
            raise


def _wait_for_group_coordinator(timeout_s: float = 60.0) -> None:
    """First offset commit on a fresh single-node broker races the
    __consumer_offsets creation; poll the coordinator until it answers
    (same reason the ducklake compose's kafka-init loops on
    kafka-consumer-groups.sh)."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": stack.KAFKA_BOOTSTRAP,
            "group.id": f"millpond-{TOPIC}-{TABLE}",
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


def _produce_events(n: int) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": stack.KAFKA_BOOTSTRAP})
    for i in range(n):
        record = {
            "uuid": str(uuidlib.uuid4()),
            "event": "pageview" if i % 2 == 0 else "$autocapture",
            "team_id": TEAMS[i % len(TEAMS)],
            "distinct_id": f"user-{i % 50}",
            "properties": json.dumps({"i": i, "browser": "firefox"}),
            "timestamp": "2026-09-17 12:00:00.123",
        }
        producer.produce(TOPIC, value=json.dumps(record).encode(), partition=i % PARTITIONS)
        if i % 100 == 0:
            producer.poll(0)
    remaining = producer.flush(30)
    assert remaining == 0, f"{remaining} events unflushed"


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
            "POD_NAME": "millpond-hog-e2e-0",
            "HOGLAKE_URL": stack.SERVER_URL,
            "HOGLAKE_CATALOG": CATALOG,
            "HOGLAKE_NAMESPACE": NAMESPACE,
            "HOGLAKE_TABLE": TABLE,
            "HOGLAKE_DATA_PATH": f"s3://{BUCKET}/lake/",
            "HOGLAKE_S3_ENDPOINT": stack.MINIO_URL,
            "HOGLAKE_S3_ACCESS_KEY": stack.S3_ACCESS_KEY,
            "HOGLAKE_S3_SECRET_KEY": stack.S3_SECRET_KEY,
            "HOGLAKE_PARTITION_BY": "team_id,month(_inserted_at)",
            "MILLPOND_SORT_BY": "team_id",
            "MILLPOND_HTTP_PORT": str(HTTP_PORT),
            "MILLPOND_LOG_FORMAT": "text",
            "FLUSH_SIZE": "262144",
            "FLUSH_INTERVAL_MS": "2000",
            "FETCH_MIN_BYTES": "1",
        }
    )
    return env


@pytest.fixture(scope="module")
def pipeline(e2e_stack, tmp_path_factory):
    """Produce events, run millpond's real entry point as a subprocess,
    wait for all rows to land, then SIGTERM it. Yields (client, log_path,
    exit_code_getter)."""
    from pyhoglake import HoglakeClient, S3Config

    _create_topic()
    _wait_for_group_coordinator()
    _produce_events(N_EVENTS)

    log_path = tmp_path_factory.mktemp("millpond") / "millpond.log"
    proc = subprocess.Popen(
        [sys.executable, "-m", "millpond.main"],
        cwd=str(REPO_ROOT),
        env=_millpond_env(),
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
    )

    client = HoglakeClient(
        stack.SERVER_URL,
        s3=S3Config(
            access_key=stack.S3_ACCESS_KEY,
            secret_key=stack.S3_SECRET_KEY,
            endpoint_override=stack.MINIO_URL,
        ),
    )

    health_during_run: int | None = None
    metrics_during_run: str = ""
    try:
        deadline = time.monotonic() + 150
        count = -1
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                pytest.fail(f"millpond exited early ({proc.returncode}); log:\n{log_path.read_text()[-4000:]}")
            count = _count(client)
            if health_during_run is None:
                try:
                    health_during_run = httpx.get(f"http://127.0.0.1:{HTTP_PORT}/healthz", timeout=2).status_code
                    metrics_during_run = httpx.get(f"http://127.0.0.1:{HTTP_PORT}/metrics", timeout=2).text
                except Exception:
                    health_during_run = None
            if count >= N_EVENTS:
                break
            time.sleep(2)

        # One more probe now that the pipeline is warm (the first may have
        # fired before the server thread was up).
        try:
            health_during_run = httpx.get(f"http://127.0.0.1:{HTTP_PORT}/healthz", timeout=2).status_code
            metrics_during_run = httpx.get(f"http://127.0.0.1:{HTTP_PORT}/metrics", timeout=2).text
        except Exception:
            pass

        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)

        yield client, log_path, proc, count, health_during_run, metrics_during_run
    finally:
        if proc.poll() is None:
            proc.kill()
        client.close()


def _count(client) -> int:
    from pyhoglake import NotFoundError

    try:
        table = client.catalog(CATALOG).namespace(NAMESPACE).table(TABLE)
    except NotFoundError:
        return 0
    return sum(f.record_count for f in table.files())


class TestHoglakeE2E:
    def test_all_produced_events_land_exactly_once(self, pipeline):
        client, log_path, proc, count, *_ = pipeline
        assert count == N_EVENTS, f"expected {N_EVENTS} rows, saw {count}; log:\n{log_path.read_text()[-4000:]}"
        # Steady final state (no in-flight flush after SIGTERM drained).
        assert _count(client) == N_EVENTS

    def test_clean_shutdown(self, pipeline):
        *_, proc, _count_, _h, _m = pipeline
        assert proc.returncode == 0

    def test_table_shape(self, pipeline):
        client, *_ = pipeline
        table = client.catalog(CATALOG).namespace(NAMESPACE).table(TABLE)
        types = {c.name: c.type for c in table.columns}
        assert types["uuid"] == "string"
        assert types["event"] == "string"
        assert types["team_id"] == "long"
        assert types["properties"] == "string"  # TEXT — no variant
        assert types["_inserted_at"] == "timestamptz"

    def test_partition_spec_applied_and_fanned_out(self, pipeline):
        client, *_ = pipeline
        table = client.catalog(CATALOG).namespace(NAMESPACE).table(TABLE)
        info = table.info()
        assert info.partition_spec is not None
        transforms = [f.transform for f in info.partition_spec.fields]
        assert transforms == ["identity", "month"]
        team_values = {f.partition_values[0] for f in table.files()}
        assert team_values == {str(t) for t in TEAMS}

    def test_sort_order_declared(self, pipeline):
        client, *_ = pipeline
        info = client.catalog(CATALOG).namespace(NAMESPACE).table(TABLE).info()
        assert info.sort_spec is not None
        assert [f.direction for f in info.sort_spec.fields] == ["asc"]

    def test_health_and_metrics_endpoints_served(self, pipeline):
        *_, health, metrics_text = pipeline
        assert health == 200
        assert "millpond_records_written_total" in metrics_text
        assert "millpond_hoglake_files_written_total" in metrics_text
