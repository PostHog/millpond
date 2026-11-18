"""Arrow IPC producer for the arrow-payload experiment.

Generates PostHog-shaped events using the existing generators in
``test/producer.py``, packs them into Arrow record batches matching the
schema in ``experiments/arrow-payload/README.md``, and publishes each
batch as a single Kafka message whose value is a full Arrow IPC
*stream* (schema + record batch + EOS).

Why we reuse test/producer.py by import (not copy):
    The realistic PostHog event generators (pageview, autocapture, etc.)
    and the carefully tuned weighted mix live in test/producer.py. We
    do not want to maintain two copies. The Dockerfile puts ``test/`` on
    PYTHONPATH so ``import producer`` works. The producer module reads
    a few Kafka env vars at import time (KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC, KAFKA_PARTITION_COUNT) as module-level globals; those
    globals are only used by producer.main(), which we never call, but
    they still must be present at import time. We therefore set
    harmless defaults via os.environ.setdefault BEFORE importing.
"""

from __future__ import annotations

import io
import json
import logging
import os
import signal
import sys
import time

# --- Stub env vars required at import time by test/producer.py -----------
# These are only consulted by producer.main() (which we do not call). They
# must exist so that module-level ``os.environ[...]`` lookups do not raise.
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "unused-at-import-time")
os.environ.setdefault("KAFKA_TOPIC", "unused-at-import-time")
os.environ.setdefault("KAFKA_PARTITION_COUNT", "1")

import producer as posthog_gen  # noqa: E402  (see stub block above)
import pyarrow as pa  # noqa: E402
from confluent_kafka import Producer  # noqa: E402

# -------------------------------------------------------------------------
# Config
# -------------------------------------------------------------------------

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
# Detect the import-time stub leaking into the runtime config — if the user
# never set the env var, the setdefault block above installed a sentinel.
# Fail loudly here rather than letting librdkafka log bogus connect errors.
if BOOTSTRAP_SERVERS == "unused-at-import-time":
    raise SystemExit("KAFKA_BOOTSTRAP_SERVERS must be set in the environment")
TOPIC = os.environ.get("KAFKA_TOPIC", "test-events-arrow")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
BATCHES_PER_SECOND = int(os.environ.get("BATCHES_PER_SECOND", "10"))
TOTAL_BATCHES = int(os.environ.get("TOTAL_BATCHES", "-1"))

# Arrow schema v1 — MUST match experiments/arrow-payload/README.md.
SCHEMA = pa.schema(
    [
        pa.field("uuid", pa.string()),
        pa.field("event", pa.string()),
        pa.field("distinct_id", pa.string()),
        pa.field("timestamp", pa.string()),
        pa.field("team_id", pa.int64()),
        pa.field("project_id", pa.int64()),
        pa.field("properties", pa.string()),
        pa.field("elements_chain", pa.string()),
    ]
)

log = logging.getLogger("arrow-producer")


# -------------------------------------------------------------------------
# Shutdown handling
# -------------------------------------------------------------------------

_shutdown = False


def _install_signal_handlers() -> None:
    def _handler(signum, _frame):
        global _shutdown
        log.info("received signal %s, shutting down", signum)
        _shutdown = True

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


# -------------------------------------------------------------------------
# Batch construction
# -------------------------------------------------------------------------


def _build_batch(n: int) -> pa.RecordBatch:
    """Generate ``n`` events and return an Arrow RecordBatch matching SCHEMA."""
    uuids: list[str] = []
    events: list[str] = []
    distinct_ids: list[str] = []
    timestamps: list[str] = []
    team_ids: list[int] = []
    project_ids: list[int] = []
    properties: list[str] = []
    elements_chains: list[str | None] = []

    for i in range(n):
        ev = posthog_gen.make_event(i)
        props = ev["properties"]

        # Pull elements_chain out of properties for $autocapture only.
        # Non-autocapture events have null. We also remove it from the
        # properties dict before JSON-encoding to avoid duplication.
        chain: str | None = None
        if ev["event"] == "$autocapture":
            chain = props.pop("$elements_chain", None)

        uuids.append(ev["uuid"])
        events.append(ev["event"])
        distinct_ids.append(ev["distinct_id"])
        timestamps.append(ev["timestamp"])
        team_ids.append(ev["team_id"])
        project_ids.append(ev["project_id"])
        # Match Millpond's production approach: JSON-encode the nested
        # properties dict into a single VARCHAR column. See
        # millpond/arrow_converter.py::_flatten_nested_to_json.
        properties.append(json.dumps(props, default=str))
        elements_chains.append(chain)

    return pa.record_batch(
        [
            pa.array(uuids, type=pa.string()),
            pa.array(events, type=pa.string()),
            pa.array(distinct_ids, type=pa.string()),
            pa.array(timestamps, type=pa.string()),
            pa.array(team_ids, type=pa.int64()),
            pa.array(project_ids, type=pa.int64()),
            pa.array(properties, type=pa.string()),
            pa.array(elements_chains, type=pa.string()),
        ],
        schema=SCHEMA,
    )


def _serialize_batch(batch: pa.RecordBatch) -> bytes:
    """Serialize a RecordBatch as a self-contained Arrow IPC *stream*.

    We intentionally use ``ipc.new_stream`` (not ``new_file``) because the
    C++ consumer uses ``arrow::ipc::RecordBatchStreamReader`` to wrap the
    librdkafka payload buffer in place. One Kafka message value contains
    one full stream: schema + record batch + EOS marker.
    """
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, SCHEMA) as writer:
        writer.write_batch(batch)
    return sink.getvalue()


# -------------------------------------------------------------------------
# Main loop
# -------------------------------------------------------------------------


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    _install_signal_handlers()

    # No producer-side compression: we want raw memcpy semantics in the
    # C++ consumer. LZ4 can be added later once the zero-copy path is
    # validated.
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "compression.type": "none",
            "linger.ms": 5,
            "queue.buffering.max.messages": 1000,
            "queue.buffering.max.kbytes": 1048576,
            # 16 MiB. Matches kafka broker `message.max.bytes` and the topic
            # `max.message.bytes` set in docker-compose.yaml. PostHog events
            # with full properties batched 1000-at-a-time push ~2-5MB.
            "message.max.bytes": 16777216,
        }
    )

    infinite = TOTAL_BATCHES < 0
    target_interval = 1.0 / BATCHES_PER_SECOND if BATCHES_PER_SECOND > 0 else 0.0
    log.info(
        "starting: topic=%s batch_size=%d batches_per_second=%d total_batches=%s",
        TOPIC,
        BATCH_SIZE,
        BATCHES_PER_SECOND,
        "infinite" if infinite else str(TOTAL_BATCHES),
    )

    n = 0
    while not _shutdown and (infinite or n < TOTAL_BATCHES):
        loop_start = time.monotonic()

        batch = _build_batch(BATCH_SIZE)
        payload = _serialize_batch(batch)

        # Let librdkafka round-robin across partitions (no key, no
        # explicit partition). This matches the contract: partition
        # assignment is not semantically meaningful for this experiment.
        producer.produce(TOPIC, value=payload)
        producer.poll(0)

        n += 1
        print(
            f"[arrow-producer] sent batch {n} records={BATCH_SIZE} bytes={len(payload)}",
            flush=True,
        )

        if target_interval > 0:
            elapsed = time.monotonic() - loop_start
            remaining = target_interval - elapsed
            if remaining > 0:
                # Sleep in small slices so SIGTERM is responsive.
                deadline = time.monotonic() + remaining
                while not _shutdown:
                    slice_s = min(0.1, deadline - time.monotonic())
                    if slice_s <= 0:
                        break
                    time.sleep(slice_s)

    log.info("flushing producer")
    producer.flush(30)
    log.info("done: sent %d batches", n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
