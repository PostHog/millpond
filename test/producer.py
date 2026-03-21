"""Produces sample JSON records to Kafka across all partitions."""

import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ["KAFKA_TOPIC"]
PARTITION_COUNT = int(os.environ["KAFKA_PARTITION_COUNT"])
RECORDS_PER_SECOND = int(os.environ.get("RECORDS_PER_SECOND", "1000"))
TOTAL_RECORDS = int(os.environ.get("TOTAL_RECORDS", "10000"))

EVENT_TYPES = ["pageview", "click", "signup", "purchase", "logout"]
URLS = ["/home", "/products", "/cart", "/checkout", "/profile", "/settings"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]


def make_event(i: int) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"user_{random.randint(1, 1000)}",
        "url": random.choice(URLS),
        "browser": random.choice(BROWSERS),
        "duration_ms": random.randint(100, 30000),
        "is_mobile": random.choice([True, False]),
        "properties": {
            "referrer": random.choice(["google", "direct", "twitter", "email", None]),
            "screen_width": random.choice([1920, 1440, 1366, 375, 414]),
        },
        "sequence": i,
    }


def main():
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    infinite = TOTAL_RECORDS == -1
    if infinite:
        print(f"Producing indefinitely to {TOPIC} ({PARTITION_COUNT} partitions)")
    else:
        print(f"Producing {TOTAL_RECORDS} records to {TOPIC} ({PARTITION_COUNT} partitions)")
    print(f"Rate: {RECORDS_PER_SECOND} records/sec")

    sent = 0
    batch_start = time.monotonic()
    batch_size = max(1, RECORDS_PER_SECOND // 10)
    i = 0

    while infinite or i < TOTAL_RECORDS:
        event = make_event(i)
        partition = i % PARTITION_COUNT
        producer.produce(
            TOPIC,
            key=event["user_id"].encode(),
            value=json.dumps(event).encode(),
            partition=partition,
        )
        sent += 1
        i += 1

        if sent % batch_size == 0:
            producer.flush()
            elapsed = time.monotonic() - batch_start
            expected = sent / RECORDS_PER_SECOND
            if elapsed < expected:
                time.sleep(expected - elapsed)

        if sent % 1000 == 0:
            if infinite:
                print(f"  sent {sent}")
            else:
                print(f"  sent {sent}/{TOTAL_RECORDS}")

    producer.flush()
    print(f"Done. Produced {sent} records.")


if __name__ == "__main__":
    sys.exit(main())
