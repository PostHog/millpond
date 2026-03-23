import logging

import orjson
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from millpond import metrics
from millpond.config import Config

log = logging.getLogger(__name__)


def _on_stats(stats_json: str) -> None:
    """Parse librdkafka stats JSON and update Prometheus gauges."""
    try:
        stats = orjson.loads(stats_json)
    except (orjson.JSONDecodeError, TypeError):
        return

    metrics.rdkafka_replyq.set(stats.get("replyq", 0))
    metrics.rdkafka_msg_cnt.set(stats.get("msg_cnt", 0))
    metrics.rdkafka_msg_size.set(stats.get("msg_size", 0))

    for broker_info in stats.get("brokers", {}).values():
        name = broker_info.get("name", "unknown")
        rtt = broker_info.get("rtt", {})
        # librdkafka reports RTT in microseconds; -1 means no samples
        avg = rtt.get("avg", -1)
        p99 = rtt.get("p99", -1)
        if avg >= 0:
            metrics.rdkafka_broker_rtt_avg.labels(broker=name).set(avg / 1_000_000)
        if p99 >= 0:
            metrics.rdkafka_broker_rtt_p99.labels(broker=name).set(p99 / 1_000_000)


def discover_partition_count(cfg: Config) -> int:
    """Discover partition count for the topic via broker metadata."""
    admin = AdminClient({"bootstrap.servers": cfg.bootstrap_servers})
    metadata = admin.list_topics(topic=cfg.topic, timeout=30)
    topic_meta = metadata.topics.get(cfg.topic)
    if topic_meta is None:
        raise RuntimeError(f"Topic {cfg.topic!r} not found")
    if topic_meta.error is not None:
        raise RuntimeError(f"Topic {cfg.topic!r} error: {topic_meta.error}")
    count = len(topic_meta.partitions)
    log.info("Discovered %d partitions for topic %s", count, cfg.topic)
    return count


def compute_assignment(partition_count: int, replica_count: int, ordinal: int) -> list[int]:
    """Compute which partitions this pod owns."""
    return [p for p in range(partition_count) if p % replica_count == ordinal]


def create(cfg: Config) -> Consumer:
    """Create and configure the Kafka consumer with static partition assignment."""
    partition_count = discover_partition_count(cfg)
    my_partitions = compute_assignment(partition_count, cfg.replica_count, cfg.ordinal)

    if not my_partitions:
        raise RuntimeError(
            f"No partitions assigned to ordinal {cfg.ordinal} "
            f"(partition_count={partition_count}, replica_count={cfg.replica_count})"
        )

    log.info("Assigned partitions: %s", my_partitions)

    consumer = Consumer(
        {
            "bootstrap.servers": cfg.bootstrap_servers,
            "group.id": cfg.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "fetch.min.bytes": cfg.fetch_min_bytes,
            "fetch.wait.max.ms": cfg.fetch_max_wait_ms,
            "statistics.interval.ms": cfg.stats_interval_ms,
            "stats_cb": _on_stats,
        }
    )

    consumer.assign([TopicPartition(cfg.topic, p) for p in my_partitions])
    return consumer
