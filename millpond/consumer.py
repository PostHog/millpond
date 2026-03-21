import logging

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from millpond.config import Config

log = logging.getLogger(__name__)


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
        }
    )

    consumer.assign([TopicPartition(cfg.topic, p) for p in my_partitions])
    return consumer
