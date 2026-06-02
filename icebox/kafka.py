"""Kafka offset-commit helper for the icebox committer.

The committer's per-cycle workflow ends with: commit the writers' Kafka
offsets so that on restart, writers resume from after the offsets that
made it into the Iceberg snapshot.

Writers send `kafka_offsets` as {partition_id: max_offset_in_batch}
inside the POST body. The committer aggregates max across all files in
the cycle and commits via AdminClient.alter_consumer_group_offsets().

Critical detail: writers DO NOT consumer.subscribe() — they use
consumer.assign(), so the group is always "Empty" from Kafka's POV.
That makes AdminClient.alter_consumer_group_offsets safe to call (the
API only refuses on a Stable group). See WarpStream compat notes in
ICEBOX-PLAN.md "Validated assumptions".

Kafka offset semantics: committed offset = NEXT offset to read. Writers
report max_offset_seen; we add 1 here to get the next-to-read.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from concurrent.futures import Future

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

log = logging.getLogger(__name__)


def build_admin_client(
    *,
    bootstrap_servers: str,
    extra_config_json: str,
) -> AdminClient:
    """Build a confluent_kafka AdminClient from bootstrap servers + a
    JSON extra-config blob (security.protocol, sasl.mechanism, etc.).

    Raises:
        ValueError: if extra_config_json doesn't parse as a JSON object.
    """
    try:
        extra = json.loads(extra_config_json) if extra_config_json else {}
    except json.JSONDecodeError as e:
        raise ValueError(f"ICEBOX_KAFKA_EXTRA_CONFIG must be a JSON object: {e}") from e
    if not isinstance(extra, dict):
        raise ValueError(
            f"ICEBOX_KAFKA_EXTRA_CONFIG must be a JSON object, got {type(extra).__name__}"
        )
    cfg = {"bootstrap.servers": bootstrap_servers, **extra}
    return AdminClient(cfg)


def merge_max_offsets(
    file_offsets: list[Mapping[str, int]],
) -> dict[int, int]:
    """Aggregate per-file kafka_offsets into one max-per-partition map.

    Each input dict is keyed by stringified partition_id (JSON wire
    format). The output is int-keyed because TopicPartition takes int
    partitions.

    Args:
        file_offsets: One dict per file in the cycle. Empty list →
            empty dict (vacuous cycle).

    Returns:
        {partition_id (int): max_offset_seen_across_files}
    """
    merged: dict[int, int] = {}
    for off_map in file_offsets:
        for part_str, offset in off_map.items():
            part = int(part_str)
            cur = merged.get(part)
            if cur is None or offset > cur:
                merged[part] = offset
    return merged


def commit_offsets(
    admin: AdminClient,
    *,
    group_id: str,
    topic: str,
    max_offsets: Mapping[int, int],
    request_timeout_seconds: float = 30.0,
) -> None:
    """Commit (max_offset + 1) for each partition in `max_offsets` to
    the consumer group's __consumer_offsets entry.

    Args:
        admin: AdminClient from build_admin_client.
        group_id: The writers' consumer-group id (must match writer
            GROUP_ID env var exactly).
        topic: The topic the offsets belong to.
        max_offsets: {partition_id (int): max_offset_seen}. Empty map
            short-circuits — vacuous cycles have nothing to commit.
        request_timeout_seconds: Per-call timeout for the admin RPC.

    Raises:
        KafkaException: if Kafka rejects the commit (group state mismatch,
            authorization, broker error). Propagated for the committer to
            mark the cycle failed and back off.
        TimeoutError: if the admin RPC doesn't complete within
            request_timeout_seconds.
    """
    if not max_offsets:
        log.info(
            "commit_offsets: no offsets to commit for group=%s topic=%s (vacuous cycle)",
            group_id,
            topic,
        )
        return

    tps = [
        TopicPartition(topic, partition, offset + 1)
        for partition, offset in sorted(max_offsets.items())
    ]
    log.info(
        "commit_offsets: committing %d partitions for group=%s topic=%s",
        len(tps),
        group_id,
        topic,
    )
    req = ConsumerGroupTopicPartitions(group_id=group_id, topic_partitions=tps)
    futures: dict[str, Future] = admin.alter_consumer_group_offsets(
        [req], request_timeout=request_timeout_seconds
    )
    # The admin API returns one future per group; await it to surface
    # errors as exceptions (the Future.result raises on KafkaException).
    for grp, fut in futures.items():
        # Block bounded by request_timeout. The c-impl honors it server-
        # side already; the explicit Python timeout guards against
        # client-side stalls if the broker dies mid-RPC.
        result = fut.result(timeout=request_timeout_seconds)
        log.info("commit_offsets: group=%s result=%r", grp, result)
