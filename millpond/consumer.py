import logging
import os

import orjson
from confluent_kafka import OFFSET_INVALID, OFFSET_STORED, Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from millpond import metrics
from millpond.config import Config

log = logging.getLogger(__name__)


def _maybe_attach_oauth_cb(config: dict) -> dict:
    """If SASL OAUTHBEARER is configured, attach the MSK IAM token callback."""
    if "sasl.mechanism" in config and "sasl.mechanisms" not in config:
        log.warning(
            "sasl.mechanism (singular) is set but librdkafka uses sasl.mechanisms (plural). "
            "Use KAFKA_CONSUMER_SASL_MECHANISMS instead of KAFKA_CONSUMER_SASL_MECHANISM."
        )
    sasl_mechanism = config.get("sasl.mechanisms")
    if sasl_mechanism != "OAUTHBEARER":
        return config
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError:
        raise RuntimeError(
            "aws-msk-iam-sasl-signer-python is required for OAUTHBEARER auth. "
            "Install with: pip install millpond[msk-iam]"
        ) from None

    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        raise RuntimeError("AWS_REGION or AWS_DEFAULT_REGION must be set when using OAUTHBEARER auth")

    # MSK rejects token refreshes if the session name changes between re-authentications.
    # Each call to generate_auth_token() creates a new botocore session with a random name.
    # Pin it so the principal stays stable across refreshes on the same connection.
    os.environ.setdefault("AWS_ROLE_SESSION_NAME", "millpond")
    log.info("MSK IAM auth enabled (region=%s)", region)

    def oauth_cb(config_str):
        try:
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
            return token, expiry_ms / 1000
        except Exception:
            log.exception("MSK IAM token generation failed")
            raise

    config["oauth_cb"] = oauth_cb
    return config


def _base_kafka_config(cfg: Config) -> dict:
    """Base config shared by all Kafka clients (AdminClient, Consumer)."""
    base = {
        "bootstrap.servers": cfg.bootstrap_servers,
        "client.id": f"millpond-{cfg.topic}-{cfg.table_label}-{cfg.ordinal}",
    }
    for key, val in cfg.kafka_config_overrides:
        base[key] = val
    return _maybe_attach_oauth_cb(base)


def _on_stats(stats_json: str) -> None:
    """Parse librdkafka stats JSON and update Prometheus gauges.

    Called on the librdkafka background thread, not the main thread.
    prometheus_client Gauge.set() is thread-safe (atomic double write).
    """
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
    admin = AdminClient(_base_kafka_config(cfg))
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


_RECOVERY_TIMEOUT_S = 10.0


def _query_watermarks(admin: AdminClient, tps: list[TopicPartition]) -> dict[int, tuple[int, int]]:
    """Batched watermark fetch via AdminClient.list_offsets.

    Returns dict[partition_id, (low, high)]. Partitions whose
    per-partition future raises are silently absent from the result — the
    caller treats "no watermark" as "skip this partition" so a per-leader
    metadata blip doesn't take down the whole recovery sweep.

    Two RPCs total (one OffsetSpec.earliest, one OffsetSpec.latest), each
    fan-out at the broker side; with 128+ partitions this is ~2-3s vs the
    >20min worst case of per-partition consumer.get_watermark_offsets().
    """
    if not tps:
        return {}

    try:
        earliest_futs = admin.list_offsets(
            {tp: OffsetSpec.earliest() for tp in tps},
            request_timeout=_RECOVERY_TIMEOUT_S,
        )
        latest_futs = admin.list_offsets(
            {tp: OffsetSpec.latest() for tp in tps},
            request_timeout=_RECOVERY_TIMEOUT_S,
        )
    except KafkaException:
        log.warning(
            "Skipping stale-offset recovery: AdminClient.list_offsets dispatch failed",
            exc_info=True,
        )
        return {}

    out: dict[int, tuple[int, int]] = {}
    for tp in tps:
        try:
            low = earliest_futs[tp].result().offset
            high = latest_futs[tp].result().offset
        except Exception:
            log.warning("Skipping watermark for %s [%d]", tp.topic, tp.partition, exc_info=True)
            continue
        out[tp.partition] = (low, high)
    return out


def _recover_stale_committed_offsets(consumer: Consumer, cfg: Config, partitions: list[int]) -> None:
    """Eagerly bring committed offsets within retention for assigned partitions.

    Without this, partitions whose stored offset is below the broker's
    log-start-offset only "recover" the next time librdkafka actually fetches a
    message from them: `auto.offset.reset` seeks the local position to
    EARLIEST/LATEST, but millpond's offsets dict (and therefore the next
    commit) is only advanced by *delivered* messages (main.py:_main_loop). On
    quiet partitions that receive zero messages during a pod's lifetime, no
    commit ever happens and the stale offset re-trips `offset_out_of_range` on
    every restart and reports inflated `millpond_consumer_lag` for those
    partitions until something delivers — see PostHog/millpond docs from
    2026-06-16.

    The recovery is a one-shot at startup:

      1. Query committed offsets for every assigned partition. Partitions that
         have never been committed (OFFSET_INVALID) are skipped — letting
         `auto.offset.reset` apply normally on first fetch is the correct
         fresh-consumer behaviour. Partitions whose committed-query carries an
         error are skipped (we'd otherwise read garbage from `.offset`).
      2. Batched query of [low, high] watermarks for every remaining partition
         via AdminClient.list_offsets (two RPCs, not 1-per-partition).
      3. If committed >= low the offset is still within retention; do nothing.
      4. Committed < low: stale. Commit the `auto.offset.reset` target —
         `low` for "earliest", `high` for "latest". This is the same position
         librdkafka would seek to on first fetch, just made durable across
         restarts. `low` is the next-to-fetch (first available record) and
         `high` is the next-to-be-produced; both match Kafka commit semantics
         where the committed value IS the next-to-fetch.

    We intentionally drop `leader_epoch` on the recovered TopicPartition. The
    epoch reported alongside the stale committed offset refers to a leader era
    in which our target offset (low or high *now*) didn't exist; carrying it
    would be semantically wrong. Modern brokers accept `-1`/None and resolve
    the current epoch from metadata on next fetch.

    Bails gracefully on per-partition errors (logs and continues) and on the
    overall commit (logs warning). The auto.offset.reset fallback still works
    if this function fails entirely — it's an optimization, not a correctness
    requirement.
    """
    if not partitions:
        return

    tps = [TopicPartition(cfg.topic, p) for p in partitions]
    try:
        committed = consumer.committed(tps, timeout=_RECOVERY_TIMEOUT_S)
    except KafkaException:
        log.warning("Skipping stale-offset recovery: failed to query committed offsets", exc_info=True)
        return

    # Filter to partitions that actually need a watermark check.
    needs_watermark: list[TopicPartition] = []
    n_fresh = 0
    n_error = 0
    for tp in committed:
        if tp.error is not None:
            log.warning(
                "Stale-offset recovery: committed() returned error for %s [%d]: %s",
                tp.topic,
                tp.partition,
                tp.error,
            )
            n_error += 1
            continue
        if tp.offset == OFFSET_INVALID:
            n_fresh += 1
            continue
        needs_watermark.append(tp)

    admin = AdminClient(_base_kafka_config(cfg))
    watermarks = _query_watermarks(admin, [TopicPartition(tp.topic, tp.partition) for tp in needs_watermark])

    to_recover: list[TopicPartition] = []
    n_in_retention = 0
    n_no_watermark = 0
    for tp in needs_watermark:
        if tp.partition not in watermarks:
            n_no_watermark += 1
            continue
        low, high = watermarks[tp.partition]
        if tp.offset >= low:
            n_in_retention += 1
            continue
        # Stale: records at tp.offset are gone from the broker. Commit the
        # auto.offset.reset target so OFFSET_STORED resolves to something
        # in-range. Per-partition WARNING so an operator triaging weird lag can
        # see exactly which partition recovered and from where.
        target = high if cfg.auto_offset_reset == "latest" else low
        log.warning(
            "Stale committed offset on %s [%d]: committed=%d, low=%d, high=%d, recovering to %d (%s)",
            tp.topic,
            tp.partition,
            tp.offset,
            low,
            high,
            target,
            cfg.auto_offset_reset,
        )
        to_recover.append(TopicPartition(tp.topic, tp.partition, target))

    if to_recover:
        log.info(
            "Recovering %d stale committed offsets (target=%s, fresh=%d, in_retention=%d, errored=%d, no_watermark=%d)",
            len(to_recover),
            cfg.auto_offset_reset,
            n_fresh,
            n_in_retention,
            n_error,
            n_no_watermark,
        )
        commit_ok = True
        try:
            consumer.commit(offsets=to_recover, asynchronous=False)
        except KafkaException:
            log.warning(
                "Failed to commit recovered offsets; auto.offset.reset fallback still applies",
                exc_info=True,
            )
            commit_ok = False
    else:
        log.info(
            "Committed offsets healthy at startup (fresh=%d, in_retention=%d, errored=%d, no_watermark=%d)",
            n_fresh,
            n_in_retention,
            n_error,
            n_no_watermark,
        )
        commit_ok = True

    # Seed gauges unconditionally so the dashboard isn't haunted by stale
    # values from prior pod runs. See _seed_startup_gauges docstring.
    _seed_startup_gauges(cfg, committed, watermarks, to_recover if commit_ok else [])


def _seed_startup_gauges(
    cfg: Config,
    committed: list,
    watermarks: dict[int, tuple[int, int]],
    recovered: list[TopicPartition],
) -> None:
    """Set `millpond_consumer_lag` and `millpond_last_committed_offset` for every
    assigned partition with a known position so the dashboard isn't haunted by
    stale gauge values from prior pod runs.

    main.py's _update_lag_metrics / _flush only set these gauges for partitions
    that delivered messages in *this* pod's lifetime. For quiet partitions
    (zero deliveries during a run) the gauges retain whatever previous pods
    last set — which for the stale-offset case is an inflated lag value that
    keeps showing on the dashboard well after recovery has fixed
    __consumer_offsets. Seed here so the gauges read truth at startup.

    For each partition with a known [low, high]:
      - Just recovered → position = recovered target (lag is 0 for "latest"
        policy, high-low for "earliest").
      - In retention (committed offset valid + within range) → position =
        committed offset.
      - Fresh (OFFSET_INVALID), errored committed, or stale-but-not-recovered
        (recovery commit failed) → position = the auto.offset.reset target.
        That's where OFFSET_STORED resolves to via librdkafka's fallback on
        first fetch, so seeding to it matches what the next delivery will
        confirm.

    Partitions without a watermark are skipped — we can't compute lag without
    `high`, and we shouldn't overwrite the gauge with a wrong value.
    """
    recovered_by_partition = {tp.partition: tp.offset for tp in recovered}
    committed_by_partition = {tp.partition: tp for tp in committed}

    n_seeded = 0
    for partition_id, (low, high) in watermarks.items():
        if partition_id in recovered_by_partition:
            position = recovered_by_partition[partition_id]
        else:
            tp = committed_by_partition.get(partition_id)
            if tp is not None and tp.error is None and tp.offset != OFFSET_INVALID and tp.offset >= low:
                position = tp.offset
            else:
                # Fresh, errored, or stale-but-not-recovered: use the
                # auto.offset.reset target librdkafka will seek to.
                position = high if cfg.auto_offset_reset == "latest" else low

        metrics.consumer_lag.labels(partition=str(partition_id)).set(high - position)
        metrics.last_committed_offset.labels(partition=str(partition_id)).set(position)
        n_seeded += 1

    if n_seeded:
        log.info("Seeded startup lag/offset gauges for %d partitions", n_seeded)


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

    consumer_config = {
        **_base_kafka_config(cfg),
        "group.id": cfg.group_id,
        "auto.offset.reset": cfg.auto_offset_reset,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "fetch.min.bytes": cfg.fetch_min_bytes,
        "fetch.wait.max.ms": cfg.fetch_max_wait_ms,
        "max.poll.interval.ms": 600000,  # 10 min — accommodates long S3 flushes
        "statistics.interval.ms": cfg.stats_interval_ms,
        "stats_cb": _on_stats,
    }
    # Default to 16MB per partition to bound librdkafka internal memory.
    # Allow override via KAFKA_CONSUMER_QUEUED_MAX_MESSAGES_KBYTES.
    consumer_config.setdefault("queued.max.messages.kbytes", 16384)

    consumer = Consumer(consumer_config)

    # Recover stale committed offsets BEFORE assign() so the OFFSET_STORED
    # resolution below picks up the fresh committed value rather than the
    # ancient out-of-retention one. Without this, quiet partitions whose
    # committed offset is below the broker's log-start-offset stay logging
    # `offset_out_of_range` on every restart and report misleading lag in
    # millpond_consumer_lag.
    _recover_stale_committed_offsets(consumer, cfg, my_partitions)

    # OFFSET_STORED: resume from committed offset in __consumer_offsets.
    # Falls back to auto.offset.reset (cfg.auto_offset_reset) if no offset
    # committed — set this to "latest" for fresh NRT consumers so they don't
    # replay the entire retention window before catching up.
    # Critical for replica count changes — new pods must resume from offsets
    # committed by whichever pod previously owned each partition.
    consumer.assign([TopicPartition(cfg.topic, p, OFFSET_STORED) for p in my_partitions])
    return consumer
