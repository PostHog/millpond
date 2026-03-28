import logging
import os
import re
from dataclasses import dataclass

_SAFE_TABLE_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Shared with ducklake._validate_partition_expr — keep in sync or import from here.
SAFE_PARTITION_EXPR = re.compile(r"^[a-zA-Z0-9_(),\s]+$")

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    # Kafka
    bootstrap_servers: str
    topic: str
    group_id: str

    # Partition assignment
    replica_count: int
    ordinal: int

    # DuckLake
    ducklake_table: str
    ducklake_data_path: str
    ducklake_connection: str

    # RDS (DuckLake metadata store)
    rds_host: str
    rds_port: str
    rds_database: str
    rds_username: str
    rds_password: str

    # Flush triggers
    flush_size: int  # bytes of accumulated Arrow data
    flush_interval_ms: int  # ms since last flush

    # Partitioning
    partition_by: str | None  # e.g. "year(timestamp),month(timestamp),day(timestamp),hour(timestamp)"

    # Consumer tuning
    fetch_min_bytes: int
    fetch_max_wait_ms: int
    consume_batch_size: int
    stats_interval_ms: int

    # Record filter (optional) — only keep records where filter_field == filter_value
    filter_field: str | None
    filter_value: str | None

    # Extra librdkafka config (from KAFKA_CONSUMER_* env vars)
    kafka_config_overrides: tuple[tuple[str, str], ...]

    @property
    def flush_interval_s(self) -> float:
        return self.flush_interval_ms / 1000.0


def _parse_ordinal(pod_name: str) -> int:
    """Extract ordinal from pod name (e.g. 'millpond-events-3' -> 3)."""
    match = re.search(r"-(\d+)$", pod_name)
    if not match:
        raise ValueError(f"Cannot parse ordinal from pod name: {pod_name!r}")
    return int(match.group(1))


def _require(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return val


def load() -> Config:
    topic = _require("KAFKA_TOPIC")
    ducklake_table = _require("DUCKLAKE_TABLE")
    if not _SAFE_TABLE_NAME.match(ducklake_table):
        raise RuntimeError(
            f"DUCKLAKE_TABLE {ducklake_table!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)"
        )

    pod_name = os.environ.get("POD_NAME") or os.environ.get("HOSTNAME", "millpond-0")
    ordinal = _parse_ordinal(pod_name)
    replica_count = int(_require("REPLICA_COUNT"))

    if ordinal >= replica_count:
        raise RuntimeError(f"Ordinal {ordinal} >= REPLICA_COUNT {replica_count}")

    group_id = os.environ.get("GROUP_ID", f"millpond-{topic}-{ducklake_table}")

    partition_by = os.environ.get("DUCKLAKE_PARTITION_BY", "").strip() or None
    if partition_by and not SAFE_PARTITION_EXPR.match(partition_by):
        raise RuntimeError(
            f"DUCKLAKE_PARTITION_BY {partition_by!r} contains unsafe characters (must match [a-zA-Z0-9_(),\\s]+)"
        )

    # Collect KAFKA_CONSUMER_* env vars as librdkafka config overrides.
    # e.g. KAFKA_CONSUMER_SECURITY_PROTOCOL=SASL_SSL -> security.protocol=SASL_SSL
    _KAFKA_CONSUMER_PREFIX = "KAFKA_CONSUMER_"
    kafka_overrides = tuple(
        (k[len(_KAFKA_CONSUMER_PREFIX) :].lower().replace("_", "."), v)
        for k, v in os.environ.items()
        if k.startswith(_KAFKA_CONSUMER_PREFIX)
    )

    cfg = Config(
        bootstrap_servers=_require("KAFKA_BOOTSTRAP_SERVERS"),
        topic=topic,
        group_id=group_id,
        replica_count=replica_count,
        ordinal=ordinal,
        ducklake_table=ducklake_table,
        ducklake_data_path=_require("DUCKLAKE_DATA_PATH"),
        ducklake_connection=_require("DUCKLAKE_CONNECTION"),
        rds_host=_require("DUCKLAKE_RDS_HOST"),
        rds_port=os.environ.get("DUCKLAKE_RDS_PORT", "5432"),
        rds_database=os.environ.get("DUCKLAKE_RDS_DATABASE", "ducklake"),
        rds_username=os.environ.get("DUCKLAKE_RDS_USERNAME", "ducklake"),
        rds_password=_require("DUCKLAKE_RDS_PASSWORD"),
        partition_by=partition_by,
        flush_size=int(os.environ.get("FLUSH_SIZE", "104857600")),
        flush_interval_ms=int(os.environ.get("FLUSH_INTERVAL_MS", "60000")),
        fetch_min_bytes=int(os.environ.get("FETCH_MIN_BYTES", "1048576")),
        fetch_max_wait_ms=int(os.environ.get("FETCH_MAX_WAIT_MS", "500")),
        consume_batch_size=int(os.environ.get("CONSUME_BATCH_SIZE", "1000")),
        stats_interval_ms=int(os.environ.get("STATS_INTERVAL_MS", "5000")),
        filter_field=os.environ.get("FILTER_FIELD"),
        filter_value=os.environ.get("FILTER_VALUE"),
        kafka_config_overrides=kafka_overrides,
    )

    if bool(cfg.filter_field) != bool(cfg.filter_value):
        raise RuntimeError("FILTER_FIELD and FILTER_VALUE must both be set or both be unset")

    log.info(
        "Config: topic=%s table=%s ordinal=%d/%d group_id=%s",
        topic,
        ducklake_table,
        ordinal,
        replica_count,
        cfg.group_id,
    )
    if cfg.filter_field:
        log.info("Filter: %s=%s", cfg.filter_field, cfg.filter_value)
    return cfg
