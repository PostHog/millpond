import logging
import os
import re
from dataclasses import dataclass

_SAFE_TABLE_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SAFE_NAMESPACE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

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

    # Iceberg
    iceberg_catalog_uri: str  # REST endpoint
    iceberg_warehouse: str
    iceberg_namespace: str
    iceberg_table: str
    iceberg_table_location: str | None  # explicit s3:// path; None lets the catalog decide
    iceberg_catalog_token: str | None  # bearer / OAuth token, optional

    # S3 (data files)
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_region: str
    s3_endpoint: str | None

    # Flush triggers
    flush_size: int  # bytes of accumulated Arrow data
    flush_interval_ms: int  # ms since last flush

    # Consumer tuning
    fetch_min_bytes: int
    fetch_max_wait_ms: int
    consume_batch_size: int
    stats_interval_ms: int

    # Broker source label for metrics (e.g. "msk", "warpstream")
    broker_source: str

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

    iceberg_table = _require("ICEBERG_TABLE")
    if not _SAFE_TABLE_NAME.match(iceberg_table):
        raise RuntimeError(
            f"ICEBERG_TABLE {iceberg_table!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)"
        )

    iceberg_namespace = _require("ICEBERG_NAMESPACE")
    if not _SAFE_NAMESPACE.match(iceberg_namespace):
        raise RuntimeError(
            f"ICEBERG_NAMESPACE {iceberg_namespace!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)"
        )

    pod_name = os.environ.get("POD_NAME") or os.environ.get("HOSTNAME", "millpond-0")
    ordinal = _parse_ordinal(pod_name)
    replica_count = int(_require("REPLICA_COUNT"))

    if ordinal >= replica_count:
        raise RuntimeError(f"Ordinal {ordinal} >= REPLICA_COUNT {replica_count}")

    group_id = os.environ.get("GROUP_ID", f"millpond-{topic}-{iceberg_table}")

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
        iceberg_catalog_uri=_require("ICEBERG_CATALOG_URI"),
        iceberg_warehouse=_require("ICEBERG_WAREHOUSE"),
        iceberg_namespace=iceberg_namespace,
        iceberg_table=iceberg_table,
        iceberg_table_location=os.environ.get("ICEBERG_TABLE_LOCATION") or None,
        iceberg_catalog_token=os.environ.get("ICEBERG_CATALOG_TOKEN") or None,
        s3_access_key_id=_require("MILLPOND_S3_ACCESS_KEY_ID"),
        s3_secret_access_key=_require("MILLPOND_S3_SECRET_ACCESS_KEY"),
        s3_region=_require("MILLPOND_S3_REGION"),
        s3_endpoint=os.environ.get("MILLPOND_S3_ENDPOINT") or None,
        flush_size=int(os.environ.get("FLUSH_SIZE", "104857600")),
        flush_interval_ms=int(os.environ.get("FLUSH_INTERVAL_MS", "60000")),
        fetch_min_bytes=int(os.environ.get("FETCH_MIN_BYTES", "1048576")),
        fetch_max_wait_ms=int(os.environ.get("FETCH_MAX_WAIT_MS", "500")),
        consume_batch_size=int(os.environ.get("CONSUME_BATCH_SIZE", "1000")),
        stats_interval_ms=int(os.environ.get("STATS_INTERVAL_MS", "5000")),
        broker_source=os.environ.get("BROKER_SOURCE", "").strip().lower(),
        kafka_config_overrides=kafka_overrides,
    )

    log.info(
        "Config: topic=%s table=%s.%s ordinal=%d/%d group_id=%s",
        topic,
        iceberg_namespace,
        iceberg_table,
        ordinal,
        replica_count,
        cfg.group_id,
    )
    return cfg
