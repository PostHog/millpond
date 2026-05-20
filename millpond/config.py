import logging
import os
import re
from dataclasses import dataclass
from typing import Literal

_SAFE_TABLE_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SAFE_NAMESPACE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Shared with ducklake._validate_partition_expr — keep in sync or import from here.
SAFE_PARTITION_EXPR = re.compile(r"^[a-zA-Z0-9_(),\s]+$")

log = logging.getLogger(__name__)

Destination = Literal["ducklake", "iceberg"]
_DESTINATIONS: tuple[Destination, ...] = ("ducklake", "iceberg")


@dataclass(frozen=True)
class Config:
    # Kafka
    bootstrap_servers: str
    topic: str
    group_id: str

    # Partition assignment
    replica_count: int
    ordinal: int

    # Destination selection — a pod writes to exactly one of these for its lifetime.
    destination: Destination

    # The DuckLake-* and Iceberg-* fields below are kept as `str | None`
    # rather than split into a tagged union (DuckLakeConfig | IcebergConfig)
    # because the load-time `if destination == ...` branch already enforces
    # presence of the right subset, and the Sink constructors raise
    # RuntimeError on missing fields (so `python -O` doesn't strip the
    # guards). A tagged-union refactor is a reasonable next step but
    # doesn't buy correctness on top of what we already have.

    # DuckLake — required when destination == "ducklake", else None.
    ducklake_table: str | None
    ducklake_data_path: str | None
    ducklake_connection: str | None
    rds_host: str | None
    rds_port: str | None
    rds_database: str | None
    rds_username: str | None
    rds_password: str | None
    partition_by: str | None  # e.g. "year(timestamp),month(timestamp),day(timestamp),hour(timestamp)"

    # Iceberg — required when destination == "iceberg", else None.
    iceberg_catalog_uri: str | None
    iceberg_warehouse: str | None
    iceberg_namespace: str | None
    iceberg_table: str | None
    iceberg_table_location: str | None  # explicit s3:// path; None lets the catalog decide
    iceberg_catalog_token: str | None  # bearer / OAuth token, optional

    # S3 for Iceberg data files. Ducklake uses DUCKDB_S3_* env vars read directly by ducklake.connect.
    s3_access_key_id: str | None
    s3_secret_access_key: str | None
    s3_region: str | None
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

    @property
    def table_label(self) -> str:
        """Single human-readable identifier for the destination table.
        Used in metrics pipeline labels and the Kafka client.id."""
        if self.destination == "iceberg":
            return f"{self.iceberg_namespace}.{self.iceberg_table}"
        return self.ducklake_table or "unknown"


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


def _load_ducklake_fields() -> dict[str, str | None]:
    ducklake_table = _require("DUCKLAKE_TABLE")
    if not _SAFE_TABLE_NAME.match(ducklake_table):
        raise RuntimeError(
            f"DUCKLAKE_TABLE {ducklake_table!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)"
        )

    partition_by = os.environ.get("DUCKLAKE_PARTITION_BY", "").strip() or None
    if partition_by and not SAFE_PARTITION_EXPR.match(partition_by):
        raise RuntimeError(
            f"DUCKLAKE_PARTITION_BY {partition_by!r} contains unsafe characters (must match [a-zA-Z0-9_(),\\s]+)"
        )

    return {
        "ducklake_table": ducklake_table,
        "ducklake_data_path": _require("DUCKLAKE_DATA_PATH"),
        "ducklake_connection": _require("DUCKLAKE_CONNECTION"),
        "rds_host": _require("DUCKLAKE_RDS_HOST"),
        "rds_port": os.environ.get("DUCKLAKE_RDS_PORT", "5432"),
        "rds_database": os.environ.get("DUCKLAKE_RDS_DATABASE", "ducklake"),
        "rds_username": os.environ.get("DUCKLAKE_RDS_USERNAME", "ducklake"),
        "rds_password": _require("DUCKLAKE_RDS_PASSWORD"),
        "partition_by": partition_by,
    }


def _load_iceberg_fields() -> dict[str, str | None]:
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

    return {
        "iceberg_catalog_uri": _require("ICEBERG_CATALOG_URI"),
        "iceberg_warehouse": _require("ICEBERG_WAREHOUSE"),
        "iceberg_namespace": iceberg_namespace,
        "iceberg_table": iceberg_table,
        "iceberg_table_location": os.environ.get("ICEBERG_TABLE_LOCATION") or None,
        "iceberg_catalog_token": os.environ.get("ICEBERG_CATALOG_TOKEN") or None,
        "s3_access_key_id": _require("MILLPOND_S3_ACCESS_KEY_ID"),
        "s3_secret_access_key": _require("MILLPOND_S3_SECRET_ACCESS_KEY"),
        "s3_region": _require("MILLPOND_S3_REGION"),
        "s3_endpoint": os.environ.get("MILLPOND_S3_ENDPOINT") or None,
    }


def load() -> Config:
    topic = _require("KAFKA_TOPIC")

    # Tolerate the common helm-template gotcha where an unset variable
    # renders as the empty string instead of being absent. Empty,
    # whitespace-only, and unset all fall back to the default. Single
    # collapsed expression: `.strip().lower()` first, then `or` once.
    destination_raw = os.environ.get("MILLPOND_DESTINATION", "").strip().lower() or "ducklake"
    if destination_raw not in _DESTINATIONS:
        raise RuntimeError(f"MILLPOND_DESTINATION {destination_raw!r} must be one of: {', '.join(_DESTINATIONS)}")
    destination: Destination = destination_raw  # type: ignore[assignment]

    pod_name = os.environ.get("POD_NAME") or os.environ.get("HOSTNAME", "millpond-0")
    ordinal = _parse_ordinal(pod_name)
    replica_count = int(_require("REPLICA_COUNT"))

    if ordinal >= replica_count:
        raise RuntimeError(f"Ordinal {ordinal} >= REPLICA_COUNT {replica_count}")

    ducklake_kwargs: dict[str, str | None] = dict.fromkeys(
        (
            "ducklake_table",
            "ducklake_data_path",
            "ducklake_connection",
            "rds_host",
            "rds_port",
            "rds_database",
            "rds_username",
            "rds_password",
            "partition_by",
        ),
        None,
    )
    iceberg_kwargs: dict[str, str | None] = dict.fromkeys(
        (
            "iceberg_catalog_uri",
            "iceberg_warehouse",
            "iceberg_namespace",
            "iceberg_table",
            "iceberg_table_location",
            "iceberg_catalog_token",
            "s3_access_key_id",
            "s3_secret_access_key",
            "s3_region",
            "s3_endpoint",
        ),
        None,
    )

    if destination == "ducklake":
        ducklake_kwargs.update(_load_ducklake_fields())
        table_label_part = ducklake_kwargs["ducklake_table"]
    else:
        iceberg_kwargs.update(_load_iceberg_fields())
        table_label_part = iceberg_kwargs["iceberg_table"]

    group_id = os.environ.get("GROUP_ID", f"millpond-{topic}-{table_label_part}")

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
        destination=destination,
        **ducklake_kwargs,
        **iceberg_kwargs,
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
        "Config: destination=%s topic=%s table=%s ordinal=%d/%d group_id=%s",
        destination,
        topic,
        cfg.table_label,
        ordinal,
        replica_count,
        cfg.group_id,
    )
    return cfg
