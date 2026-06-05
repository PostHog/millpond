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

Destination = Literal["ducklake", "iceberg", "icebox"]
_DESTINATIONS: tuple[Destination, ...] = ("ducklake", "iceberg", "icebox")


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

    # Icebox — required when destination == "icebox", else None.
    # icebox_bucket + icebox_warehouse_prefix are the deterministic-path
    # components the sink uses when writing staged parquet to S3 — they
    # MUST match the icebox-side warehouse config or files land where
    # the catalog can't find them.
    icebox_bucket: str | None
    icebox_warehouse_prefix: str | None
    # PG connection for the writer-side IceboxClient (direct INSERT
    # into icebox_files). All seven fields are required together when
    # destination == "icebox".
    icebox_pg_host: str | None
    icebox_pg_port: int | None
    icebox_pg_database: str | None
    icebox_pg_username: str | None
    icebox_pg_password: str | None
    icebox_pg_schema: str | None
    icebox_pg_sslmode: str | None

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

    # Optional record filter. Exactly one of `filter_keep_field` /
    # `filter_drop_field` may be set; whichever is set names the column to
    # check against `filter_values`. Keep = allowlist (keep records whose
    # value is in filter_values, drop the rest). Drop = denylist (drop
    # records whose value is in filter_values, keep the rest). Only the
    # keep direction is implemented today; the drop slot reserves the
    # namespace and the mutual-exclusion contract for a future add.
    # `filter_values` is parsed at load time and is homogeneous — either a
    # tuple of ints (if all comma-separated tokens parsed as int) or a tuple
    # of strings (otherwise). main.py applies this after JSON→Arrow but
    # before the pending buffer.
    filter_keep_field: str | None
    filter_drop_field: str | None
    filter_values: tuple[int, ...] | tuple[str, ...] | None

    # Optional pre-write sort. Tuple of column names; sort is ascending
    # in tuple order. Applied to the consolidated batch right before
    # sink.write() so both DuckLake and Iceberg paths see pre-sorted
    # data. None disables the sort entirely.
    sort_by: tuple[str, ...] | None

    # Extra librdkafka config (from KAFKA_CONSUMER_* env vars)
    kafka_config_overrides: tuple[tuple[str, str], ...]

    # Optional PostHog Logs export via OTLP/HTTP. ON when
    # ``posthog_project_token`` is set, OFF otherwise. Endpoint
    # defaults to the US PostHog Cloud ingress; override for EU or
    # self-hosted PostHog. service_namespace + service_instance_id
    # match the icebox taxonomy (see shared/structured_logging.py +
    # icebox/README.md). service_instance_id is typically the
    # consumer-key the chart uses for the StatefulSet name, e.g.
    # "events-icebox" or "events".
    posthog_project_token: str | None = None
    posthog_logs_endpoint: str = "https://us.i.posthog.com/i/v1/logs"
    service_namespace: str = "millpond"
    service_instance_id: str | None = None
    # service.version reported in OTLP resource attrs. Defaults to the
    # millpond package version. Operators can override via
    # MILLPOND_SERVICE_VERSION (e.g. to expose the image digest).
    service_version: str = "unknown"

    @property
    def flush_interval_s(self) -> float:
        return self.flush_interval_ms / 1000.0

    @property
    def table_label(self) -> str:
        """Single human-readable identifier for the destination table.
        Used in metrics pipeline labels and the Kafka client.id."""
        if self.destination in ("iceberg", "icebox"):
            return f"{self.iceberg_namespace}.{self.iceberg_table}"
        return self.ducklake_table or "unknown"


def _default_service_version() -> str:
    """Best-effort service version string for OTLP resource attrs.

    Falls back to the millpond setuptools-scm version baked into the
    package; that maps cleanly to a git rev in non-prod builds and to a
    clean tag in prod images.
    """
    try:
        from millpond._version import version

        return str(version)
    except Exception:
        return "unknown"


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


_SAFE_COLUMN_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _parse_filter_values(raw: str) -> tuple[int, ...] | tuple[str, ...]:
    """Parse MILLPOND_FILTER_VALUES into a homogeneous tuple.

    Try int-tuple first; fall back to string-tuple if any token fails to
    parse. Whitespace is trimmed; empty tokens (e.g. trailing commas) are
    dropped. An empty input raises — callers should validate the env var
    is non-empty before calling.
    """
    tokens = tuple(t.strip() for t in raw.split(",") if t.strip())
    if not tokens:
        raise RuntimeError("MILLPOND_FILTER_VALUES must contain at least one value")
    try:
        return tuple(int(t) for t in tokens)
    except ValueError:
        return tokens


def _load_filter_fields() -> tuple[str | None, str | None, tuple[int, ...] | tuple[str, ...] | None]:
    """Read MILLPOND_FILTER_{KEEP,DROP}_FIELD_NAME + MILLPOND_FILTER_VALUES.

    At most one of keep/drop may be set. If either is set, FILTER_VALUES is
    required. Field names must be safe identifiers so misconfigurations
    surface at startup rather than under load. Drop is reserved for a
    future change — config rejects it explicitly with a clear message
    rather than silently accepting and doing nothing.
    """
    keep = os.environ.get("MILLPOND_FILTER_KEEP_FIELD_NAME", "").strip() or None
    drop = os.environ.get("MILLPOND_FILTER_DROP_FIELD_NAME", "").strip() or None
    values_raw = os.environ.get("MILLPOND_FILTER_VALUES", "").strip()

    if keep and drop:
        raise RuntimeError("MILLPOND_FILTER_KEEP_FIELD_NAME and MILLPOND_FILTER_DROP_FIELD_NAME are mutually exclusive")

    active = keep or drop
    if bool(active) != bool(values_raw):
        raise RuntimeError(
            "MILLPOND_FILTER_VALUES must be set together with "
            "MILLPOND_FILTER_KEEP_FIELD_NAME (or MILLPOND_FILTER_DROP_FIELD_NAME)"
        )

    if active is None:
        return None, None, None

    if not _SAFE_COLUMN_NAME.match(active):
        raise RuntimeError(
            f"Filter field name {active!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)"
        )

    if drop:
        # Reserved for a future change; rejecting explicitly today keeps the
        # env-var namespace and the keep/drop mutual-exclusion contract
        # stable so a later commit can flip the implementation on without
        # any operator-facing config rename.
        raise RuntimeError("MILLPOND_FILTER_DROP_FIELD_NAME is reserved for a future release; not implemented yet")

    return keep, None, _parse_filter_values(values_raw)


def _load_sort_by() -> tuple[str, ...] | None:
    """Parse MILLPOND_SORT_BY into a tuple of column names.

    Comma-separated; whitespace trimmed; empty tokens dropped. Each name
    must match the safe-identifier pattern (`[a-zA-Z_][a-zA-Z0-9_]*`) so
    a misconfiguration surfaces at startup, not at the first flush.
    Returns None when the env var is absent or whitespace-only.
    """
    raw = os.environ.get("MILLPOND_SORT_BY", "").strip()
    if not raw:
        return None
    fields = tuple(t.strip() for t in raw.split(",") if t.strip())
    if not fields:
        return None
    for field in fields:
        if not _SAFE_COLUMN_NAME.match(field):
            raise RuntimeError(
                f"MILLPOND_SORT_BY field {field!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)"
            )
    return fields


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

    icebox_kwargs: dict[str, str | int | None] = dict.fromkeys(
        (
            "icebox_bucket",
            "icebox_warehouse_prefix",
            "icebox_pg_host",
            "icebox_pg_port",
            "icebox_pg_database",
            "icebox_pg_username",
            "icebox_pg_password",
            "icebox_pg_schema",
            "icebox_pg_sslmode",
        ),
        None,
    )

    if destination == "ducklake":
        ducklake_kwargs.update(_load_ducklake_fields())
        table_label_part = ducklake_kwargs["ducklake_table"]
    elif destination == "iceberg":
        iceberg_kwargs.update(_load_iceberg_fields())
        table_label_part = iceberg_kwargs["iceberg_table"]
    else:  # icebox: writer ships parquet to S3 + INSERTs to icebox_files
        # Icebox writers still need to know the Iceberg target (namespace
        # + table) so the deterministic file path matches what the daemon
        # registers. The catalog handle itself lives on the daemon side.
        iceberg_kwargs.update(_load_iceberg_fields())
        icebox_kwargs["icebox_bucket"] = _require("ICEBOX_BUCKET")
        icebox_kwargs["icebox_warehouse_prefix"] = _require("ICEBOX_WAREHOUSE_PREFIX")
        icebox_kwargs["icebox_pg_host"] = _require("ICEBOX_PG_HOST")
        icebox_kwargs["icebox_pg_port"] = int(os.environ.get("ICEBOX_PG_PORT", "5432"))
        icebox_kwargs["icebox_pg_database"] = _require("ICEBOX_PG_DATABASE")
        icebox_kwargs["icebox_pg_username"] = _require("ICEBOX_PG_USERNAME")
        icebox_kwargs["icebox_pg_password"] = _require("ICEBOX_PG_PASSWORD")
        icebox_kwargs["icebox_pg_schema"] = _require("ICEBOX_PG_SCHEMA")
        icebox_kwargs["icebox_pg_sslmode"] = os.environ.get("ICEBOX_PG_SSLMODE", "require")
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

    filter_keep_field, filter_drop_field, filter_values = _load_filter_fields()
    sort_by = _load_sort_by()

    cfg = Config(
        bootstrap_servers=_require("KAFKA_BOOTSTRAP_SERVERS"),
        topic=topic,
        group_id=group_id,
        replica_count=replica_count,
        ordinal=ordinal,
        destination=destination,
        **ducklake_kwargs,
        **iceberg_kwargs,
        **icebox_kwargs,
        flush_size=int(os.environ.get("FLUSH_SIZE", "104857600")),
        flush_interval_ms=int(os.environ.get("FLUSH_INTERVAL_MS", "60000")),
        fetch_min_bytes=int(os.environ.get("FETCH_MIN_BYTES", "1048576")),
        fetch_max_wait_ms=int(os.environ.get("FETCH_MAX_WAIT_MS", "500")),
        consume_batch_size=int(os.environ.get("CONSUME_BATCH_SIZE", "1000")),
        stats_interval_ms=int(os.environ.get("STATS_INTERVAL_MS", "5000")),
        broker_source=os.environ.get("BROKER_SOURCE", "").strip().lower(),
        filter_keep_field=filter_keep_field,
        filter_drop_field=filter_drop_field,
        filter_values=filter_values,
        sort_by=sort_by,
        kafka_config_overrides=kafka_overrides,
        # No ``MILLPOND_`` prefix on POSTHOG_PROJECT_TOKEN: it's a
        # PostHog-wide secret typically sourced from a shared K8s Secret
        # (the same one other PostHog SDKs consume), so the canonical
        # PostHog name is what operators expect to see.
        posthog_project_token=(os.environ.get("POSTHOG_PROJECT_TOKEN") or None),
        posthog_logs_endpoint=os.environ.get(
            "POSTHOG_LOGS_ENDPOINT",
            "https://us.i.posthog.com/i/v1/logs",
        ),
        service_namespace=os.environ.get("MILLPOND_SERVICE_NAMESPACE", "millpond"),
        service_instance_id=os.environ.get("MILLPOND_SERVICE_INSTANCE_ID") or None,
        service_version=os.environ.get("MILLPOND_SERVICE_VERSION", _default_service_version()),
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
    if cfg.filter_keep_field is not None:
        log.info("Filter (keep): %s in %s", cfg.filter_keep_field, cfg.filter_values)
    if cfg.sort_by is not None:
        log.info("Sort by: %s (ascending)", ", ".join(cfg.sort_by))
    return cfg
