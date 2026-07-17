import logging
import os
import re
from dataclasses import dataclass

from millpond import arrow_converter

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

    # DuckLake destination
    ducklake_schema: str
    ducklake_table: str
    ducklake_data_path: str
    ducklake_connection: str
    rds_host: str
    rds_port: str
    rds_database: str
    rds_username: str
    rds_password: str
    partition_by: str | None  # e.g. "year(timestamp),month(timestamp),day(timestamp),hour(timestamp)"

    # DuckLake commit-retry budget. DuckLake's default is 10, which is not
    # enough when many writers commit against the same metadata catalog
    # concurrently — losers of snapshot-id allocation races burn through
    # 10 retries quickly and surface as PK collisions on
    # ducklake_snapshot_pkey. Loaded from DUCKLAKE_MAX_RETRY_COUNT with a
    # 100 default (the value DuckLake's own error message suggests).
    ducklake_max_retry_count: int

    # Flush triggers
    flush_size: int  # bytes of accumulated Arrow data
    flush_interval_ms: int  # ms since last flush

    # Consumer tuning
    fetch_min_bytes: int
    fetch_max_wait_ms: int
    consume_batch_size: int
    stats_interval_ms: int
    # Applied only when no offset is committed for (group_id, partition).
    # "earliest" replays the whole retention window — appropriate for catch-up
    # / backfill consumers. "latest" starts at the head — appropriate for NRT
    # consumers (filter-keep workflows) where backlog replay is wasted I/O.
    auto_offset_reset: str

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

    # Optional dynamic source for the keep-filter's include set (see
    # include_values.py). URL unset = today's static behavior. Mode
    # "shadow" (default) polls and reports diff metrics while the static
    # list stays authoritative; "authoritative" makes the polled set the
    # live filter, with the static list as the bootstrap/fallback seed.
    # The auth header is generic (name + token) — nothing here knows what
    # the endpoint is.
    include_values_url: str | None
    include_values_mode: str
    include_values_poll_interval_s: float
    include_values_removal_polls: int
    include_values_request_timeout_s: float
    include_values_startup_timeout_s: float
    include_values_auth_header_name: str | None
    include_values_auth_token: str | None

    # Optional pre-write sort. Tuple of column names; sort is ascending
    # in tuple order. Applied to the consolidated batch right before
    # sink.write(). None disables the sort entirely.
    sort_by: tuple[str, ...] | None

    # Optional (column_name, target_type) pairs to pin to a target type before
    # write (see arrow_converter.coerce_typed_columns). JSON carries no type
    # schema, so inference can diverge from the destination column and wedge
    # DuckLake's widening-only schema evolution — date-times infer VARCHAR vs a
    # TIMESTAMPTZ column, and an all-null `project_id` infers VARCHAR vs BIGINT
    # (writing NRT events into the duckling backfill's typed `posthog.events`).
    # None disables coercion (the default — every existing consumer that owns its
    # own freshly-created table is unaffected).
    typed_columns: tuple[tuple[str, str], ...] | None

    # Extra librdkafka config (from KAFKA_CONSUMER_* env vars)
    kafka_config_overrides: tuple[tuple[str, str], ...]

    # Optional PostHog Logs export via OTLP/HTTP. ON when
    # ``posthog_project_token`` is set, OFF otherwise. Endpoint
    # defaults to the US PostHog Cloud ingress; override for EU or
    # self-hosted PostHog. service_namespace + service_instance_id
    # feed the OTLP resource attrs (see millpond/structured_logging.py).
    # service_instance_id is typically the consumer-key the chart uses
    # for the StatefulSet name, e.g. "events".
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


def _load_include_values_config(
    filter_keep_field: str | None,
    filter_values: tuple[int, ...] | tuple[str, ...] | None,
) -> dict:
    """Read the MILLPOND_INCLUDE_VALUES_* group and validate it against the
    static filter config. Startup-refusal beats runtime surprise:

    - a URL without an active keep-filter has nothing to feed;
    - MODE (or auth) without a URL means the operator INTENDED a dynamic
      source and a typo'd/missing URL would silently degrade to static —
      refuse rather than run on the wrong set;
    - shadow mode without static values has nothing to diff against;
    - a lone auth header name or token is always a misconfiguration.
    """
    url = os.environ.get("MILLPOND_INCLUDE_VALUES_URL", "").strip() or None
    mode_raw = os.environ.get("MILLPOND_INCLUDE_VALUES_MODE", "").strip().lower()
    mode = mode_raw or "shadow"
    header_name = os.environ.get("MILLPOND_INCLUDE_VALUES_AUTH_HEADER_NAME", "").strip() or None
    token = os.environ.get("MILLPOND_INCLUDE_VALUES_AUTH_TOKEN", "").strip() or None

    if url is None:
        if mode_raw:
            raise RuntimeError(
                "MILLPOND_INCLUDE_VALUES_MODE is set but MILLPOND_INCLUDE_VALUES_URL is not — "
                "a dynamic source was intended; refusing to silently run static-only"
            )
        if header_name or token:
            raise RuntimeError("MILLPOND_INCLUDE_VALUES_AUTH_* requires MILLPOND_INCLUDE_VALUES_URL")
        return dict(
            include_values_url=None,
            include_values_mode="static",
            include_values_poll_interval_s=60.0,
            include_values_removal_polls=5,
            include_values_request_timeout_s=10.0,
            include_values_startup_timeout_s=60.0,
            include_values_auth_header_name=None,
            include_values_auth_token=None,
        )

    if mode not in ("shadow", "authoritative"):
        raise RuntimeError(f"MILLPOND_INCLUDE_VALUES_MODE must be 'shadow' or 'authoritative', got {mode!r}")
    if filter_keep_field is None:
        raise RuntimeError("MILLPOND_INCLUDE_VALUES_URL requires MILLPOND_FILTER_KEEP_FIELD_NAME to be set")
    if mode == "shadow" and filter_values is None:
        raise RuntimeError("MILLPOND_INCLUDE_VALUES_MODE=shadow requires static MILLPOND_FILTER_VALUES to diff against")
    if bool(header_name) != bool(token):
        raise RuntimeError(
            "MILLPOND_INCLUDE_VALUES_AUTH_HEADER_NAME and MILLPOND_INCLUDE_VALUES_AUTH_TOKEN must be set together"
        )

    def _parse_number(env_name: str, default: str, cast):
        raw = os.environ.get(env_name, default)
        try:
            return cast(raw)
        except ValueError:
            raise RuntimeError(f"{env_name} must be a number, got {raw!r}") from None

    poll_interval = _parse_number("MILLPOND_INCLUDE_VALUES_POLL_INTERVAL_S", "60", float)
    removal_polls = _parse_number("MILLPOND_INCLUDE_VALUES_REMOVAL_POLLS", "5", int)
    request_timeout = _parse_number("MILLPOND_INCLUDE_VALUES_REQUEST_TIMEOUT_S", "10", float)
    startup_timeout = _parse_number("MILLPOND_INCLUDE_VALUES_STARTUP_TIMEOUT_S", "60", float)
    if poll_interval <= 0:
        raise RuntimeError("MILLPOND_INCLUDE_VALUES_POLL_INTERVAL_S must be positive")
    if removal_polls < 1:
        raise RuntimeError("MILLPOND_INCLUDE_VALUES_REMOVAL_POLLS must be >= 1")
    if removal_polls == 1:
        log.warning(
            "MILLPOND_INCLUDE_VALUES_REMOVAL_POLLS=1 disables removal damping — a single poll "
            "omitting a value removes it immediately"
        )
    if request_timeout <= 0 or startup_timeout <= 0:
        raise RuntimeError("MILLPOND_INCLUDE_VALUES_*_TIMEOUT_S must be positive")

    return dict(
        include_values_url=url,
        include_values_mode=mode,
        include_values_poll_interval_s=poll_interval,
        include_values_removal_polls=removal_polls,
        include_values_request_timeout_s=request_timeout,
        include_values_startup_timeout_s=startup_timeout,
        include_values_auth_header_name=header_name,
        include_values_auth_token=token,
    )


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


def _load_typed_columns() -> tuple[tuple[str, str], ...] | None:
    """Parse MILLPOND_TYPED_COLUMNS into ordered (column_name, type_name) pairs.

    Format: comma-separated ``column:type`` entries, e.g.
    ``timestamp:timestamptz,project_id:bigint``. Whitespace trimmed; empty tokens
    dropped; type names lower-cased. Column names must match the safe-identifier
    pattern and type names must be in arrow_converter.COERCIBLE_TYPES, so a
    misconfiguration surfaces at startup not at the first flush. A column listed
    twice with the same type is de-duplicated; listed twice with conflicting
    types is a startup error. Returns None when the env var is absent/whitespace.

    For re-pointing a consumer at the duckling backfill's events table, pin the
    eight TIMESTAMPTZ columns and project_id:
    timestamp,created_at,person_created_at,group0..4_created_at -> timestamptz;
    project_id -> bigint.
    """
    raw = os.environ.get("MILLPOND_TYPED_COLUMNS", "").strip()
    if not raw:
        return None

    seen: dict[str, str] = {}
    pairs: list[tuple[str, str]] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        if ":" not in token:
            raise RuntimeError(f"MILLPOND_TYPED_COLUMNS entry {token!r} must be 'column:type'")
        name, _, type_name = token.partition(":")
        name, type_name = name.strip(), type_name.strip().lower()
        if not _SAFE_COLUMN_NAME.match(name):
            raise RuntimeError(
                f"MILLPOND_TYPED_COLUMNS column {name!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)"
            )
        if type_name not in arrow_converter.COERCIBLE_TYPES:
            raise RuntimeError(
                f"MILLPOND_TYPED_COLUMNS type {type_name!r} for column {name!r} must be one of "
                f"{sorted(arrow_converter.COERCIBLE_TYPES)}"
            )
        if name in seen:
            if seen[name] != type_name:
                raise RuntimeError(f"MILLPOND_TYPED_COLUMNS maps {name!r} to both {seen[name]!r} and {type_name!r}")
            continue
        seen[name] = type_name
        pairs.append((name, type_name))

    if not pairs:
        return None
    return tuple(pairs)


_VALID_AUTO_OFFSET_RESET = ("earliest", "latest")


def _load_auto_offset_reset() -> str:
    """Parse KAFKA_AUTO_OFFSET_RESET (default: 'earliest' for back-compat).

    librdkafka also accepts 'error', but for a streaming sink that's never
    what you want — every fresh consumer group would crash. Restrict to the
    two policies that make sense in this codebase.
    """
    raw = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest").strip().lower()
    if raw not in _VALID_AUTO_OFFSET_RESET:
        raise RuntimeError(f"KAFKA_AUTO_OFFSET_RESET={raw!r} must be one of {_VALID_AUTO_OFFSET_RESET}")
    return raw


def _load_ducklake_fields() -> dict[str, str | None]:
    ducklake_table = _require("DUCKLAKE_TABLE")
    if not _SAFE_TABLE_NAME.match(ducklake_table):
        raise RuntimeError(
            f"DUCKLAKE_TABLE {ducklake_table!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)"
        )

    # Default `main` preserves the historical DuckDB schema for any
    # millpond instance that doesn't set DUCKLAKE_SCHEMA. Schema
    # identifiers follow the same DuckDB SQL-identifier rules as table
    # names, so we reuse the table-name regex rather than introducing a
    # second pattern.
    ducklake_schema = os.environ.get("DUCKLAKE_SCHEMA", "").strip() or "main"
    if not _SAFE_TABLE_NAME.match(ducklake_schema):
        raise RuntimeError(
            f"DUCKLAKE_SCHEMA {ducklake_schema!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)"
        )

    partition_by = os.environ.get("DUCKLAKE_PARTITION_BY", "").strip() or None
    if partition_by and not SAFE_PARTITION_EXPR.match(partition_by):
        raise RuntimeError(
            f"DUCKLAKE_PARTITION_BY {partition_by!r} contains unsafe characters (must match [a-zA-Z0-9_(),\\s]+)"
        )

    # Reject 0 explicitly — DuckLake itself accepts 0 (no retries), but in
    # this codebase a 0 budget under multi-writer concurrency degenerates
    # straight to the PK-collision crash the default was raised to avoid.
    # An operator misrendering an unset env var as "0" should fail loudly.
    ducklake_max_retry_count = int(os.environ.get("DUCKLAKE_MAX_RETRY_COUNT", "100"))
    if ducklake_max_retry_count <= 0:
        raise RuntimeError(f"DUCKLAKE_MAX_RETRY_COUNT={ducklake_max_retry_count!r} must be a positive integer")

    return {
        "ducklake_schema": ducklake_schema,
        "ducklake_table": ducklake_table,
        "ducklake_data_path": _require("DUCKLAKE_DATA_PATH"),
        "ducklake_connection": _require("DUCKLAKE_CONNECTION"),
        "rds_host": _require("DUCKLAKE_RDS_HOST"),
        "rds_port": os.environ.get("DUCKLAKE_RDS_PORT", "5432"),
        "rds_database": os.environ.get("DUCKLAKE_RDS_DATABASE", "ducklake"),
        "rds_username": os.environ.get("DUCKLAKE_RDS_USERNAME", "ducklake"),
        "rds_password": _require("DUCKLAKE_RDS_PASSWORD"),
        "partition_by": partition_by,
        "ducklake_max_retry_count": ducklake_max_retry_count,
    }


def load() -> Config:
    topic = _require("KAFKA_TOPIC")

    # DuckLake is the only destination since the iceberg/icebox sink
    # removal (see the `final-iceberg` tag for the last commit with it).
    # Reject any other value loudly so a pod deployed with stale config
    # fails at startup instead of silently writing to the wrong place.
    # Empty/whitespace-only values fall back to the default to tolerate
    # the helm-template gotcha where unset renders as "".
    destination_raw = os.environ.get("MILLPOND_DESTINATION", "").strip().lower() or "ducklake"
    if destination_raw != "ducklake":
        raise RuntimeError(
            f"MILLPOND_DESTINATION {destination_raw!r} is not supported; "
            f"the iceberg/icebox sinks were removed (last shipped at tag final-iceberg)"
        )

    pod_name = os.environ.get("POD_NAME") or os.environ.get("HOSTNAME", "millpond-0")
    ordinal = _parse_ordinal(pod_name)
    replica_count = int(_require("REPLICA_COUNT"))

    if ordinal >= replica_count:
        raise RuntimeError(f"Ordinal {ordinal} >= REPLICA_COUNT {replica_count}")

    ducklake_fields = _load_ducklake_fields()
    group_id = os.environ.get("GROUP_ID", f"millpond-{topic}-{ducklake_fields['ducklake_table']}")

    # Collect KAFKA_CONSUMER_* env vars as librdkafka config overrides.
    # e.g. KAFKA_CONSUMER_SECURITY_PROTOCOL=SASL_SSL -> security.protocol=SASL_SSL
    _KAFKA_CONSUMER_PREFIX = "KAFKA_CONSUMER_"
    kafka_overrides = tuple(
        (k[len(_KAFKA_CONSUMER_PREFIX) :].lower().replace("_", "."), v)
        for k, v in os.environ.items()
        if k.startswith(_KAFKA_CONSUMER_PREFIX)
    )
    # auto.offset.reset has a dedicated env var with validation + an
    # earliest/latest allowlist. The KAFKA_CONSUMER_* passthrough would
    # silently lose to consumer.create()'s explicit `cfg.auto_offset_reset`
    # write, so an operator setting KAFKA_CONSUMER_AUTO_OFFSET_RESET=latest
    # would deploy thinking it set the policy and instead get the default.
    # Refuse the ambiguous configuration loudly at startup.
    if any(k == "auto.offset.reset" for k, _ in kafka_overrides):
        raise RuntimeError(
            "KAFKA_CONSUMER_AUTO_OFFSET_RESET is not honored — use KAFKA_AUTO_OFFSET_RESET "
            "(allowed: earliest, latest) so the value gets validated."
        )

    filter_keep_field, filter_drop_field, filter_values = _load_filter_fields()
    sort_by = _load_sort_by()
    typed_columns = _load_typed_columns()

    cfg = Config(
        bootstrap_servers=_require("KAFKA_BOOTSTRAP_SERVERS"),
        topic=topic,
        group_id=group_id,
        replica_count=replica_count,
        ordinal=ordinal,
        **ducklake_fields,
        flush_size=int(os.environ.get("FLUSH_SIZE", "104857600")),
        flush_interval_ms=int(os.environ.get("FLUSH_INTERVAL_MS", "60000")),
        fetch_min_bytes=int(os.environ.get("FETCH_MIN_BYTES", "1048576")),
        fetch_max_wait_ms=int(os.environ.get("FETCH_MAX_WAIT_MS", "500")),
        consume_batch_size=int(os.environ.get("CONSUME_BATCH_SIZE", "1000")),
        stats_interval_ms=int(os.environ.get("STATS_INTERVAL_MS", "5000")),
        auto_offset_reset=_load_auto_offset_reset(),
        broker_source=os.environ.get("BROKER_SOURCE", "").strip().lower(),
        filter_keep_field=filter_keep_field,
        filter_drop_field=filter_drop_field,
        filter_values=filter_values,
        **_load_include_values_config(filter_keep_field, filter_values),
        sort_by=sort_by,
        typed_columns=typed_columns,
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
        "Config: topic=%s schema=%s table=%s ordinal=%d/%d group_id=%s",
        topic,
        cfg.ducklake_schema,
        cfg.table_label,
        ordinal,
        replica_count,
        cfg.group_id,
    )
    if cfg.filter_keep_field is not None:
        log.info("Filter (keep): %s in %s", cfg.filter_keep_field, cfg.filter_values)
    if cfg.sort_by is not None:
        log.info("Sort by: %s (ascending)", ", ".join(cfg.sort_by))
    if cfg.typed_columns is not None:
        log.info("Coerce typed columns: %s", ", ".join(f"{n}:{t}" for n, t in cfg.typed_columns))
    return cfg
