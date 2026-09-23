import logging
import os
import re
from dataclasses import dataclass

from millpond import arrow_converter

# Read from the sink seam, NOT from millpond.schema: schema.py imports
# duckdb, and config.py is loaded by every pod including the hoglake
# ones, which have no DuckDB in their world at all.
from millpond.sink import SAFE_IDENTIFIER, VARIANT_COLUMN_SUFFIX

_SAFE_TABLE_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Shared with ducklake._validate_partition_expr — keep in sync or import from here.
SAFE_PARTITION_EXPR = re.compile(r"^[a-zA-Z0-9_(),\s]+$")

# Hoglake identifier rules, mirrored from the server's OpenAPI spec /
# schema CHECKs so a bad name fails at startup instead of as a 422 at
# the first flush. Namespace/table: ^[A-Za-z_][A-Za-z0-9_-]{0,127}$.
# Catalog names are stricter (lower-case start, max 63).
_HOGLAKE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}$")
_HOGLAKE_CATALOG_NAME = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")

# The server's partition-transform vocabulary (PartitionField.transform in
# the hoglake OpenAPI spec). NB: pyhoglake also implements `truncate`
# client-side, but the server does not accept it yet — refuse it here
# rather than 422 per commit.
_HOGLAKE_TRANSFORMS = ("identity", "year", "month", "day", "hour", "bucket")

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    # Which backend this pod writes to for its lifetime. `make_sink`
    # (millpond/sink.py) dispatches on it; load() validates membership in
    # _DESTINATIONS so an unknown value fails at startup, never at the
    # first flush.
    destination: str

    # Kafka
    bootstrap_servers: str
    topic: str
    group_id: str

    # Partition assignment
    replica_count: int
    ordinal: int

    # DuckLake destination — required when destination == "ducklake",
    # else None. Kept as `str | None` rather than a tagged union because
    # the load-time `if destination == ...` branch already enforces
    # presence of the right subset, and the Sink constructors raise
    # RuntimeError on missing fields (so `python -O` doesn't strip the
    # guards).
    ducklake_schema: str | None
    ducklake_table: str | None
    ducklake_data_path: str | None
    ducklake_connection: str | None
    rds_host: str | None
    rds_port: str | None
    rds_database: str | None
    rds_username: str | None
    rds_password: str | None
    partition_by: str | None  # e.g. "year(timestamp),month(timestamp),day(timestamp),hour(timestamp)"

    # DuckLake commit-retry budget. DuckLake's default is 10, which is not
    # enough when many writers commit against the same metadata catalog
    # concurrently — losers of snapshot-id allocation races burn through
    # 10 retries quickly and surface as PK collisions on
    # ducklake_snapshot_pkey. Loaded from DUCKLAKE_MAX_RETRY_COUNT with a
    # 100 default (the value DuckLake's own error message suggests).
    ducklake_max_retry_count: int | None

    # Optional DuckDB memory_limit (e.g. "6GB"). Unset, DuckDB budgets
    # ~80% of the cgroup limit and competes with the Arrow pending buffer
    # for RSS during the partitioned INSERT — at large FLUSH_SIZE the sum
    # can OOM the pod. Set, DuckDB spills to its temp dir beside the
    # on-disk database file (the /tmp emptyDir in k8s) instead. Loaded
    # from DUCKDB_MEMORY_LIMIT; None preserves the default behavior.
    duckdb_memory_limit: str | None

    # Hoglake destination — required when destination == "hoglake",
    # else None. The catalog control plane is addressed by URL; the
    # writer path (pyhoglake) writes parquet to object storage itself,
    # so it needs its own S3 credentials, independent of the DuckLake
    # DUCKDB_S3_* env vars (which ducklake.connect reads directly).
    hoglake_url: str | None
    hoglake_catalog: str | None
    hoglake_namespace: str | None
    hoglake_table: str | None
    # Optional: when set, a missing catalog is created with this data
    # path at first write. When unset, a missing catalog is a startup
    # error (catalog provisioning stays an ops decision).
    hoglake_data_path: str | None
    hoglake_s3_endpoint: str | None  # e.g. http://localhost:29000 for MinIO; None = AWS
    hoglake_s3_access_key: str | None
    hoglake_s3_secret_key: str | None
    hoglake_s3_region: str | None
    # Parsed HOGLAKE_PARTITION_BY: ordered (column, transform, param)
    # triples, validated against the server's transform vocabulary at
    # load() — an unmappable expression refuses startup, never a batch.
    hoglake_partition_by: tuple[tuple[str, str, int | None], ...] | None
    # Write-path retry budget for the hoglake destination — the
    # counterpart of ducklake_max_retry_count, and needed for the same
    # reason from the other direction: DuckLake retries internally
    # (100x) under millpond's 3 outer attempts, while hoglake retries
    # not at all, so those 3 attempts were the entire budget against a
    # catalog whose backpressure signal (503 + Retry-After: 1) assumes
    # the client will come back.
    hoglake_max_retry_count: int | None
    # Per-request HTTP timeout for the catalog client. pyhoglake's own
    # default is a hardcoded 30s with no way to change it from the
    # constructor's caller unless it is passed explicitly.
    hoglake_request_timeout_s: float | None

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

    # Optional record filters. Keep = allowlist (keep records whose value
    # in `filter_keep_field` is in `filter_values`, drop the rest). Drop =
    # denylist (drop records whose value in `filter_drop_field` is in
    # `filter_drop_values`, keep the rest). The directions COMPOSE: keep
    # runs first, drop refines the survivors — that's the point (the keep
    # side may be the CP-driven dynamic include set, while drop is a
    # static operator blacklist, e.g. muting one tenant's firehose share
    # during an incident). Each direction has its own values var so they
    # never share a list. Values are parsed at load time and homogeneous —
    # tuple of ints (if every comma-separated token parses as int) or of
    # strings (otherwise). main.py applies both after JSON→Arrow but
    # before the pending buffer. Failure semantics differ by direction
    # (see main.py): an unevaluable ALLOWLIST drops the batch (fail
    # closed); an unevaluable DENYLIST keeps it (fail open — a blacklist
    # that can't evaluate must not turn schema drift into data loss).
    filter_keep_field: str | None
    filter_drop_field: str | None
    filter_values: tuple[int, ...] | tuple[str, ...] | None
    filter_drop_values: tuple[int, ...] | tuple[str, ...] | None

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

    # Optional source column names to dual-write as DuckLake VARIANT columns.
    # Each listed column is kept as-is (typically VARCHAR JSON text) and also
    # written to `{name}{VARIANT_COLUMN_SUFFIX}` via
    # try_cast(try_cast(col AS JSON) AS VARIANT). DuckDB auto-shreds VARIANT
    # on Parquet write. None disables dual-write (default — existing consumers
    # are unaffected). See ducklake.write.
    variant_columns: tuple[str, ...] | None

    # Extra librdkafka config (from KAFKA_CONSUMER_* env vars)
    kafka_config_overrides: tuple[tuple[str, str], ...]

    # Port for the /metrics + /healthz + /readyz HTTP server. Lives here
    # rather than being read from the environment inside server.start():
    # a knob that bypasses config.py is invisible to the startup config
    # log and to every caller holding a Config. Defaulted (rather than
    # required) so the many Config(...) call sites in the tests keep
    # working — 8000 is the historical port charts and probes expect.
    http_port: int = 8000

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
        if self.destination == "hoglake":
            return self.hoglake_table or "unknown"
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


def _parse_filter_values(raw: str) -> tuple[int, ...] | tuple[str, ...]:
    """Parse MILLPOND_FILTER_VALUES into a homogeneous tuple.

    Try int-tuple first; fall back to string-tuple if any token fails to
    parse. Whitespace is trimmed; empty tokens (e.g. trailing commas) are
    dropped. An empty input raises — callers should validate the env var
    is non-empty before calling.
    """
    tokens = tuple(t.strip() for t in raw.split(",") if t.strip())
    if not tokens:
        # Both directions parse through here; name both vars so a 3am
        # operator debugging the drop side isn't misdirected to the keep var.
        raise RuntimeError("MILLPOND_FILTER_VALUES / MILLPOND_FILTER_DROP_VALUES must contain at least one value")
    try:
        return tuple(int(t) for t in tokens)
    except ValueError:
        return tokens


def _load_filter_fields() -> tuple[
    str | None,
    str | None,
    tuple[int, ...] | tuple[str, ...] | None,
    tuple[int, ...] | tuple[str, ...] | None,
]:
    """Read MILLPOND_FILTER_{KEEP,DROP}_FIELD_NAME + their values vars.

    Keep pairs with MILLPOND_FILTER_VALUES (unchanged contract); drop
    pairs with MILLPOND_FILTER_DROP_VALUES. The directions may be set
    together (keep ∩ ¬drop) or alone; each field-with-values pairing is
    enforced so a half-configured filter surfaces at startup rather than
    silently passing everything. Field names must be safe identifiers.

    Note: the original drop reservation shared MILLPOND_FILTER_VALUES.
    Implemented drop uses its OWN values var instead — sharing one list
    between an allowlist and a denylist would make the composed case
    (CP-driven keep + operator blacklist) unexpressible.
    """
    keep = os.environ.get("MILLPOND_FILTER_KEEP_FIELD_NAME", "").strip() or None
    drop = os.environ.get("MILLPOND_FILTER_DROP_FIELD_NAME", "").strip() or None
    values_raw = os.environ.get("MILLPOND_FILTER_VALUES", "").strip()
    drop_values_raw = os.environ.get("MILLPOND_FILTER_DROP_VALUES", "").strip()

    if bool(keep) != bool(values_raw):
        raise RuntimeError("MILLPOND_FILTER_VALUES must be set together with MILLPOND_FILTER_KEEP_FIELD_NAME")
    if bool(drop) != bool(drop_values_raw):
        raise RuntimeError("MILLPOND_FILTER_DROP_VALUES must be set together with MILLPOND_FILTER_DROP_FIELD_NAME")

    for field in (keep, drop):
        if field is not None and not SAFE_IDENTIFIER.match(field):
            raise RuntimeError(
                f"Filter field name {field!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)"
            )

    return (
        keep,
        drop,
        _parse_filter_values(values_raw) if keep else None,
        _parse_filter_values(drop_values_raw) if drop else None,
    )


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


def _require_safe_column(env_name: str, name: str) -> None:
    """Raise at startup when a configured column name is unsafe for generated SQL.

    Uses schema.SAFE_IDENTIFIER — the same gate the write path applies per
    field — so a name accepted here can never be silently skipped at flush time.
    """
    if not SAFE_IDENTIFIER.match(name):
        raise RuntimeError(f"{env_name} column {name!r} contains unsafe characters (must match [a-zA-Z_][a-zA-Z0-9_]*)")


def _parse_column_list(env_name: str) -> tuple[str, ...] | None:
    """Parse a comma-separated env var into an ordered tuple of column names.

    Whitespace trimmed; empty tokens dropped; duplicates de-duplicated (first
    wins). Each name must match the safe-identifier pattern so a
    misconfiguration surfaces at startup, not at the first flush. Returns None
    when the env var is absent or whitespace-only.
    """
    raw = os.environ.get(env_name, "").strip()
    if not raw:
        return None
    seen: set[str] = set()
    cols: list[str] = []
    for token in raw.split(","):
        name = token.strip()
        if not name:
            continue
        _require_safe_column(env_name, name)
        if name in seen:
            continue
        seen.add(name)
        cols.append(name)
    if not cols:
        return None
    return tuple(cols)


def _load_sort_by() -> tuple[str, ...] | None:
    """Parse MILLPOND_SORT_BY into a tuple of column names."""
    return _parse_column_list("MILLPOND_SORT_BY")


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
        _require_safe_column("MILLPOND_TYPED_COLUMNS", name)
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


def _load_variant_columns() -> tuple[str, ...] | None:
    """Parse MILLPOND_VARIANT_COLUMNS into an ordered tuple of source column names.

    Format: comma-separated column names, e.g. ``properties,person_properties``.
    Whitespace trimmed; empty tokens dropped; duplicates de-duplicated (first
    wins). Column names must match the safe-identifier pattern — each source is
    dual-written to ``{name}{VARIANT_COLUMN_SUFFIX}`` via generated SQL, so
    injection-safe identifiers are mandatory. Returns None when the env var is
    absent/whitespace.
    """
    cols = _parse_column_list("MILLPOND_VARIANT_COLUMNS")
    if cols is None:
        return None
    for name in cols:
        # Reject names that already end in the dual-write suffix — the derived
        # column would be ``foo_variant_variant``, which is almost always a
        # misconfiguration (operator listed the sink column, not the source).
        # Case-insensitive: DuckDB resolves identifiers case-insensitively, so
        # ``properties_VARIANT`` names the same sink column.
        if name.lower().endswith(VARIANT_COLUMN_SUFFIX):
            raise RuntimeError(
                f"MILLPOND_VARIANT_COLUMNS column {name!r} already ends with "
                f"{VARIANT_COLUMN_SUFFIX!r}; list the source JSON/VARCHAR column "
                f"(e.g. 'properties'), not the derived VARIANT column name"
            )
    return cols


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
        "duckdb_memory_limit": os.environ.get("DUCKDB_MEMORY_LIMIT") or None,
    }


# The full env-var surface of the inactive destination is nulled rather
# than loaded so stray vars from the other backend can never affect a
# deployment (mirrors the stray-ICEBERG_* posture after that removal).
_NONE_DUCKLAKE_FIELDS: dict = dict.fromkeys(
    (
        "ducklake_schema",
        "ducklake_table",
        "ducklake_data_path",
        "ducklake_connection",
        "rds_host",
        "rds_port",
        "rds_database",
        "rds_username",
        "rds_password",
        "partition_by",
        "ducklake_max_retry_count",
        "duckdb_memory_limit",
    )
)
_NONE_HOGLAKE_FIELDS: dict = dict.fromkeys(
    (
        "hoglake_url",
        "hoglake_catalog",
        "hoglake_namespace",
        "hoglake_table",
        "hoglake_data_path",
        "hoglake_s3_endpoint",
        "hoglake_s3_access_key",
        "hoglake_s3_secret_key",
        "hoglake_s3_region",
        "hoglake_partition_by",
        "hoglake_max_retry_count",
        "hoglake_request_timeout_s",
    )
)


def _split_top_level_commas(raw: str) -> list[str]:
    """Split on commas outside parentheses, so `bucket(team_id, 16)` stays
    one entry. Blank entries are dropped (trailing commas tolerated)."""
    entries: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in raw:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            entries.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    entries.append("".join(current).strip())
    return [e for e in entries if e]


_HOGLAKE_PARTITION_CALL = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(.*?)\s*\)$")


def _parse_hoglake_partition_by(raw: str) -> tuple[tuple[str, str, int | None], ...]:
    """Parse HOGLAKE_PARTITION_BY into ordered (column, transform, param)
    triples against the server's transform vocabulary.

    Grammar (comma-separated entries):
      - `col`                → identity(col)
      - `identity(col)`      → identity(col)
      - `year(col)` / `month(col)` / `day(col)` / `hour(col)`
      - `bucket(col, N)`     → bucket with a positive integer param

    Everything else is refused HERE, at startup, with the vocabulary in
    the message — a partition spec the server would 422 must never make
    it to the first flush. Column names are held to SAFE_IDENTIFIER (the
    same gate the write path applies per field).
    """

    def _bad(entry: str, why: str) -> RuntimeError:
        return RuntimeError(
            f"HOGLAKE_PARTITION_BY entry {entry!r} {why}; supported forms: "
            f"col | transform(col) | bucket(col, N) with transforms "
            f"{', '.join(_HOGLAKE_TRANSFORMS)}"
        )

    entries = _split_top_level_commas(raw)
    if not entries:
        raise RuntimeError("HOGLAKE_PARTITION_BY is set but contains no entries")

    fields: list[tuple[str, str, int | None]] = []
    seen: set[tuple[str, str, int | None]] = set()
    for entry in entries:
        call = _HOGLAKE_PARTITION_CALL.match(entry)
        if call is None:
            if "(" in entry or ")" in entry:
                raise _bad(entry, "is malformed")
            transform, column, param = "identity", entry, None
        else:
            transform = call.group(1).lower()
            args = [a.strip() for a in call.group(2).split(",")] if call.group(2) else []
            if transform not in _HOGLAKE_TRANSFORMS:
                raise _bad(entry, f"uses unknown transform {transform!r}")
            if transform == "bucket":
                if len(args) != 2:
                    raise _bad(entry, "must be bucket(col, N)")
                column = args[0]
                try:
                    param = int(args[1])
                except ValueError:
                    raise _bad(entry, f"has non-integer bucket count {args[1]!r}") from None
                if param <= 0:
                    raise _bad(entry, f"has non-positive bucket count {param}")
            else:
                if len(args) != 1 or not args[0]:
                    raise _bad(entry, f"must be {transform}(col)")
                column, param = args[0], None
        if not SAFE_IDENTIFIER.match(column):
            raise _bad(entry, f"has unsafe column name {column!r} (must match [a-zA-Z_][a-zA-Z0-9_]*)")
        triple = (column, transform, param)
        if triple in seen:
            raise _bad(entry, "is duplicated")
        seen.add(triple)
        fields.append(triple)
    return tuple(fields)


# The liveness deadline the write path has to fit inside:
# server.HealthState.max_poll_age_s. record_poll() runs only between
# consume() calls, so every second a flush spends retrying is a second
# the probe sees no poll.
_LIVENESS_BUDGET_S = 480.0
# main.py's backoff ladder: base 1s, doubling, capped at 30s a step,
# plus up to 25% jitter on each step.
_BACKOFF_BASE_S = 1.0
_BACKOFF_CAP_S = 30.0
_BACKOFF_JITTER = 0.25


def _hoglake_worst_case_flush_s(max_retries: int, timeout_s: float) -> float:
    """Longest a single sink.write() can take: every attempt spending its
    full request timeout, with the whole backoff ladder between them."""
    ladder = sum(min(_BACKOFF_BASE_S * (2**attempt), _BACKOFF_CAP_S) for attempt in range(max(0, max_retries - 1)))
    return max_retries * timeout_s + ladder * (1 + _BACKOFF_JITTER)


def _check_hoglake_liveness_budget(max_retries: int, timeout_s: float) -> None:
    """Refuse a retry budget that can outlive the liveness deadline.

    HOGLAKE_MAX_RETRY_COUNT was unbounded while its interaction with
    liveness was documented in a comment — so the documented trap was
    one values-file edit away, and springing it looks like a pod
    SIGKILLed mid-flush with no explanation in its own logs. The
    arithmetic that comment describes is now the check.
    """
    worst = _hoglake_worst_case_flush_s(max_retries, timeout_s)
    if worst <= _LIVENESS_BUDGET_S:
        return
    raise RuntimeError(
        f"HOGLAKE_MAX_RETRY_COUNT={max_retries} with HOGLAKE_REQUEST_TIMEOUT_S={timeout_s} allows a "
        f"single flush to spend up to {worst:.0f}s inside sink.write(), past the {_LIVENESS_BUDGET_S:.0f}s "
        f"liveness deadline (server.HealthState.max_poll_age_s): the consume loop is single threaded, so "
        f"the pod would be killed mid-flush rather than crashing with an error. Lower either knob."
    )


_S3_URI = re.compile(r"^s3://[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9](/.*)?$")


def _hoglake_data_path() -> str | None:
    """HOGLAKE_DATA_PATH, validated as an s3:// URI.

    This value is only read when the catalog does not exist yet, and
    then it is FROZEN into the catalog row as the root every data file
    of every table under it is written beneath. The server has no
    delete-catalog route, so a typo does not fail — it mints a
    permanently unusable catalog under a name the operator now cannot
    reuse, and the first sign of it is an S3 error on the first flush.
    Cheap to check here; impossible to undo there.
    """
    raw = os.environ.get("HOGLAKE_DATA_PATH", "").strip()
    if not raw:
        return None
    if not _S3_URI.match(raw):
        raise RuntimeError(
            f"HOGLAKE_DATA_PATH {raw!r} is not an s3:// URI (expected s3://bucket/prefix/). It is "
            f"frozen into the catalog when millpond creates it and hoglake has no route to delete a "
            f"catalog, so a typo here mints a permanently unusable catalog under that name."
        )
    return raw


def _load_hoglake_fields() -> dict:
    """Read the HOGLAKE_* env group; validate names against the server's
    identifier rules so misconfig fails at startup, not as a 422."""
    catalog = _require("HOGLAKE_CATALOG")
    if not _HOGLAKE_CATALOG_NAME.match(catalog):
        raise RuntimeError(
            f"HOGLAKE_CATALOG {catalog!r} is not a valid hoglake catalog name (must match [a-z][a-z0-9_-]{{0,62}})"
        )
    names = {}
    for env_name in ("HOGLAKE_NAMESPACE", "HOGLAKE_TABLE"):
        value = _require(env_name)
        if not _HOGLAKE_IDENTIFIER.match(value):
            raise RuntimeError(
                f"{env_name} {value!r} is not a valid hoglake identifier (must match [A-Za-z_][A-Za-z0-9_-]{{0,127}})"
            )
        names[env_name] = value

    # Retry budget + request timeout. Their product is most of the
    # worst-case time a single flush can spend inside sink.write(), and
    # the consume loop is single threaded: server.health marks the
    # process dead at max_poll_age_s=480 and record_poll only runs
    # between consume() calls. So the two knobs are not independent, and
    # _check_hoglake_liveness_budget refuses a combination that could
    # outlive the liveness deadline instead of leaving the pod to be
    # SIGKILLed mid-flush.
    #
    # The timeout default is 45s, not pyhoglake's 30s, because the
    # server's own commit-lock admission bound is 30s: at an equal
    # timeout the client gives up at the same instant the server would
    # have answered 503 + Retry-After, so its explicit backpressure
    # signal was nearly unreachable and surfaced as a transport-uncertain
    # failure instead — the one outcome that has to hold a prepared
    # payload and resend it blind.
    #
    # The defaults (8 attempts x 45s, plus a jittered ladder main.py caps
    # at 30s a step) come to ~474s of the 480s budget. That is the
    # all-eight-attempts-black-hole case and it is deliberately close to
    # the line: a catalog that has not answered a single request in eight
    # minutes is one this pod should be dying over. What the check
    # prevents is the same arithmetic going unnoticed when an operator
    # raises either knob.
    max_retries = _positive_int("HOGLAKE_MAX_RETRY_COUNT", 8)
    timeout_s = _positive_float("HOGLAKE_REQUEST_TIMEOUT_S", 45.0)
    _check_hoglake_liveness_budget(max_retries, timeout_s)

    partition_raw = os.environ.get("HOGLAKE_PARTITION_BY", "").strip()
    if not partition_raw and os.environ.get("DUCKLAKE_PARTITION_BY", "").strip():
        # The destination-flip trap. Every other stray var from the
        # inactive destination is genuinely harmless, which is why they
        # are nulled — but this one changes the shape of the DATA. An
        # operator who flips MILLPOND_DESTINATION on an existing values
        # file keeps DUCKLAKE_PARTITION_BY, the hoglake block nulls it,
        # and the pipeline that was partitioned yesterday creates an
        # unpartitioned hoglake table today with nothing in the logs.
        # Partitioning is also the one property that is painful to add
        # afterwards (existing files keep their vintage forever), so the
        # cost of the silent version is unusually high. Refuse and make
        # the intent explicit, exactly as MILLPOND_VARIANT_COLUMNS does.
        raise RuntimeError(
            "DUCKLAKE_PARTITION_BY is set but MILLPOND_DESTINATION=hoglake reads "
            "HOGLAKE_PARTITION_BY, which is unset — this pipeline would create an "
            "UNPARTITIONED hoglake table. Set HOGLAKE_PARTITION_BY (the same expression "
            "style, restricted to identity/year/month/day/hour/bucket), or remove "
            "DUCKLAKE_PARTITION_BY to confirm the table is meant to be unpartitioned."
        )
    return {
        "hoglake_max_retry_count": max_retries,
        "hoglake_request_timeout_s": timeout_s,
        "hoglake_url": _require("HOGLAKE_URL"),
        "hoglake_catalog": catalog,
        "hoglake_namespace": names["HOGLAKE_NAMESPACE"],
        "hoglake_table": names["HOGLAKE_TABLE"],
        "hoglake_data_path": _hoglake_data_path(),
        "hoglake_s3_endpoint": os.environ.get("HOGLAKE_S3_ENDPOINT", "").strip() or None,
        "hoglake_s3_access_key": _require("HOGLAKE_S3_ACCESS_KEY"),
        "hoglake_s3_secret_key": _require("HOGLAKE_S3_SECRET_KEY"),
        "hoglake_s3_region": os.environ.get("HOGLAKE_S3_REGION", "").strip() or None,
        "hoglake_partition_by": _parse_hoglake_partition_by(partition_raw) if partition_raw else None,
    }


def _positive_int(env_name: str, default: int) -> int:
    """A positive-integer env knob, or its default. Zero is refused for
    the same reason DUCKLAKE_MAX_RETRY_COUNT refuses it: an operator
    misrendering an unset value as "0" should fail loudly, not silently
    deploy a pipeline with no retries at all."""
    raw = os.environ.get(env_name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise RuntimeError(f"{env_name}={raw!r} must be a positive integer") from None
    if value <= 0:
        raise RuntimeError(f"{env_name}={value!r} must be a positive integer")
    return value


def _positive_float(env_name: str, default: float) -> float:
    raw = os.environ.get(env_name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise RuntimeError(f"{env_name}={raw!r} must be a positive number") from None
    if value <= 0:
        raise RuntimeError(f"{env_name}={value!r} must be a positive number")
    return value


def _load_http_port() -> int:
    """MILLPOND_HTTP_PORT, 8000 by default (the historical port — charts
    and probes depend on it). The override exists for test harnesses
    running millpond as a host process next to other services."""
    raw = os.environ.get("MILLPOND_HTTP_PORT", "").strip() or "8000"
    try:
        port = int(raw)
    except ValueError:
        raise RuntimeError(f"MILLPOND_HTTP_PORT {raw!r} is not an integer") from None
    if not 0 <= port <= 65535:
        raise RuntimeError(f"MILLPOND_HTTP_PORT {port} is out of range (0-65535; 0 = ephemeral)")
    return port


def _default_group_id(destination: str, topic: str, dest_table: str) -> str:
    """Default Kafka group id (offset storage only — millpond assigns
    partitions statically by pod ordinal).

    The DuckLake form is frozen: `millpond-{topic}-{table}` is where every
    deployed pipeline's offsets already live, and changing it would replay
    the whole retention window on the next rollout. Every OTHER destination
    carries its name in the id, so a shadow deployment — same topic, same
    table name, different destination, which is exactly how a migration is
    canaried — cannot share an offset namespace with the pipeline it
    shadows. Sharing one would not duplicate work, it would SPLIT it: both
    pods commit into the same `__consumer_offsets` keys and each ends up
    writing part of the stream.
    """
    if destination == "ducklake":
        return f"millpond-{topic}-{dest_table}"
    return f"millpond-{destination}-{topic}-{dest_table}"


_DESTINATIONS = ("ducklake", "hoglake")


def load() -> Config:
    topic = _require("KAFKA_TOPIC")

    # Reject unknown destinations loudly so a pod deployed with stale
    # config fails at startup instead of silently writing to the wrong
    # place (the iceberg/icebox sinks were removed at tag final-iceberg).
    # Empty/whitespace-only values fall back to the default to tolerate
    # the helm-template gotcha where unset renders as "".
    destination = os.environ.get("MILLPOND_DESTINATION", "").strip().lower() or "ducklake"
    if destination not in _DESTINATIONS:
        raise RuntimeError(f"MILLPOND_DESTINATION {destination!r} must be one of: {', '.join(_DESTINATIONS)}")

    pod_name = os.environ.get("POD_NAME") or os.environ.get("HOSTNAME", "millpond-0")
    ordinal = _parse_ordinal(pod_name)
    replica_count = int(_require("REPLICA_COUNT"))

    if ordinal >= replica_count:
        raise RuntimeError(f"Ordinal {ordinal} >= REPLICA_COUNT {replica_count}")

    # Load only the active destination's env group; the inactive one is
    # all-None so stray vars from the other backend can never leak in.
    if destination == "hoglake":
        destination_fields = {**_NONE_DUCKLAKE_FIELDS, **_load_hoglake_fields()}
        dest_table = destination_fields["hoglake_table"]
    else:
        destination_fields = {**_NONE_HOGLAKE_FIELDS, **_load_ducklake_fields()}
        dest_table = destination_fields["ducklake_table"]
    group_id = os.environ.get("GROUP_ID") or _default_group_id(destination, topic, dest_table)

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

    filter_keep_field, filter_drop_field, filter_values, filter_drop_values = _load_filter_fields()
    sort_by = _load_sort_by()
    typed_columns = _load_typed_columns()
    variant_columns = _load_variant_columns()
    if destination == "hoglake" and variant_columns is not None:
        # Hoglake has no VARIANT column type; the DuckLake dual-write
        # feature cannot port. Refuse loudly rather than silently
        # skipping the companions — silent config no-ops are how mixed
        # fleets rot. Revisit when hoglake grows a variant/json path
        # millpond can target.
        raise RuntimeError(
            "MILLPOND_VARIANT_COLUMNS is not supported with MILLPOND_DESTINATION=hoglake "
            "(hoglake has no VARIANT column type; events land as text). Remove the "
            "variant config or use the ducklake destination."
        )

    cfg = Config(
        destination=destination,
        bootstrap_servers=_require("KAFKA_BOOTSTRAP_SERVERS"),
        topic=topic,
        group_id=group_id,
        replica_count=replica_count,
        ordinal=ordinal,
        **destination_fields,
        flush_size=int(os.environ.get("FLUSH_SIZE", "104857600")),
        flush_interval_ms=int(os.environ.get("FLUSH_INTERVAL_MS", "60000")),
        fetch_min_bytes=int(os.environ.get("FETCH_MIN_BYTES", "1048576")),
        fetch_max_wait_ms=int(os.environ.get("FETCH_MAX_WAIT_MS", "500")),
        consume_batch_size=int(os.environ.get("CONSUME_BATCH_SIZE", "1000")),
        stats_interval_ms=int(os.environ.get("STATS_INTERVAL_MS", "5000")),
        auto_offset_reset=_load_auto_offset_reset(),
        broker_source=os.environ.get("BROKER_SOURCE", "").strip().lower(),
        http_port=_load_http_port(),
        filter_keep_field=filter_keep_field,
        filter_drop_field=filter_drop_field,
        filter_values=filter_values,
        filter_drop_values=filter_drop_values,
        **_load_include_values_config(filter_keep_field, filter_values),
        sort_by=sort_by,
        typed_columns=typed_columns,
        variant_columns=variant_columns,
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
        "Config: destination=%s topic=%s schema=%s table=%s ordinal=%d/%d group_id=%s",
        destination,
        topic,
        cfg.ducklake_schema if destination == "ducklake" else cfg.hoglake_namespace,
        cfg.table_label,
        ordinal,
        replica_count,
        cfg.group_id,
    )
    if cfg.hoglake_partition_by is not None:
        log.info(
            "Hoglake partition spec: %s",
            ", ".join(f"{t}({c}{', ' + str(p) if p is not None else ''})" for c, t, p in cfg.hoglake_partition_by),
        )
    if cfg.filter_keep_field is not None:
        log.info("Filter (keep): %s in %s", cfg.filter_keep_field, cfg.filter_values)
    if cfg.filter_drop_field is not None:
        log.info("Filter (drop): %s in %s", cfg.filter_drop_field, cfg.filter_drop_values)
    if cfg.sort_by is not None:
        log.info("Sort by: %s (ascending)", ", ".join(cfg.sort_by))
    if cfg.typed_columns is not None:
        log.info("Coerce typed columns: %s", ", ".join(f"{n}:{t}" for n, t in cfg.typed_columns))
    if cfg.variant_columns is not None:
        log.info(
            "Dual-write VARIANT columns: %s",
            ", ".join(f"{n} -> {n}{VARIANT_COLUMN_SUFFIX}" for n in cfg.variant_columns),
        )
    return cfg
