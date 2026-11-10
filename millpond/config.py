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

    # The wait between those retries: DuckLake sleeps retry_wait_ms x
    # retry_backoff^n before retry n+1, with no cap. Its default backoff
    # (1.5) makes retry 22 alone sleep 8 minutes, past the liveness
    # deadline, so a writer that loses ~20 commit races in a row is
    # SIGKILLed mid-sleep. A flat wait (backoff 1.0) keeps the whole ladder
    # bounded: 100 retries x 250 ms = 25 s. Loaded from
    # DUCKLAKE_RETRY_WAIT_MS / DUCKLAKE_RETRY_BACKOFF and checked against
    # the liveness budget at load.
    ducklake_retry_wait_ms: int | None
    ducklake_retry_backoff: float | None

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
    # Static object-store keys, or BOTH None for the AWS default
    # credential chain. pyhoglake passes each key to pyarrow's
    # S3FileSystem only when it is not None, so "neither" is how the pod
    # authenticates with the ServiceAccount's web-identity token under
    # IRSA — the same model the hoglake server itself uses. Static keys
    # remain the MinIO shape (local dev, CI). load() refuses one without
    # the other; the endpoint and the region stay independent of both.
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
    # Parquet codec (and level) for every object a hoglake flush writes,
    # passed to `prepare_append_tables`. millpond defaults to zstd level 3
    # because pyhoglake does NOT: its default is pyarrow's, SNAPPY, which
    # measured ~4x larger on prod-us events. See _hoglake_compression.
    hoglake_compression: str | None
    hoglake_compression_level: int | None
    # Register every hoglake file with its column stats DEFERRED to the
    # server's hydrator (`prepare_append_tables(deferred_stats=True)`)
    # instead of shipped inline in the commit. See _hoglake_deferred_stats.
    hoglake_deferred_stats: bool | None
    # What a hoglake flush does with a row whose JSON text cannot be
    # encoded into a live `variant` column: `null` (the row lands with SQL
    # NULL in that column, counted) or `raise` (the flush fails). See
    # _hoglake_variant_on_invalid.
    hoglake_variant_on_invalid: str | None
    # Whether a pod whose variant codec resolved to pyhoglake's Python
    # encoder may start (MILLPOND_ALLOW_PYTHON_VARIANT_CODEC). Default
    # False: the Rust codec is a dependency, not an optimisation. See
    # _hoglake_allow_python_variant_codec.
    hoglake_allow_python_variant_codec: bool | None

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


def _require_uuid_filter_values(
    typed_columns: tuple[tuple[str, str], ...] | None,
    field: str | None,
    values: tuple[int, ...] | tuple[str, ...] | None,
    values_var: str,
) -> None:
    """Refuse a filter value that is not a UUID when its field is pinned `uuid`.

    A uuid-pinned filter column is compared on its 16 storage bytes, so every
    configured value has to parse as a UUID to become a comparable one. If one
    does not, the keep-filter drops the WHOLE batch every flush (an allowlist
    that cannot be evaluated fails closed) and the drop-filter stops denying
    anything (a denylist fails open) — both silent, both permanent, neither
    visible as anything but a throughput cliff. Startup is where that belongs.

    Only the STATIC values are reachable here. A dynamic include-values poll
    can still serve a non-UUID; `main._apply_filter` handles that at runtime
    the same way it handles any unusable value array, failing closed with
    `records_skipped_total{reason="filter_field_missing"}`.
    """
    if typed_columns is None or field is None or values is None:
        return
    if dict(typed_columns).get(field) != "uuid":
        return
    for value in values:
        try:
            arrow_converter.uuid_bytes(value)
        except ValueError as e:
            raise RuntimeError(
                f"{values_var} entry {value!r} is not a UUID, but MILLPOND_TYPED_COLUMNS pins "
                f"{field!r} to uuid — the filter compares UUID bytes, so an unparseable value "
                f"would silently disable the filter ({e})"
            ) from None


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

    No target is destination-restricted, `uuid` included: the wire form it
    produces (`pa.uuid()`) is a native `UUID` column on DuckLake AND the
    `uuid` column type on hoglake, so there is no per-destination refusal to
    make here (unlike MILLPOND_VARIANT_COLUMNS, which load() rejects for
    hoglake). See arrow_converter's coercer registry for why that one wire
    form, and not `pa.binary(16)`.

    For re-pointing a consumer at the duckling backfill's events table, pin the
    eight TIMESTAMPTZ columns and project_id:
    timestamp,created_at,person_created_at,group0..4_created_at -> timestamptz;
    project_id -> bigint. For the hoglake `events_raw` table, add the two
    ClickHouse `UUID` columns: uuid,person_id -> uuid.
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
    ducklake_retry_wait_ms = int(os.environ.get("DUCKLAKE_RETRY_WAIT_MS", "250"))
    if ducklake_retry_wait_ms <= 0:
        raise RuntimeError(f"DUCKLAKE_RETRY_WAIT_MS={ducklake_retry_wait_ms!r} must be a positive integer")
    ducklake_retry_backoff = float(os.environ.get("DUCKLAKE_RETRY_BACKOFF", "1.0"))
    if not ducklake_retry_backoff >= 1.0:
        raise RuntimeError(f"DUCKLAKE_RETRY_BACKOFF={ducklake_retry_backoff!r} must be >= 1.0")
    _check_ducklake_liveness_budget(ducklake_max_retry_count, ducklake_retry_wait_ms, ducklake_retry_backoff)

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
        "ducklake_retry_wait_ms": ducklake_retry_wait_ms,
        "ducklake_retry_backoff": ducklake_retry_backoff,
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
        "ducklake_retry_wait_ms",
        "ducklake_retry_backoff",
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
        "hoglake_compression",
        "hoglake_compression_level",
        "hoglake_deferred_stats",
        "hoglake_variant_on_invalid",
        "hoglake_allow_python_variant_codec",
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
# main.py's outer attempts around a DuckLake write (_WRITE_MAX_RETRIES).
_DUCKLAKE_OUTER_ATTEMPTS = 3


# Local work every hoglake attempt does before it issues its commit, and
# therefore outside the request timeout the model used to be built
# entirely from: encode the fanout's parquet and upload it. It is charged
# PER ATTEMPT because a rebuild really does redo it — which it did not
# used to, because a refused commit was replayed rather than rebuilt, and
# a `ddl_since_read_snapshot` refusal is now a routine per-attempt cost
# while a producer rollout adds columns under a running fleet.
#
# BASIS, so the number is auditable rather than round. Two flush shapes:
#
# * The prod-us events writer (string columns): ~4.3 s to encode and
#   ~5 waves of round trips to upload ~105K rows over ~271 partitions,
#   call it ~10 s with the object store misbehaving — doubled.
# * The whales writer (seven VARIANT columns, ~13.8 KB/row, 2 GiB Arrow
#   / ~155k rows / ~532 partition groups per flush): the Rust codec
#   encodes at ~27 us/row on an M4, ~50-65 us/row on prod's Graviton
#   (1.8-2.5x slower per core), so ~4-10 s of local work per attempt on
#   the laptop and ~10-25 s on prod, before uploads. The Python encoder
#   is 6-10x that (~50-85 s on the M4, several minutes on prod) and a
#   variant table is REFUSED under it for exactly this reason (see
#   `_hoglake_allow_python_variant_codec`): a constant that covered it
#   would not fit six attempts inside the liveness budget at all.
#
# So this is a FLOOR for the native codec, not a ceiling for any flush:
# the runtime deadline (`hoglake_flush_deadline`) replaces it with the
# MEASURED duration of the sink's last prepare times `_PREPARE_SAFETY`
# whenever that is larger, so a pod whose flushes really take 40 s
# reserves 60 s per attempt and stops its ladder early rather than
# starting an attempt the probe will kill. The startup model below keeps
# the floor only, because it has nothing measured yet; its job is to
# refuse a budget that is obviously impossible, not to be the
# enforcement.
_PREPARE_ALLOWANCE_S = 20.0
# What a measured prepare is multiplied by to become the next attempt's
# reservation. The next attempt rebuilds the same batch, so its encode is
# the same work; what varies is the object store, and 1.5x covers one
# bad upload wave without reserving so much that a healthy ladder is cut
# short.
_PREPARE_SAFETY = 1.5


def _hoglake_worst_case_flush_s(max_retries: int, timeout_s: float) -> float:
    """Longest a single sink.write() can take: every attempt spending its
    full request timeout plus its own encode-and-upload, with the whole
    backoff ladder between them."""
    ladder = sum(min(_BACKOFF_BASE_S * (2**attempt), _BACKOFF_CAP_S) for attempt in range(max(0, max_retries - 1)))
    return max_retries * (timeout_s + _PREPARE_ALLOWANCE_S) + ladder * (1 + _BACKOFF_JITTER)


def _ducklake_worst_case_flush_s(max_retries: int, wait_ms: int, backoff: float) -> float:
    """Longest a single sink.write() can sleep: DuckLake's inner ladder
    (wait x backoff^n before each of its retries) in every one of
    main.py's outer attempts, plus the outer backoff ladder between them.
    Sleep only: the commits themselves are not priced."""
    attempts = _DUCKLAKE_OUTER_ATTEMPTS
    inner = sum(wait_ms / 1000.0 * backoff**n for n in range(max_retries))
    ladder = sum(min(_BACKOFF_BASE_S * (2**attempt), _BACKOFF_CAP_S) for attempt in range(attempts - 1))
    return attempts * inner + ladder * (1 + _BACKOFF_JITTER)


def _check_ducklake_liveness_budget(max_retries: int, wait_ms: int, backoff: float) -> None:
    """Refuse a DuckLake retry ladder that can sleep past the liveness
    deadline. DuckLake's inner commit retry sleeps inside the INSERT, on
    the consume loop's only thread, with no log line, so springing it
    looks like a pod that goes silent and is SIGKILLed ~28 minutes later
    (480 s poll age + 40 failed probes)."""
    worst = _ducklake_worst_case_flush_s(max_retries, wait_ms, backoff)
    if worst <= _LIVENESS_BUDGET_S:
        return
    raise RuntimeError(
        f"DUCKLAKE_MAX_RETRY_COUNT={max_retries} with DUCKLAKE_RETRY_WAIT_MS={wait_ms} and "
        f"DUCKLAKE_RETRY_BACKOFF={backoff} allows a single flush to sleep up to {worst:.0f}s inside "
        f"sink.write(), past the {_LIVENESS_BUDGET_S:.0f}s liveness deadline "
        f"(server.HealthState.max_poll_age_s). Lower one of the three."
    )


# How much of the liveness budget the runtime deadline refuses to spend,
# so that the attempt it lets through still has room to finish and the
# consume loop still has room to record a poll afterwards. The startup
# model has no margin of its own because it is not the enforcement.
_FLUSH_DEADLINE_MARGIN_S = 30.0


def hoglake_flush_deadline(timeout_s: float, measured_prepare_s: float | None = None) -> tuple[float, float]:
    """(wall-clock budget for one flush, cost to reserve per attempt).

    The RUNTIME half of the liveness story, read by
    `main._write_with_retry` before every backoff sleep — re-read each
    time, because the reservation MOVES: `measured_prepare_s` is the
    wall time the sink's most recent prepare (encode + upload, success
    or failure) actually took, and the next attempt is charged
    `max(_PREPARE_ALLOWANCE_S, measured x _PREPARE_SAFETY)` on top of its
    request timeout. A VARIANT flush is what made the constant
    insufficient: the encode is per-attempt work (a rebuild after
    `ddl_since_read_snapshot` redoes it) and its size is the batch's,
    not a property of the code — 2 GiB of whale rows is seconds under
    the Rust codec and a minute or more under Python, and a fixed 20 s
    would let the loop start an attempt the liveness probe kills halfway
    through. None (no prepare yet) is the floor alone. The startup model
    below cannot see what a real flush actually spends:

    * `_retry_delay` floors each gap with the server's own `Retry-After`
      and clamps only the GAP (30 s), not the sum — six attempts and
      five floored gaps already exceed the budget on their own;
    * a rebuild (which is what every `ddl_since_read_snapshot` refusal
      forces) re-resolves the namespace and table and re-reads the
      shape, each under its own HTTP timeout, none of which the
      attempts x timeout product counts;
    * boto3's upload client retries four times at a 30 s read timeout
      per object and `prepare_append_tables` waits for every in-flight
      upload before it re-raises, so a stalled object store can put one
      prepare well past `_PREPARE_ALLOWANCE_S`.

    Each of those is bounded on its own and none of them is bounded
    TOGETHER, which is what a wall clock is for. Returned as a pair
    rather than read off module constants by main.py so the whole
    model — budget, margin and per-attempt reservation — stays in the
    file that owns the arithmetic.
    """
    allowance = _PREPARE_ALLOWANCE_S
    if measured_prepare_s is not None:
        allowance = max(allowance, measured_prepare_s * _PREPARE_SAFETY)
    return _LIVENESS_BUDGET_S - _FLUSH_DEADLINE_MARGIN_S, timeout_s + allowance


def _check_hoglake_liveness_budget(max_retries: int, timeout_s: float) -> None:
    """Refuse a retry budget that can outlive the liveness deadline.

    HOGLAKE_MAX_RETRY_COUNT was unbounded while its interaction with
    liveness was documented in a comment — so the documented trap was
    one values-file edit away, and springing it looks like a pod
    SIGKILLed mid-flush with no explanation in its own logs. The
    arithmetic that comment describes is now the check.

    A SANITY MODEL, NOT THE ENFORCEMENT. It prices attempts x (timeout +
    prepare) plus the unfloored ladder, which is a lower bound on what a
    flush can spend — see `hoglake_flush_deadline` for the three things
    it cannot see. Its job is to refuse a budget that is obviously
    impossible at config time, where the operator can still fix it; the
    runtime deadline in `main._write_with_retry` is what actually keeps
    a flush inside the liveness window.
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


# pyhoglake's upload fan-out knob. Not a millpond variable, which is
# exactly why it is checked here: pyhoglake reads it inside
# `resolve_concurrency`, on the prepared-append path, and refuses a bad
# value as a client-side `ValidationError` — which millpond's
# `is_retryable` correctly calls PERMANENT. So a typo'd fan-out is a pod
# that starts clean, passes its probes, takes its partitions, and then
# crashes on its first flush with a 422-shaped error about a value no
# millpond code mentions. Every other knob is validated at load; this one
# was only not because it belongs to someone else.
_UPLOAD_CONCURRENCY_ENV = "PYHOGLAKE_UPLOAD_CONCURRENCY"


def _check_upload_concurrency() -> None:
    """Refuse a PYHOGLAKE_UPLOAD_CONCURRENCY pyhoglake would refuse.

    Parsed exactly as `pyhoglake.upload.resolve_concurrency` parses it —
    strip, `int()`, reject below 1 — so this cannot refuse a value
    pyhoglake accepts or accept one it refuses. Unset is the library's
    default (64) and is left alone; an upper bound is deliberately NOT
    imposed, because the ceiling is the object store's and pyhoglake
    caps the fan-out at the group count anyway.
    """
    raw = os.environ.get(_UPLOAD_CONCURRENCY_ENV, "").strip()
    if not raw:
        return
    try:
        value = int(raw)
    except ValueError:
        raise RuntimeError(
            f"{_UPLOAD_CONCURRENCY_ENV} must be a positive integer, got {raw!r}. pyhoglake reads it "
            f"on the upload path and refuses a bad value permanently, so this would crash the first "
            f"flush rather than the pod start."
        ) from None
    if value < 1:
        raise RuntimeError(
            f"{_UPLOAD_CONCURRENCY_ENV} must be a positive integer, got {value}. pyhoglake reads it "
            f"on the upload path and refuses a bad value permanently, so this would crash the first "
            f"flush rather than the pod start."
        )


# Parquet codec for hoglake objects. pyhoglake's own default is pyarrow's,
# SNAPPY, and it keeps it on purpose; millpond overrides it because on the
# events firehose SNAPPY is ~4x larger than zstd-3 (one 413K-row prod-us
# slice: 1,138 MB vs 281 MB). The cost is encode time on the flush path:
# zstd-3 encoded ~1.9x slower than snappy on a synthetic events benchmark
# (0.27 s vs 0.14 s per 112 MB of Arrow), a few seconds per 2 GiB flush.
#
# The codec is permanent only for objects at or over the 512 MiB compaction
# target: smaller ones are rewritten by compaction in the server's codec.
# On prod-us events that is the daytime size-triggered objects (~560-620 MiB
# as SNAPPY); zstd puts them under the target, so they become compaction
# candidates for the first time.
#
# What is allowed (zstd 1-9, snappy, none) is pyhoglake's decision, made by
# `pyhoglake.parquet_write_options`, which is also what the write path runs.
# millpond calls it here rather than mirroring the rules, so the two cannot
# drift, and a pyhoglake too old to take a codec fails the pod at start
# (ImportError) instead of on its first flush (TypeError, non-retryable).
_HOGLAKE_DEFAULT_CODEC = "zstd"
_HOGLAKE_DEFAULT_ZSTD_LEVEL = 3


def _hoglake_compression() -> tuple[str, int | None]:
    """HOGLAKE_COMPRESSION / HOGLAKE_COMPRESSION_LEVEL, validated by pyhoglake.

    Unset codec is zstd. Unset level is 3 for zstd and none otherwise.
    `snappy` with no level reproduces the pre-knob bytes exactly, which is
    the rollback.
    """
    # Imported here, not at module top: config.py loads in every pod,
    # including DuckLake ones, and only the hoglake path needs it.
    from pyhoglake import ValidationError, parquet_write_options

    codec = os.environ.get("HOGLAKE_COMPRESSION", "").strip().lower() or _HOGLAKE_DEFAULT_CODEC
    raw_level = os.environ.get("HOGLAKE_COMPRESSION_LEVEL", "").strip()
    if not raw_level:
        level = _HOGLAKE_DEFAULT_ZSTD_LEVEL if codec == "zstd" else None
    else:
        try:
            level = int(raw_level)
        except ValueError:
            raise RuntimeError(f"HOGLAKE_COMPRESSION_LEVEL must be an integer, got {raw_level!r}") from None
    try:
        parquet_write_options(codec, level)
    except ValidationError as e:
        raise RuntimeError(
            f"HOGLAKE_COMPRESSION={codec!r} HOGLAKE_COMPRESSION_LEVEL={level!r} is refused by pyhoglake: "
            f"{e.message}. pyhoglake would refuse it permanently on the first flush."
        ) from None
    return codec, level


# Column stats for hoglake objects: shipped inline in the commit, or left
# to the server's hydrator. With `deferred_stats=True` pyhoglake leaves
# `column_stats` out of every file registration; the server registers
# each file `stats_state='pending'` and its hydrator reads the footer
# later, outside the per-catalog commit lock. What that buys is the
# per-file stats work the server otherwise does INSIDE the lock, which on
# a wide, heavily partitioned flush (hundreds of files x hundreds of
# columns) is most of the time the commit holds it — and every other
# writer to the catalog queues behind it.
#
# What it costs:
#   * readers see no column bounds for a file until the hydrator has
#     swept it (its cadence: seconds to minutes);
#   * files under the hydrator's size floor never get stats rows at all
#     (they register `provided` with none, until compaction rewrites
#     them).
#
# Pending files do NOT block `add_column` / `rename_column` on a hoglake
# 1.4.6+ server: its 409 `idless_files_present` counts only files PROVEN
# to lack field ids, which pyhoglake never writes. If another writer
# leaves one, the sink fails the flush rather than dropping the column
# (see HoglakeSink._add_columns and IdlessFilesLiveError).
#
# Default ON. `false` ships stats inline exactly as before: the rollback.
_HOGLAKE_DEFAULT_DEFERRED_STATS = True
_BOOLS = {"true": True, "false": False}


def _hoglake_deferred_stats() -> bool:
    """HOGLAKE_DEFERRED_STATS: `true` (default) or `false`, case-insensitive.

    Strict on purpose: `0`, `no`, `off` and typos are refused rather than
    guessed, because the two readings of a guess are a commit-lock
    regression one way and a fleet-wide stats change the other.

    Also refuses a pyhoglake whose `prepare_append_tables` cannot take
    the keyword: the sink passes it on every flush, and the TypeError an
    older client raises there is permanent, so the pod would start clean
    and crash on its first flush instead of here.
    """
    raw = os.environ.get("HOGLAKE_DEFERRED_STATS", "").strip()
    if not raw:
        value = _HOGLAKE_DEFAULT_DEFERRED_STATS
    else:
        try:
            value = _BOOLS[raw.lower()]
        except KeyError:
            raise RuntimeError(f"HOGLAKE_DEFERRED_STATS={raw!r} must be `true` or `false`") from None
    import inspect

    from pyhoglake import Table

    if "deferred_stats" not in inspect.signature(Table.prepare_append_tables).parameters:
        raise RuntimeError(
            "HOGLAKE_DEFERRED_STATS needs a pyhoglake whose `prepare_append_tables` takes `deferred_stats=` (1.4.5+); "
            "the installed client would refuse every flush."
        )
    return value


# What a flush does with a row whose JSON text cannot be encoded into a
# live `variant` column of the destination (text that is not JSON, a
# duplicate key, NaN/Infinity, an integer of more than 38 digits, nesting
# deeper than 128 — pyhoglake's encoder decides, millpond only chooses the
# consequence). Passed as `variant_on_invalid=` to every
# `prepare_append_tables`:
#
#   null  — the row lands with SQL NULL in that column, the other columns
#           intact, and is counted on
#           `millpond_variant_invalid_total{reason}`. The default, and
#           millrace's posture: one bad row is one null, never a stuck
#           partition.
#   raise — pyhoglake raises `VariantEncodingError` at the first such row,
#           before anything is uploaded; it is a `ValidationError`, so the
#           sink's classifier calls it permanent and the pod crashes on
#           the batch. The partition does not advance until the row is
#           gone from the topic. For a pipeline where a silent null is
#           worse than an outage.
#
# pyhoglake refuses `null` when any live variant column is NOT NULL (an
# invalid row would have nowhere to go); the sink refuses that destination
# at resolve rather than on the first flush (HoglakeSink._adopt_columns).
_HOGLAKE_DEFAULT_VARIANT_ON_INVALID = "null"
_HOGLAKE_VARIANT_ON_INVALID = ("null", "raise")


def _hoglake_variant_on_invalid() -> str:
    """MILLPOND_HOGLAKE_VARIANT_ON_INVALID: `null` (default) or `raise`,
    case-insensitive. Anything else is refused: the two readings of a
    guess are a crash-looping partition one way and silently nulled
    columns the other."""
    raw = os.environ.get("MILLPOND_HOGLAKE_VARIANT_ON_INVALID", "").strip()
    if not raw:
        return _HOGLAKE_DEFAULT_VARIANT_ON_INVALID
    value = raw.lower()
    if value not in _HOGLAKE_VARIANT_ON_INVALID:
        raise RuntimeError(
            f"MILLPOND_HOGLAKE_VARIANT_ON_INVALID={raw!r} must be one of {', '.join(_HOGLAKE_VARIANT_ON_INVALID)}"
        )
    return value


def variant_codec_backend() -> str:
    """Which codec pyhoglake encodes variant rows with, for THIS process:
    `native` (the Rust pyhoglake-variant-codec, the `variant-codec`
    extra), `python` (its reference encoder), or `unknown`.

    Read through pyhoglake's own selector, `_variant_backend`, which is
    private — so this is best effort and never raises: a pyhoglake that
    moves the module answers `unknown` rather than refusing startup. The
    answer is fixed per process: pyhoglake reads PYHOGLAKE_VARIANT_CODEC
    ONCE, at import, into `MODE` — so that constant is what is read here,
    not the environment again, which may have changed since. `auto` (its
    default) is reported as whichever codec it resolved to. The same
    rule millrace's `variant_codec()` applies, so the two pipelines'
    `backend` labels mean the same thing.

    Only the process-level resolution. Under `auto` pyhoglake can still
    route an individual encode call to Python (a plan the Rust codec
    does not take, text that is not UTF-8); those are counted per call
    on `millpond_variant_codec_routed_to_python_total`, not here.
    """
    try:
        from pyhoglake import _variant_backend as backend

        if backend.MODE == "python" or backend.native()[0] is None:
            return "python"
        return "native"
    except Exception:  # noqa: BLE001 - a label, never a failure
        return "unknown"


def _hoglake_allow_python_variant_codec() -> bool:
    """MILLPOND_ALLOW_PYTHON_VARIANT_CODEC: `true` or `false` (default),
    case-insensitive; anything else is refused.

    The gate on the one silent CPU regression this destination has. The
    Rust codec encodes a whale-shaped row (~13.8 KB, seven variant
    columns) in ~27 us on an M4 (~50-65 us on prod's Graviton); the
    Python encoder takes ~172 us (~310-430 us), 6-10x, which turns a
    2 GiB flush from seconds into a minute or more of pause on the
    consume thread and halves a pod's throughput. The image asserts the
    wheel is importable at build, and `pyhoglake[variant-codec]` pins it,
    so a pod that resolves to Python is a broken image or a laptop; the
    default refuses to WRITE A VARIANT TABLE with it rather than letting
    the pod run slow with nothing in its logs but a `backend="python"`
    label. `true` is for a deliberate canary, or a platform the wheel
    does not cover yet.

    Only parsed here. The refusal is the sink's, at resolve
    (`HoglakeSink._check_variant_columns`): it fires when the destination
    is found to have a variant column — the first write, an alter
    receipt, a TTL re-read — and never for a string-only table, so a
    pod that encodes nothing is not coupled to the codec it would never
    use. The codec the process resolved to is logged at load either way.
    """
    raw = os.environ.get("MILLPOND_ALLOW_PYTHON_VARIANT_CODEC", "").strip()
    if not raw:
        return False
    try:
        return _BOOLS[raw.lower()]
    except KeyError:
        raise RuntimeError(f"MILLPOND_ALLOW_PYTHON_VARIANT_CODEC={raw!r} must be `true` or `false`") from None


def python_variant_codec_refusal() -> str:
    """The one message for a pod whose variant codec resolved to Python
    and whose table has a variant column (`HoglakeSink._check_variant_columns`)."""
    try:
        from pyhoglake import _variant_backend as backend

        mode, (_module, why) = backend.MODE, backend.native()
    except Exception:  # noqa: BLE001 - diagnostics only
        mode, why = "?", "pyhoglake's codec selector could not be read"
    return (
        f"the variant codec resolved to pyhoglake's Python encoder (PYHOGLAKE_VARIANT_CODEC={mode}; "
        f"{why or 'the Rust codec is not in use'}), which encodes a whale-shaped row at 6-10x the Rust codec's "
        f"CPU and would turn every 2 GiB flush into a minute or more of consume-thread pause. Install the "
        f"`variant-codec` extra (pyhoglake-variant-codec; the image asserts it at build), or set "
        f"MILLPOND_ALLOW_PYTHON_VARIANT_CODEC=true to run on the Python encoder deliberately."
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


def _hoglake_s3_keys() -> tuple[str | None, str | None]:
    """HOGLAKE_S3_ACCESS_KEY / HOGLAKE_S3_SECRET_KEY — both, or neither.

    Both set is the static-credential shape (MinIO in local dev and CI).
    Neither set hands pyhoglake an S3Config with no keys, and pyhoglake
    passes a key to pyarrow's S3FileSystem only when it is not None, so
    pyarrow falls through to the AWS SDK's default credential chain —
    which in Kubernetes is the ServiceAccount's web-identity token
    (IRSA), the same way the hoglake server authenticates.

    One without the other is refused HERE, at config load, because of
    where and how — not because anything downstream would accept it.
    pyarrow refuses half a pair on its own: `S3FileSystem(access_key=...)`
    with no secret raises `ValueError: In order to initialize with
    explicit credentials both access_key and secret_key must be
    provided`. But that fires when pyhoglake first builds the
    filesystem, names neither environment variable, and reaches the
    operator as a pyarrow traceback from inside a third-party client.
    This check costs no network, runs before anything else is built, and
    names both variables plus the missing half.
    """
    access_key = os.environ.get("HOGLAKE_S3_ACCESS_KEY", "").strip() or None
    secret_key = os.environ.get("HOGLAKE_S3_SECRET_KEY", "").strip() or None
    if (access_key is None) != (secret_key is None):
        missing = "HOGLAKE_S3_ACCESS_KEY" if access_key is None else "HOGLAKE_S3_SECRET_KEY"
        raise RuntimeError(
            f"HOGLAKE_S3_ACCESS_KEY and HOGLAKE_S3_SECRET_KEY must be set together, and {missing} is "
            f"not set. Set both for static object-store credentials (MinIO, local dev, CI), or neither "
            f"to use the AWS default credential chain (IRSA in Kubernetes)."
        )
    return access_key, secret_key


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
    # The model charges each attempt its request timeout AND a
    # _PREPARE_ALLOWANCE_S of local encode-and-upload, because on the
    # hoglake path a retry REBUILDS the flush rather than replaying it
    # whenever the server refuses the commit as `ddl_since_read_snapshot`
    # — which a producer rollout adding columns under a running fleet
    # makes a routine per-attempt cost, not a rarity. That term is what
    # moved the default: 8 attempts x (45s + 20s) plus the jittered
    # ladder is ~634s, well past the 480s deadline, so the default is 6
    # attempts (~429s, leaving ~51s of margin). The previous default of 8
    # fit only because the model did not charge for the work outside the
    # request, and "deliberately close to the line" is not a thing to be
    # about a SIGKILL: a flush that is killed mid-commit is the one
    # outcome with no error in the pod's own logs.
    #
    # Six is still a real ladder against the case the ladder exists for
    # (a convoyed catalog answering 503 + Retry-After, which main.py
    # honours as a floor), and an operator who wants more has to lower
    # the timeout to buy it — which is the trade being made explicit
    # rather than discovered.
    max_retries = _positive_int("HOGLAKE_MAX_RETRY_COUNT", 6)
    timeout_s = _positive_float("HOGLAKE_REQUEST_TIMEOUT_S", 45.0)
    _check_hoglake_liveness_budget(max_retries, timeout_s)
    _check_upload_concurrency()

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
    s3_access_key, s3_secret_key = _hoglake_s3_keys()
    compression, compression_level = _hoglake_compression()
    deferred_stats = _hoglake_deferred_stats()
    return {
        "hoglake_max_retry_count": max_retries,
        "hoglake_request_timeout_s": timeout_s,
        "hoglake_compression": compression,
        "hoglake_compression_level": compression_level,
        "hoglake_deferred_stats": deferred_stats,
        "hoglake_variant_on_invalid": _hoglake_variant_on_invalid(),
        "hoglake_allow_python_variant_codec": _hoglake_allow_python_variant_codec(),
        "hoglake_url": _require("HOGLAKE_URL"),
        "hoglake_catalog": catalog,
        "hoglake_namespace": names["HOGLAKE_NAMESPACE"],
        "hoglake_table": names["HOGLAKE_TABLE"],
        "hoglake_data_path": _hoglake_data_path(),
        "hoglake_s3_endpoint": os.environ.get("HOGLAKE_S3_ENDPOINT", "").strip() or None,
        "hoglake_s3_access_key": s3_access_key,
        "hoglake_s3_secret_key": s3_secret_key,
        # Independent of the credential source: the AWS SDK needs the
        # region whether the keys are static or come from the chain.
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
    _require_uuid_filter_values(typed_columns, filter_keep_field, filter_values, "MILLPOND_FILTER_VALUES")
    _require_uuid_filter_values(typed_columns, filter_drop_field, filter_drop_values, "MILLPOND_FILTER_DROP_VALUES")
    variant_columns = _load_variant_columns()
    if destination == "hoglake" and variant_columns is not None:
        # MILLPOND_VARIANT_COLUMNS is the DuckLake dual-write: a `_variant`
        # COMPANION column beside the string, projected in SQL. The hoglake
        # sink has no companions and no knob. A live `variant` column of
        # the destination is written from the same-named source field
        # directly (the whales table keeps the source names: `properties`
        # is a variant column, not `properties_variant` beside a string),
        # and which columns those are is the TABLE's declaration — a
        # setup script creates them with their shredding, millpond never
        # does. Refuse rather than ignore: a silent config no-op is how a
        # mixed fleet rots, and an operator who set this expected a
        # companion that will never appear.
        raise RuntimeError(
            "MILLPOND_VARIANT_COLUMNS is the DuckLake dual-write (a `_variant` companion column "
            "beside the string) and has no meaning with MILLPOND_DESTINATION=hoglake. A hoglake "
            "destination's live `variant` columns are written automatically from the same-named "
            "source field — declare them on the table (with their shredding) and remove this "
            "setting; see README 'VARIANT columns' under the hoglake destination."
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
    if cfg.hoglake_variant_on_invalid is not None:
        log.info(
            "Hoglake variant columns: invalid JSON -> %s (MILLPOND_HOGLAKE_VARIANT_ON_INVALID); codec: %s "
            "(PYHOGLAKE_VARIANT_CODEC; MILLPOND_ALLOW_PYTHON_VARIANT_CODEC=%s)",
            cfg.hoglake_variant_on_invalid,
            variant_codec_backend(),
            "true" if cfg.hoglake_allow_python_variant_codec else "false",
        )
    return cfg
