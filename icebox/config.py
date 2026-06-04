"""Env-driven configuration for the icebox service.

Mirrors millpond's config style: frozen dataclass + explicit `load()`
that reads env vars. No magic; no auto-resolution of optional fields.

The icebox runs in mw-dev and mw-prod-us; each env supplies the
config via K8s env vars set by the Helm chart values. The chart's
icebox-deployment.yaml maps values.yaml fields to env vars one-to-one.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass

log = logging.getLogger(__name__)


# Schema names get interpolated into the conninfo
# `options=-csearch_path=...` parameter (PG protocol doesn't allow
# parameterized session options), so we validate strictly at load time
# to make injection structurally impossible.
#
# Restricted to LOWERCASE only because BOTH pools (psycopg + asyncpg)
# send the schema name through PG's GUC parser at connection-startup
# time — psycopg via `options=-csearch_path=<name>` in the conninfo
# string, asyncpg via `server_settings={"search_path": <name>}` in the
# StartupMessage (verified in asyncpg/protocol/coreproto.pyx). Both
# paths case-fold unquoted identifiers to lowercase. Allowing uppercase
# in the regex would silently break operator intent: ICEBOX_PG_SCHEMA
# =MyIcebox creates `myicebox`, not `MyIcebox`, and any external
# tooling that expects the literal name has to mirror PG's folding
# rules. Easier to reject the case at config-load.
_SAFE_PG_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

# Schema names that are syntactically valid but semantically wrong:
# PG reserves the `pg_*` prefix and a handful of well-known names.
# - `pg_*` names: PG refuses CREATE SCHEMA with SQLSTATE 42939.
# - `information_schema`, `public`, `pg_catalog`, `pg_toast`,
#   `pg_temp`: would either fail or succeed-but-commingle, which is
#   worse than failing fast.
_RESERVED_SCHEMA_NAMES = frozenset({
    "public",
    "information_schema",
    "pg_catalog",
    "pg_toast",
    "pg_temp",
})

# PG SQL reserved words (subset). Even if quoted these would work, but
# the conninfo interpolation can't quote them safely without rewriting
# the whole pipeline. Reject the common ones at config load. Not
# exhaustive — covers the cases an operator might plausibly type.
_RESERVED_SQL_KEYWORDS = frozenset({
    "select", "from", "where", "table", "schema", "database",
    "create", "drop", "alter", "insert", "update", "delete",
    "commit", "rollback", "begin", "end", "union", "join",
    "on", "as", "is", "in", "and", "or", "not", "null", "true", "false",
    "primary", "foreign", "key", "index", "constraint", "default",
    "user", "group", "order", "by", "having", "limit", "offset",
    "with", "values", "returning",
})


@dataclass(frozen=True)
class Config:
    # Postgres connection — v1 reuses Lakekeeper's credentials with
    # GRANT CREATE/USAGE on the icebox database.
    pg_host: str
    pg_port: int
    pg_database: str
    pg_username: str
    pg_password: str
    pg_sslmode: str  # require | prefer | disable
    # PG schema this icebox owns. Each deployment runs in its own
    # schema so multiple iceboxes can share a backing PG instance
    # without coordinating on row visibility. Connections are pinned
    # to this schema via `options=-csearch_path=<schema>` in conninfo,
    # so all SQL stays unqualified.
    pg_schema: str

    # Connection pool sizing — the asyncpg pool serves API requests;
    # the psycopg pool is for the synchronous committer + bootstrap.
    asyncpg_pool_min: int
    asyncpg_pool_max: int
    psycopg_pool_min: int
    psycopg_pool_max: int

    # Iceberg catalog (Lakekeeper) — in-cluster URL
    iceberg_catalog_uri: str
    iceberg_warehouse: str
    # The (namespace, table) pair this icebox serves. Each deployment
    # is per-table; these fields decide which Iceberg table the
    # committer's `load_table` opens. Also used to validate incoming
    # POSTs — writers can include `expected_iceberg_namespace` /
    # `expected_iceberg_table` in the body and we 400 on mismatch
    # (catches a misconfigured writer hitting the wrong icebox URL).
    iceberg_namespace: str
    iceberg_table: str

    # Kafka — the committer commits offsets on writers' behalf
    kafka_bootstrap_servers: str
    # Topic the writers are consuming. Single topic per icebox instance
    # (one icebox per (topic, table) pair).
    kafka_topic: str
    # Consumer-group id the writers use as their offset-storage key.
    # Writers DON'T join this group (consumer.assign() only) — group is
    # used purely as a write-target for offset commits the icebox makes
    # on their behalf. Must match the writers' GROUP_ID env var exactly.
    kafka_group_id: str
    # Extra Kafka config as a JSON string env var, parsed into dict.
    # Used for security.protocol, sasl.mechanism, etc. on WarpStream.
    kafka_extra_config_json: str

    # Committer behavior
    committer_cadence_seconds: int
    committer_max_pending_files: int
    committer_degraded_failure_threshold: int

    # Heartbeat staleness — POSTs are rejected with 503 if the
    # committer hasn't written a heartbeat within this multiple of
    # the cadence.
    committer_heartbeat_stale_multiple: float

    # REST API
    api_host: str
    api_port: int

    # Logging
    log_level: str
    # ``json`` | ``text``. JSON is what mw-prod-us ships; ``text`` is
    # the friendlier local-dev shape. Defaults at the dataclass level
    # so test builders that don't set these still work.
    log_format: str = "json"
    # PostHog Logs (OTLP/HTTP) — ON when ``posthog_project_token`` is
    # set, OFF otherwise. ``posthog_logs_endpoint`` defaults to the US
    # PostHog Cloud ingress; override for EU or self-hosted PostHog.
    posthog_project_token: str | None = None
    posthog_logs_endpoint: str = "https://us.i.posthog.com/i/v1/logs"
    # service.version reported in OTLP resource attributes. Defaults
    # to the millpond package version. Operators can override via
    # ICEBOX_SERVICE_VERSION (e.g., to expose the image digest).
    service_version: str = "unknown"
    # service.namespace per OTel semconv — "logical grouping of related
    # services in a deployment unit". For PostHog this is the release
    # family ("millpond"). NOT the data-side namespace (we previously
    # misused this field for the PG schema; the icebox.* custom attrs
    # carry the per-(warehouse, namespace, table) facets now).
    service_namespace: str = "millpond"
    # service.instance.id per OTel semconv — distinguishes instances of
    # the same service. Set by the chart to the consumer key (e.g.
    # ``events-icebox``) so PostHog Logs can filter by instance the
    # same way it does by service. Optional; unset = no attr.
    service_instance_id: str | None = None

    # Schema-fingerprint cache TTL — how long the API perimeter
    # trusts its in-memory copy of the Iceberg table's current
    # fingerprint before refreshing from the catalog. Mismatches
    # force an immediate refresh regardless of TTL, so this is the
    # max staleness window during which a stale writer would slip a
    # POST through that the committer would later reject — keep it
    # short.
    schema_fingerprint_cache_ttl_seconds: float = 60.0


def _require(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _optional(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise RuntimeError(f"Env var {name} must be an integer, got {raw!r}") from e


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise RuntimeError(f"Env var {name} must be a float, got {raw!r}") from e


def load() -> Config:
    """Read env vars into a Config. Raises RuntimeError on missing
    required fields.

    Required (no default):
      ICEBOX_PG_HOST, ICEBOX_PG_PASSWORD,
      ICEBOX_ICEBERG_CATALOG_URI,
      ICEBOX_KAFKA_BOOTSTRAP_SERVERS,
      ICEBOX_KAFKA_TOPIC, ICEBOX_KAFKA_GROUP_ID

    Optional fields use the dataclass defaults declared above.
    """
    cadence = _int("ICEBOX_COMMITTER_CADENCE_SECONDS", 60)
    if cadence < 1:
        raise RuntimeError(
            f"ICEBOX_COMMITTER_CADENCE_SECONDS must be >= 1, got {cadence}"
        )

    pg_database = _optional("ICEBOX_PG_DATABASE", "icebox")
    if not _SAFE_PG_IDENTIFIER.match(pg_database):
        # PE-review #1: the DB name flows into psycopg/asyncpg conninfo
        # unquoted. Without validation, an operator typo (e.g.,
        # ICEBOX_PG_DATABASE="icebox prod" with a space) boot-loops the
        # pod with a cryptic libpq error rather than a clean RuntimeError
        # at config-load time. Same identifier discipline as pg_schema.
        raise RuntimeError(
            f"ICEBOX_PG_DATABASE {pg_database!r} is not a valid PG identifier "
            f"(must match {_SAFE_PG_IDENTIFIER.pattern}; lowercase only)"
        )
    if pg_database.startswith("pg_"):
        raise RuntimeError(
            f"ICEBOX_PG_DATABASE {pg_database!r} starts with 'pg_' which is "
            f"reserved by Postgres for system databases"
        )

    pg_schema = _optional("ICEBOX_PG_SCHEMA", "icebox")
    if not _SAFE_PG_IDENTIFIER.match(pg_schema):
        # Three classes of failure surface here:
        # 1. Empty, illegal chars (dashes, dots, spaces, unicode, SQL
        #    injection attempts) — none of these are legal PG
        #    identifiers.
        # 2. Uppercase letters — disallowed because the two pool
        #    drivers handle case differently (see _SAFE_PG_IDENTIFIER
        #    docstring for the split-brain hazard).
        # 3. Names longer than 63 bytes — PG's NAMEDATALEN limit.
        raise RuntimeError(
            f"ICEBOX_PG_SCHEMA {pg_schema!r} is not a valid PG identifier "
            f"(must match {_SAFE_PG_IDENTIFIER.pattern}; note: lowercase "
            f"only — see icebox/config.py for the rationale)"
        )
    if pg_schema.startswith("pg_"):
        # PG reserves the `pg_*` prefix; CREATE SCHEMA refuses these
        # with SQLSTATE 42939. Catch at config load with a clear
        # message rather than letting it surface as a cryptic boot
        # failure 30 seconds later.
        raise RuntimeError(
            f"ICEBOX_PG_SCHEMA {pg_schema!r} starts with 'pg_' which is "
            f"reserved by Postgres for system schemas"
        )
    if pg_schema in _RESERVED_SCHEMA_NAMES:
        raise RuntimeError(
            f"ICEBOX_PG_SCHEMA {pg_schema!r} is a PG-reserved schema "
            f"name (would either fail to create or commingle with system "
            f"or shared state). Pick a different name like "
            f"'icebox_<table>'."
        )
    if pg_schema in _RESERVED_SQL_KEYWORDS:
        raise RuntimeError(
            f"ICEBOX_PG_SCHEMA {pg_schema!r} is a SQL reserved word. "
            f"It would require quoting in every reference, which the "
            f"conninfo `options=-csearch_path=` interpolation can't "
            f"do reliably. Pick a different name."
        )

    psycopg_pool_max = _int("ICEBOX_PSYCOPG_POOL_MAX", 2)
    if psycopg_pool_max < 2:
        # The committer thread holds ONE pool connection for the
        # lifetime of the process (the advisory lock conn — see
        # icebox/committer.py:committer_loop). Cycle work + heartbeat
        # need at least one additional slot. A pool sized to 1 would
        # deadlock at the first cycle's pg_pool.connection() call.
        raise RuntimeError(
            f"ICEBOX_PSYCOPG_POOL_MAX must be >= 2 (committer holds 1 conn "
            f"for the advisory lock + needs ≥1 more for cycle work), got "
            f"{psycopg_pool_max}"
        )

    return Config(
        pg_host=_require("ICEBOX_PG_HOST"),
        pg_port=_int("ICEBOX_PG_PORT", 5432),
        pg_database=pg_database,
        pg_schema=pg_schema,
        pg_username=_optional("ICEBOX_PG_USERNAME", "lakekeeper"),
        pg_password=_require("ICEBOX_PG_PASSWORD"),
        pg_sslmode=_optional("ICEBOX_PG_SSLMODE", "require"),
        asyncpg_pool_min=_int("ICEBOX_ASYNCPG_POOL_MIN", 2),
        asyncpg_pool_max=_int("ICEBOX_ASYNCPG_POOL_MAX", 8),
        psycopg_pool_min=_int("ICEBOX_PSYCOPG_POOL_MIN", 1),
        psycopg_pool_max=psycopg_pool_max,
        iceberg_catalog_uri=_require("ICEBOX_ICEBERG_CATALOG_URI"),
        iceberg_warehouse=_optional("ICEBOX_ICEBERG_WAREHOUSE", "ingest"),
        iceberg_namespace=_require("ICEBOX_ICEBERG_NAMESPACE"),
        iceberg_table=_require("ICEBOX_ICEBERG_TABLE"),
        kafka_bootstrap_servers=_require("ICEBOX_KAFKA_BOOTSTRAP_SERVERS"),
        kafka_topic=_require("ICEBOX_KAFKA_TOPIC"),
        kafka_group_id=_require("ICEBOX_KAFKA_GROUP_ID"),
        kafka_extra_config_json=_optional("ICEBOX_KAFKA_EXTRA_CONFIG", "{}"),
        committer_cadence_seconds=cadence,
        committer_max_pending_files=_int("ICEBOX_COMMITTER_MAX_PENDING_FILES", 1000),
        committer_degraded_failure_threshold=_int(
            "ICEBOX_COMMITTER_DEGRADED_FAILURE_THRESHOLD", 2
        ),
        committer_heartbeat_stale_multiple=_float(
            "ICEBOX_COMMITTER_HEARTBEAT_STALE_MULTIPLE", 3.0
        ),
        api_host=_optional("ICEBOX_API_HOST", "0.0.0.0"),
        api_port=_int("ICEBOX_API_PORT", 8000),
        log_level=_optional("ICEBOX_LOG_LEVEL", "INFO"),
        log_format=_optional("ICEBOX_LOG_FORMAT", "json"),
        # No ``ICEBOX_`` prefix on POSTHOG_PROJECT_TOKEN: it's a
        # PostHog-wide secret typically sourced from a shared K8s
        # Secret (the same one other PostHog SDKs consume), so the
        # canonical PostHog name is what operators expect to see.
        posthog_project_token=(
            os.environ.get("POSTHOG_PROJECT_TOKEN") or None
        ),
        posthog_logs_endpoint=_optional(
            "POSTHOG_LOGS_ENDPOINT",
            "https://us.i.posthog.com/i/v1/logs",
        ),
        service_version=_optional("ICEBOX_SERVICE_VERSION", _default_service_version()),
        service_namespace=_optional("ICEBOX_SERVICE_NAMESPACE", "millpond"),
        service_instance_id=os.environ.get("ICEBOX_SERVICE_INSTANCE_ID") or None,
        schema_fingerprint_cache_ttl_seconds=_float(
            "ICEBOX_SCHEMA_FINGERPRINT_CACHE_TTL_SECONDS", 60.0
        ),
    )


def _default_service_version() -> str:
    """Best-effort service version string for OTLP resource attrs.

    Falls back to the millpond setuptools-scm version baked into the
    package; that maps cleanly to a git rev in non-prod builds and
    to a clean tag in prod images.
    """
    try:
        from millpond._version import version

        return str(version)
    except Exception:
        return "unknown"
