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
from dataclasses import dataclass

from shared.pg_identifier import SAFE_PG_IDENTIFIER, validate_pg_schema

log = logging.getLogger(__name__)


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
    psycopg_pool_min: int
    psycopg_pool_max: int

    # Iceberg catalog (Lakekeeper) — in-cluster URL
    iceberg_catalog_uri: str
    iceberg_warehouse: str
    # The (namespace, table) pair this icebox serves. Each deployment
    # is per-table; these fields decide which Iceberg table the
    # daemon's `load_table` opens.
    iceberg_namespace: str
    iceberg_table: str

    # Kafka — the daemon commits offsets on writers' behalf
    kafka_bootstrap_servers: str
    kafka_topic: str
    # Consumer-group id the writers use as their offset-storage key.
    # Writers DON'T join this group (consumer.assign() only) — group is
    # used purely as a write-target for offset commits the icebox makes
    # on their behalf. Must match the writers' GROUP_ID env var exactly.
    kafka_group_id: str
    # Extra Kafka config as a JSON string env var, parsed into dict.
    # Used for security.protocol, sasl.mechanism, etc. on WarpStream.
    kafka_extra_config_json: str

    # Daemon behavior
    committer_cadence_seconds: int
    committer_max_pending_files: int
    # Heartbeat staleness — /healthz returns 503 (k8s liveness
    # restarts the pod) if the daemon hasn't written a heartbeat
    # within this multiple of the cadence.
    committer_heartbeat_stale_multiple: float

    # HTTP server (probes + /metrics only)
    api_host: str
    api_port: int

    # Logging
    log_level: str
    log_format: str = "json"
    posthog_project_token: str | None = None
    posthog_logs_endpoint: str = "https://us.i.posthog.com/i/v1/logs"
    service_version: str = "unknown"
    service_namespace: str = "millpond"
    service_instance_id: str | None = None

    # Daemon knobs. See docs/icebox-self-healing-recovery.md.
    # `iceberg_timeout_s`: wall-clock budget on commit_data_files; the
    # with_timeout wrapper fires after this. Bounds row-lock hold time
    # during Lakekeeper degradation.
    # `age_filter_seconds`: pending rows younger than this are NOT
    # eligible for the daemon's SELECT — gives the writer time to
    # accumulate enough files for a worthwhile snapshot.
    iceberg_timeout_s: float = 5.0
    age_filter_seconds: float = 60.0


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
    if not SAFE_PG_IDENTIFIER.match(pg_database):
        # The DB name flows into psycopg/asyncpg conninfo unquoted.
        # Without validation, an operator typo (e.g.,
        # ICEBOX_PG_DATABASE="icebox prod" with a space) boot-loops the
        # pod with a cryptic libpq error rather than a clean RuntimeError
        # at config-load time. Same identifier discipline as pg_schema.
        raise RuntimeError(
            f"ICEBOX_PG_DATABASE {pg_database!r} is not a valid PG identifier "
            f"(must match {SAFE_PG_IDENTIFIER.pattern}; lowercase only)"
        )
    if pg_database.startswith("pg_"):
        raise RuntimeError(
            f"ICEBOX_PG_DATABASE {pg_database!r} starts with 'pg_' which is "
            f"reserved by Postgres for system databases"
        )

    pg_schema = _optional("ICEBOX_PG_SCHEMA", "icebox")
    validate_pg_schema(pg_schema, "ICEBOX_PG_SCHEMA")

    psycopg_pool_max = _int("ICEBOX_PSYCOPG_POOL_MAX", 4)
    if psycopg_pool_max < 3:
        # The daemon's tick holds one connection across the Iceberg
        # commit (up to iceberg_timeout_s, default 5s); the probe
        # server's /healthz handler reads PG on every kubelet probe
        # AND every Prometheus scrape. With max=2 a slow tick can
        # starve probes → 503 → kubelet restarts a healthy daemon.
        # Min 3 leaves room for tick + one concurrent probe; default
        # 4 covers liveness + readiness + a Prometheus scrape stack.
        raise RuntimeError(
            f"ICEBOX_PSYCOPG_POOL_MAX must be >= 3 (daemon tick holds 1 conn "
            f"across the Iceberg commit; probe server needs ≥1 more for "
            f"concurrent /healthz + /metrics callers), got {psycopg_pool_max}"
        )

    return Config(
        pg_host=_require("ICEBOX_PG_HOST"),
        pg_port=_int("ICEBOX_PG_PORT", 5432),
        pg_database=pg_database,
        pg_schema=pg_schema,
        pg_username=_optional("ICEBOX_PG_USERNAME", "lakekeeper"),
        pg_password=_require("ICEBOX_PG_PASSWORD"),
        pg_sslmode=_optional("ICEBOX_PG_SSLMODE", "require"),
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
        committer_max_pending_files=_int("ICEBOX_COMMITTER_MAX_PENDING_FILES", 100),
        committer_heartbeat_stale_multiple=_float(
            "ICEBOX_COMMITTER_HEARTBEAT_STALE_MULTIPLE", 3.0
        ),
        iceberg_timeout_s=_float("ICEBOX_ICEBERG_TIMEOUT_S", 5.0),
        age_filter_seconds=_float("ICEBOX_AGE_FILTER_SECONDS", 60.0),
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
