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

    # Connection pool sizing — see ICEBOX-PLAN.md "Async-vs-sync"
    asyncpg_pool_min: int
    asyncpg_pool_max: int
    psycopg_pool_min: int
    psycopg_pool_max: int

    # Iceberg catalog (Lakekeeper) — in-cluster URL
    iceberg_catalog_uri: str
    iceberg_warehouse: str

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
    # the cadence. See ICEBOX-PLAN.md "Committer thread liveness".
    committer_heartbeat_stale_multiple: float

    # REST API
    api_host: str
    api_port: int

    # Logging
    log_level: str


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

    Optional with defaults match the ICEBOX-PLAN.md spec.
    """
    cadence = _int("ICEBOX_COMMITTER_CADENCE_SECONDS", 60)
    if cadence < 1:
        raise RuntimeError(
            f"ICEBOX_COMMITTER_CADENCE_SECONDS must be >= 1, got {cadence}"
        )

    return Config(
        pg_host=_require("ICEBOX_PG_HOST"),
        pg_port=_int("ICEBOX_PG_PORT", 5432),
        pg_database=_optional("ICEBOX_PG_DATABASE", "icebox"),
        pg_username=_optional("ICEBOX_PG_USERNAME", "lakekeeper"),
        pg_password=_require("ICEBOX_PG_PASSWORD"),
        pg_sslmode=_optional("ICEBOX_PG_SSLMODE", "require"),
        asyncpg_pool_min=_int("ICEBOX_ASYNCPG_POOL_MIN", 2),
        asyncpg_pool_max=_int("ICEBOX_ASYNCPG_POOL_MAX", 8),
        psycopg_pool_min=_int("ICEBOX_PSYCOPG_POOL_MIN", 1),
        psycopg_pool_max=_int("ICEBOX_PSYCOPG_POOL_MAX", 2),
        iceberg_catalog_uri=_require("ICEBOX_ICEBERG_CATALOG_URI"),
        iceberg_warehouse=_optional("ICEBOX_ICEBERG_WAREHOUSE", "ingest"),
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
    )
