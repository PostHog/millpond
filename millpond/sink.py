"""Sink protocol — the thin interface between main.py and a destination backend.

A `Sink` is the only thing `main.py` knows about. Both `DuckLakeSink` and
`IcebergSink` implement it; `make_sink(cfg)` picks one based on
`cfg.destination`. A given Millpond pod writes to exactly one destination
for its lifetime — there is no per-batch routing.

This module also exports two shared helpers the backends both use:

* `SAFE_IDENTIFIER` — regex for column names that are safe to embed in
  generated SQL / pass to PyIceberg's schema constructor.
* `check_reserved_collision(batch_schema, reserved, backend_name)` —
  raises early with a uniform `ValueError` when a source-schema column
  collides with a backend-managed metadata column (`_inserted_at`,
  `year`, `month`, `day`, `hour`). Each backend keeps its own
  `RESERVED_COLUMNS` constant; both happen to hold the same set today
  so a deployment-time destination switch doesn't suddenly start
  accepting or rejecting batches based on column-name collisions.
  DuckLake reserves `year/month/day/hour` defensively even though it
  doesn't produce them itself — that's the trade-off for deployment-
  swap safety. Sinks call this at the top of `write()`; the validation
  produces a clear error instead of PyIceberg's cryptic
  "Invalid schema, multiple fields for name" deep in the stack.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pyarrow as pa

    from millpond.config import Config


# Column names safe to embed in SQL / pass to PyIceberg's schema constructor.
# Both backends apply this check; field names that don't match are skipped
# with a `records_skipped_total{reason="unsafe_field_name"}` metric bump.
SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def check_reserved_collision(
    batch_schema: pa.Schema,
    reserved: Iterable[str],
    backend_name: str,
) -> None:
    """Raise early on source-schema collision with backend-managed columns.

    Each backend appends one or more metadata columns at write time
    (`_inserted_at` always, plus `year/month/day/hour` on Iceberg). If a
    source column has the same name, the backend's append step explodes
    deep in the stack (Iceberg: `ValueError("Invalid schema, multiple
    fields for name year")`; DuckLake: duplicate column on the post-
    write projection). Catch it here with a clear message instead.

    Raised at the Sink boundary — before any backend-specific work — so
    a single misconfigured producer surfaces uniformly across backends.
    """
    reserved_set = set(reserved)
    collisions = sorted(name for name in batch_schema.names if name in reserved_set)
    if collisions:
        raise ValueError(
            f"Source schema column(s) {collisions!r} collide with "
            f"{backend_name}-reserved metadata column names; rename them "
            f"upstream or filter them out before write()."
        )


class Sink(Protocol):
    """A destination for Arrow batches. Owns its own connection, table cache, and schema state.

    Contract:
      * `write()` must not be called with a zero-row batch. `main.py` gates
        on `pending_records > 0` before flushing; backends may short-circuit
        on empty input but are not required to. (The two backends diverge
        here: DuckLake creates the table eagerly on any call; Iceberg
        skips the catalog round-trip and creates lazily on first non-empty
        batch. Neither path is exercised in steady state.)
      * `reset_caches()` is invoked only by the write-retry loop in
        `main.py` after a write failure. Sinks should not self-reset
        on internal recovery; surface the failure and let the retry path
        drive cache invalidation.
      * `close()` is called exactly once at pod shutdown.
    """

    def write(self, batch: pa.Table) -> None:
        """Append `batch` (must be non-empty) to the destination table.
        Implementations handle schema evolution, table creation, and
        per-backend metadata columns internally."""
        ...

    def reset_caches(self) -> None:
        """Drop any cached table/schema state. Called from the write-retry path
        after a failure, so the next attempt re-checks the catalog."""
        ...

    def close(self) -> None:
        """Release any underlying resources. Called once at pod shutdown."""
        ...


def make_sink(cfg: Config) -> Sink:
    """Dispatch on `cfg.destination`. Imports the backend module lazily so we
    don't pay the import cost of an unused backend.

    Lazy import matters: pyiceberg pulls cryptography/aiohttp/etc.
    transitively (~150ms cold start on a small pod), so DuckLake-only
    deployments shouldn't pay it. The conformance test in test_sink.py
    asserts this stays a lazy import by source inspection.
    """
    if cfg.destination == "iceberg":
        from millpond.iceberg import IcebergSink

        return IcebergSink(cfg)
    if cfg.destination == "ducklake":
        from millpond.ducklake import DuckLakeSink

        return DuckLakeSink(cfg)
    if cfg.destination == "icebox":
        # Lazy import: pulls httpx + fastapi-adjacent transitive deps that
        # DuckLake-only deployments don't need.
        from millpond.icebox_sink import IceboxClient, IceboxSink

        client = IceboxClient(
            base_url=cfg.icebox_url,
            max_attempts=cfg.icebox_max_attempts,
            max_backoff_s=cfg.icebox_max_backoff_s,
            timeout_s=cfg.icebox_timeout_s,
        )
        return IceboxSink(
            client=client,
            writer_ordinal=cfg.ordinal,
            bucket=cfg.icebox_bucket,
            warehouse_prefix=cfg.icebox_warehouse_prefix,
            namespace=cfg.iceberg_namespace,
            table=cfg.iceberg_table,
        )
    # ValueError, not RuntimeError — this is an unknown-enum input, the
    # idiomatic Python exception for "the value I got isn't in the set
    # I accept." config.load() should have already rejected this at
    # startup; reaching here means the caller bypassed config.load().
    raise ValueError(f"Unknown destination: {cfg.destination!r}")
