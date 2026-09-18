"""Sink protocol — the thin interface between main.py and a destination backend.

A `Sink` is the only thing `main.py` knows about. Both `DuckLakeSink` and
`HoglakeSink` implement it; `make_sink(cfg)` picks one based on
`cfg.destination`. A given Millpond pod writes to exactly one destination
for its lifetime — there is no per-batch routing.

This seam existed once before (DuckLake + Iceberg + icebox; last shipped
at tag `final-iceberg`) and was removed with the iceberg backends. It is
recovered here for the hoglake destination, with one deliberate change:
`write()` now returns the record count actually written (the DuckLake
backend grew that return value for the VARIANT companion-collision skip
path while the seam was gone, and main.py feeds it to
`records_written_total`).

This module also exports two shared helpers the backends both use:

* `SAFE_IDENTIFIER` — regex for column names that are safe to embed in
  generated SQL / send to the hoglake DDL surface. (schema.py re-exports
  it so its historical importers keep working.)
* `check_reserved_collision(batch_schema, reserved, backend_name)` —
  raises early with a uniform `ValueError` when a source-schema column
  collides with a backend-managed metadata column (`_inserted_at`,
  `year`, `month`, `day`, `hour`). Each backend keeps its own
  `RESERVED_COLUMNS` constant; both hold the same set today so a
  deployment-time destination switch doesn't suddenly start accepting
  or rejecting batches based on column-name collisions. DuckLake
  reserves `year/month/day/hour` defensively even though it doesn't
  produce them itself — that's the trade-off for deployment-swap
  safety, and Hoglake inherits the same posture. Sinks call this at
  the top of `write()` so the validation produces a clear error
  instead of a failure deep in the backend's append stack.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pyarrow as pa

    from millpond.config import Config


# Column names safe to embed in SQL / send to hoglake's DDL surface.
# Both backends apply this check; field names that don't match are skipped
# with a `records_skipped_total{reason="unsafe_field_name"}` metric bump.
SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def check_reserved_collision(
    batch_schema: pa.Schema,
    reserved: Iterable[str],
    backend_name: str,
) -> None:
    """Raise early on source-schema collision with backend-managed columns.

    Each backend appends a metadata column at write time (`_inserted_at`).
    If a source column has the same name, the backend's append step
    explodes deep in the stack (DuckLake: duplicate column on the
    post-write projection; Hoglake: pyhoglake's align/cast refusal).
    Catch it here with a clear message instead.

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
        on empty input but are not required to. (DuckLake creates the table
        eagerly on any call including empty; Hoglake refuses a zero-row
        append on a partitioned table — neither path is exercised in
        steady state.)
      * `write()` returns the record count actually written (0 when the
        backend skipped the batch whole, e.g. every column was a VARIANT
        companion collision) so main.py keeps `records_written_total`
        honest.
      * `reset_caches()` is invoked only by the write-retry loop in
        `main.py` after a write failure. Sinks should not self-reset
        on internal recovery; surface the failure and let the retry path
        drive cache invalidation.
      * `close()` is called exactly once at pod shutdown.
    """

    def write(self, batch: pa.Table) -> int:
        """Append `batch` (must be non-empty) to the destination table.
        Implementations handle schema evolution, table creation, and
        per-backend metadata columns internally. Returns the record
        count actually written."""
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

    Lazy import matters twice over: pyhoglake pulls httpx and the DuckLake
    module pulls duckdb, so each destination's pods skip the other's
    import cost — and a broken optional backend module can never take
    down the other destination's pods at import time. The lazy-import
    test in test_sink.py asserts this stays a lazy import by source
    inspection.
    """
    if cfg.destination == "ducklake":
        from millpond.ducklake import DuckLakeSink

        return DuckLakeSink(cfg)
    if cfg.destination == "hoglake":
        from millpond.hoglake import HoglakeSink

        return HoglakeSink(cfg)
    # ValueError, not RuntimeError — this is an unknown-enum input, the
    # idiomatic Python exception for "the value I got isn't in the set
    # I accept." config.load() should have already rejected this at
    # startup; reaching here means the caller bypassed config.load().
    raise ValueError(f"Unknown destination: {cfg.destination!r}")
