"""Cache for the Iceberg table's current schema fingerprint.

The icebox API perimeter rejects POSTs whose ``schema_fingerprint`` field
doesn't match the table's current schema (per
``shared.fingerprint.schema_fingerprint``). Loading the table from the
catalog on every POST is too expensive (Lakekeeper REST takes ~50-200ms
per ``load_table``), so we keep a single cached fingerprint per icebox
deployment with a short TTL.

Design constraints:

  - Each icebox serves exactly one (namespace, table). One cache
    instance per app.
  - The catalog is the source of truth. We accept up to ``ttl_seconds``
    of staleness on the cache.
  - When a writer presents a fingerprint that doesn't match the cached
    value, we **refresh once and re-validate** before rejecting. This
    handles the legitimate schema-migration race where the catalog
    just got an ALTER TABLE and our cache hasn't expired yet —
    refusing in that window would manufacture false mismatches.
  - When ``load_table`` raises (catalog unreachable), we **fail open**.
    The committer's own fingerprint check is preserved as defense in
    depth; rejecting POSTs because the catalog is unreachable would
    violate the existing ``/readyz`` contract that says downstream
    outages do NOT fail the icebox.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from shared.fingerprint import schema_fingerprint

log = logging.getLogger(__name__)


@dataclass
class _CacheState:
    fingerprint: str | None
    cached_at_monotonic: float


class SchemaFingerprintCache:
    """Async-safe TTL cache for the current Iceberg table fingerprint."""

    def __init__(
        self,
        load_table: Callable[[], Any],
        *,
        ttl_seconds: float = 60.0,
    ) -> None:
        self._load_table = load_table
        self._ttl_seconds = ttl_seconds
        self._state = _CacheState(fingerprint=None, cached_at_monotonic=0.0)
        # Single-flight: only one refresh at a time, even under
        # concurrent POSTs. Subsequent awaiters see the just-refreshed
        # value after the lock releases.
        self._lock = asyncio.Lock()

    @property
    def ttl_seconds(self) -> float:
        return self._ttl_seconds

    def _is_fresh(self, state: _CacheState) -> bool:
        if state.fingerprint is None:
            return False
        return (time.monotonic() - state.cached_at_monotonic) < self._ttl_seconds

    async def current(self, *, force_refresh: bool = False) -> str:
        """Return the current cached fingerprint, refreshing if expired
        or if ``force_refresh`` is True.

        Raises whatever ``load_table()`` raises if a refresh is
        attempted and the catalog is unreachable. Callers that want
        fail-open semantics should catch and decide.
        """
        if not force_refresh and self._is_fresh(self._state):
            return self._state.fingerprint  # type: ignore[return-value]
        async with self._lock:
            # Re-check after acquiring the lock — another coroutine
            # may have refreshed while we waited.
            if not force_refresh and self._is_fresh(self._state):
                return self._state.fingerprint  # type: ignore[return-value]
            # ``load_table`` is synchronous (it calls into PyIceberg's
            # ``catalog.load_table``, which issues blocking HTTP). Run
            # it in a thread so the event loop stays responsive while
            # the catalog responds.
            table = await asyncio.to_thread(self._load_table)
            fp = schema_fingerprint(table.schema())
            self._state = _CacheState(
                fingerprint=fp, cached_at_monotonic=time.monotonic()
            )
            return fp

    async def validate(self, claimed_fingerprint: str) -> bool:
        """Return True iff the claimed fingerprint matches the table's
        current schema. On mismatch with the cached value, force a
        refresh and re-check before returning False — this handles the
        legitimate post-ALTER-TABLE race where the cache is stale.

        Propagates exceptions from ``load_table`` so the caller can
        decide fail-open vs fail-closed.
        """
        current = await self.current()
        if claimed_fingerprint == current:
            return True
        log.info(
            "schema_fingerprint_cache: cached fingerprint did not match "
            "writer-claimed value; forcing refresh"
        )
        refreshed = await self.current(force_refresh=True)
        return claimed_fingerprint == refreshed
