"""Unit tests for icebox/schema_cache.py.

Covers cache hit, cache miss after TTL expiry, refresh-on-mismatch,
fail-open propagation, and single-flight refresh under concurrent
``validate`` calls.
"""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from icebox.schema_cache import SchemaFingerprintCache


def _table_with_schema(fp: str) -> MagicMock:
    """Build a minimal mock of a PyIceberg Table whose schema's
    ``schema_fingerprint(...)`` resolves to ``fp``."""
    table = MagicMock()
    # We mock the schema_fingerprint call site directly via a stub
    # ``schema()`` that returns a sentinel; the test monkeypatches
    # ``shared.fingerprint.schema_fingerprint`` to map sentinel -> fp.
    schema = MagicMock()
    schema._stub_fp = fp
    table.schema.return_value = schema
    return table


@pytest.fixture
def fp_stub(monkeypatch):
    """Patch ``shared.fingerprint.schema_fingerprint`` so the cache
    deterministically derives a fingerprint from our mock schemas."""

    def fake_fp(schema):
        return getattr(schema, "_stub_fp", "unknown")

    monkeypatch.setattr(
        "icebox.schema_cache.schema_fingerprint", fake_fp
    )


async def _run(coro):
    return await coro


def test_cache_returns_loaded_fingerprint_on_first_call(fp_stub):
    loader = MagicMock(return_value=_table_with_schema("fp-A"))
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=60.0)
    current = asyncio.run(cache.current())
    assert current == "fp-A"
    loader.assert_called_once()


def test_cache_hit_within_ttl_does_not_reload(fp_stub):
    loader = MagicMock(return_value=_table_with_schema("fp-A"))
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=60.0)
    asyncio.run(cache.current())
    asyncio.run(cache.current())
    asyncio.run(cache.current())
    loader.assert_called_once()


def test_cache_reloads_after_ttl_expires(fp_stub):
    loader = MagicMock(return_value=_table_with_schema("fp-A"))
    # Zero TTL ⇒ every call is a miss.
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=0.0)
    asyncio.run(cache.current())
    asyncio.run(cache.current())
    assert loader.call_count == 2


def test_validate_returns_true_on_cached_match(fp_stub):
    loader = MagicMock(return_value=_table_with_schema("fp-X"))
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=60.0)
    assert asyncio.run(cache.validate("fp-X")) is True
    # Loader called exactly once — the validate path is a cache hit
    # after the initial population.
    assert loader.call_count == 1


def test_validate_forces_refresh_on_mismatch_and_reads_fresh(fp_stub):
    # Loader returns "fp-A" once, then "fp-B" on the forced refresh.
    loader = MagicMock(
        side_effect=[
            _table_with_schema("fp-A"),  # initial populate
            _table_with_schema("fp-B"),  # forced refresh after mismatch
        ]
    )
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=60.0)
    # Writer presents fp-B; cached is fp-A; refresh sees fp-B; accept.
    result = asyncio.run(cache.validate("fp-B"))
    assert result is True
    assert loader.call_count == 2


def test_validate_returns_false_when_mismatch_persists_after_refresh(fp_stub):
    loader = MagicMock(
        side_effect=[
            _table_with_schema("fp-A"),
            _table_with_schema("fp-A"),  # refresh still shows A
        ]
    )
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=60.0)
    assert asyncio.run(cache.validate("fp-Z")) is False
    # Two loads: initial + the forced-refresh on mismatch.
    assert loader.call_count == 2


def _miss_count(reason: str) -> float:
    """Read the labeled cache-miss counter via the public registry idiom.
    Returns 0.0 when the label hasn't been seen yet (sample absent)."""
    from prometheus_client import REGISTRY

    value = REGISTRY.get_sample_value(
        "icebox_schema_fingerprint_cache_misses_total",
        labels={"reason": reason},
    )
    return value or 0.0


def test_validate_mismatch_with_stale_cache_reports_cache_stale_after_alter(fp_stub):
    """Cache held fp-A but the catalog (post-ALTER) actually serves
    fp-B; writer claims fp-B. Reason: cache_stale_after_alter — the
    cache was just behind reality. Normal; not alertable."""
    loader = MagicMock(
        side_effect=[
            _table_with_schema("fp-A"),  # initial populate
            _table_with_schema("fp-B"),  # forced refresh after mismatch
        ]
    )
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=60.0)
    before_stale = _miss_count("cache_stale_after_alter")
    before_mismatch = _miss_count("fingerprint_mismatch")
    asyncio.run(cache.validate("fp-B"))
    assert _miss_count("cache_stale_after_alter") == before_stale + 1
    assert _miss_count("fingerprint_mismatch") == before_mismatch


def test_validate_mismatch_with_unknown_fingerprint_reports_fingerprint_mismatch(fp_stub):
    """Cache held fp-A; refresh still returns fp-A; writer claims fp-Z
    — the catalog doesn't know fp-Z. Reason: fingerprint_mismatch.
    Alertable."""
    loader = MagicMock(
        side_effect=[
            _table_with_schema("fp-A"),
            _table_with_schema("fp-A"),  # refresh still shows A
        ]
    )
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=60.0)
    before_stale = _miss_count("cache_stale_after_alter")
    before_mismatch = _miss_count("fingerprint_mismatch")
    asyncio.run(cache.validate("fp-Z"))
    assert _miss_count("cache_stale_after_alter") == before_stale
    assert _miss_count("fingerprint_mismatch") == before_mismatch + 1


def test_validate_cached_match_does_not_increment_cache_miss_counter(fp_stub):
    """Sanity check the inverse — a cache hit must NOT touch either
    miss-counter label."""
    loader = MagicMock(return_value=_table_with_schema("fp-X"))
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=60.0)
    before_stale = _miss_count("cache_stale_after_alter")
    before_mismatch = _miss_count("fingerprint_mismatch")
    asyncio.run(cache.validate("fp-X"))
    assert _miss_count("cache_stale_after_alter") == before_stale
    assert _miss_count("fingerprint_mismatch") == before_mismatch


def test_current_propagates_loader_exception(fp_stub):
    loader = MagicMock(side_effect=RuntimeError("catalog unreachable"))
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=60.0)
    with pytest.raises(RuntimeError, match="catalog unreachable"):
        asyncio.run(cache.current())


def test_validate_propagates_loader_exception_when_cache_empty(fp_stub):
    """Caller is responsible for fail-open semantics — the cache itself
    surfaces catalog errors honestly."""
    loader = MagicMock(side_effect=RuntimeError("catalog unreachable"))
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=60.0)
    with pytest.raises(RuntimeError):
        asyncio.run(cache.validate("fp-X"))


def test_single_flight_under_concurrent_validate(fp_stub):
    """Two concurrent validate() calls on an empty cache must result in
    exactly ONE load_table invocation (single-flight via asyncio.Lock).
    """
    import threading

    in_loader = threading.Event()
    proceed = threading.Event()

    def slow_loader():
        in_loader.set()
        proceed.wait(timeout=5)
        return _table_with_schema("fp-A")

    loader = MagicMock(side_effect=slow_loader)
    cache = SchemaFingerprintCache(load_table=loader, ttl_seconds=60.0)

    async def _two_in_flight():
        # Kick off two validate() coroutines; let the first land in
        # the loader, then release.
        t1 = asyncio.create_task(cache.validate("fp-A"))
        t2 = asyncio.create_task(cache.validate("fp-A"))
        # Give task 1 enough time to enter the loader.
        await asyncio.sleep(0.05)
        proceed.set()
        return await asyncio.gather(t1, t2)

    r1, r2 = asyncio.run(_two_in_flight())
    assert r1 is True
    assert r2 is True
    # Single-flight: only one loader invocation, even with two
    # concurrent validate calls on an empty cache.
    assert loader.call_count == 1
