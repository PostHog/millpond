"""Engine-behavior contract for the pinned DuckDB + DuckLake build.

`tests/unit/test_ducklake_pin.py` pins WHICH build we run;
this file pins the physical behaviors millpond's variant guard ASSUMES of
that build, probed the way the 2026-08-12 incident actually executed:
raw `try_cast(... AS VARIANT)` INSERTs against a real DuckLake catalog,
with millpond's guard deliberately bypassed.

The load-bearing invariant is set containment, not any specific boundary:

    every value the engine refuses to shred is a value
    `sanitize_variant_sources` rewrites first

If a version bump shifts the rejection window (wider: prod crash-loops on
values the prefilter never flags; narrower: the guard is stringifying
values that now shred fine), or changes the failure's error signature so
`_is_unshreddable_value_error` stops matching (write-time backstop goes
blind), this file fails at the bump PR instead of on the running fleet.
The 1.5.2 -> 1.5.5 bump is the motivating example: it changed the inlined
failure's reported width (UINT64 -> INT128) and the out-of-range
representation (VARCHAR -> DOUBLE) — both invisible to in-memory tests.
"""

from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from millpond.ducklake import (
    _is_unshreddable_value_error,
    sanitize_variant_sources,
)


def _doc(value_literal: str) -> str:
    return f'{{"v": {value_literal}}}'


# JSON-document probes around every edge the guard's window logic and Arrow
# prefilter are shaped to, plus non-scalar shapes (shredding is per-path, so
# nested/array rejection behavior can diverge from top-level independently).
# `shreds` is what the CURRENT pinned build does with the raw document; the
# containment assertion below is what running millponds actually depend on.
# Probes are pre-rendered JSON strings so no Python-repr formatting (floats,
# bools) can leak into the SQL literal.
_PROBES = [
    pytest.param(_doc(str(2**63 - 1)), True, id="int64-max-shreds"),
    pytest.param(_doc(str(2**63)), False, id="window-start-rejected"),
    pytest.param(_doc("9223372036854775999"), False, id="incident-shape-rejected"),
    pytest.param(_doc(str(2**64 - 1)), False, id="uint64-max-rejected"),
    pytest.param(_doc(str(2**64)), True, id="above-uint64-shreds-as-double"),
    pytest.param(_doc(str(10**30)), True, id="huge-shreds-as-double"),
    pytest.param(_doc(str(-(2**63))), True, id="int64-min-shreds"),
    pytest.param(_doc(str(-(2**63) - 1)), True, id="below-int64-min-shreds-as-double"),
    pytest.param(_doc("1723526400000000000"), True, id="ns-timestamp-shreds"),
    pytest.param(_doc("1234567890123456789"), True, id="snowflake-id-shreds"),
    # JSON's reader types any decimal/exponent form as DOUBLE, so these shred
    # regardless of magnitude — and the guard must NOT rewrite them.
    pytest.param(_doc("9.3e18"), True, id="sci-notation-shreds-as-double"),
    pytest.param(_doc("9223372036854775808.5"), True, id="decimal-shreds-as-double"),
    # Window values below the top level: rejected and guarded identically
    # today; pinned separately because per-path shredding could change one
    # without the other.
    pytest.param(f'{{"outer": {{"inner": {2**63}}}}}', False, id="nested-window-rejected"),
    pytest.param(f'{{"arr": [{2**63}]}}', False, id="array-window-rejected"),
]


def _raw_variant_insert(conn: duckdb.DuckDBPyConnection, doc: str) -> None:
    """The incident's execution shape: guard bypassed, shredded Parquet write."""
    conn.execute("CREATE TABLE IF NOT EXISTS lake.main.contract (id INT, v VARIANT)")
    conn.execute(f"INSERT INTO lake.main.contract SELECT 1, try_cast('{doc}'::JSON AS VARIANT)")


def _guard_rewrites(doc: str) -> bool:
    """Does the REAL production guard chain (Arrow prefilter -> precise JSON
    pass) rewrite this document? Testing through sanitize_variant_sources
    rather than _coerce_unshreddable_ints so a prefilter miss counts as a
    miss."""
    batch = pa.table({"properties": [doc]})
    _, projection_map = sanitize_variant_sources(batch, ("properties",))
    return "properties" in projection_map


@pytest.mark.integration
@pytest.mark.parametrize(("doc", "shreds"), _PROBES)
def test_engine_rejections_are_a_subset_of_guard_rewrites(ducklake_conn, doc, shreds):
    conn = ducklake_conn
    try:
        _raw_variant_insert(conn, doc)
        engine_rejects = False
    except Exception as exc:  # noqa: BLE001 — classified below, not swallowed
        engine_rejects = True
        # The write-time backstop must recognize the failure, whatever
        # integer width this build reports it as.
        assert _is_unshreddable_value_error(exc), f"unmatched signature: {exc}"

    assert engine_rejects == (not shreds), (
        f"pinned build changed shred behavior for {doc}: "
        f"expected shreds={shreds}. Re-derive the danger window and realign "
        f"_INT64_MAX/_UINT64_MAX and _MAYBE_UNSHEDDABLE_DIGITS in ducklake.py."
    )

    # THE contract: anything the engine rejects, the guard must have
    # rewritten before it ever reaches the engine. The converse is pinned
    # too — a guard false-positive on a shreddable value costs companion
    # typing fidelity (ns timestamps as strings was the pre-#118 heuristic's
    # failure mode), so these probe docs must round-trip untouched.
    if engine_rejects:
        assert _guard_rewrites(doc), (
            f"{doc} fails the shredded write but escapes "
            f"sanitize_variant_sources — running millponds would crash-loop "
            f"on it (2026-08-12 shape)."
        )
    else:
        assert not _guard_rewrites(doc), (
            f"{doc} shreds fine but the guard rewrites it — companion typing "
            f"fidelity lost for a value the engine accepts."
        )


@pytest.mark.integration
def test_inlined_commit_detonates_at_flush_with_matched_signature(ducklake_conn_inlining):
    """The deferred failure mode: an unshreddable value COMMITS when the write
    is small enough to inline, then fails ducklake_flush_inlined_data later —
    where no write-time retry can reach it. Running millponds rely on the
    per-row guard being the only defense here; this pins that the mode still
    exists, still commits silently, and still reports a signature the matcher
    recognizes (1.5.5 changed its reported width to INT128)."""
    conn = ducklake_conn_inlining
    _raw_variant_insert(conn, _doc(str(2**63)))  # window start; commits inlined
    with pytest.raises(Exception) as excinfo:
        conn.execute("CALL ducklake_flush_inlined_data('lake')")
    assert _is_unshreddable_value_error(excinfo.value), (
        f"inlined-flush failure signature no longer matched: {excinfo.value}"
    )
