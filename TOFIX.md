# TOFIX: pyducklake DETACH-before-close (Leak A) — did not ship in 1.0.17

> **STATUS 2026-08-14: FIXED in pyducklake 1.0.18** (published 22:50Z,
> wheel-verified: `Catalog.close()` DETACHes best-effort with a quoted
> identifier before `conn.close()`; duckdb pin unchanged at ==1.5.5).
> viaduck pins `>=1.0.18` on branch `jakob/watermark-self-recycle`
> (897 tests green). Remaining chain step: bump MILLPOND to
> `pyducklake>=1.0.18` + advance its `exclude-newer-package` date past
> 2026-08-14T22:50Z. The implementation notes below are kept for the
> record; the verification section still applies to the millpond bump.

## What this is

The DETACH-before-close fix for **Leak A** — ~5.5MB of DuckLake catalog
state orphaned permanently per **conflicted** connection close — was planned
for pyducklake 1.0.17 but is **not in the published wheel**. Verified
2026-08-14 by downloading and diffing the 1.0.16 and 1.0.17 wheels from the
uv.lock URLs: `Catalog.close()` is byte-identical (`self._conn.close()`, no
DETACH; zero `DETACH` hits anywhere in the package). The full 1.0.17 delta is
the `duckdb==1.5.5` pin move, append-profiling plumbing (`catalog.py`,
`table.py`, new `profiling.py`), an `older_than`/`versions` mutual-exclusion
check in `maintenance.py`, and `pyarrow<26`.

Until this ships, viaduck's watermark self-recycle is the **only** mitigation
for Leak A, and both viaduck's and millpond's pyproject comments should not
claim otherwise (viaduck's was corrected 2026-08-14).

## The fix (in jghoman/pyducklake, target release 1.0.18)

In `pyducklake/catalog.py`, `Catalog.close()`: best-effort `DETACH` of the
ducklake attachment **before** `self._conn.close()`:

```python
def close(self):
    try:
        self._conn.execute(f'DETACH "{self._attach_name}"')
    except Exception:
        pass  # already detached / conn dead — close() below is the fallback
    self._conn.close()
```

Details for the implementer to verify (from the original diagnosis,
`~/src/viaduck/hypothesis-2.txt`, "Fix directions" §1):

- The leak only manifests on **conflicted** closes (a connection whose last
  transaction aborted on an OCC conflict). Clean closes are already
  leak-free — so the DETACH must be safe/no-op on both.
- Whether a `ROLLBACK` is needed before the DETACH on a conflict-tainted
  connection: the hypothesis-2 A/B ran DETACH directly and measured flat, but
  re-verify with the repro rather than assuming.
- Quote the attach name defensively (identifiers with special chars).

## Why it works (rationale)

The DuckLake extension frees its per-connection catalog caches (stats,
schemas, transaction residue) on **DETACH**. `conn.close()` without a DETACH
takes duckdb-core's instance-teardown path, and when the attached catalog
carries conflict residue (an aborted `FlushChanges` — see
`ducklake_transaction.cpp` catch path), that teardown skips the cleanup and
the allocation is orphaned for the life of the process. Measured in
hypothesis-2's A/B: ~5.5MB per conflicted close on the repro catalog; flat
(slope −0.36MB/iter over iters 30–99) with DETACH-before-close. At prod
metadata volume the extrapolation is ~160MB per viaduck evict cycle
(extrapolation, not measurement — see hypothesis-2 open questions).

Root-cause alternatives (optional, NOT blockers for 1.0.18): fork-side
`PQclear` in postgres_scanner's ExecuteQueries/schema-load, and the
duckdb-core teardown-with-conflict-residue path (hypothesis-2 "Fix
directions" §4).

## Verification

- Repro methodology and artifacts: `~/src/viaduck/hypothesis-2.txt`
  ("Repro artifacts" section) — loop: attach postgres-backed ducklake,
  force an OCC conflict, close; watch RSS slope over ≥100 iterations.
  Expect: ~+5.5MB/iter without the fix, ≤0 with it.
- Existing pyducklake test suite must pass; add a unit test that `close()`
  issues the DETACH and tolerates a dead connection (double-close, closed
  underlying conn).

## Release + downstream chain

1. pyducklake 1.0.18 to PyPI (keep `duckdb==1.5.5` pin unchanged).
2. viaduck: bump `pyducklake>=1.0.18` and advance the
   `[tool.uv] exclude-newer-package` pyducklake date past the publish
   timestamp (currently `2026-08-14T00:00:00Z`); same in millpond
   (`pyproject.toml`, same carve-out pattern, from millpond#119).
3. Correct viaduck's pyproject comment (it currently says the fix is
   "still pending a future pyducklake release" — flip to naming 1.0.18)
   and this file: delete it once the chain lands.

viaduck's watermark self-recycle stays in place regardless — it also covers
the distinct in-lifetime Leak B residual (~2.5–4 GiB/h), which this fix does
not address.
