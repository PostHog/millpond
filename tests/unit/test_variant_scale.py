"""Scale measurement for the VARIANT write path, at production shape.

NOT a CI test. It builds a whale-shaped batch of ~155k rows (~13.8 KB a
row: `properties` ~9.6 KB, `person_properties` ~4 KB, sparse groups) over
~532 (team, month) partition groups — roughly the 2 GiB Arrow flush the
whales writer makes every 120 s — and runs it through the real sink's
local work (null-fill, align, partition split, and pyhoglake's `write_part`
per group with the native codec; no uploads), measuring:

* the wall time of the prepare, against `_PREPARE_ALLOWANCE_S` (the floor
  the startup model prices an attempt at; the runtime deadline reserves the
  measured time instead, see `config.hoglake_flush_deadline`);
* the per-group fixed overhead, by difference against a one-group run;
* peak RSS over the input's Arrow size, variant columns against the same
  batch into string columns.

Opt in with MILLPOND_RUN_SCALE=1 (and MILLPOND_SCALE_ROWS to shrink it);
`-s` prints the numbers. Needs several GiB free.

Run:
    MILLPOND_RUN_SCALE=1 uv run pytest tests/unit/test_variant_scale.py -s -m slow
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pyarrow as pa
import pytest
from pyhoglake import Column, PartitionField, PartitionSpec

from millpond.config import _PREPARE_ALLOWANCE_S
from tests.hoglake_stack.variant_table import GROUP_DECL, PERSON_DECL, PROPERTIES_SV
from tests.unit.test_hoglake import _cfg, _sink, _wire_real_encode

pytestmark = pytest.mark.slow

ROWS = int(os.environ.get("MILLPOND_SCALE_ROWS", "155000"))
GROUPS = 532
TS_TYPE = pa.timestamp("us", tz="UTC")
# Peak RSS over the input's Arrow bytes the variant path may cost, each run
# in a fresh interpreter. The partition split copies the input once, and
# the encoded storage of one group is ~1.1x that group's Arrow size for the
# duration of its write; uniform groups make the second term small, the
# one-group (hot team) run pays it whole. Measured 2026-11-09, M4 Pro,
# 155k rows / 2.14 GiB, each run its own interpreter: string columns
# 2.5 s, +0.70 GiB (0.33x); variant over 532 groups 6.4 s (41 us/row,
# ~25 us/row of it the encode), +0.71 GiB (0.33x); variant into ONE group
# 3.0 s, +4.09 GiB (1.92x) — the whole-flush transient the hot-team shape
# pays; ~6.5 ms of fixed overhead per group. The bound leaves ~30% over
# the one-group figure.
K_RSS = 2.5

if not os.environ.get("MILLPOND_RUN_SCALE"):
    pytest.skip("scale measurement; set MILLPOND_RUN_SCALE=1 to run", allow_module_level=True)


def _columns(variant: bool) -> list[Column]:
    def payload(name, field_id, decl):
        kind = {"type": "variant", "type_params": {"shredding": decl}} if variant else {"type": "string"}
        return Column(name=name, field_id=field_id, ordinal=field_id, **kind)

    return [
        Column(name="uuid", type="string", field_id=1, ordinal=1),
        Column(name="event", type="string", field_id=2, ordinal=2),
        Column(name="team_id", type="long", field_id=3, ordinal=3),
        Column(name="timestamp", type="timestamptz", field_id=4, ordinal=4),
        payload("properties", 5, PROPERTIES_SV),
        payload("person_properties", 6, PERSON_DECL),
        *(payload(f"group{i}_properties", 7 + i, GROUP_DECL) for i in range(5)),
        Column(name="_inserted_at", type="timestamptz", field_id=12, ordinal=12),
    ]


def _properties(i: int) -> str:
    """~9.6 KB: the sixteen declared leaves, plus undeclared keys that carry
    the bulk, as a real `properties` does."""
    declared = {f["name"]: f"v{i % 97}-{f['name'][1:]}" for f in PROPERTIES_SV["fields"]}
    filler = {f"$custom_{k}": f"{i:08d}-" + "x" * 180 for k in range(48)}
    return json.dumps({**declared, **filler, "n": i, "nested": {"depth": [1, 2, {"x": i % 7}]}})


def _person(i: int) -> str:
    """~4 KB."""
    return json.dumps({"email": f"p{i}@ex.am", "age": "thirty", **{f"trait_{k}": "y" * 90 for k in range(40)}})


def _batch(rows: int) -> pa.Table:
    return pa.table(
        {
            "uuid": [f"{i:032x}" for i in range(rows)],
            "event": ["$pageview" if i % 2 else "$autocapture" for i in range(rows)],
            "team_id": [i % GROUPS for i in range(rows)],
            "timestamp": pa.array([1_789_000_000_000_000 + i for i in range(rows)], pa.int64()).cast(TS_TYPE),
            "properties": [_properties(i) for i in range(rows)],
            "person_properties": [_person(i) for i in range(rows)],
            "group0_properties": [json.dumps({"name": "acme"}) if i % 2 == 0 else None for i in range(rows)],
        }
    )


def _rss_bytes() -> int:
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True, check=True)
    return int(out.stdout.strip()) * 1024


class _PeakRss:
    """Sample this process's RSS on a thread while the body runs."""

    def __init__(self) -> None:
        self.peak = 0
        self._stop = threading.Event()

    def __enter__(self):
        self.peak = _rss_bytes()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.wait(0.05):
            self.peak = max(self.peak, _rss_bytes())

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join()
        self.peak = max(self.peak, _rss_bytes())


def _measure(rows: int, *, variant: bool, groups: int) -> dict:
    """One write of a `rows`-row whale batch through the real local path
    against a mocked server, IN THIS PROCESS: (prepare wall seconds, RSS
    before, peak RSS, the sink's measured prepare and reservation). Run
    through `_in_subprocess` so one measurement's retained allocator
    memory cannot skew the next's delta."""
    batch = _batch(rows)
    fid = {c.name: c.field_id for c in _columns(variant)}
    fields = (PartitionField(fid["team_id"], "identity", None), PartitionField(fid["timestamp"], "month", None))
    spec = PartitionSpec(spec_id=1, fields=fields)
    cfg = _cfg(hoglake_partition_by=(("team_id", "identity", None), ("timestamp", "month", None)))
    s, client, catalog, ns, table = _sink(cfg, columns=_columns(variant), partition_spec=spec)
    _wire_real_encode(table)
    if groups == 1:
        batch = batch.set_column(batch.schema.get_field_index("team_id"), "team_id", pa.array([1] * batch.num_rows))
    before = _rss_bytes()
    with _PeakRss() as rss:
        started = time.perf_counter()
        assert s.write(batch, kafka_offsets=(("whales", 0, 0, batch.num_rows - 1),)) == batch.num_rows
        elapsed = time.perf_counter() - started
    budget, reserve = s.write_flush_deadline()
    return {
        "rows": batch.num_rows,
        "nbytes": batch.nbytes,
        "elapsed": elapsed,
        "rss_before": before,
        "rss_peak": rss.peak,
        "backend": s._variant_backend,
        "last_prepare_s": s._last_prepare_s,
        "reserve": reserve,
        "budget": budget,
    }


def _in_subprocess(rows: int, *, variant: bool, groups: int) -> dict:
    """`_measure` in a fresh interpreter, so each run starts from the same
    baseline RSS: pyarrow's allocator retains memory a previous run freed,
    and a second measurement in the same process would read a delta over
    an already-grown footprint (or hide inside it)."""
    env = {**os.environ, "MILLPOND_RUN_SCALE": "1"}
    out = subprocess.run(
        [sys.executable, __file__, str(rows), "variant" if variant else "string", str(groups)],
        capture_output=True,
        text=True,
        check=True,
        env=env,
        cwd=str(Path(__file__).resolve().parents[2]),
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def _report(label: str, m: dict) -> float:
    k = (m["rss_peak"] - m["rss_before"]) / m["nbytes"]
    print(
        f"{label}: prepare {m['elapsed']:.2f} s ({m['elapsed'] / m['rows'] * 1e6:.1f} us/row); "
        f"peak RSS +{(m['rss_peak'] - m['rss_before']) / 2**30:.2f} GiB = {k:.2f}x input"
    )
    return k


def test_a_whale_flush_prepares_under_the_allowance_with_bounded_memory():
    string = _in_subprocess(ROWS, variant=False, groups=GROUPS)
    gib, kib = string["nbytes"] / 2**30, string["nbytes"] / string["rows"] / 1024
    print(f"\nbatch: {string['rows']} rows, {gib:.2f} GiB Arrow, {kib:.1f} KiB/row")
    _report(f"string columns, {GROUPS} groups", string)

    spread = _in_subprocess(ROWS, variant=True, groups=GROUPS)
    k_spread = _report(f"variant columns (native), {GROUPS} groups", spread)
    measured, reserve, budget = spread["last_prepare_s"], spread["reserve"], spread["budget"]
    print(f"measured prepare {measured:.2f} s -> next attempt reserves {reserve:.1f} s of {budget:.0f} s")

    # The hot-team shape: every row in ONE group, so the encoded storage
    # transient is the whole flush's, not a 1/532 slice of it.
    hot = _in_subprocess(ROWS, variant=True, groups=1)
    k_hot = _report("variant columns (native), 1 group", hot)
    per_group_ms = max(0.0, spread["elapsed"] - hot["elapsed"]) / (GROUPS - 1) * 1000
    print(f"per-group overhead ~{per_group_ms:.1f} ms over {GROUPS} groups")

    assert spread["backend"] == hot["backend"] == "native"
    assert spread["elapsed"] < _PREPARE_ALLOWANCE_S, f"a native prepare of a whale flush took {spread['elapsed']:.1f} s"
    assert k_spread <= K_RSS, f"variant peak RSS ({GROUPS} groups) was {k_spread:.2f}x the input; the bound is {K_RSS}x"
    assert k_hot <= K_RSS, f"variant peak RSS (1 group) was {k_hot:.2f}x the input; the bound is {K_RSS}x"


if __name__ == "__main__":
    # `_in_subprocess` entry: rows, variant|string, groups -> one JSON line.
    from millpond import metrics

    metrics.init("scale")
    rows_, kind_, groups_ = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
    print(json.dumps(_measure(rows_, variant=kind_ == "variant", groups=groups_)))
