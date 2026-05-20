"""Characterise Lakekeeper's concurrent-commit behaviour under contention.

The actual workers run *inside the docker network* (the `stress-driver`
service in compose.lakekeeper.yaml uses the millpond image and runs
`stress_driver.py`). This test is a thin host-side wrapper that:

  1. Brings up the Lakekeeper + MinIO + Postgres stack.
  2. Invokes the in-network driver via `docker compose run --rm stress-driver`.
  3. Parses the driver's stdout for `STRESS_RESULT` lines and a final
     `STRESS_SUMMARY` line; pretty-prints a comparison table; archives
     the summary JSON next to this file.

Driving from inside the network matters: Lakekeeper bakes the storage
profile's S3 endpoint (`http://lakekeeper-minio:9000`) into the per-table
config it returns to clients. That hostname only resolves inside the
compose network. Running the writers in-network sidesteps the host-vs-
container endpoint mismatch and is also the production deployment shape —
millpond pods, Lakekeeper, and S3 live on the same network.

Marker-gated (`@pytest.mark.stress`). Trigger via `just stress-lakekeeper`.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from testcontainers.compose import DockerCompose

STRESS_DIR = Path(__file__).parent
COMPOSE_FILE = "compose.lakekeeper.yaml"

SUMMARY_PREFIX = "STRESS_SUMMARY "
RESULT_PREFIX = "STRESS_RESULT "


@pytest.fixture(scope="module")
def lakekeeper_stack():
    """Bring up MinIO + Postgres + Lakekeeper (with bootstrap + warehouse).

    `wait=False` because docker-compose `--wait` returns non-zero when
    one-shot init containers exit 0 (compose quirk). Health-readiness of
    the long-running services is guaranteed by `depends_on` in the compose.
    """
    with DockerCompose(str(STRESS_DIR), compose_file_name=COMPOSE_FILE, pull=True, wait=False) as compose:
        yield compose


@pytest.mark.stress
def test_concurrent_commits(lakekeeper_stack):
    """Single test running the full N=2…32 sweep in one driver invocation."""
    cmd = [
        "docker",
        "compose",
        "-f",
        COMPOSE_FILE,
        "--profile",
        "driver",
        "run",
        "--rm",
        "--build",
        "stress-driver",
    ]
    result = subprocess.run(
        cmd,
        cwd=str(STRESS_DIR),
        capture_output=True,
        text=True,
        timeout=1800,
        check=False,
    )
    # Always show stderr — it's the human-readable channel for the driver.
    if result.stderr:
        print("DRIVER STDERR:\n" + result.stderr)
    if result.returncode != 0:
        print("DRIVER STDOUT:\n" + result.stdout)
        pytest.fail(f"stress-driver exited {result.returncode}")

    # Parse the SUMMARY line — it's the authoritative aggregate.
    summary_line: str | None = None
    per_n_results: list[dict] = []
    for line in result.stdout.splitlines():
        if line.startswith(SUMMARY_PREFIX):
            summary_line = line[len(SUMMARY_PREFIX) :]
        elif line.startswith(RESULT_PREFIX):
            per_n_results.append(json.loads(line[len(RESULT_PREFIX) :]))

    assert summary_line is not None, "driver did not emit STRESS_SUMMARY line; stdout was:\n" + result.stdout
    summary = json.loads(summary_line)

    # Archive next to the test for the writeup.
    (STRESS_DIR / "summary.json").write_text(json.dumps(summary, indent=2))

    # Pretty-print so the run shows results clearly under `pytest -s`.
    print()
    print(f"{'N':>4}  {'success%':>8}  {'conflict%':>9}  {'error':>5}  {'p50ms':>6}  {'p95ms':>6}  {'p99ms':>6}")
    for r in summary:
        print(
            f"{r['n_writers']:>4}  "
            f"{r['success_rate'] * 100:>7.1f}%  "
            f"{r['conflict_rate'] * 100:>8.1f}%  "
            f"{r['error']:>5}  "
            f"{r['success_p50_ms']:>6.1f}  "
            f"{r['success_p95_ms']:>6.1f}  "
            f"{r['success_p99_ms']:>6.1f}"
        )

    # Characterisation, not pass/fail. Only fail on non-conflict errors.
    bad = [r for r in summary if r["error"] > 0]
    assert not bad, (
        f"unexpected non-conflict errors in writer counts: {[(r['n_writers'], r['error_types']) for r in bad]}"
    )
