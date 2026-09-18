"""Shared plumbing for the throwaway hoglake test stack.

Compose project `millpond-hog-it`, high 127.0.0.1-bound ports only —
this must be able to coexist with a live hoglake dev stack on the same
machine (8080/9000/5432/5173 are strictly off-limits). Full teardown
(`down -v --remove-orphans`) on fixture exit.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

import httpx

COMPOSE_FILE = Path(__file__).with_name("docker-compose.yaml")
PROJECT = "millpond-hog-it"

SERVER_URL = "http://127.0.0.1:28080"
MINIO_URL = "http://127.0.0.1:29000"
KAFKA_BOOTSTRAP = "127.0.0.1:29092"
S3_ACCESS_KEY = "hoglake"
S3_SECRET_KEY = "hoglake123"

# Pinned by DIGEST, not `:latest`.
#
# These suites are a CONTRACT test: they assert what the hoglake server
# does with millpond's commits (idempotency receipts, partition-spec
# validation, the 422 a bad transform returns). A floating tag makes
# every unrelated PR's CI run a hostage to whatever was published that
# morning — a green run proves nothing was broken by THIS change only if
# the other side held still. Pinning also means a server-side behaviour
# change lands as a deliberate, reviewable bump here instead of as a
# mystery failure on someone else's branch.
#
# This is a multi-arch index digest, so it resolves on both amd64 (CI)
# and arm64 (laptops).
#
# TO BUMP: pull the tag you want, read its index digest, and paste it
# here in the same commit as any millpond change the new behaviour
# needs:
#     docker pull ghcr.io/posthog/hoglake-server:latest
#     docker buildx imagetools inspect ghcr.io/posthog/hoglake-server:latest
#     # copy the top-level `Digest:` line
# Then run `just test-hoglake-integration` and `just test-hoglake-e2e`
# locally before pushing, and say in the commit message what changed on
# the server side. HOGLAKE_SERVER_IMAGE overrides this for a one-off run
# against a locally built server (e.g. while developing a server change).
DEFAULT_IMAGE = "ghcr.io/posthog/hoglake-server@sha256:c95500cae32940270e94228dfae6787b6bfa32d33729ec059204f14d950865c7"

# CI sets this. Without it, a machine with no Docker (or no access to
# ghcr.io) skips the docker-gated suites so local unit-test runs stay
# useful; with it, the same conditions are a FAILURE — a CI job that
# silently skips its only real-server coverage is a green tick that
# means nothing.
REQUIRE_ENV = "MILLPOND_REQUIRE_DOCKER_STACK"


def _compose_base() -> list[str]:
    return ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE)]


def compose(*args: str, profile: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    cmd = _compose_base()
    if profile:
        cmd += ["--profile", profile]
    cmd += list(args)
    return subprocess.run(cmd, check=check, capture_output=True, text=True, timeout=600)


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=30).returncode == 0
    except Exception:
        return False


def server_image() -> str:
    return os.environ.get("HOGLAKE_SERVER_IMAGE", DEFAULT_IMAGE)


def ensure_image() -> bool:
    """True when the hoglake server image is locally present or pullable."""
    image = server_image()
    if subprocess.run(["docker", "image", "inspect", image], capture_output=True).returncode == 0:
        return True
    return subprocess.run(["docker", "pull", image], capture_output=True, timeout=600).returncode == 0


def required() -> bool:
    """Whether an unavailable stack must fail rather than skip. See REQUIRE_ENV."""
    return os.environ.get(REQUIRE_ENV, "").strip().lower() not in ("", "0", "false", "no")


def require_or_skip(reason: str) -> None:
    """The one place the skip-vs-fail decision is made, so both suites
    behave identically and neither can go quietly green in CI."""
    import pytest

    if required():
        pytest.fail(f"{reason} ({REQUIRE_ENV} is set, so this is a failure, not a skip)")
    pytest.skip(f"{reason} (set {REQUIRE_ENV}=1 to make this a failure)")


def ensure_available() -> None:
    """Assert the throwaway stack can run at all: Docker reachable and the
    pinned server image present or pullable."""
    if not docker_available():
        require_or_skip("Docker is not available")
    if not ensure_image():
        require_or_skip(f"hoglake server image unavailable: {server_image()}")


def wait_http_ok(url: str, timeout_s: float = 120.0) -> None:
    deadline = time.monotonic() + timeout_s
    last: str = "never reached"
    while time.monotonic() < deadline:
        try:
            resp = httpx.get(url, timeout=2.0)
            if resp.status_code == 200:
                return
            last = f"HTTP {resp.status_code}"
        except Exception as e:  # noqa: BLE001 - poll loop
            last = repr(e)
        time.sleep(1.0)
    raise TimeoutError(f"{url} not healthy within {timeout_s}s (last: {last})")


def up(profile: str | None = None) -> None:
    compose("up", "-d", profile=profile)
    wait_http_ok(f"{MINIO_URL}/minio/health/live")
    wait_http_ok(f"{SERVER_URL}/healthz")


def down() -> None:
    # --remove-orphans catches profile services (kafka) even when the
    # teardown call doesn't pass the profile.
    compose("down", "-v", "--remove-orphans", "-t", "5", profile="e2e", check=False)


def make_bucket(name: str) -> None:
    """Idempotently create an S3 bucket on the stack MinIO. Test-side
    only — the sink's own S3Config never allows bucket creation."""
    from pyarrow import fs

    s3 = fs.S3FileSystem(
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        endpoint_override=MINIO_URL,
        allow_bucket_creation=True,
    )
    s3.create_dir(name)
