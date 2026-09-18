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
DEFAULT_IMAGE = "ghcr.io/posthog/hoglake-server:latest"


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
