"""End-to-end integration test exercising the real icebox Docker image.

This module promotes the in-process e2e (test_icebox_e2e.py) onto the
actual `icebox` console script running inside a container built from
the repo Dockerfile. It complements — does not replace — the in-process
tests, which remain the fast feedback loop for refactor regressions.

What only this test can exercise:
  - The image actually builds and `icebox` is on PATH.
  - The full boot sequence (icebox/main.py): DB bootstrap → schema
    bootstrap → psycopg pool + migrations → asyncpg pool → uvicorn.
    This is the scenario that would catch the "database does not
    exist" boot-loop a deploy hits when PG-side provisioning lags.
  - The committer thread, started by main(), fires its own cycles
    against a real REST catalog and a real Kafka broker.

Stack composition (all on a shared docker network):
  - Postgres 16-alpine (testcontainers PostgresContainer)
  - MinIO + minio-init (S3 backend for Iceberg data)
  - tabulario/iceberg-rest (Iceberg REST catalog)
  - Redpanda single-node (confluent-kafka-compatible broker)
  - The icebox image built from the repo Dockerfile

Cost: ~30-60s for the first image build, ~5s per session boot for the
dependency stack. Marked @pytest.mark.integration; deselected by the
default unit run.

NOT xdist-safe: every test uses `NAMESPACE="kafka"` + `TABLE="events"`
(or `events_a`/`persons_b` in the multi-icebox case), and the REST
catalog is session-scoped. The `_create_iceberg_table` helper
drops-and-recreates inside each test, which would race under
``pytest-xdist`` workers. Run this module with ``-n0`` (the default
when xdist isn't in deps; we don't ship it). Per-test unique table
names are a follow-up.
"""
from __future__ import annotations

import datetime as dt
import functools
import io
import json
import socket
import time
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.transforms import IdentityTransform
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from icebox.iceberg import CYCLE_ID_SUMMARY_KEY
from icebox.main import SHUTDOWN_COMPLETE_MARKER
from shared.fingerprint import schema_fingerprint
from shared.models import ParquetStats, RegisterFileRequest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
NAMESPACE = "kafka"
TABLE = "events"
PG_DB = "icebox"


@dataclass(frozen=True)
class IceboxHandle:
    """Bundle of handles a test needs against a running icebox container."""

    base_url: str
    container: DockerContainer
    schema_name: str
    namespace: str
    table: str


# ---------------------------------------------------------------------------
# Polling helpers
# ---------------------------------------------------------------------------


def _wait_for_http(url: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if 200 <= resp.status < 300:
                    return
        except Exception as e:
            last_err = e
        time.sleep(0.5)
    raise RuntimeError(
        f"endpoint {url} never came up within {timeout}s; last_err={last_err!r}"
    )


def _wait_for_tcp(host: str, port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except Exception as e:
            last_err = e
        time.sleep(0.5)
    raise RuntimeError(
        f"{host}:{port} never accepted a connection within {timeout}s; "
        f"last_err={last_err!r}"
    )


def _wait_for_readyz(base_url: str, timeout: float = 90.0) -> None:
    """Poll /readyz until 200. The icebox boot path includes DB+schema
    bootstrap + migrations + asyncpg pool open, all of which run before
    uvicorn binds the port — so connection refused is a normal early
    state, not a failure.
    """
    deadline = time.monotonic() + timeout
    last_status: Any = None
    last_body: Any = None
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{base_url}/readyz", timeout=2)
            last_status = r.status_code
            last_body = r.text
            if r.status_code == 200:
                return
        except Exception as e:
            last_status = "exc"
            last_body = repr(e)
        time.sleep(0.5)
    raise RuntimeError(
        f"/readyz never reached 200 within {timeout}s; "
        f"last_status={last_status}, last_body={last_body!r}"
    )


def _wait_for_container_exit(container: DockerContainer, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        c = container.get_wrapped_container()
        c.reload()
        if c.status == "exited":
            return
        time.sleep(0.5)
    raise RuntimeError(f"container did not exit within {timeout}s")


@functools.lru_cache(maxsize=1)
def _resolve_test_platform() -> str:
    """Decide which ``--platform`` to use for the icebox image.

    Resolution order:
      1. ``MILLPOND_TEST_PLATFORM`` env var if set (empty string ⇒
         no --platform flag at all).
      2. Apple-Silicon hosts (Darwin + arm64): default to
         ``linux/arm64`` for native-speed iteration. The Rosetta
         emulation cost of building/running linux/amd64 on M-series
         (~30% slowdown) makes the default friction-y, and Apple-
         Silicon devs running these tests are virtually always doing
         iterative work, not CI parity validation.
      3. Everywhere else: ``linux/amd64`` (matches what the chart
         publishes to ECR; what runs in mw-prod-us).

    Devs who want strict prod-arch parity on Apple Silicon can
    ``MILLPOND_TEST_PLATFORM=linux/amd64``.
    """
    import os
    import platform as host_platform

    env_override = os.environ.get("MILLPOND_TEST_PLATFORM")
    if env_override is not None:
        return env_override
    if host_platform.system() == "Darwin" and host_platform.machine() == "arm64":
        print(
            "[icebox-docker-tests] Apple Silicon detected; defaulting build "
            "to linux/arm64 for native iteration. Export "
            "MILLPOND_TEST_PLATFORM=linux/amd64 to test against the prod arch."
        )
        return "linux/arm64"
    return "linux/amd64"


# ---------------------------------------------------------------------------
# Session-scoped: image build + shared docker network
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def icebox_image() -> Iterator[str]:
    """Build the millpond image once per session from the repo Dockerfile.

    Shells out to `docker build` rather than calling docker-py's image
    build API: docker-py iterates every entry in ``~/.docker/config.json``
    auths during build and probes each via the configured credsStore,
    which on macOS pops a Keychain prompt per registry. ``docker build``
    only fetches creds for registries it actually needs.

    Docker layer cache makes repeat sessions fast unless application code
    changed. The image carries both `millpond` and `icebox` console
    scripts; we run it with command="icebox".
    """
    import subprocess

    tag = "millpond:icebox-integration-test"
    # Resolution lives in _resolve_test_platform: explicit env-var
    # override wins; Apple Silicon defaults to native arm64; else
    # linux/amd64 (matches what ships).
    platform = _resolve_test_platform()
    build_cmd = ["docker", "build"]
    if platform:
        build_cmd += ["--platform", platform]
    build_cmd += ["-t", tag, "-f", "Dockerfile", "."]
    result = subprocess.run(
        build_cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"docker build failed (exit {result.returncode}).\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )
    yield tag


@pytest.fixture(scope="session")
def docker_net() -> Iterator[Network]:
    with Network() as net:
        yield net


# ---------------------------------------------------------------------------
# Session-scoped: dependency containers (PG, MinIO, iceberg-rest, redpanda)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_on_net(docker_net: Network) -> Iterator[Any]:
    """Postgres 16 on the shared network, aliased as 'pg'.

    Boots without precreating the icebox database — that proves the
    icebox/main.py bootstrap path creates it. The default
    PostgresContainer creates a default database from POSTGRES_DB env;
    we override that with a placeholder so the icebox bootstrap is the
    only thing creating 'icebox'.
    """
    from testcontainers.postgres import PostgresContainer

    pg = PostgresContainer("postgres:16-alpine", dbname="placeholder")
    pg.with_network(docker_net).with_network_aliases("pg")
    pg.start()
    # Wait for PG to actually accept connections before yielding.
    host = pg.get_container_host_ip()
    port = int(pg.get_exposed_port(5432))
    _wait_for_tcp(host, port, timeout=30)
    try:
        yield pg
    finally:
        pg.stop()


@pytest.fixture(scope="session")
def minio_on_net(docker_net: Network) -> Iterator[DockerContainer]:
    """MinIO + warehouse bucket pre-created via the mc image.

    Image tags match tests/integration/compose.yaml so we're exercising
    the same versions the writer's iceberg integration test uses.
    """
    minio = (
        DockerContainer("minio/minio:RELEASE.2025-04-22T22-12-26Z")
        .with_command("server /data --console-address :9001")
        .with_env("MINIO_ROOT_USER", "minioadmin")
        .with_env("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_exposed_ports(9000)
        .with_network(docker_net)
        .with_network_aliases("minio")
    )
    minio.start()
    host = minio.get_container_host_ip()
    port = int(minio.get_exposed_port(9000))
    _wait_for_http(f"http://{host}:{port}/minio/health/live", timeout=60)

    # Create the 'warehouse' bucket. mc exits after the mb command.
    # The mc image's ENTRYPOINT is `mc`, so we have to override to run
    # /bin/sh -c with our script.
    init = (
        DockerContainer("minio/mc:RELEASE.2025-04-16T18-13-26Z")
        .with_kwargs(
            entrypoint=[
                "/bin/sh",
                "-c",
                "until mc alias set local http://minio:9000 minioadmin minioadmin "
                ">/dev/null 2>&1; do sleep 0.2; done; mc mb -p local/warehouse",
            ]
        )
        .with_network(docker_net)
    )
    init.start()
    _wait_for_container_exit(init, timeout=30)
    # Verify the bucket exists by re-running mc; surfaces a clear error
    # if the previous script silently failed before mb completed.
    verify = (
        DockerContainer("minio/mc:RELEASE.2025-04-16T18-13-26Z")
        .with_kwargs(
            entrypoint=[
                "/bin/sh",
                "-c",
                "mc alias set local http://minio:9000 minioadmin minioadmin "
                ">/dev/null && mc ls local/warehouse >/dev/null",
            ]
        )
        .with_network(docker_net)
    )
    verify.start()
    _wait_for_container_exit(verify, timeout=15)
    rc = verify.get_wrapped_container().attrs["State"]["ExitCode"]
    if rc != 0:
        out, err = verify.get_logs()
        raise RuntimeError(
            f"minio bucket 'warehouse' not reachable after init "
            f"(exit={rc}): stdout={out!r} stderr={err!r}"
        )
    init.stop()
    verify.stop()

    try:
        yield minio
    finally:
        minio.stop()


@pytest.fixture(scope="session")
def iceberg_rest_on_net(
    docker_net: Network, minio_on_net: DockerContainer
) -> Iterator[DockerContainer]:
    """Tabulario REST catalog backed by the MinIO container.

    Pinned by sha256 (matching compose.yaml) — upstream only publishes
    `:latest`, so a digest pin is the only stable handle.
    """
    rest = (
        DockerContainer(
            "tabulario/iceberg-rest@sha256:"
            "3b7d31bdfec626b68e97531c9778a1b9119659e456fe28545a49f6aa6a9ce472"
        )
        .with_env("AWS_ACCESS_KEY_ID", "minioadmin")
        .with_env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .with_env("AWS_REGION", "us-east-1")
        .with_env("CATALOG_WAREHOUSE", "s3://warehouse/")
        .with_env("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env("CATALOG_S3_ENDPOINT", "http://minio:9000")
        .with_env("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .with_exposed_ports(8181)
        .with_network(docker_net)
        .with_network_aliases("iceberg-rest")
    )
    rest.start()
    host = rest.get_container_host_ip()
    port = int(rest.get_exposed_port(8181))
    _wait_for_http(f"http://{host}:{port}/v1/config", timeout=60)
    try:
        yield rest
    finally:
        rest.stop()


@pytest.fixture(scope="session")
def redpanda_on_net(docker_net: Network) -> Iterator[DockerContainer]:
    """Redpanda single-node broker — accepts the icebox's offset commits.

    Advertised as redpanda:9092 inside the network so the icebox
    container's AdminClient finds it via the bootstrap hostname.
    """
    rp = (
        DockerContainer("docker.redpanda.com/redpandadata/redpanda:v24.3.4")
        .with_command(
            "redpanda start "
            "--overprovisioned --smp 1 --memory 512M --reserve-memory 0M "
            "--node-id 0 --check=false "
            "--kafka-addr PLAINTEXT://0.0.0.0:9092 "
            "--advertise-kafka-addr PLAINTEXT://redpanda:9092"
        )
        .with_exposed_ports(9092)
        .with_network(docker_net)
        .with_network_aliases("redpanda")
    )
    rp.waiting_for(
        LogMessageWaitStrategy("Successfully started Redpanda!").with_startup_timeout(60)
    )
    rp.start()
    try:
        yield rp
    finally:
        rp.stop()


# ---------------------------------------------------------------------------
# Per-test: host-side REST catalog client (table creation, snapshot inspection)
# ---------------------------------------------------------------------------


@pytest.fixture
def host_rest_catalog(
    iceberg_rest_on_net: DockerContainer, minio_on_net: DockerContainer
) -> RestCatalog:
    """A RestCatalog wired with HOST-side endpoints.

    The icebox container talks to ``http://iceberg-rest:8181`` and
    ``http://minio:9000`` (network-internal). The test runs on the host
    and uses random-mapped ports. Passing ``s3.endpoint`` explicitly
    overrides the catalog's default s3 endpoint for client-side I/O
    (table-create writes manifest/metadata via the catalog, but
    pyiceberg's data-file I/O is client-side).
    """
    rest_host = iceberg_rest_on_net.get_container_host_ip()
    rest_port = int(iceberg_rest_on_net.get_exposed_port(8181))
    minio_host = minio_on_net.get_container_host_ip()
    minio_port = int(minio_on_net.get_exposed_port(9000))
    return RestCatalog(
        "icebox-docker-test",
        **{
            "uri": f"http://{rest_host}:{rest_port}",
            "warehouse": "s3://warehouse/",
            "s3.endpoint": f"http://{minio_host}:{minio_port}",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


# ---------------------------------------------------------------------------
# Per-test: the icebox container itself
# ---------------------------------------------------------------------------


def _spawn_icebox(
    *,
    icebox_image: str,
    docker_net: Network,
    pg_on_net: Any,
    schema_name: str,
    table: str = TABLE,
    namespace: str = NAMESPACE,
    extra_env: dict[str, str] | None = None,
    network_alias: str = "icebox",
    wait_for_ready: bool = True,
) -> IceboxHandle:
    """Launch one icebox container on the shared network.

    Factored out of the per-test fixture so the multi-icebox test
    (and any future tests that need a custom container shape) can
    invoke it directly with overridden table/namespace/env.
    """
    env: dict[str, str] = {
        "ICEBOX_PG_HOST": "pg",
        "ICEBOX_PG_PORT": "5432",
        "ICEBOX_PG_DATABASE": PG_DB,
        "ICEBOX_PG_USERNAME": pg_on_net.username,
        "ICEBOX_PG_PASSWORD": pg_on_net.password,
        "ICEBOX_PG_SSLMODE": "disable",
        "ICEBOX_PG_SCHEMA": schema_name,
        "ICEBOX_ICEBERG_CATALOG_URI": "http://iceberg-rest:8181",
        "ICEBOX_ICEBERG_WAREHOUSE": "s3://warehouse/",
        "ICEBOX_ICEBERG_NAMESPACE": namespace,
        "ICEBOX_ICEBERG_TABLE": table,
        "ICEBOX_KAFKA_BOOTSTRAP_SERVERS": "redpanda:9092",
        "ICEBOX_KAFKA_TOPIC": f"kafka.{table}",
        "ICEBOX_KAFKA_GROUP_ID": f"icebox-docker-test-{table}",
        # PyIceberg per-catalog s3 props — PyArrowFileIO doesn't read
        # AWS_ACCESS_KEY_ID env vars, and tabulario/iceberg-rest doesn't
        # propagate creds. The catalog name in icebox/main.py is "icebox".
        "PYICEBERG_CATALOG__ICEBOX__S3__ENDPOINT": "http://minio:9000",
        "PYICEBERG_CATALOG__ICEBOX__S3__ACCESS_KEY_ID": "minioadmin",
        "PYICEBERG_CATALOG__ICEBOX__S3__SECRET_ACCESS_KEY": "minioadmin",
        "PYICEBERG_CATALOG__ICEBOX__S3__REGION": "us-east-1",
        "PYICEBERG_CATALOG__ICEBOX__S3__PATH_STYLE_ACCESS": "true",
        "ICEBOX_COMMITTER_CADENCE_SECONDS": "2",
        "ICEBOX_COMMITTER_HEARTBEAT_STALE_MULTIPLE": "5",
        "ICEBOX_API_HOST": "0.0.0.0",
        "ICEBOX_API_PORT": "8000",
        "ICEBOX_LOG_LEVEL": "INFO",
    }
    if extra_env:
        env.update(extra_env)

    # Run-time platform must match build-time platform; reuse the
    # same resolution rules (env-var override, Apple-Silicon default
    # to arm64, everywhere else amd64).
    run_platform = _resolve_test_platform()
    container_kwargs: dict[str, Any] = {"entrypoint": ["icebox"]}
    if run_platform:
        container_kwargs["platform"] = run_platform
    container = (
        DockerContainer(icebox_image)
        # Dockerfile pins ENTRYPOINT=["millpond"]; the K8s chart overrides
        # via `command:` to `["icebox"]`. For docker-run we must override
        # entrypoint explicitly — `with_command` alone would pass
        # "icebox" as an arg to millpond.
        .with_kwargs(**container_kwargs)
        .with_exposed_ports(8000)
        .with_network(docker_net)
        .with_network_aliases(network_alias)
    )
    for k, v in env.items():
        container.with_env(k, v)
    container.start()

    host_ip = container.get_container_host_ip()
    port = int(container.get_exposed_port(8000))
    base_url = f"http://{host_ip}:{port}"

    if wait_for_ready:
        try:
            _wait_for_readyz(base_url, timeout=90)
        except Exception:
            # Dump the container's logs so the operator sees what
            # crashed during boot (config errors, PG bootstrap
            # failures, etc.) instead of just "/readyz never reached
            # 200".
            try:
                out, err = container.get_logs()
                print(f"--- icebox boot stdout (schema={schema_name}) ---")
                print(out.decode(errors="replace"))
                print(f"--- icebox boot stderr (schema={schema_name}) ---")
                print(err.decode(errors="replace"))
            except Exception as log_e:
                print(f"(failed to fetch boot logs: {log_e!r})")
            try:
                container.stop()
            except Exception:
                pass
            raise

    return IceboxHandle(
        base_url=base_url,
        container=container,
        schema_name=schema_name,
        namespace=namespace,
        table=table,
    )


def _teardown_icebox(handle: IceboxHandle, *, failed: bool) -> None:
    """Stop a container, dumping logs on failure for diagnostics.

    ``failed`` is computed by the caller from
    ``request.node.rep_call.failed`` so we catch BOTH exception-raising
    teardowns AND plain assertion failures (which never raise into
    the fixture's call stack).
    """
    if failed:
        try:
            out, err = handle.container.get_logs()
            print(f"--- icebox container stdout (schema={handle.schema_name}) ---")
            print(out.decode(errors="replace"))
            print(f"--- icebox container stderr (schema={handle.schema_name}) ---")
            print(err.decode(errors="replace"))
        except Exception as e:
            print(f"(failed to fetch icebox container logs: {e!r})")
    # Container.stop() is idempotent on already-exited / killed
    # containers in testcontainers — just ignore errors to keep
    # teardown clean.
    try:
        handle.container.stop()
    except Exception as e:
        print(f"(container.stop() failed; container may have been killed: {e!r})")


def _test_failed(request: pytest.FixtureRequest) -> bool:
    """True iff the test that just ran failed.

    Requires the ``pytest_runtest_makereport`` hook in
    ``tests/integration/conftest.py`` that attaches the call report
    to the test node.
    """
    rep_call = getattr(request.node, "rep_call", None)
    if rep_call is None:
        # The hook didn't fire — be conservative and dump logs so we
        # don't silently lose diagnostics.
        return True
    return bool(rep_call.failed)


@pytest.fixture
def icebox_container(
    icebox_image: str,
    docker_net: Network,
    pg_on_net: Any,
    iceberg_rest_on_net: DockerContainer,
    minio_on_net: DockerContainer,
    redpanda_on_net: DockerContainer,
    request: pytest.FixtureRequest,
) -> Iterator[IceboxHandle]:
    """Default per-test icebox container with a unique PG schema.

    Cadence is set to 2s so a single cycle can fire within test runtime.
    DB and schema are NOT pre-created — the boot path is exercised by
    every test that uses this fixture.

    The REST catalog table (NAMESPACE, TABLE) is DROPPED at fixture
    setup so each test starts from a clean catalog state. Tests that
    need the table call ``_create_iceberg_table`` to recreate; tests
    that don't get the API-perimeter fingerprint check fail-open
    (load_table raises NoSuchTable → cache falls through, POST proceeds).
    """
    _drop_catalog_table_if_exists(iceberg_rest_on_net, minio_on_net)
    schema_name = f"icebox_t_{uuid.uuid4().hex[:12]}"
    handle = _spawn_icebox(
        icebox_image=icebox_image,
        docker_net=docker_net,
        pg_on_net=pg_on_net,
        schema_name=schema_name,
    )
    try:
        yield handle
    finally:
        _teardown_icebox(handle, failed=_test_failed(request))


def _drop_catalog_table_if_exists(
    iceberg_rest: DockerContainer,
    minio: DockerContainer,
    *,
    namespace: str = NAMESPACE,
    table: str = TABLE,
) -> None:
    """Drop the (namespace, table) from the session REST catalog if it
    exists. Used by per-test fixtures to start each test from a known
    empty catalog so the API-perimeter fingerprint check fails open
    by default and tests that don't care about the table aren't
    rejected on fingerprint mismatch from a leftover schema."""
    rest_host = iceberg_rest.get_container_host_ip()
    rest_port = int(iceberg_rest.get_exposed_port(8181))
    minio_host = minio.get_container_host_ip()
    minio_port = int(minio.get_exposed_port(9000))
    catalog = RestCatalog(
        "icebox-docker-test-cleanup",
        **{
            "uri": f"http://{rest_host}:{rest_port}",
            "warehouse": "s3://warehouse/",
            "s3.endpoint": f"http://{minio_host}:{minio_port}",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )
    try:
        catalog.drop_table((namespace, table))
    except Exception:
        # NoSuchTable is the normal case; ignore.
        pass


@pytest.fixture
def icebox_factory(
    icebox_image: str,
    docker_net: Network,
    pg_on_net: Any,
    iceberg_rest_on_net: DockerContainer,
    minio_on_net: DockerContainer,
    redpanda_on_net: DockerContainer,
    request: pytest.FixtureRequest,
) -> Iterator[Callable[..., IceboxHandle]]:
    """Spawn additional icebox containers with custom (schema, table, env).

    Tracks every spawned handle so the fixture tears them all down at
    the end of the test, with logs-on-failure dumped per container.

    On each spawn the factory drops the (namespace, table) from the
    session REST catalog so the API-perimeter fingerprint cache fails
    open by default — tests that need the table call
    ``_create_iceberg_table`` to recreate.
    """
    handles: list[IceboxHandle] = []

    def _factory(
        *,
        schema_name: str | None = None,
        table: str = TABLE,
        namespace: str = NAMESPACE,
        extra_env: dict[str, str] | None = None,
        network_alias: str | None = None,
        wait_for_ready: bool = True,
        drop_catalog_table_first: bool = True,
    ) -> IceboxHandle:
        if drop_catalog_table_first:
            _drop_catalog_table_if_exists(
                iceberg_rest_on_net, minio_on_net, namespace=namespace, table=table
            )
        sn = schema_name or f"icebox_t_{uuid.uuid4().hex[:12]}"
        alias = network_alias or f"icebox-{sn.replace('_', '-')}"
        handle = _spawn_icebox(
            icebox_image=icebox_image,
            docker_net=docker_net,
            pg_on_net=pg_on_net,
            schema_name=sn,
            table=table,
            namespace=namespace,
            extra_env=extra_env,
            network_alias=alias,
            wait_for_ready=wait_for_ready,
        )
        handles.append(handle)
        return handle

    try:
        yield _factory
    finally:
        failed = _test_failed(request)
        for h in handles:
            _teardown_icebox(h, failed=failed)


# ---------------------------------------------------------------------------
# Host-side PG conn for inspecting / manipulating icebox state
# ---------------------------------------------------------------------------


def _pg_conn(
    pg_on_net: Any,
    *,
    schema: str | None = None,
    autocommit: bool = True,
) -> psycopg.Connection:
    """Open a psycopg connection to the icebox DB from the host.

    When ``schema`` is provided, pins ``search_path`` to it so SQL can
    reference icebox tables unqualified (matching what the icebox's own
    pool does via ``options=-csearch_path``).
    """
    host = pg_on_net.get_container_host_ip()
    port = int(pg_on_net.get_exposed_port(5432))
    options = f"-csearch_path={schema}" if schema else ""
    conn = psycopg.connect(
        host=host,
        port=port,
        dbname=PG_DB,
        user=pg_on_net.username,
        password=pg_on_net.password,
        options=options,
        autocommit=autocommit,
    )
    return conn


# ---------------------------------------------------------------------------
# Helpers (parallel structure to test_icebox_e2e.py)
# ---------------------------------------------------------------------------


def _create_iceberg_table(
    catalog: RestCatalog,
    *,
    namespace: str = NAMESPACE,
    table: str = TABLE,
) -> Any:
    """Create the (namespace, table) table in the REST catalog.

    Schema matches the writer's augmented batch shape so the
    schema_fingerprint computed here equals what the writer would
    produce.
    """
    arrow_schema = pa.schema(
        [
            pa.field("team_id", pa.int64(), nullable=True),
            pa.field("_inserted_at", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("year", pa.int32(), nullable=True),
            pa.field("month", pa.int32(), nullable=True),
            pa.field("day", pa.int32(), nullable=True),
            pa.field("hour", pa.int32(), nullable=True),
        ]
    )
    ice_schema = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(arrow_schema))
    name_to_id = {f.name: f.field_id for f in ice_schema.fields}
    partition_fields = []
    next_pid = 1000
    for col in ("year", "month", "day", "hour"):
        partition_fields.append(
            PartitionField(
                source_id=name_to_id[col],
                field_id=next_pid,
                transform=IdentityTransform(),
                name=col,
            )
        )
        next_pid += 1
    spec = PartitionSpec(*partition_fields)
    catalog.create_namespace_if_not_exists(namespace)
    # The REST catalog persists across tests in a session; drop any
    # leftover before recreating so each test starts from an empty
    # table with zero snapshots.
    try:
        catalog.drop_table((namespace, table))
    except Exception:
        pass
    return catalog.create_table((namespace, table), schema=ice_schema, partition_spec=spec)


def _upload_parquet_to_minio(
    *,
    minio_on_net: DockerContainer,
    table_location: str,
    partition_values: dict[str, int],
) -> tuple[str, int, int]:
    """Write a small parquet file to MinIO under the table's data path.

    Returns (s3_uri, file_size_bytes, row_count). The s3_uri uses the
    s3:// scheme so it routes through PyIceberg's FileIO when the
    committer commits the data file (both sides resolve the endpoint
    from their own AWS_ENDPOINT_URL_S3 setting).
    """
    from pyarrow.fs import S3FileSystem

    minio_host = minio_on_net.get_container_host_ip()
    minio_port = int(minio_on_net.get_exposed_port(9000))

    row_count = 3
    inserted_at = dt.datetime(
        partition_values["year"],
        partition_values["month"],
        partition_values["day"],
        partition_values["hour"],
        0,
        0,
        tzinfo=dt.UTC,
    )
    batch = pa.table(
        {
            "team_id": pa.array([1, 2, 3], type=pa.int64()),
            "_inserted_at": pa.array(
                [inserted_at] * row_count, type=pa.timestamp("us", tz="UTC")
            ),
            "year": pa.array([partition_values["year"]] * row_count, type=pa.int32()),
            "month": pa.array([partition_values["month"]] * row_count, type=pa.int32()),
            "day": pa.array([partition_values["day"]] * row_count, type=pa.int32()),
            "hour": pa.array([partition_values["hour"]] * row_count, type=pa.int32()),
        }
    )
    buf = io.BytesIO()
    pq.write_table(batch, buf)
    parquet_bytes = buf.getvalue()

    # Path under the table location matching the partition convention.
    # table_location is e.g. 's3://warehouse/kafka.db/events'.
    assert table_location.startswith("s3://"), table_location
    rel_key = (
        f"data/year={partition_values['year']}/"
        f"month={partition_values['month']:02d}/"
        f"day={partition_values['day']:02d}/"
        f"hour={partition_values['hour']:02d}/"
        f"writer-0-{uuid.uuid4().hex[:16]}.parquet"
    )
    # Strip s3:// and split out bucket/key prefix.
    no_scheme = table_location[len("s3://"):]
    bucket, _, prefix = no_scheme.partition("/")
    key = f"{prefix.rstrip('/')}/{rel_key}"

    fs = S3FileSystem(
        access_key="minioadmin",
        secret_key="minioadmin",
        region="us-east-1",
        endpoint_override=f"http://{minio_host}:{minio_port}",
        scheme="http",
        background_writes=False,
    )
    with fs.open_output_stream(f"{bucket}/{key}") as out:
        out.write(parquet_bytes)
    return f"s3://{bucket}/{key}", len(parquet_bytes), row_count


def _build_register_request(
    table: Any,
    *,
    file_path: str,
    record_count: int,
    file_size: int,
    partition_values: dict[str, int],
    expected_namespace: str = NAMESPACE,
    expected_table: str = TABLE,
    schema_fingerprint_override: str | None = None,
) -> RegisterFileRequest:
    ice_schema = table.schema()
    name_to_id = {f.name: f.field_id for f in ice_schema.fields}
    team_id_fid = str(name_to_id["team_id"])
    stats = ParquetStats(
        column_sizes={team_id_fid: 64},
        value_counts={team_id_fid: record_count},
        null_value_counts={team_id_fid: 0},
        lower_bounds={team_id_fid: 1},
        upper_bounds={team_id_fid: 3},
    )
    return RegisterFileRequest(
        file_path=file_path,
        writer_ordinal=0,
        kafka_offsets={"0": 12345, "1": 67890},
        partition_values=partition_values,
        record_count=record_count,
        file_size=file_size,
        schema_version="v1",
        schema_fingerprint=schema_fingerprint_override or schema_fingerprint(ice_schema),
        parquet_stats=stats,
        expected_iceberg_namespace=expected_namespace,
        expected_iceberg_table=expected_table,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_image_boots_and_healthz_immediate(icebox_container: IceboxHandle) -> None:
    """The image starts, the `icebox` console script runs, /healthz is
    served immediately and /readyz flips to 200 after PG bootstrap +
    schema bootstrap + migrations complete.

    /readyz reaching 200 is the fixture's wait condition, so by the
    time we get here the bootstrap path has already succeeded. This
    test pins the additional invariant that /healthz is liveness-only
    (no PG dependency).
    """
    base_url = icebox_container.base_url
    r = httpx.get(f"{base_url}/healthz", timeout=2)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body == {"alive": True}, body

    r = httpx.get(f"{base_url}/readyz", timeout=2)
    assert r.status_code == 200, r.text


@pytest.mark.integration
def test_register_file_happy_path_via_docker_image(
    icebox_container: IceboxHandle,
    host_rest_catalog: RestCatalog,
) -> None:
    """POST /v1/files succeeds against the containerized icebox.

    Pre-creates the Iceberg table (so schema_fingerprint validation
    inside the committer would see a matching schema if the request
    advanced that far), then POSTs a request with matching expected_*
    fields. The committer may or may not have run a cycle by the time
    the POST returns — that's tested separately below.
    """
    base_url = icebox_container.base_url
    table = _create_iceberg_table(host_rest_catalog)

    req = _build_register_request(
        table,
        file_path="s3://warehouse/placeholder/path.parquet",
        record_count=3,
        file_size=1234,
        partition_values={"year": 2026, "month": 6, "day": 2, "hour": 10},
    )
    r = httpx.post(
        f"{base_url}/v1/files", json=req.model_dump(mode="json"), timeout=10
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert "row_id" in body
    assert "queued_at" in body


@pytest.mark.integration
def test_register_file_rejects_mismatched_expected_namespace(
    icebox_container: IceboxHandle,
) -> None:
    """A writer pointed at the wrong icebox URL (mismatched namespace)
    is rejected at the API perimeter with 400. The body must NOT echo
    the request fields verbatim — confirming the redaction we added in
    the API.
    """
    base_url = icebox_container.base_url

    # Build a minimally valid wire body with a deliberately wrong
    # expected_iceberg_namespace. The schema is irrelevant; the
    # mismatch check happens before fingerprint validation.
    body = {
        "file_path": "s3://warehouse/x.parquet",
        "writer_ordinal": 0,
        "kafka_offsets": {"0": 1},
        "partition_values": {"year": 2026, "month": 6, "day": 2, "hour": 10},
        "record_count": 1,
        "file_size": 100,
        "schema_version": "v1",
        "schema_fingerprint": "deadbeef" * 8,
        "parquet_stats": {
            "column_sizes": {},
            "value_counts": {},
            "null_value_counts": {},
            "lower_bounds": {},
            "upper_bounds": {},
        },
        "expected_iceberg_namespace": "wrong_namespace",
        "expected_iceberg_table": TABLE,
    }
    r = httpx.post(f"{base_url}/v1/files", json=body, timeout=10)
    assert r.status_code == 400, r.text
    # The 400 body must NOT verbatim echo request fields that could
    # carry sensitive content (file paths, kafka offsets, etc.). The
    # API's mismatch response includes structured diagnostic fields
    # (writer_expected, icebox_serves) but should not include the
    # request body's free-form values.
    assert "s3://warehouse/x.parquet" not in r.text
    assert '"kafka_offsets"' not in r.text


@pytest.mark.integration
def test_full_cycle_through_docker_image(
    icebox_container: IceboxHandle,
    host_rest_catalog: RestCatalog,
    minio_on_net: DockerContainer,
) -> None:
    """End-to-end: pre-create Iceberg table → upload parquet to MinIO →
    POST /v1/files → wait for the committer thread inside the container
    to fire a cycle → assert a snapshot lands in the REST catalog with
    the cycle_id summary key.

    Demonstrates that the real committer thread (started by main()),
    the real PyIceberg client (writing manifests through the network
    to MinIO via AWS_ENDPOINT_URL_S3), and the real AdminClient (Kafka
    offset commit to redpanda) all stitch together.
    """
    base_url = icebox_container.base_url
    table = _create_iceberg_table(host_rest_catalog)

    partition_values = {"year": 2026, "month": 6, "day": 2, "hour": 11}
    s3_uri, file_size, row_count = _upload_parquet_to_minio(
        minio_on_net=minio_on_net,
        table_location=table.location(),
        partition_values=partition_values,
    )
    req = _build_register_request(
        table,
        file_path=s3_uri,
        record_count=row_count,
        file_size=file_size,
        partition_values=partition_values,
    )

    r = httpx.post(
        f"{base_url}/v1/files", json=req.model_dump(mode="json"), timeout=10
    )
    assert r.status_code == 201, r.text

    # Cadence is 2s; allow up to ~60s for the cycle to commit + the
    # catalog to reflect the new snapshot. The wider budget tolerates
    # cold CI Docker daemons where iceberg-rest can be slow to respond.
    deadline = time.monotonic() + 60.0
    seen_snapshot = None
    while time.monotonic() < deadline:
        reloaded = host_rest_catalog.load_table((NAMESPACE, TABLE))
        snapshots = list(reloaded.snapshots())
        if snapshots:
            seen_snapshot = snapshots[-1]
            break
        time.sleep(0.5)

    if seen_snapshot is None:
        # Diagnostic: include the icebox container's logs in the failure.
        out, err = icebox_container.container.get_logs()
        raise AssertionError(
            "no snapshot committed within 30s. icebox stdout:\n"
            f"{out.decode(errors='replace')}\n"
            f"icebox stderr:\n{err.decode(errors='replace')}"
        )

    assert seen_snapshot.summary is not None
    cycle_id_in_summary = seen_snapshot.summary.get(CYCLE_ID_SUMMARY_KEY)
    assert cycle_id_in_summary is not None, (
        f"snapshot {seen_snapshot.snapshot_id} missing cycle_id summary; "
        f"got {dict(seen_snapshot.summary)!r}"
    )


# ---------------------------------------------------------------------------
# Hardening: non-happy paths
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_register_file_rejects_mismatched_expected_table(
    icebox_container: IceboxHandle,
) -> None:
    """A POST whose expected_iceberg_table doesn't match the icebox's
    configured table is rejected with 400 — same path as the namespace
    mismatch, asserted independently because they're independent fields.
    """
    base_url = icebox_container.base_url
    body = {
        "file_path": "s3://warehouse/x.parquet",
        "writer_ordinal": 0,
        "kafka_offsets": {"0": 1},
        "partition_values": {"year": 2026, "month": 6, "day": 2, "hour": 10},
        "record_count": 1,
        "file_size": 100,
        "schema_version": "v1",
        "schema_fingerprint": "deadbeef" * 8,
        "parquet_stats": {
            "column_sizes": {},
            "value_counts": {},
            "null_value_counts": {},
            "lower_bounds": {},
            "upper_bounds": {},
        },
        "expected_iceberg_namespace": NAMESPACE,
        "expected_iceberg_table": "wrong_table",
    }
    r = httpx.post(f"{base_url}/v1/files", json=body, timeout=10)
    assert r.status_code == 400, r.text
    # See the namespace-mismatch test for rationale on body redaction.
    assert "s3://warehouse/x.parquet" not in r.text
    assert '"kafka_offsets"' not in r.text


@pytest.mark.integration
def test_register_file_rejects_malformed_body(icebox_container: IceboxHandle) -> None:
    """A POST missing required fields is rejected by FastAPI's Pydantic
    boundary with 422 — proving the wire-format validator is wired up.
    """
    base_url = icebox_container.base_url
    # Drop several required fields; what's left can't form a RegisterFileRequest.
    body = {
        "file_path": "s3://warehouse/x.parquet",
        "writer_ordinal": 0,
        # missing kafka_offsets, partition_values, record_count, file_size, ...
    }
    r = httpx.post(f"{base_url}/v1/files", json=body, timeout=10)
    assert r.status_code == 422, r.text


@pytest.mark.integration
def test_committer_skips_cycle_on_stale_fingerprint_row_in_pg(
    icebox_container: IceboxHandle,
    host_rest_catalog: RestCatalog,
    pg_on_net: Any,
) -> None:
    """The committer's defense-in-depth fingerprint check still applies
    even though the API perimeter now rejects mismatches upfront. A
    file row inserted directly into PG with a stale fingerprint
    (simulates: writer POSTed before an ALTER TABLE happened, then
    cycle runs after) must hit the committer's mismatch path:
    skipped_reason='schema_mismatch', file claim released,
    failure_counter incremented.

    We bypass the API by inserting directly into the icebox schema's
    ``files`` table from a host-side PG connection — the API perimeter
    would catch the bad fingerprint at the door now, but this row
    arrives pre-staged.
    """
    _create_iceberg_table(host_rest_catalog)

    # Insert a file row directly via host PG, mimicking what the API
    # would have written before the perimeter check was added.
    bad_fp = "deadbeef" * 8
    file_path = f"s3://warehouse/stale-fp-{uuid.uuid4().hex[:8]}.parquet"
    partition_values = {"year": 2026, "month": 6, "day": 2, "hour": 12}
    parquet_stats: dict[str, Any] = {
        "column_sizes": {},
        "value_counts": {},
        "null_value_counts": {},
        "lower_bounds": {},
        "upper_bounds": {},
    }
    with _pg_conn(pg_on_net, schema=icebox_container.schema_name) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO files (
                    file_path, writer_ordinal, kafka_offsets, partition_values,
                    record_count, file_size, schema_version, schema_fingerprint,
                    parquet_stats
                ) VALUES (
                    %s, %s, %s::jsonb, %s::jsonb,
                    %s, %s, %s, %s, %s::jsonb
                ) RETURNING id
                """,
                (
                    file_path,
                    0,
                    json.dumps({"0": 1}),
                    json.dumps(partition_values),
                    1,
                    100,
                    "v1",
                    bad_fp,
                    json.dumps(parquet_stats),
                ),
            )
            file_row_id = cur.fetchone()[0]

    # Wait for the committer to attempt a cycle and detect the mismatch.
    # Observable signals after release_cycle_claim + record_failure:
    #   1. status.consecutive_failures >= 1
    #   2. file row's cycle_id is NULL again (claim released) and
    #      committed_at is NULL (nothing was committed)
    # skipped_reason isn't persisted — it lives in the CycleResult
    # only, not in commit_cycles.
    deadline = time.monotonic() + 20.0
    saw_failure = False
    last_state: dict[str, Any] = {}
    while time.monotonic() < deadline:
        with _pg_conn(pg_on_net, schema=icebox_container.schema_name) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT consecutive_failures FROM status")
                status_row = cur.fetchone()
                cur.execute(
                    "SELECT cycle_id, committed_at FROM files WHERE id = %s",
                    (file_row_id,),
                )
                file_row = cur.fetchone()
        last_state = {"status": status_row, "file": file_row}
        if status_row and status_row[0] >= 1:
            saw_failure = True
            break
        time.sleep(0.5)

    assert saw_failure, (
        f"never saw consecutive_failures >= 1; last={last_state!r}"
    )
    # File claim must be released so a future cycle can re-batch once
    # the writer catches up — cycle_id back to NULL, committed_at NULL.
    assert last_state["file"] == (None, None), (
        f"file row should be unclaimed after mismatch; got {last_state['file']!r}"
    )


# ---------------------------------------------------------------------------
# Hardening: multi-file cycle, multi-partition
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_multi_file_cycle_commits_all_in_one_snapshot(
    icebox_container: IceboxHandle,
    host_rest_catalog: RestCatalog,
    minio_on_net: DockerContainer,
) -> None:
    """Five files across two partitions, POSTed back-to-back, are
    batched into a single committer cycle and land in one snapshot
    with all five data files in its manifest.

    Exercises the batching path the cycle is designed around — the
    in-process tests cover N=1.
    """
    base_url = icebox_container.base_url
    table = _create_iceberg_table(host_rest_catalog)

    partition_a = {"year": 2026, "month": 6, "day": 2, "hour": 13}
    partition_b = {"year": 2026, "month": 6, "day": 2, "hour": 14}

    posted = 0
    for partition_values in (partition_a, partition_a, partition_a, partition_b, partition_b):
        s3_uri, file_size, row_count = _upload_parquet_to_minio(
            minio_on_net=minio_on_net,
            table_location=table.location(),
            partition_values=partition_values,
        )
        req = _build_register_request(
            table,
            file_path=s3_uri,
            record_count=row_count,
            file_size=file_size,
            partition_values=partition_values,
        )
        r = httpx.post(
            f"{base_url}/v1/files", json=req.model_dump(mode="json"), timeout=10
        )
        assert r.status_code == 201, r.text
        posted += 1
    assert posted == 5

    # Wait for a snapshot. Because all five files were POSTed before
    # the first cycle fired (cadence=2s), the cycle should batch them
    # all into a single snapshot. If the test races the committer
    # (POSTs spilled across two cycles), tolerate up to two snapshots
    # with a combined added_files count of 5.
    deadline = time.monotonic() + 60.0
    total_added = 0
    snapshots: list[Any] = []
    while time.monotonic() < deadline:
        reloaded = host_rest_catalog.load_table((NAMESPACE, TABLE))
        snapshots = list(reloaded.snapshots())
        total_added = sum(
            int(s.summary.get("added-data-files", 0)) for s in snapshots if s.summary
        )
        if total_added >= 5:
            break
        time.sleep(0.5)
    assert total_added == 5, (
        f"expected 5 data files committed across snapshots; "
        f"got total_added={total_added}, snapshots={[dict(s.summary) for s in snapshots if s.summary]!r}"
    )
    assert len(snapshots) in (1, 2), (
        f"expected 1 or 2 snapshots (single cycle ideal; 2 acceptable for cadence race); "
        f"got {len(snapshots)}"
    )


# ---------------------------------------------------------------------------
# Hardening: two iceboxes side-by-side, different (schema, table)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_two_iceboxes_isolate_by_schema_and_table(
    icebox_factory: Callable[..., IceboxHandle],
    host_rest_catalog: RestCatalog,
    minio_on_net: DockerContainer,
) -> None:
    """Two icebox containers, two schemas, two tables, one PG + one
    catalog. Each commits its own snapshot independently — the
    per-schema advisory lock + per-deployment table config keep them
    from stepping on each other.
    """
    handle_events = icebox_factory(
        schema_name=f"icebox_a_{uuid.uuid4().hex[:8]}", table="events_a"
    )
    handle_persons = icebox_factory(
        schema_name=f"icebox_b_{uuid.uuid4().hex[:8]}", table="persons_b"
    )

    table_a = _create_iceberg_table(host_rest_catalog, table="events_a")
    table_b = _create_iceberg_table(host_rest_catalog, table="persons_b")

    # POST a file to icebox A (events_a) and a file to icebox B (persons_b).
    for handle, table in ((handle_events, table_a), (handle_persons, table_b)):
        partition_values = {"year": 2026, "month": 6, "day": 2, "hour": 15}
        s3_uri, file_size, row_count = _upload_parquet_to_minio(
            minio_on_net=minio_on_net,
            table_location=table.location(),
            partition_values=partition_values,
        )
        req = _build_register_request(
            table,
            file_path=s3_uri,
            record_count=row_count,
            file_size=file_size,
            partition_values=partition_values,
            expected_table=handle.table,
        )
        r = httpx.post(
            f"{handle.base_url}/v1/files",
            json=req.model_dump(mode="json"),
            timeout=10,
        )
        assert r.status_code == 201, (handle.table, r.text)

    # Wait for both to commit a snapshot independently.
    deadline = time.monotonic() + 60.0
    snap_a: Any = None
    snap_b: Any = None
    while time.monotonic() < deadline:
        if snap_a is None:
            snaps = list(host_rest_catalog.load_table((NAMESPACE, "events_a")).snapshots())
            if snaps:
                snap_a = snaps[-1]
        if snap_b is None:
            snaps = list(host_rest_catalog.load_table((NAMESPACE, "persons_b")).snapshots())
            if snaps:
                snap_b = snaps[-1]
        if snap_a and snap_b:
            break
        time.sleep(0.5)

    assert snap_a is not None, "icebox A never produced a snapshot for events_a"
    assert snap_b is not None, "icebox B never produced a snapshot for persons_b"
    # The two cycle ids must be different (one came from each container).
    cid_a = snap_a.summary.get(CYCLE_ID_SUMMARY_KEY)
    cid_b = snap_b.summary.get(CYCLE_ID_SUMMARY_KEY)
    assert cid_a is not None
    assert cid_b is not None
    assert cid_a != cid_b, (
        f"both snapshots carry the same cycle_id={cid_a} — schemas are not isolated"
    )


# ---------------------------------------------------------------------------
# Hardening: heartbeat-stale 503
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_heartbeat_stale_returns_503(
    icebox_container: IceboxHandle,
    pg_on_net: Any,
) -> None:
    """When the committer's heartbeat goes stale past
    ``cadence × heartbeat_stale_multiple``, /v1/files returns 503.

    Mechanism: open a host-side PG connection, ``SELECT ... FOR UPDATE``
    the status row to block the committer's heartbeat UPDATE. After
    cadence (2s) × stale_multiple (5) = 10s, the API must flip to 503
    on subsequent POSTs.

    Recovery (lock-released → fresh heartbeat → POSTs accept) is NOT
    asserted here: the test container's degraded-failure threshold would
    fire on any unrelated path that pushed cycles into the failure
    counter, so the recovery shape depends on the rest of the suite's
    state. The in-process tests cover the recovery branch directly.
    """
    base_url = icebox_container.base_url

    # A valid-shaped body that the API would insert IF the heartbeat
    # check passed. Since the heartbeat check runs BEFORE insert, this
    # body never lands in PG once staleness fires — which keeps the
    # committer from acquiring a doomed file row mid-test.
    valid_body = {
        "file_path": "s3://warehouse/never-inserted.parquet",
        "writer_ordinal": 0,
        "kafka_offsets": {"0": 1},
        "partition_values": {"year": 2026, "month": 6, "day": 2, "hour": 16},
        "record_count": 1,
        "file_size": 100,
        "schema_version": "v1",
        "schema_fingerprint": "a" * 64,
        "parquet_stats": {
            "column_sizes": {},
            "value_counts": {},
            "null_value_counts": {},
            "lower_bounds": {},
            "upper_bounds": {},
        },
        "expected_iceberg_namespace": NAMESPACE,
        "expected_iceberg_table": TABLE,
    }

    # Take a host-side row lock on the status row. autocommit=False so
    # the SELECT FOR UPDATE holds across the test body. The committer's
    # next UPDATE on status (every cycle, for heartbeat) will block on
    # our lock until rollback.
    with _pg_conn(
        pg_on_net, schema=icebox_container.schema_name, autocommit=False
    ) as host_conn:
        with host_conn.cursor() as cur:
            cur.execute("SELECT id FROM status FOR UPDATE")
            assert cur.fetchone() is not None

        # cadence=2s, stale_multiple=5 → 10s threshold. Allow slack for
        # the cycle that's mid-flight when we lock to finish.
        deadline = time.monotonic() + 30.0
        saw_503 = False
        last_status: Any = None
        last_err: Any = None
        while time.monotonic() < deadline:
            try:
                resp = httpx.post(f"{base_url}/v1/files", json=valid_body, timeout=5)
                last_status = resp.status_code
            except Exception as e:
                last_err = repr(e)
                last_status = "exc"
            if last_status == 503:
                # The 503 must be the heartbeat-stale branch specifically,
                # not the degraded branch — otherwise a refactor that
                # reorders the backpressure priority (degraded fires
                # first) would pass this test silently.
                assert resp.json().get("reason") == "committer heartbeat stale", (
                    f"503 returned but reason was not heartbeat-stale: {resp.text}"
                )
                saw_503 = True
                break
            time.sleep(1.0)
        if not saw_503:
            out, err = icebox_container.container.get_logs()
            tail_out = out.decode(errors="replace").splitlines()[-40:]
            tail_err = err.decode(errors="replace").splitlines()[-40:]
            host_conn.rollback()
            raise AssertionError(
                f"/v1/files never flipped to 503; last_status={last_status}, "
                f"last_err={last_err}\n"
                f"--- icebox tail stdout ---\n" + "\n".join(tail_out) + "\n"
                "--- icebox tail stderr ---\n" + "\n".join(tail_err)
            )
        host_conn.rollback()  # release the row-lock so teardown is quick


# ---------------------------------------------------------------------------
# Hardening: SIGTERM drain
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_sigterm_drains_cleanly(icebox_container: IceboxHandle) -> None:
    """Sending SIGTERM to the icebox container's PID 1 results in a
    clean shutdown: the committer thread drains within its budget,
    uvicorn exits, and the container's exit code is 0.

    Exercises the SIGTERM → stop_event wiring in icebox/main.py and
    the drain-budget timing that protects K8s' terminationGracePeriod.
    """
    handle = icebox_container
    # Send SIGTERM to PID 1 inside the container.
    handle.container.get_wrapped_container().kill(signal="SIGTERM")

    # Container should exit within the drain budget. The committer is
    # idle (no files pending), so the cycle loop exits on the next
    # stop_event check; uvicorn shuts down after that.
    deadline = time.monotonic() + 30.0
    exit_code: int | None = None
    while time.monotonic() < deadline:
        c = handle.container.get_wrapped_container()
        c.reload()
        if c.status == "exited":
            exit_code = c.attrs["State"]["ExitCode"]
            break
        time.sleep(0.5)
    assert exit_code is not None, "container did not exit within 30s of SIGTERM"
    assert exit_code == 0, f"icebox exited non-zero on SIGTERM: {exit_code}"

    # The "shutdown complete" log line is the explicit success marker
    # in icebox/main.py — pin it so a refactor that drops the drain
    # path is caught.
    out, err = handle.container.get_logs()
    combined = out.decode(errors="replace") + err.decode(errors="replace")
    assert SHUTDOWN_COMPLETE_MARKER in combined, (
        f"expected clean-shutdown marker in logs; got:\n{combined[-2000:]}"
    )


# ---------------------------------------------------------------------------
# Hardening: API perimeter — protocol version, idempotent replay, queue depth
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_protocol_version_mismatch_returns_400(icebox_container: IceboxHandle) -> None:
    """A writer running a different protocol version than the icebox
    is rejected at the API perimeter with 400. This is the only check
    that catches a deploy-skew baked into the image — the unit suite
    covers the handler, but only the Docker test runs against the
    actual binary that ships.
    """
    base_url = icebox_container.base_url
    body = {
        "protocol_version": 99,  # current is 1; 99 is intentionally future
        "file_path": "s3://warehouse/x.parquet",
        "writer_ordinal": 0,
        "kafka_offsets": {"0": 1},
        "partition_values": {"year": 2026, "month": 6, "day": 2, "hour": 17},
        "record_count": 1,
        "file_size": 100,
        "schema_version": "v1",
        "schema_fingerprint": "a" * 64,
        "parquet_stats": {
            "column_sizes": {},
            "value_counts": {},
            "null_value_counts": {},
            "lower_bounds": {},
            "upper_bounds": {},
        },
    }
    r = httpx.post(f"{base_url}/v1/files", json=body, timeout=10)
    assert r.status_code == 400, r.text
    detail = r.json().get("detail", {})
    assert detail.get("error") == "protocol_version_mismatch", r.text


@pytest.mark.integration
def test_replayed_post_returns_409_with_same_row_id(
    icebox_container: IceboxHandle,
) -> None:
    """A writer that crashed after writing parquet but before getting
    the 201 may replay the same POST. The icebox returns 409 the
    second time with the SAME row_id as the original 201 — so writers
    can treat 409 as idempotent success.

    This is the only test that exercises the INSERT ... ON CONFLICT
    DO NOTHING + LOOKUP path against the real image.
    """
    base_url = icebox_container.base_url
    unique_path = f"s3://warehouse/replay-{uuid.uuid4().hex[:12]}.parquet"
    body = {
        "file_path": unique_path,
        "writer_ordinal": 0,
        "kafka_offsets": {"0": 1},
        "partition_values": {"year": 2026, "month": 6, "day": 2, "hour": 18},
        "record_count": 1,
        "file_size": 100,
        "schema_version": "v1",
        "schema_fingerprint": "a" * 64,
        "parquet_stats": {
            "column_sizes": {},
            "value_counts": {},
            "null_value_counts": {},
            "lower_bounds": {},
            "upper_bounds": {},
        },
        "expected_iceberg_namespace": NAMESPACE,
        "expected_iceberg_table": TABLE,
    }
    r1 = httpx.post(f"{base_url}/v1/files", json=body, timeout=10)
    assert r1.status_code == 201, r1.text
    first_row_id = r1.json()["row_id"]

    r2 = httpx.post(f"{base_url}/v1/files", json=body, timeout=10)
    assert r2.status_code == 409, r2.text
    second_row_id = r2.json()["row_id"]
    assert first_row_id == second_row_id, (
        f"replay returned different row_id: {first_row_id} -> {second_row_id}"
    )


@pytest.mark.integration
def test_queue_depth_backpressure_returns_429(
    icebox_factory: Callable[..., IceboxHandle],
) -> None:
    """When pending files reach ``ICEBOX_COMMITTER_MAX_PENDING_FILES``,
    /v1/files returns 429 with a Retry-After header. Uses a 600s
    cadence so the committer never drains the queue during the test.

    This is the only place the pending-count subquery runs against
    real PG — the unit suite mocks the count.
    """
    handle = icebox_factory(
        extra_env={
            "ICEBOX_COMMITTER_MAX_PENDING_FILES": "2",
            # Keep the committer dormant so it doesn't drain the queue
            # before we observe 429. Heartbeat-stale threshold is
            # 600 * 5 = 3000s, way past the test runtime.
            "ICEBOX_COMMITTER_CADENCE_SECONDS": "600",
        }
    )

    def _body(path: str) -> dict[str, Any]:
        return {
            "file_path": path,
            "writer_ordinal": 0,
            "kafka_offsets": {"0": 1},
            "partition_values": {"year": 2026, "month": 6, "day": 2, "hour": 19},
            "record_count": 1,
            "file_size": 100,
            "schema_version": "v1",
            "schema_fingerprint": "a" * 64,
            "parquet_stats": {
                "column_sizes": {},
                "value_counts": {},
                "null_value_counts": {},
                "lower_bounds": {},
                "upper_bounds": {},
            },
            "expected_iceberg_namespace": NAMESPACE,
            "expected_iceberg_table": TABLE,
        }

    # First two POSTs fill the queue.
    for i in range(2):
        path = f"s3://warehouse/qdepth-{i}-{uuid.uuid4().hex[:8]}.parquet"
        r = httpx.post(f"{handle.base_url}/v1/files", json=_body(path), timeout=10)
        assert r.status_code == 201, (i, r.text)

    # Third POST should hit the queue-depth gate.
    third_path = f"s3://warehouse/qdepth-2-{uuid.uuid4().hex[:8]}.parquet"
    r = httpx.post(f"{handle.base_url}/v1/files", json=_body(third_path), timeout=10)
    assert r.status_code == 429, r.text
    assert r.json().get("reason") == "queue full", r.text
    assert "Retry-After" in r.headers, dict(r.headers)


# ---------------------------------------------------------------------------
# Hardening: degraded 503 + downstream-outage graceful degradation
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_downstream_outage_graceful_degradation(
    icebox_factory: Callable[..., IceboxHandle],
    pg_on_net: Any,
) -> None:
    """When the Iceberg catalog is unreachable, the icebox MUST:
      - Keep /readyz at 200 (downstream outages do NOT fail readyz —
        per the contract in icebox/api.py:_handle_readyz).
      - Keep accepting POSTs as long as heartbeat is fresh and the
        degraded threshold isn't crossed.
      - After ``degraded_failure_threshold`` cycle failures, start
        returning 503 with reason="committer degraded".

    Mechanism: point the icebox at a non-existent catalog host. Boot
    succeeds (load_catalog is lazy). The cycle's first load_table()
    fails. With threshold=1, the very next POST returns 503.
    """
    handle = icebox_factory(
        extra_env={
            "ICEBOX_ICEBERG_CATALOG_URI": "http://nonexistent-catalog:8181",
            "ICEBOX_COMMITTER_DEGRADED_FAILURE_THRESHOLD": "1",
        }
    )

    # /readyz must be 200 before we even POST — the boot path succeeded
    # even though the catalog is unreachable.
    r = httpx.get(f"{handle.base_url}/readyz", timeout=5)
    assert r.status_code == 200, r.text

    # Build a valid body. The committer will try to commit it and fail
    # at load_table() (unreachable host). One failure → threshold met.
    body = {
        "file_path": f"s3://warehouse/dgrd-{uuid.uuid4().hex[:8]}.parquet",
        "writer_ordinal": 0,
        "kafka_offsets": {"0": 1},
        "partition_values": {"year": 2026, "month": 6, "day": 2, "hour": 20},
        "record_count": 1,
        "file_size": 100,
        "schema_version": "v1",
        "schema_fingerprint": "a" * 64,
        "parquet_stats": {
            "column_sizes": {},
            "value_counts": {},
            "null_value_counts": {},
            "lower_bounds": {},
            "upper_bounds": {},
        },
        "expected_iceberg_namespace": NAMESPACE,
        "expected_iceberg_table": TABLE,
    }
    # First POST goes through — no failures yet.
    r = httpx.post(f"{handle.base_url}/v1/files", json=body, timeout=10)
    assert r.status_code == 201, r.text

    # Wait for at least one cycle to fail. Cadence=2s default.
    deadline = time.monotonic() + 30.0
    saw_failure = False
    while time.monotonic() < deadline:
        with _pg_conn(pg_on_net, schema=handle.schema_name) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT consecutive_failures FROM status")
                row = cur.fetchone()
        if row and row[0] >= 1:
            saw_failure = True
            break
        time.sleep(0.5)
    assert saw_failure, "committer never recorded a failure against the bad catalog"

    # /readyz must STILL be 200 — downstream outage doesn't gate readiness.
    r = httpx.get(f"{handle.base_url}/readyz", timeout=5)
    assert r.status_code == 200, r.text

    # The next POST must be rejected with degraded 503. Poll briefly
    # because the API's heartbeat freshness window might still admit
    # one more POST until the committer's next heartbeat advances.
    deadline = time.monotonic() + 15.0
    saw_degraded = False
    last_status: Any = None
    while time.monotonic() < deadline:
        # Use a unique path to avoid the 409 replay branch.
        body["file_path"] = f"s3://warehouse/dgrd-{uuid.uuid4().hex[:8]}.parquet"
        resp = httpx.post(f"{handle.base_url}/v1/files", json=body, timeout=5)
        last_status = resp.status_code
        if resp.status_code == 503:
            assert resp.json().get("reason") == "committer degraded", resp.text
            saw_degraded = True
            break
        time.sleep(0.5)
    assert saw_degraded, f"/v1/files never returned degraded 503; last={last_status}"


# ---------------------------------------------------------------------------
# Hardening: recovery after a crashed container
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_recovery_after_sigkill_completes_file(
    icebox_factory: Callable[..., IceboxHandle],
    host_rest_catalog: RestCatalog,
    minio_on_net: DockerContainer,
    pg_on_net: Any,
) -> None:
    """Crash recovery: kill an icebox while a file is being processed;
    spawn a fresh container with the same PG schema; assert the file
    eventually lands in a snapshot.

    The recovery branch that fires (A: cycle deleted; B: kafka-only
    retry; C: finalize) depends on exactly when SIGKILL lands relative
    to the cycle's iceberg/PG/kafka steps. This test asserts the
    end-state regardless — any recovery branch must converge to a
    committed snapshot for the in-flight file.
    """
    # Pre-create the table on the catalog.
    table = _create_iceberg_table(host_rest_catalog)
    # Upload a real parquet so the recovery cycle can actually commit.
    partition_values = {"year": 2026, "month": 6, "day": 2, "hour": 21}
    s3_uri, file_size, row_count = _upload_parquet_to_minio(
        minio_on_net=minio_on_net,
        table_location=table.location(),
        partition_values=partition_values,
    )

    # Spawn icebox A. Use a stable schema so B can reuse it.
    schema = f"icebox_rec_{uuid.uuid4().hex[:8]}"
    # drop_catalog_table_first=False on both spawns: the test
    # pre-created the table BEFORE spawning A, and B's recovery path
    # needs the same table to still exist.
    handle_a = icebox_factory(schema_name=schema, drop_catalog_table_first=False)

    req = _build_register_request(
        table,
        file_path=s3_uri,
        record_count=row_count,
        file_size=file_size,
        partition_values=partition_values,
    )
    r = httpx.post(
        f"{handle_a.base_url}/v1/files", json=req.model_dump(mode="json"), timeout=10
    )
    assert r.status_code == 201, r.text
    file_row_id = r.json()["row_id"]

    # Wait until the committer has claimed the file (cycle_id set on
    # the row). Then SIGKILL — this is mid-flight relative to the
    # cycle's iceberg/PG/kafka steps in some indeterminate position.
    deadline = time.monotonic() + 15.0
    claimed = False
    while time.monotonic() < deadline:
        with _pg_conn(pg_on_net, schema=schema) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT cycle_id, committed_at FROM files WHERE id = %s",
                    (file_row_id,),
                )
                row = cur.fetchone()
        if row and row[0] is not None:
            claimed = True
            break
        time.sleep(0.2)
    assert claimed, "committer never claimed the file before SIGKILL window expired"

    # SIGKILL = no drain, no graceful shutdown. The advisory lock is
    # auto-released by PG when the lock_conn's TCP closes.
    handle_a.container.get_wrapped_container().kill(signal="SIGKILL")

    # Spawn icebox B with the SAME schema. Its committer thread's
    # startup recovery (icebox/committer.py) detects the orphan cycle
    # (if any) and either deletes it or finalizes it; the file then
    # gets re-batched or finalized.
    handle_b = icebox_factory(schema_name=schema, drop_catalog_table_first=False)

    # Wait for the file row to be marked fully committed. The
    # iceberg snapshot lands in the catalog before files.committed_at
    # is set (complete_cycle runs AFTER mark_iceberg_committed +
    # kafka_commit), so polling on the catalog and then querying PG
    # races against the cycle. Poll the file row's committed_at
    # directly — that's the post-complete_cycle marker.
    deadline = time.monotonic() + 60.0
    committed_row: tuple[Any, ...] | None = None
    while time.monotonic() < deadline:
        with _pg_conn(pg_on_net, schema=schema) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT committed_at, iceberg_snapshot_id FROM files WHERE id = %s",
                    (file_row_id,),
                )
                row = cur.fetchone()
        if row is not None and row[0] is not None and row[1] is not None:
            committed_row = row
            break
        time.sleep(0.5)
    if committed_row is None:
        out, err = handle_b.container.get_logs()
        raise AssertionError(
            f"file row {file_row_id} never reached committed state. Last PG row: "
            f"{row!r}\n--- icebox B logs ---\n"
            f"{out.decode(errors='replace')}\n"
            f"{err.decode(errors='replace')}"
        )

    # And there must be a corresponding snapshot in the catalog.
    reloaded = host_rest_catalog.load_table((NAMESPACE, TABLE))
    snaps = list(reloaded.snapshots())
    assert snaps, "no snapshot in catalog after file row was marked committed"


# ---------------------------------------------------------------------------
# Hardening: SIGTERM mid-cycle (drain budget allows the in-flight cycle to finish)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_sigterm_mid_cycle_drains_completes_in_flight_file(
    icebox_container: IceboxHandle,
    host_rest_catalog: RestCatalog,
    minio_on_net: DockerContainer,
    pg_on_net: Any,
) -> None:
    """SIGTERM arriving while the committer is mid-cycle: the drain
    budget (cadence × 5) allows the in-flight cycle to complete; the
    file lands in a snapshot before exit.

    This is the deploy-rollout shape: K8s sends SIGTERM, the icebox
    has terminationGracePeriodSeconds (much wider than the internal
    drain budget) to land any in-flight work.
    """
    table = _create_iceberg_table(host_rest_catalog)
    partition_values = {"year": 2026, "month": 6, "day": 2, "hour": 22}
    s3_uri, file_size, row_count = _upload_parquet_to_minio(
        minio_on_net=minio_on_net,
        table_location=table.location(),
        partition_values=partition_values,
    )

    req = _build_register_request(
        table,
        file_path=s3_uri,
        record_count=row_count,
        file_size=file_size,
        partition_values=partition_values,
    )
    r = httpx.post(
        f"{icebox_container.base_url}/v1/files",
        json=req.model_dump(mode="json"),
        timeout=10,
    )
    assert r.status_code == 201, r.text
    file_row_id = r.json()["row_id"]

    # Wait for the committer to claim the file — that puts us
    # squarely "mid-cycle" relative to the file's lifecycle.
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        with _pg_conn(pg_on_net, schema=icebox_container.schema_name) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT cycle_id FROM files WHERE id = %s", (file_row_id,)
                )
                row = cur.fetchone()
        if row and row[0] is not None:
            break
        time.sleep(0.1)
    else:
        raise AssertionError("committer never claimed the file before SIGTERM")

    # SIGTERM — the committer thread should drain the current cycle.
    icebox_container.container.get_wrapped_container().kill(signal="SIGTERM")

    # Container should exit within the drain budget (cadence × 5 = 10s)
    # plus some slack for uvicorn shutdown.
    deadline = time.monotonic() + 30.0
    exit_code: int | None = None
    while time.monotonic() < deadline:
        c = icebox_container.container.get_wrapped_container()
        c.reload()
        if c.status == "exited":
            exit_code = c.attrs["State"]["ExitCode"]
            break
        time.sleep(0.5)
    assert exit_code == 0, f"icebox exited non-zero on mid-cycle SIGTERM: {exit_code}"

    # The drained cycle should have committed the file.
    with _pg_conn(pg_on_net, schema=icebox_container.schema_name) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT committed_at, iceberg_snapshot_id FROM files WHERE id = %s",
                (file_row_id,),
            )
            row = cur.fetchone()
    assert row is not None
    assert row[0] is not None, (
        f"mid-cycle SIGTERM left the file uncommitted; drain budget did not "
        f"complete the cycle: {row!r}"
    )

    # And the snapshot must be in the catalog.
    reloaded = host_rest_catalog.load_table((NAMESPACE, TABLE))
    assert list(reloaded.snapshots()), "no snapshots in catalog after drain"

    # Clean-shutdown marker present.
    out, err = icebox_container.container.get_logs()
    combined = out.decode(errors="replace") + err.decode(errors="replace")
    assert SHUTDOWN_COMPLETE_MARKER in combined


# ---------------------------------------------------------------------------
# Hardening: singleton-committer invariant under same-schema contention
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_same_schema_second_container_blocks_on_advisory_lock(
    icebox_factory: Callable[..., IceboxHandle],
) -> None:
    """Two icebox containers pointed at the SAME PG schema must enforce
    the singleton-committer invariant via the advisory lock derived
    from the schema name. Container A acquires the lock; container B's
    committer thread spins in the retry loop, never committing
    cycles.

    Production runs replicas=1 + strategy.type: Recreate, so this
    scenario only happens during a rollout overlap or operator
    mistake. The test pins the invariant for any future replicas>1
    attempt.
    """
    shared_schema = f"icebox_lock_{uuid.uuid4().hex[:8]}"

    handle_a = icebox_factory(schema_name=shared_schema)

    # A's committer must have acquired the lock by the time /readyz
    # returned 200. Confirm via the log marker.
    out_a, err_a = handle_a.container.get_logs()
    logs_a = out_a.decode(errors="replace") + err_a.decode(errors="replace")
    assert "acquired singleton advisory lock" in logs_a, (
        f"icebox A never logged lock acquisition; tail:\n{logs_a[-2000:]}"
    )

    # Spawn B with the SAME schema. B's committer will fail to acquire
    # the lock and retry every ADVISORY_LOCK_RETRY_SECONDS (=5s in
    # icebox/committer.py). B's API still comes up — the API process
    # doesn't gate on lock acquisition (heartbeat is NULL on B, but
    # is_heartbeat_stale treats NULL as "first-cycle pending; not
    # stale", so POSTs accept).
    handle_b = icebox_factory(schema_name=shared_schema)

    # B's API must respond to /readyz (boot succeeded; lock waiter is
    # in a separate thread).
    r = httpx.get(f"{handle_b.base_url}/readyz", timeout=5)
    assert r.status_code == 200, r.text

    # Give B time for at least one retry log line.
    time.sleep(6.0)

    out_b, err_b = handle_b.container.get_logs()
    logs_b = out_b.decode(errors="replace") + err_b.decode(errors="replace")
    assert "advisory lock held by another committer" in logs_b, (
        f"icebox B never logged lock contention; expected the retry-loop "
        f"marker from icebox/committer.py. Tail:\n{logs_b[-2000:]}"
    )
    assert "acquired singleton advisory lock" not in logs_b, (
        f"icebox B should NOT have acquired the lock while A holds it. "
        f"Logs:\n{logs_b[-2000:]}"
    )


# ---------------------------------------------------------------------------
# Hardening: concurrent POST probe (32 simultaneous, matches prod writer fanout)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_thirty_two_concurrent_posts_all_accepted(
    icebox_container: IceboxHandle,
    host_rest_catalog: RestCatalog,
    minio_on_net: DockerContainer,
) -> None:
    """32 concurrent POSTs (matching the prod writer fanout) all return
    201 with distinct row_ids and the committer batches them into one
    or a few snapshots.

    Exercises:
      - asyncpg pool capacity (default max=8) under burst load
      - PG ``UNIQUE(file_path)`` ON CONFLICT semantics under concurrency
        (no duplicate row_ids returned for distinct paths)
      - the committer's claim-batch upper bound vs. burst size
    """
    base_url = icebox_container.base_url
    table = _create_iceberg_table(host_rest_catalog)

    # Pre-upload 32 parquet files to MinIO across 4 partitions so the
    # committer has real data files to commit.
    partitions = [
        {"year": 2026, "month": 6, "day": 3, "hour": h} for h in range(4)
    ]
    file_specs: list[tuple[str, int, int, dict[str, int]]] = []
    for i in range(32):
        partition = partitions[i % len(partitions)]
        s3_uri, file_size, row_count = _upload_parquet_to_minio(
            minio_on_net=minio_on_net,
            table_location=table.location(),
            partition_values=partition,
        )
        file_specs.append((s3_uri, file_size, row_count, partition))

    bodies = [
        _build_register_request(
            table,
            file_path=spec[0],
            record_count=spec[2],
            file_size=spec[1],
            partition_values=spec[3],
        ).model_dump(mode="json")
        for spec in file_specs
    ]

    # Fire all 32 POSTs concurrently via asyncio.gather. httpx
    # AsyncClient handles connection pooling against the icebox.
    import asyncio

    async def _post_all() -> list[Any]:
        async with httpx.AsyncClient(timeout=30) as client:
            return await asyncio.gather(
                *[client.post(f"{base_url}/v1/files", json=b) for b in bodies]
            )

    responses = asyncio.run(_post_all())
    assert len(responses) == 32

    row_ids: set[int] = set()
    for i, r in enumerate(responses):
        assert r.status_code == 201, (i, r.status_code, r.text)
        row_id = r.json()["row_id"]
        assert row_id not in row_ids, (
            f"duplicate row_id {row_id} returned for two distinct file_paths "
            f"under concurrent POST — UNIQUE(file_path) contract regressed"
        )
        row_ids.add(row_id)
    assert len(row_ids) == 32

    # Wait for the committer to drain. With cadence=2s and 32 files,
    # one to a few cycles should empty the queue.
    deadline = time.monotonic() + 90.0
    total_committed = 0
    snapshots: list[Any] = []
    while time.monotonic() < deadline:
        reloaded = host_rest_catalog.load_table((NAMESPACE, TABLE))
        snapshots = list(reloaded.snapshots())
        total_committed = sum(
            int(s.summary.get("added-data-files", 0)) for s in snapshots if s.summary
        )
        if total_committed >= 32:
            break
        time.sleep(1.0)
    assert total_committed == 32, (
        f"expected 32 data files committed across snapshots; "
        f"got total_committed={total_committed}, "
        f"snapshots={[dict(s.summary) for s in snapshots if s.summary]!r}"
    )


# ---------------------------------------------------------------------------
# Hardening: /metrics endpoint + JSON log shape + cycle_id stamping
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_metrics_endpoint_serves_prometheus_format(
    icebox_container: IceboxHandle,
) -> None:
    """``/metrics`` returns text/plain in Prometheus exposition format
    with the icebox-specific metric names defined in
    icebox/metrics.py. The live gauges are populated from the
    just-completed PG read.
    """
    base_url = icebox_container.base_url
    r = httpx.get(f"{base_url}/metrics", timeout=5)
    assert r.status_code == 200, r.text
    assert "text/plain" in r.headers["content-type"]
    body = r.text
    # HELP lines for each metric name we declare.
    for name in (
        "icebox_pending_files",
        "icebox_oldest_pending_age_seconds",
        "icebox_consecutive_failures",
        "icebox_committer_heartbeat_age_seconds",
        "icebox_cycles_total",
        "icebox_cycle_duration_seconds",
        "icebox_files_committed_total",
        "icebox_post_total",
    ):
        assert f"# HELP {name} " in body, f"missing HELP for {name} in /metrics body"


@pytest.mark.integration
def test_metrics_post_total_increments_after_post(
    icebox_container: IceboxHandle,
) -> None:
    """A POST /v1/files must show up in icebox_post_total{status="201"}
    on the next /metrics scrape — the perimeter middleware in
    icebox/api.py is what ensures status codes are counted regardless
    of which return path the handler took.
    """
    base_url = icebox_container.base_url

    def _scrape_count(status: str) -> float:
        r = httpx.get(f"{base_url}/metrics", timeout=5)
        r.raise_for_status()
        for line in r.text.splitlines():
            # Match e.g. icebox_post_total{status="201"} 5.0
            if line.startswith("icebox_post_total{") and f'status="{status}"' in line:
                return float(line.split()[-1])
        return 0.0

    before = _scrape_count("201")

    body = {
        "file_path": f"s3://warehouse/metrics-{uuid.uuid4().hex[:8]}.parquet",
        "writer_ordinal": 0,
        "kafka_offsets": {"0": 1},
        "partition_values": {"year": 2026, "month": 6, "day": 3, "hour": 0},
        "record_count": 1,
        "file_size": 100,
        "schema_version": "v1",
        "schema_fingerprint": "a" * 64,
        "parquet_stats": {
            "column_sizes": {},
            "value_counts": {},
            "null_value_counts": {},
            "lower_bounds": {},
            "upper_bounds": {},
        },
        "expected_iceberg_namespace": NAMESPACE,
        "expected_iceberg_table": TABLE,
    }
    r = httpx.post(f"{base_url}/v1/files", json=body, timeout=10)
    assert r.status_code == 201, r.text

    after = _scrape_count("201")
    assert after == before + 1.0, (
        f"icebox_post_total{{status=201}} did not increment: before={before} after={after}"
    )


@pytest.mark.integration
def test_log_output_is_json_with_cycle_id_during_cycle(
    icebox_container: IceboxHandle,
    host_rest_catalog: RestCatalog,
    minio_on_net: DockerContainer,
) -> None:
    """Container stdout must be one-JSON-object-per-line, and at least
    one line emitted during a successful cycle must carry the
    ``cycle_id`` field set by the committer's ContextVar.
    """
    table = _create_iceberg_table(host_rest_catalog)
    partition_values = {"year": 2026, "month": 6, "day": 3, "hour": 1}
    s3_uri, file_size, row_count = _upload_parquet_to_minio(
        minio_on_net=minio_on_net,
        table_location=table.location(),
        partition_values=partition_values,
    )
    req = _build_register_request(
        table,
        file_path=s3_uri,
        record_count=row_count,
        file_size=file_size,
        partition_values=partition_values,
    )
    r = httpx.post(
        f"{icebox_container.base_url}/v1/files",
        json=req.model_dump(mode="json"),
        timeout=10,
    )
    assert r.status_code == 201, r.text

    # Wait until a snapshot lands (proves a cycle fired end-to-end).
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        reloaded = host_rest_catalog.load_table((NAMESPACE, TABLE))
        if list(reloaded.snapshots()):
            break
        time.sleep(0.5)
    else:
        raise AssertionError("no cycle fired within 30s")

    # Grab the container's full stdout + stderr.
    out, err = icebox_container.container.get_logs()
    stream = out.decode(errors="replace") + err.decode(errors="replace")

    # Each non-empty line of the icebox's own output should be a
    # parseable JSON object. uvicorn's access log uses Python's
    # logging but goes through the same root handler, so it's JSON
    # too. testcontainers may interleave a "Container started" line
    # from its own ryuk supervisor on some setups — tolerate non-JSON
    # lines but require that AT LEAST ONE line has cycle_id and AT
    # LEAST ONE line decodes as JSON with our required keys.
    parsed: list[dict[str, Any]] = []
    for line in stream.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            parsed.append(obj)

    assert parsed, (
        f"no JSON-formatted log lines found in container output. "
        f"first 1000 chars:\n{stream[:1000]}"
    )
    # Every parsed line should have the standard fields.
    for obj in parsed[:5]:
        assert {"ts", "level", "logger", "msg"} <= obj.keys(), obj

    # At least one line emitted from inside run_cycle (committer step
    # markers) must carry cycle_id.
    with_cycle_id = [o for o in parsed if "cycle_id" in o]
    assert with_cycle_id, (
        "no log line carried cycle_id; the committer's ContextVar "
        "stamping is not wired up. Sample JSON lines:\n"
        + "\n".join(json.dumps(o) for o in parsed[:5])
    )


# ---------------------------------------------------------------------------
# Hardening: schema fingerprint check at the API perimeter
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_schema_fingerprint_mismatch_rejected_at_api_perimeter(
    icebox_container: IceboxHandle,
    host_rest_catalog: RestCatalog,
) -> None:
    """A POST whose ``schema_fingerprint`` doesn't match the Iceberg
    table's current schema must be rejected synchronously with 400
    at the API. The committer's own fingerprint check still applies
    as defense in depth, but a writer with a stale schema should see
    the rejection immediately rather than having its file row stall
    in PG waiting for a cycle that will then skip.
    """
    base_url = icebox_container.base_url
    # Pre-create the Iceberg table so the cache has a current schema
    # to validate against.
    _create_iceberg_table(host_rest_catalog)

    bad_body = {
        "file_path": f"s3://warehouse/fp-mismatch-{uuid.uuid4().hex[:8]}.parquet",
        "writer_ordinal": 0,
        "kafka_offsets": {"0": 1},
        "partition_values": {"year": 2026, "month": 6, "day": 3, "hour": 2},
        "record_count": 1,
        "file_size": 100,
        "schema_version": "v1",
        # Deliberately not the table's fingerprint.
        "schema_fingerprint": "ff" * 32,
        "parquet_stats": {
            "column_sizes": {},
            "value_counts": {},
            "null_value_counts": {},
            "lower_bounds": {},
            "upper_bounds": {},
        },
        "expected_iceberg_namespace": NAMESPACE,
        "expected_iceberg_table": TABLE,
    }
    r = httpx.post(f"{base_url}/v1/files", json=bad_body, timeout=10)
    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert detail["error"] == "schema_fingerprint_mismatch", r.text
    # The 400 must NOT echo the request body verbatim — same redaction
    # rationale as the namespace/table mismatch tests.
    assert bad_body["file_path"] not in r.text
    # The fingerprint itself appears in the structured detail (writer
    # debugging needs it) but the rest of the body shouldn't.
    assert '"kafka_offsets"' not in r.text
