"""A throwaway real Postgres for integration tests.

Shared by the suites that need a real catalog database
(drop-orphan-inline-tables, the postgres catalog mode of ducklake_metrics).
The fixtures that use these helpers live in tests/integration/conftest.py.

Postgres source, first available wins:
  1. MILLPOND_TEST_PG_DSN — an existing server; each test creates and drops
     a throwaway database on it (needs CREATEDB).
  2. initdb / pg_ctl on PATH (or under /usr/lib/postgresql/*/bin, where the
     GitHub ubuntu runner keeps them) WITH the `postgres` server binary beside
     them — a throwaway cluster in tmp.
  3. docker — a throwaway postgres:17 container on a random 127.0.0.1 port.
Otherwise the tests skip (fail when MILLPOND_REQUIRE_DOCKER_STACK is set,
same contract as the hoglake suites).
"""

from __future__ import annotations

import contextlib
import glob
import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid

import psycopg

from tests.hoglake_stack import stack

_USER = "ducklake"
_PASSWORD = "ducklake"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(conninfo: str, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            psycopg.connect(conninfo, connect_timeout=2).close()
            return
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.5)


def _pg_bin(name: str) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    candidates = sorted(glob.glob(f"/usr/lib/postgresql/*/bin/{name}"))
    return candidates[-1] if candidates else None


def _has_local_server() -> bool:
    """initdb/pg_ctl on PATH are not enough: brew's `libpq` ships both WITHOUT
    the `postgres` server binary, and initdb then fails at run time instead of
    letting the fixture fall through to docker. Require an EXECUTABLE server
    next to the initdb we would actually call (initdb resolves its own real
    path and looks for `postgres` there)."""
    initdb, pg_ctl = _pg_bin("initdb"), _pg_bin("pg_ctl")
    if not (initdb and pg_ctl):
        return False
    return os.access(os.path.join(os.path.dirname(initdb), "postgres"), os.X_OK)


@contextlib.contextmanager
def _local_cluster():
    initdb, pg_ctl = _pg_bin("initdb"), _pg_bin("pg_ctl")
    datadir = tempfile.mkdtemp(prefix="millpond-pg-")
    port = _free_port()
    try:
        subprocess.run(
            [initdb, "-D", datadir, "-U", _USER, "--auth=trust"], check=True, capture_output=True, timeout=120
        )
        # TCP only: tmp paths on macOS exceed the unix-socket path limit.
        opts = f"-p {port} -c listen_addresses=127.0.0.1 -c unix_socket_directories=''"
        subprocess.run(
            [pg_ctl, "-D", datadir, "-o", opts, "-w", "-l", os.path.join(datadir, "log"), "start"],
            check=True,
            capture_output=True,
            timeout=120,
        )
        yield {"host": "127.0.0.1", "port": port, "user": _USER, "password": _PASSWORD, "dbname": "postgres"}
    finally:
        subprocess.run([pg_ctl, "-D", datadir, "-m", "immediate", "stop"], capture_output=True, timeout=60)
        shutil.rmtree(datadir, ignore_errors=True)


@contextlib.contextmanager
def _docker_cluster():
    port = _free_port()
    name = f"millpond-pg-it-{uuid.uuid4().hex[:8]}"
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-e",
            f"POSTGRES_USER={_USER}",
            "-e",
            f"POSTGRES_PASSWORD={_PASSWORD}",
            "-p",
            f"127.0.0.1:{port}:5432",
            "--tmpfs",
            "/var/lib/postgresql/data",
            "postgres:17",
        ],
        check=True,
        capture_output=True,
        timeout=600,
    )
    try:
        yield {"host": "127.0.0.1", "port": port, "user": _USER, "password": _PASSWORD, "dbname": "postgres"}
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=60)


def _conninfo(p: dict) -> str:
    return f"host={p['host']} port={p['port']} user={p['user']} password={p['password']} dbname={p['dbname']}"


@contextlib.contextmanager
def pg_server_context():
    """Yield connection parameters for a server, per the source order above."""
    dsn = os.environ.get("MILLPOND_TEST_PG_DSN")
    if dsn:
        info = psycopg.conninfo.conninfo_to_dict(dsn)
        yield {
            "host": info.get("host", "127.0.0.1"),
            "port": int(info.get("port", 5432)),
            "user": info.get("user", _USER),
            "password": info.get("password", ""),
            "dbname": info.get("dbname", "postgres"),
        }
        return
    if _has_local_server():
        cm = _local_cluster()
    elif stack.docker_available():
        cm = _docker_cluster()
    else:
        stack.require_or_skip(
            "no Postgres available: no MILLPOND_TEST_PG_DSN, no local initdb/pg_ctl with an executable "
            "`postgres` server beside them, no docker"
        )
    with cm as server:
        _wait_ready(_conninfo(server))
        yield server


@contextlib.contextmanager
def throwaway_database(server: dict):
    """Create a fresh database on `server`, yield its parameters, drop it."""
    dbname = f"it_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(_conninfo(server), autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{dbname}"')
    try:
        yield {**server, "dbname": dbname}
    finally:
        with psycopg.connect(_conninfo(server), autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
