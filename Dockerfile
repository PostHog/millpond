FROM ghcr.io/astral-sh/uv:0.7 AS uv
FROM python:3.12-slim AS builder

COPY --from=uv /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project --extra msk-iam

COPY millpond/ millpond/
ARG MILLPOND_VERSION=0.0.0.dev0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=$MILLPOND_VERSION
RUN uv sync --frozen --no-dev --extra msk-iam

# Install DuckDB CLI from the upstream GitHub release. duckdb-cli is a single
# static binary, so we avoid `uv tool install` here: that path installs a full
# Python venv just to PATH a self-contained C++ binary, and its default install
# location (/root/.local, mode 700 on python:3.12-slim) is unreachable by the
# non-root `millpond` runtime user. Pin the version from pyproject.toml so the
# CLI tracks the duckdb Python package.
RUN V=$(python -c "import tomllib; d=tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']; print(next(x.split('==')[1] for x in d if x.startswith('duckdb==')))") \
    && apt-get update && apt-get install -y --no-install-recommends curl unzip \
    && curl -fsSL "https://github.com/duckdb/duckdb/releases/download/v${V}/duckdb_cli-linux-amd64.zip" -o /tmp/duckdb.zip \
    && unzip -d /usr/local/bin /tmp/duckdb.zip \
    && chmod 0755 /usr/local/bin/duckdb \
    && rm /tmp/duckdb.zip \
    && apt-get remove -y curl unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends just=1.40.0* && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/millpond /app/millpond
COPY --from=builder /usr/local/bin/duckdb /usr/local/bin/duckdb
COPY tools/justfile /justfile
COPY tools/ducklake_maintenance.py /app/tools/ducklake_maintenance.py
COPY tools/ducklake_maintenance.sql /app/tools/ducklake_maintenance.sql
COPY tools/ducklake_metrics.py /app/tools/ducklake_metrics.py

ENV PATH="/app/.venv/bin:$PATH"

RUN useradd --create-home --shell /bin/false millpond
USER millpond

# Pre-install DuckDB extensions at build time to avoid runtime network dependency.
# Must run as millpond user so extensions land in ~/.duckdb/extensions/ (not /root/).
# httpfs must be installed before ducklake — there's a race condition with S3 access
# if ducklake loads first and tries to use httpfs before it's available.
#
# The DuckLake extension cannot be version-pinned at install time — the extension
# repository doesn't expose pinned versions for it (INSTALL ducklake VERSION '...'
# returns 404). The DuckDB Python pin (1.5.2 in pyproject.toml) locks the
# DuckLake major line, and tests/unit/test_ducklake_pin.py asserts the loaded
# extension's build SHA at runtime so a drift trips in CI rather than in prod.
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL ducklake'); c.execute('INSTALL postgres')"

# Build-time smoke test for the duckdb CLI as the runtime user. Catches any
# regression where the CLI is installed somewhere the `millpond` user can't
# execute it (the failure mode we just patched). Cheap (~ms) and runs in the
# same layer the regression would land in.
RUN duckdb -c "SELECT 1;" >/dev/null

# Health check for non-K8s environments (K8s uses liveness/readiness probes in statefulset.yaml).
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')"]

ENTRYPOINT ["millpond"]
