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

# Install DuckDB CLI (pinned to match the Python package version from pyproject.toml)
RUN V=$(python -c "import tomllib; d=tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']; print(next(x.split('==')[1] for x in d if x.startswith('duckdb==')))") \
    && uv tool install "duckdb-cli==$V"

FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends just=1.40.0* && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/millpond /app/millpond
COPY --from=builder /root/.local/bin/duckdb /usr/local/bin/duckdb
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
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL ducklake'); c.execute('INSTALL postgres')"

# Health check for non-K8s environments (K8s uses liveness/readiness probes in statefulset.yaml)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')"]

ENTRYPOINT ["millpond"]
