FROM ghcr.io/astral-sh/uv:0.7 AS uv
FROM python:3.12-slim AS builder

COPY --from=uv /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project --extra msk-iam

# Copy ALL Python packages the image needs to install:
#   millpond/   — the writer (and the writer-side icebox sink)
#   icebox/     — the icebox service (committer + FastAPI)
#   shared/     — wire format + path helpers used by both sides
# pyproject.toml registers two console scripts:
#   millpond = "millpond.main:main"
#   icebox   = "icebox.main:main"
# The same image carries both; the container's command selects which
# binary runs (the chart's deployment.yaml sets either `["millpond"]`
# or `["icebox"]`).
COPY millpond/ millpond/
COPY icebox/ icebox/
COPY shared/ shared/
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
COPY --from=builder /app/icebox /app/icebox
COPY --from=builder /app/shared /app/shared
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
#
# The DuckLake extension cannot be version-pinned at install time — the extension
# repository doesn't expose pinned versions for it (INSTALL ducklake VERSION '...'
# returns 404). The DuckDB Python pin (1.5.2 in pyproject.toml) locks the
# DuckLake major line, and tests/unit/test_ducklake_pin.py asserts the loaded
# extension's build SHA at runtime so a drift trips in CI rather than in prod.
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL ducklake'); c.execute('INSTALL postgres')"

# Health check for non-K8s environments (K8s uses liveness/readiness probes in statefulset.yaml).
# Both `millpond` and `icebox` binaries serve /healthz on port 8000 so the same HEALTHCHECK
# works regardless of which entrypoint the container runs.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')"]

# Default entrypoint is the writer (`millpond`). For the icebox service, the
# chart overrides via `command: ["icebox"]` in the pod spec.
ENTRYPOINT ["millpond"]
