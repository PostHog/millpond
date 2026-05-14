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

FROM python:3.12-slim

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/millpond /app/millpond

ENV PATH="/app/.venv/bin:$PATH"

RUN useradd --create-home --shell /bin/false millpond
USER millpond

# Health check for non-K8s environments (K8s uses liveness/readiness probes in statefulset.yaml)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')"]

ENTRYPOINT ["millpond"]
