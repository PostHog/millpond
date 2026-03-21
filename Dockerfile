FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY dsk2d/ dsk2d/
RUN uv sync --frozen --no-dev

FROM python:3.12-slim

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/dsk2d /app/dsk2d

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["dsk2d"]
