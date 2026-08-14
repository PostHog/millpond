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

# PostHog/duckdb fork (VARIANT shred allowlist). pyproject pins duckdb==1.5.5
# so CI/dev can use the official PyPI wheel; the image overwrites it with the
# fork cp312 wheel and installs the matching CLI. source_id is asserted in
# the runtime stage.
#
# TARGETARCH is a BuildKit automatic ARG (linux/amd64 → amd64, linux/arm64
# → arm64). Wheel tags use the GNU tuple (x86_64 / aarch64). Install the
# wheel from its release URL so the PEP 427 filename is preserved (uv 0.12
# rejects a dest named duckdb.whl). --reinstall is required: the fork is
# also versioned 1.5.5, so without it uv leaves the official PyPI wheel
# (source_id d8cdaa33fd) in the venv.
ARG TARGETARCH
ARG DUCKDB_RELEASE=v1.5.5-posthog.2
RUN test -n "$TARGETARCH" \
    && case "$TARGETARCH" in amd64) WHEEL_ARCH=x86_64 ;; arm64) WHEEL_ARCH=aarch64 ;; *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; esac \
    && apt-get update && apt-get install -y --no-install-recommends curl unzip \
    && curl -fsSL "https://github.com/PostHog/duckdb/releases/download/${DUCKDB_RELEASE}/duckdb_cli-linux-${TARGETARCH}.zip" -o /tmp/duckdb.zip \
    && unzip -j -d /usr/local/bin /tmp/duckdb.zip duckdb \
    && chmod 0755 /usr/local/bin/duckdb \
    && rm /tmp/duckdb.zip \
    && uv pip install --python /app/.venv/bin/python --reinstall --no-deps \
         "https://github.com/PostHog/duckdb/releases/download/${DUCKDB_RELEASE}/duckdb-1.5.5-cp312-cp312-linux_${WHEEL_ARCH}.whl" \
    && apt-get remove -y curl unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

FROM python:3.12-slim

# postgresql-client: the justfile's bootstrap-index-* recipes shell out to
# psql for CREATE INDEX CONCURRENTLY (cannot run inside a transaction, so
# the python/duckdb connection path is not a substitute).
RUN apt-get update && apt-get install -y --no-install-recommends just=1.40.0* postgresql-client && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/millpond /app/millpond
COPY --from=builder /usr/local/bin/duckdb /usr/local/bin/duckdb
COPY tools/justfile /justfile
COPY tools/ducklake_maintenance.py /app/tools/ducklake_maintenance.py
COPY tools/ducklake_maintenance.sql /app/tools/ducklake_maintenance.sql
COPY tools/ducklake_metrics.py /app/tools/ducklake_metrics.py

# Shell banner: interactive shells (kubectl exec) print which DuckLake
# catalog the container targets, derived from DUCKLAKE_* env (override the
# label with MILLPOND_SHELL_LABEL). Debian bash reads /etc/bash.bashrc for
# interactive non-login shells; appended at build because the runtime
# rootfs is read-only.
COPY tools/shell-banner.bashrc /tmp/shell-banner.bashrc
RUN cat /tmp/shell-banner.bashrc >> /etc/bash.bashrc && rm /tmp/shell-banner.bashrc

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
# returns 404), and the channel the duckdb pin selects is LIVE: it can re-serve
# a new ducklake build under the same URL at any time. So the build SHA is
# asserted HERE, failing the image build on drift — the release workflow runs
# in parallel with CI, so the CI canary (tests/unit/test_ducklake_pin.py,
# same SHA constant; update both together) alone cannot stop a drifted build
# from reaching the fleet via the mutable tag. An unexpected ducklake build
# must be unable to produce an image at all.
# aws: required by CREATE SECRET (TYPE s3, PROVIDER credential_chain) — the
# Pod-Identity path the tenant maintenance crons use. Without it DuckDB
# auto-installs at runtime, which dies on a read-only root filesystem.
#
# source_id must be the PostHog fork (v1.5.5-posthog.2). Official 1.5.5
# wheels report a different hash (d8cdaa33fd) and do not honor
# variant_shred_key_prefix.
ARG DUCKLAKE_SHA=d8a1881e
RUN python -c "\
import duckdb, os; c = duckdb.connect(); \
ver, sid = c.execute('SELECT library_version, source_id FROM pragma_version()').fetchone(); \
assert ver == 'v1.5.5', ver; \
assert sid.startswith('2a514c18f7'), sid; \
c.execute('INSTALL httpfs'); c.execute('INSTALL ducklake'); c.execute('INSTALL postgres'); c.execute('INSTALL aws'); \
c.execute('LOAD ducklake'); \
v = c.execute(\"SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'\").fetchone()[0]; \
want = os.environ['DUCKLAKE_SHA']; \
assert v == want, f'ducklake extension channel drift: channel serves {v}, image validated against {want}'"

# Build-time smoke test for the duckdb CLI as the runtime user. Catches any
# regression where the CLI is installed somewhere the `millpond` user can't
# execute it (the failure mode we just patched). Cheap (~ms) and runs in the
# same layer the regression would land in.
RUN duckdb -c "SELECT 1;" >/dev/null

# Health check for non-K8s environments (K8s uses liveness/readiness probes in statefulset.yaml).
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')"]

ENTRYPOINT ["millpond"]
