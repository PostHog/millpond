# Millpond — Kafka to DuckLake

# Default recipe: list all available recipes
default:
    @just --list

# === Dev ===

# Install dependencies
[group('dev')]
sync:
    uv sync

# Run the application
[group('dev')]
run:
    uv run millpond

# Format code
[group('dev')]
fmt:
    uv run ruff format millpond/

# Check formatting
[group('dev')]
fmt-check:
    uv run ruff format --check millpond/

# Lint code
[group('dev')]
lint:
    uv run ruff check millpond/

# Lint and fix
[group('dev')]
lint-fix:
    uv run ruff check --fix millpond/

# === Test ===

# Run unit tests
[group('test')]
test:
    uv run pytest tests/unit

# Run integration tests
[group('test')]
test-integration:
    uv run pytest tests/integration

# Run E2E test (brings up docker-compose stack automatically)
[group('test')]
test-e2e:
    uv run pytest tests/e2e -v -s

# Full CI check
[group('test')]
ci: fmt-check lint test

# === Docker ===

# Start the docker-compose dev environment (Kafka, Postgres, MinIO, producer, 2 millpond pods)
[group('docker')]
up:
    docker compose build
    docker compose up -d

# Stop the docker-compose dev environment
[group('docker')]
down:
    docker compose down -v

# Open a DuckDB shell attached to the local DuckLake (requires `just up` first)
[group('docker')]
duck:
    duckdb -init test/ducklake-init.sql

# Open the Grafana dashboard (requires `just up` first)
[group('docker')]
dashboard:
    open http://localhost:3000/d/millpond/millpond

# Open the MinIO console (requires `just up` first, login: minioadmin/minioadmin)
[group('docker')]
minio:
    open http://localhost:9001

# === Build ===

# Build Docker image
[group('build')]
build:
    docker build -t millpond .

# Clean build artifacts
[group('build')]
clean:
    rm -rf .venv dist *.egg-info __pycache__ millpond/__pycache__

# === Metrics ===

# Compare LOC against ducklake-kafka-connect
[group('dev')]
loc:
    #!/usr/bin/env bash
    set -euo pipefail
    tmpdir=$(mktemp -d)
    trap "rm -rf $tmpdir" EXIT
    git clone --quiet --depth 1 https://github.com/PostHog/ducklake-kafka-connect.git "$tmpdir/dkc" 2>/dev/null
    echo ""
    echo "┌─────────────────────────────────────────────────────────────────┐"
    echo "│              LOC Comparison: Millpond vs Kafka Connect         │"
    echo "└─────────────────────────────────────────────────────────────────┘"
    echo ""
    echo "=== Millpond (Python) ==="
    cloc --quiet millpond/ tests/
    echo ""
    echo "=== ducklake-kafka-connect (Java) ==="
    cloc --quiet "$tmpdir/dkc/src/"
    echo ""
    # Summary table
    mp_prod=$(cloc --csv --quiet millpond/ | tail -1 | cut -d, -f5)
    mp_test=$(cloc --csv --quiet tests/ | tail -1 | cut -d, -f5)
    dkc_prod=$(cloc --csv --quiet "$tmpdir/dkc/src/main/" | tail -1 | cut -d, -f5)
    dkc_test=$(cloc --csv --quiet "$tmpdir/dkc/src/test/" "$tmpdir/dkc/src/integrationTest/" | tail -1 | cut -d, -f5)
    echo "┌──────────────────┬──────────────────┬──────────────┬───────┐"
    echo "│                  │ Kafka Connect    │ Millpond     │ Ratio │"
    echo "├──────────────────┼──────────────────┼──────────────┼───────┤"
    printf "│ Production       │ %'16d │ %'12d │ %4.1fx │\n" "$dkc_prod" "$mp_prod" "$(echo "$dkc_prod/$mp_prod" | bc -l)"
    printf "│ Tests            │ %'16d │ %'12d │ %4.1fx │\n" "$dkc_test" "$mp_test" "$(echo "$dkc_test/$mp_test" | bc -l)"
    printf "│ Total            │ %'16d │ %'12d │ %4.1fx │\n" "$((dkc_prod+dkc_test))" "$((mp_prod+mp_test))" "$(echo "($dkc_prod+$dkc_test)/($mp_prod+$mp_test)" | bc -l)"
    echo "└──────────────────┴──────────────────┴──────────────┴───────┘"
