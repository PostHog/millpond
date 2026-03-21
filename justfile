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
    uv run pytest

# Run integration tests (requires Docker)
[group('test')]
test-integration:
    uv run pytest -m integration

# Full CI check
[group('test')]
ci: fmt-check lint test

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
