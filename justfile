# Millpond — Kafka to Iceberg

# Default recipe: list all available recipes
default:
    @just --list

# === Dev ===

# Install dependencies
[group('dev')]
sync:
    uv sync

# Install git hooks
[group('dev')]
install-hooks:
    git config core.hooksPath .githooks

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
    uv run python -m pytest tests/unit

# Run integration tests
[group('test')]
test-integration:
    uv run python -m pytest tests/integration

# Run E2E test (brings up docker-compose stack automatically)
[group('test')]
test-e2e:
    uv run python -m pytest tests/e2e -v -s

# Full CI check
[group('test')]
ci: fmt-check lint test

# === Docker ===

# Generate SSL certs for local Kafka (one-time setup)
[group('docker')]
ssl-certs:
    ./test/generate-ssl-certs.sh

# Start the docker-compose dev environment (Kafka, Postgres, MinIO, producer, 2 millpond pods)
[group('docker')]
up:
    docker compose build
    docker compose up -d

# Start the docker-compose dev environment with SSL Kafka
[group('docker')]
up-ssl: ssl-certs
    docker compose -f docker-compose.yaml -f docker-compose.ssl.yaml build
    docker compose -f docker-compose.yaml -f docker-compose.ssl.yaml up -d

# Stop the docker-compose dev environment
[group('docker')]
down:
    docker compose down -v

# Stop the SSL docker-compose dev environment
[group('docker')]
down-ssl:
    docker compose -f docker-compose.yaml -f docker-compose.ssl.yaml down -v

# Open the Grafana dashboard (requires `just up` first)
[group('docker')]
dashboard:
    open http://localhost:3000/d/millpond/millpond

# Open the MinIO console (requires `just up` first, login: minioadmin/minioadmin)
[group('docker')]
minio:
    open http://localhost:9001

# Open the sizing calculator
[group('docker')]
sizing:
    open tools/sizing-calculator.html

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

