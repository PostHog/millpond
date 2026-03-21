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
