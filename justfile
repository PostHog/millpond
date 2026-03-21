# DSK2D — Dead Simple Kafka to DuckLake

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
    uv run dsk2d

# Format code
[group('dev')]
fmt:
    uv run ruff format dsk2d/

# Check formatting
[group('dev')]
fmt-check:
    uv run ruff format --check dsk2d/

# Lint code
[group('dev')]
lint:
    uv run ruff check dsk2d/

# Lint and fix
[group('dev')]
lint-fix:
    uv run ruff check --fix dsk2d/

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
    docker build -t dsk2d .

# Clean build artifacts
[group('build')]
clean:
    rm -rf .venv dist *.egg-info __pycache__ dsk2d/__pycache__
