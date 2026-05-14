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

# === Misc ===

# Open the sizing calculator
[group('misc')]
sizing:
    open tools/sizing-calculator.html
