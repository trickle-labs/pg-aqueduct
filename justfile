# justfile for pg-aqueduct
# Run `just --list` to see all available recipes.

# Default recipe.
default:
    @just --list

# ── Build ───────────────────────────────────────────────────────────────────

# Build all crates.
build:
    cargo build --all-targets

# Build in release mode.
build-release:
    cargo build --release

# ── Test ────────────────────────────────────────────────────────────────────

# Run all unit tests (no database required).
test:
    cargo test --lib --all

# Run integration tests (requires Docker for Testcontainers).
test-integration:
    cargo test --test integration --package aqueduct-core -- --nocapture

# Run CLI integration tests (requires Docker for Testcontainers).
test-cli:
    cargo test --test cli_integration --package aqueduct-cli -- --nocapture

# Run all tests (unit + integration).
test-all:
    cargo test --all -- --nocapture

# ── Lint ────────────────────────────────────────────────────────────────────

# Check formatting.
fmt-check:
    cargo fmt --all -- --check

# Apply formatting.
fmt:
    cargo fmt --all

# Run Clippy linter.
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# ── Coverage ────────────────────────────────────────────────────────────────

# Run coverage with tarpaulin.
coverage:
    cargo tarpaulin --packages aqueduct-core --lib --out Html --timeout 120

# ── Clean ────────────────────────────────────────────────────────────────────

# Clean build artefacts.
clean:
    cargo clean

# ── Dev ─────────────────────────────────────────────────────────────────────

# Run the aqueduct binary in development mode.
run *args:
    cargo run --bin aqueduct -- {{args}}

# Show help for the aqueduct binary.
help:
    cargo run --bin aqueduct -- --help
