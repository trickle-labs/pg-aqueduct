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

# ── Docs ─────────────────────────────────────────────────────────────────────

# Build the mdBook documentation site.
docs-build:
    mdbook build

# Test mdBook code blocks.
docs-test:
    mdbook test

# Generate CLI reference from --help output (U-01/D-02).
# Writes a Markdown summary of every subcommand's --help to stdout.
# Compare against docs/api-reference.md to detect drift.
docs-cli:
    #!/usr/bin/env bash
    set -euo pipefail
    BIN="./target/debug/aqueduct"
    if [ ! -f "$BIN" ]; then cargo build --bin aqueduct; fi
    echo "# CLI Reference (generated)"
    echo ""
    echo "Binary version: $($BIN --version)"
    echo ""
    for cmd in plan apply diff status validate lint import rollback promote destroy unlock init preview ingest fmt; do
        echo "## aqueduct $cmd"
        echo ""
        echo '```'
        $BIN "$cmd" --help 2>&1 || true
        echo '```'
        echo ""
    done

# Generate docs/api-reference.md from aqueduct --help (ERG-4).
# Run this after any CLI flag change to keep the reference in sync.
# CI verifies the committed file matches this output.
gen-docs:
    #!/usr/bin/env bash
    set -euo pipefail
    BIN="./target/debug/aqueduct"
    if [ ! -f "$BIN" ]; then cargo build --bin aqueduct; fi
    {
        echo "# API Reference"
        echo ""
        echo "> **Auto-generated** from \`aqueduct --help\` output."
        echo "> Do not edit by hand — run \`just gen-docs\` to regenerate."
        echo ""
        echo "Binary version: \`$($BIN --version | head -1)\`"
        echo ""
        for cmd in plan apply diff status validate lint import rollback promote destroy unlock init preview ingest fmt; do
            echo "## aqueduct $cmd"
            echo ""
            echo '```text'
            $BIN "$cmd" --help 2>&1 || true
            echo '```'
            echo ""
        done
    } > docs/api-reference.md
    echo "docs/api-reference.md updated."

# ── Dev ─────────────────────────────────────────────────────────────────────

# Run the aqueduct binary in development mode.
run *args:
    cargo run --bin aqueduct -- {{args}}

# Show help for the aqueduct binary.
help:
    cargo run --bin aqueduct -- --help
