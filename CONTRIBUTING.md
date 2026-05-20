# Contributing to pg-aqueduct

Thank you for your interest in contributing to **pg-aqueduct**. This guide covers
everything you need to set up a development environment, run the tests, and submit
changes.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick start](#quick-start)
3. [Development workflow](#development-workflow)
4. [Running tests](#running-tests)
5. [Running against a specific PostgreSQL version](#running-against-a-specific-postgresql-version)
6. [Adding a new cookbook recipe](#adding-a-new-cookbook-recipe)
7. [Coding standards](#coding-standards)
8. [Submitting a pull request](#submitting-a-pull-request)

---

## Prerequisites

| Tool | Minimum version | Install |
|------|----------------|---------|
| Rust toolchain | 1.88 (see `rust-version` in `Cargo.toml`) | `rustup` |
| Docker Desktop (or compatible runtime) | any recent stable | [docker.com](https://www.docker.com) |
| `just` | any recent stable | `brew install just` / `cargo install just` |
| `mdbook` (optional, for docs) | 0.4 | `cargo install mdbook` |

### Docker / Testcontainers

Integration tests use [Testcontainers](https://testcontainers.com/) to spin up a
PostgreSQL container on-the-fly. Docker must be running locally.

If you use Colima, Rancher Desktop, or Podman Desktop, ensure the Docker socket is
exposed at the default path (`/var/run/docker.sock` on Linux/macOS).

**Environment variables:**

| Variable | Purpose | Default |
|----------|---------|---------|
| `AQUEDUCT_TEST_PG_IMAGE` | Override the PostgreSQL Docker image | `postgres:18-alpine` |
| `AQUEDUCT_TEST_DSN` | Pre-existing DSN to use instead of Testcontainers | _(unset)_ |

---

## Quick start

```bash
git clone https://github.com/trickle-labs/pg-aqueduct.git
cd pg-aqueduct
just build           # compile all crates
just test            # run unit tests (no Docker required)
just test-cli        # run CLI integration tests (requires Docker)
just test-all        # run everything
```

---

## Development workflow

```bash
# Format code
just fmt

# Lint (clippy)
just lint

# Check formatting without modifying files
just fmt-check

# Build the release binary
just build-release

# Regenerate docs/api-reference.md from --help output
just gen-docs

# Run the binary in development mode
just run -- plan --help
```

---

## Running tests

### Unit tests (no database required)

```bash
cargo test --lib --all
```

### Integration tests for `aqueduct-core` (requires Docker)

```bash
cargo test --test integration --package aqueduct-core -- --nocapture
```

### Integration tests for `aqueduct-cli` (requires Docker)

```bash
cargo test --test cli_integration --package aqueduct-cli -- --nocapture
```

### All tests

```bash
just test-all
```

---

## Running against a specific PostgreSQL version

By default, integration tests target `postgres:18-alpine` (the minimum version
supported by pg_trickle). To test against a newer release:

```bash
AQUEDUCT_TEST_PG_IMAGE=postgres:19-alpine cargo test --test cli_integration --package aqueduct-cli
```

The CI matrix currently validates against PostgreSQL 18+. Additional versions may be
added to `.gitlab-ci.yml` as they become available.

---

## Adding a new cookbook recipe

Cookbook recipes live in `docs/cookbook/`. Each recipe is a numbered Markdown file that
documents a specific migration scenario end-to-end.

1. Pick the next available number (e.g., `31-my-scenario.md`).
2. Copy the structure from an existing recipe (e.g.,
   [01-change-schedule-faster.md](docs/cookbook/01-change-schedule-faster.md)).
3. Add your recipe to `docs/SUMMARY.md` under the `Cookbook` section.
4. Add a corresponding integration test in
   `crates/aqueduct-cli/tests/cli_integration.rs` that verifies the scenario works
   against a live database.
5. Run `just test-cli` to confirm the test passes.

---

## Coding standards

- **Rust edition:** 2021.
- **Error handling:** use `anyhow::Result` in application code; `thiserror` in library
  code.
- **No `unwrap()` in library code** unless the invariant is proven by construction.
  Use `expect("reason")` when a panic is truly impossible.
- **Parameterised SQL:** never interpolate identifiers or values into SQL strings.
  Use `$1`, `$2`, … for values and the `CatalogSchema` newtype for validated identifiers.
- **Tests:** every new public function in `aqueduct-core` must have a unit test.
  Every new CLI flag or exit code must have an `assert_cmd` binary test.
- **YAML output:** use `serde_yaml::to_string(&struct)` — never hand-roll YAML strings.
- **Clippy:** `cargo clippy -- -D warnings` must pass with no new warnings.
- **Format:** `cargo fmt --all` must produce no diff.

---

## Submitting a pull request

1. Fork the repository and create a feature branch from `main`.
2. Make your changes and add tests.
3. Run `just fmt lint test-all` locally and fix any failures.
4. If you changed any CLI flags, run `just gen-docs` to regenerate
   `docs/api-reference.md` and commit it.
5. Update `CHANGELOG.md` under `[Unreleased]` following the
   [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.
6. Open a merge request. CI must pass before review.

CI runs on every MR:
- `cargo fmt --check` (formatting)
- `cargo clippy` (linting)
- `cargo test --lib --all` (unit tests)
- `cargo test --test cli_integration` (integration tests on PG 18)
