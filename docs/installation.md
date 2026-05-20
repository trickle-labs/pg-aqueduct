# Installation

`aqueduct` is distributed as a single static binary — no runtime dependencies, no PostgreSQL client libraries to install.

---

## Download a pre-built binary

Every tagged release publishes archives for four platforms to the [GitHub Releases page](https://github.com/trickle-labs/pg-aqueduct/releases).

| Platform | Archive |
|---|---|
| Linux x86-64 | `aqueduct-<version>-linux-amd64.tar.gz` |
| Linux ARM64 | `aqueduct-<version>-linux-arm64.tar.gz` |
| macOS Apple Silicon | `aqueduct-<version>-macos-arm64.tar.gz` |
| Windows x86-64 | `aqueduct-<version>-windows-amd64.zip` |

A `SHA256SUMS.txt` file is attached to each release so you can verify the download before use.

### Linux / macOS

```bash
# Replace <version> and <platform> as appropriate
VERSION=0.20.0
PLATFORM=linux-amd64   # or: linux-arm64, macos-arm64

curl -fsSL \
  "https://github.com/trickle-labs/pg-aqueduct/releases/download/v${VERSION}/aqueduct-${VERSION}-${PLATFORM}.tar.gz" \
  -o "aqueduct-${VERSION}-${PLATFORM}.tar.gz"

# Verify checksum (optional but recommended)
curl -fsSL \
  "https://github.com/trickle-labs/pg-aqueduct/releases/download/v${VERSION}/SHA256SUMS.txt" \
  | grep "${PLATFORM}" | sha256sum --check

tar xzf "aqueduct-${VERSION}-${PLATFORM}.tar.gz"
sudo mv "aqueduct-${VERSION}-${PLATFORM}/aqueduct" /usr/local/bin/
aqueduct --version
```

### Windows

Download `aqueduct-<version>-windows-amd64.zip` from the releases page, extract it, and move `aqueduct.exe` to a directory that is on your `%PATH%`.

---

## Build from source

You need the [Rust toolchain](https://rustup.rs/) (stable, 1.88 or later).

```bash
git clone https://github.com/trickle-labs/pg-aqueduct.git
cd pg-aqueduct
cargo build --release --bin aqueduct
# Binary is at target/release/aqueduct
```

---

## Verify the installation

```bash
aqueduct --version
# aqueduct 0.17.0
```

## Verify build provenance (v0.17+)

Starting from v0.17.0, each release carries a GitHub-native build attestation.
You can verify that the binary you downloaded was built from the official source:

```bash
# Requires the GitHub CLI (gh)
gh attestation verify "aqueduct-0.17.0-linux-amd64.tar.gz" \
  --repo trickle-labs/pg-aqueduct
```

Once installed, continue with the [5-Minute Tutorial](tutorial-5min.md) to connect `aqueduct` to your PostgreSQL instance and run your first migration plan.
