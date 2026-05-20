# Security Guide

## Overview

`pg_aqueduct` is designed to operate without superuser privileges and with a minimal
footprint on the target PostgreSQL cluster.

## Least-privilege role

Create a dedicated `aqueduct_admin` role for all `aqueduct` operations:

```sql
CREATE ROLE aqueduct_admin LOGIN PASSWORD '...';

-- Access to the aqueduct catalog schema.
GRANT USAGE, CREATE ON SCHEMA aqueduct TO aqueduct_admin;

-- Access to pg_trickle functions.
GRANT USAGE ON SCHEMA pgtrickle TO aqueduct_admin;

-- Read access to system catalogs (for live-state queries).
GRANT SELECT ON pg_catalog.pg_class TO aqueduct_admin;
GRANT SELECT ON pg_catalog.pg_attribute TO aqueduct_admin;
GRANT SELECT ON pg_catalog.pg_type TO aqueduct_admin;
GRANT SELECT ON pg_catalog.pg_namespace TO aqueduct_admin;

-- DDL rights on the stream-table schemas only.
GRANT CREATE ON SCHEMA public TO aqueduct_admin;
-- (add each schema where stream tables live)
```

The role requires:
- **No** `SUPERUSER`
- **No** `CREATEROLE`
- **No** replication privilege

## Connection string security

### Environment variables (default)

```toml
[targets.prod]
dsn = "${AQUEDUCT_PROD_DSN}"
```

`pg_aqueduct` expands `${VAR}` at runtime. The DSN is never written to disk or logged.

### Plaintext passwords

The CLI refuses plaintext passwords in config files:

```text
Error: DSN contains a plaintext password. Use ${ENV_VAR} or a secret backend.
       Pass --allow-plaintext-password to override (not recommended in CI).
```

### Secret backends

Use the `${secret:BACKEND:KEY}` inline syntax to fetch credentials from an external
secret store at apply time:

```toml
[targets.prod]
dsn = "postgresql://app:${secret:vault:database/prod-dsn}@db.example.com/app"
```

Supported backends:

| Backend | Flag value | Status | Credential source |
|---------|-----------|--------|------------------|
| Environment variable | `env` | **Implemented** | `${VAR}` (default) |
| SOPS-encrypted file | `sops` | **Implemented** | `sops -d <file>` subprocess |
| age-encrypted file | `age` | **Implemented** | `age -d -i <identity> <file>` subprocess |
| AWS Secrets Manager | `aws` | **Implemented** | `AWS_REGION` + SDK credentials |
| GCP Secret Manager | `gcp` | **Implemented** | `GOOGLE_APPLICATION_CREDENTIALS` |
| HashiCorp Vault | `vault` | **Implemented** | `VAULT_ADDR` + `VAULT_TOKEN` |

> **Note:** The `aws`, `gcp`, and `vault` backends are implemented as of v0.12.
> Each backend resolves credentials at apply time and never writes them to disk.

## Read-only transactions

`aqueduct plan`, `aqueduct status`, and `aqueduct validate` open read-only transactions
(`SET TRANSACTION READ ONLY`). These commands **cannot** accidentally mutate data.

## Audit trail

Every `aqueduct apply` records the following in `aqueduct.migrations`:

- Authenticated PostgreSQL role (`current_role`)
- Client IP address (`inet_client_addr()`)
- CLI version
- Full plan (all steps)
- Start and finish timestamps

Query the audit trail:

```sql
SELECT started_at, applied_by, cli_version, status
FROM aqueduct.migrations
ORDER BY started_at DESC
LIMIT 20;
```

## HA security

`aqueduct apply` refuses to run against a hot standby:

```sql
SELECT pg_is_in_recovery();   -- returns true on a standby
```

If the result is `true`, the CLI exits non-zero with a clear error:
`Error: target is a hot standby — apply requires a primary.`

For Patroni clusters, `aqueduct` performs a synchronous HTTP check against the Patroni
REST endpoint (`GET /master`) before applying. Pass `--patroni-endpoint <url>`.

## OWASP considerations

- **Injection:** All database queries use parameterised statements (`$1`, `$2`, ...). SQL
  identifiers are quoted with `format('%I', name)` in PostgreSQL functions. The Rust
  layer does not perform string interpolation into SQL; explicit trust boundaries for
  identifier construction were introduced in v0.14 (see `CatalogSchema` newtype and
  the `catalog.rs` query builder).
- **Consumer SQL injection (SEC-1):** Consumer view bodies are validated as a single
  `SELECT` statement by `validate_consumer_sql_is_single_select()` before any
  `CREATE OR REPLACE VIEW` is executed. Multi-statement bodies and bare DDL are rejected
  with `AqueductError::UntrustedSqlBody` before any database call is made.
- **Sensitive data exposure:** DSNs containing passwords are masked in log output. Secret
  values are never echoed or stored.
- **Misconfiguration:** `aqueduct lint` warns about overly permissive configurations
  (e.g., `allow_full_refresh = true` in a production target).

## CNPG preview TLS

When using `aqueduct preview` with CloudNativePG, the CLI validates TLS certificates
on all Kubernetes API calls by default. To connect to a development cluster with a
self-signed certificate, set the **development-only** escape hatch:

```bash
export CNPG_INSECURE_SKIP_VERIFY=1
aqueduct preview create --branch feat/my-branch
```

> **Warning:** Never set `CNPG_INSECURE_SKIP_VERIFY` in production. Disabling TLS
> verification exposes the connection to man-in-the-middle attacks.

## Age identity file security

The `age` secret backend validates both the encrypted-file path (`key`) and the
identity file path (`AGE_KEY_FILE` / `SOPS_AGE_KEY_FILE`) before passing them to
the `age` subprocess:

1. **Path traversal:** paths containing `..` components are rejected with
   `AqueductError::InvalidSecretPath`.
2. **Flag injection:** identity file paths starting with `-` are rejected to prevent
   the value from being interpreted as a CLI flag by the `age` binary.
