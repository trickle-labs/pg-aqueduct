# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in `pg_aqueduct`, please **do not** open a
public GitHub issue. Instead, report it privately so we can assess and fix it before
any public disclosure.

**Contact:** security@trickle-labs.io

**Response SLA:**
- Acknowledgement within **2 business days**.
- Initial assessment and severity classification within **5 business days**.
- A patch or mitigation plan communicated within **14 business days** for critical or
  high severity findings.

**Coordinated disclosure:** We follow responsible / coordinated disclosure. Once a fix
is available, we will coordinate a public advisory with the reporter and publish a
`GHSA` advisory on GitHub.

## Supported Versions

| Version | Supported |
|---------|-----------|
| Latest release (0.18.x) | ✓ Security patches |
| Previous release (0.17.x) | Critical fixes only |
| Older releases | Not supported |

## Implemented Security Controls

For a full description of the security controls built into `pg_aqueduct`, see
[docs/security.md](../docs/security.md). Key controls include:

- **Consumer SQL validation:** every consumer view body is validated to be a single
  `SELECT` statement before any database call is made (SEC-1).
- **TLS validation:** the CNPG preview client validates TLS certificates by default;
  insecure mode requires explicit opt-in via `CNPG_INSECURE_SKIP_VERIFY` (SEC-2).
- **Secret path validation:** `age` and `sops` backend paths are validated against
  path-traversal and flag-injection before being passed to subprocesses (SEC-3).
- **Read-only transactions:** `plan`, `status`, and `validate` operate in
  `READ ONLY` transactions.
- **Plaintext password guard:** the CLI rejects DSNs with plaintext passwords unless
  `--allow-plaintext-password` is explicitly passed.
- **Least-privilege role:** `aqueduct_admin` requires no `SUPERUSER` or `CREATEROLE`
  privileges.
