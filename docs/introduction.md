# pg_aqueduct

**Declarative schema evolution and migration for stream-table DAGs.**

`pg_aqueduct` is the missing migration tool for teams running
[`pg_trickle`](https://github.com/trickle-labs/pg-trickle) in production.
Where Atlas manages relational schema and Terraform manages infrastructure,
`pg_aqueduct` manages the third axis that neither tool covers: the *evolution
of a streaming, incrementally-maintained DAG of materialized views* over time —
without losing differential state, without taking the pipeline offline, and
without making the topology of your stream tables your problem to figure out by hand.

> **Status:** v0.18.0 — Security Hardening, Documentation Correctness & Operational Quality.

---

## The Problem

If you operate a `pg_trickle` deployment, your stream-table DAG is your most
valuable database asset. It is also the hardest one to evolve safely. Adding a
column to a base table, changing an aggregation, splitting a node into two — any
of these changes today requires a `drop_stream_table()` followed by a full
recreate, which discards the materialized rows, triggers an expensive full
refresh that can take minutes or hours on a large table, and forces every
downstream stream table to also be recreated in exactly the right topological
order.

For a small five-node DAG this is an inconvenience. For a 200-node production
DAG it is an outage.

`pg_aqueduct` solves this by classifying every change into the cheapest
applicable migration class — and only doing as much work as the change actually
requires.

| Class | Example | Cost |
|-------|---------|------|
| **Free** | Schedule change, CDC mode change | < 1 s, zero downtime |
| **In-place** | Add aggregate column (same GROUP BY) | Seconds, zero downtime |
| **Rebuild** | Change GROUP BY keys, change a JOIN | Minutes (maintenance window) |
| **Blue/green** | Restructure sub-DAG topology | Parallel green DAG + atomic swap |

---

## Quick Start

```sh
aqueduct init                   # scaffold a project and bootstrap the catalog
aqueduct import --from prod     # bootstrap from an existing pg_trickle deployment
aqueduct plan --to prod         # diff desired vs actual; produce a readable plan
aqueduct apply --to prod        # execute the plan and record the migration
aqueduct status --to prod       # show drift, last migration, refresh lag
aqueduct rollback --to prod     # revert to the previous DAG version
```

---

## What's in This Book

- **[API Reference](api-reference.md)** — CLI commands, front-matter directives, `aqueduct.toml`
- **[Migration Cookbook](cookbook/README.md)** — 30 worked patterns, one per common change type
- **[Security Guide](security.md)** — Least-privilege role, secret backends, audit trail
- **[HA Operations](ha-operations.md)** — Patroni, CloudNativePG, maintenance windows

Source code: [trickle-labs/pg-aqueduct](https://github.com/trickle-labs/pg-aqueduct)  
License: [Apache 2.0](https://github.com/trickle-labs/pg-aqueduct/blob/main/LICENSE)
