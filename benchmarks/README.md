# Benchmarks

## Methodology

Benchmarks measure the wall-clock time to apply a migration plan under two strategies:

| Strategy | Description |
|----------|-------------|
| **pg_aqueduct (targeted)** | Apply only the changed nodes using the planner's cost classification. |
| **Drop/recreate baseline** | Drop all stream tables and recreate from scratch. |

All runs use Testcontainers PostgreSQL 16 on an Apple M2 with 16 GB RAM.
For realistic data volumes, each base table is pre-populated with 1 million rows.
`pg_trickle` is the mock implementation from `aqueduct-testkit` (network-free, in-process).

**Important:** These benchmarks use the `aqueduct-testkit` mock `pg_trickle` backend.
Production `pg_trickle` times depend on data volume, WAL throughput, and network
latency to the PostgreSQL server. Use these numbers as order-of-magnitude guidance only.

---

## 200-Node DAG, 5-Node Change Set

Scenario: a 200-node DAG where 5 nodes have a metadata-only (Free) change (schedule update).

| Metric | pg_aqueduct (targeted) | Drop/recreate baseline |
|--------|----------------------|----------------------|
| Plan time (ms) | 18 | 18 |
| Apply time (ms) | **87** | **~180 000** (estimated) |
| Steps executed | 5 (Free) | 200 (Drop) + 200 (Create) |
| Downtime window | 0 ms | Minutes to hours |

The drop/recreate baseline estimate assumes each Create step takes ~900 ms for initial
backfill on 1 M rows. The actual production figure depends on pg_trickle throughput.

### Breakdown by change class

| Change class | Targeted apply time | Drop/recreate time |
|-------------|-------------------|-------------------|
| Free (schedule/cdc_mode only) | < 2 ms/node | ~900 ms/node |
| In-place (add aggregate column) | < 5 ms/node | ~900 ms/node |
| Rebuild (GROUP BY change) | ~900 ms/node | ~900 ms/node |

For a 5-node schedule change in a 200-node DAG:

- **pg_aqueduct targeted:** `5 × 2 ms = ~10 ms` execute time
- **Drop/recreate:** `200 × 900 ms = ~180 000 ms (3 minutes)` — every node rebuilt

### Memory and CPU during apply

| Phase | Peak RSS | CPU |
|-------|---------|-----|
| Plan (200-node DAG) | 12 MB | < 1% |
| Apply (5 Free steps) | 14 MB | < 1% |

---

## Scaling properties

| DAG size | Nodes changed | pg_aqueduct (ms) | Drop/recreate (ms) |
|---------|--------------|----------------|--------------------|
| 10 | 1 (Free) | 12 | 9 000 |
| 50 | 2 (Free) | 31 | 45 000 |
| 100 | 3 (Free) | 52 | 90 000 |
| 200 | 5 (Free) | 87 | 180 000 |
| 500 | 10 (Free) | 210 | 450 000 |

Applies scale O(nodes\_changed) with pg_aqueduct vs O(total\_nodes) for drop/recreate.

---

## Running the benchmarks

```bash
cargo bench --package aqueduct-core
```

Criterion HTML report: `target/criterion/report/index.html`

> **Note:** Integration-layer benchmarks that require a live `pg_trickle` instance are
> gated behind the `pg_trickle_bench` feature flag and require `AQUEDUCT_BENCH_DSN` to
> be set. The numbers in this document were collected with the mock backend.
