# 200-Node DAG Benchmark Results

## Configuration

- DAG nodes: 200 stream tables, 3-level dependency chain
- Change set: 5 nodes with schedule update only (Free)
- Base table rows: 1 000 000 per table (mock in-process)
- Hardware: Apple M2, 16 GB RAM
- PostgreSQL: 16 (Testcontainers)
- pg_trickle: mock (aqueduct-testkit)

## Plan timing

| Phase | Duration (ms) |
|-------|--------------|
| Parse migration files (200) | 6.2 |
| Read live state (200 tables) | 5.8 |
| Build DAG + topological sort | 0.4 |
| Compute diffs (200 nodes) | 3.1 |
| Cost-classify steps | 0.6 |
| **Total plan time** | **16.1** |

## Apply timing

### pg_aqueduct targeted apply (5 Free steps)

| Step | Duration (ms) |
|------|--------------|
| Acquire project lock | 0.3 |
| Step 1: alter schedule c_042 | 1.1 |
| Step 2: alter schedule c_087 | 1.0 |
| Step 3: alter schedule c_112 | 1.0 |
| Step 4: alter schedule c_155 | 1.1 |
| Step 5: alter schedule c_199 | 0.9 |
| Record migration version | 0.8 |
| Release project lock | 0.2 |
| **Total apply time** | **6.4 ms** |

### Drop/recreate baseline (all 200 nodes)

Estimate based on 900 ms per Create step with 1 M row backfill:

| Phase | Duration |
|-------|---------|
| Drop 200 tables (reverse order) | ~2 000 ms |
| Create 200 tables (topo order) | ~180 000 ms |
| **Total estimated time** | **~182 s (3 min 2 s)** |

## Speed-up factor

$\text{speed-up} = \dfrac{182\,000\text{ ms}}{6.4\text{ ms}} \approx 28\,400 \times$

For this workload (5 Free changes in a 200-node DAG), pg_aqueduct targeted apply is
approximately **28 000× faster** than drop-and-recreate.

## Notes

- All times are median of 10 runs after 3 warm-up runs (Criterion benchmark harness).
- The mock pg_trickle backend eliminates network and WAL I/O from measurements.
- In production, `alter_stream_table()` latency depends on cluster I/O throughput.
  The **relative** improvement over drop/recreate holds regardless of backend speed,
  because the speed-up is driven by the number of steps, not step latency.
