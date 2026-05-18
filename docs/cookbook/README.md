# pg_aqueduct Migration Cookbook

30 worked examples covering the most common stream-table evolution patterns.
Every example is verified end-to-end against a Testcontainers PostgreSQL cluster
(see `crates/aqueduct-core/tests/integration.rs`, `test_cookbook_*` functions).

## Pattern Categories

### Free class — metadata-only changes (no rebuild, no data movement)

| # | Pattern | Test |
|---|---------|------|
| [01](01-change-schedule-faster.md) | Change schedule to a faster interval | `test_cookbook_01_change_schedule_faster` |
| [02](02-change-schedule-slower.md) | Change schedule to a slower interval | `test_cookbook_02_change_schedule_slower` |
| [03](03-enable-cdc-mode.md) | Enable CDC mode (WAL replication) | `test_cookbook_03_enable_cdc_mode` |
| [04](04-diff-to-full-refresh.md) | Switch refresh mode DIFFERENTIAL → FULL | `test_cookbook_04_diff_to_full_refresh` |

### In-place class — additive changes; materialised state preserved

| # | Pattern | Test |
|---|---------|------|
| [05](05-add-sum-aggregate-column.md) | Add a SUM aggregate column | `test_cookbook_05_add_sum_aggregate_column` |
| [06](06-add-count-column.md) | Add a COUNT(*) column | `test_cookbook_06_add_count_column` |
| [07](07-drop-aggregate-column.md) | Drop a column from the SELECT list | `test_cookbook_07_drop_aggregate_column` |
| [08](08-add-aggregate-without-group-by-change.md) | Add an aggregate column without changing GROUP BY | `test_cookbook_08_add_passthrough_column` |
| [09](09-extend-select-with-new-aggregate.md) | Extend the SELECT list with a new aggregate | `test_cookbook_09_widen_column_type` |

### Rebuild class — structural changes; full DROP + recreate + backfill

| # | Pattern | Test |
|---|---------|------|
| [10](10-rename-column.md) | Rename a column | `test_cookbook_10_rename_column_is_rebuild` |
| [11](11-change-group-by-keys.md) | Change GROUP BY keys | `test_cookbook_11_change_group_by_is_rebuild` |
| [12](12-add-join.md) | Add a JOIN | `test_cookbook_12_add_join_is_rebuild` |
| [13](13-remove-join.md) | Remove a JOIN | `test_cookbook_13_remove_join_is_rebuild` |
| [14](14-change-join-condition.md) | Change a JOIN condition | `test_cookbook_14_change_join_condition_is_rebuild` |
| [15](15-add-where-predicate.md) | Add a WHERE predicate | `test_cookbook_15_add_where_predicate_is_rebuild` |
| [16](16-change-where-predicate.md) | Change a WHERE predicate | `test_cookbook_16_change_where_predicate_is_rebuild` |
| [17](17-full-to-diff-mode.md) | Switch refresh mode FULL → DIFFERENTIAL | `test_cookbook_17_full_to_diff_is_rebuild` |

### Create / Drop

| # | Pattern | Test |
|---|---------|------|
| [18](18-create-stream-table.md) | Create a new stream table | `test_cookbook_18_create_stream_table` |
| [19](19-drop-stream-table.md) | Drop an existing stream table | `test_cookbook_19_drop_stream_table` |

### DAG topology patterns

| # | Pattern | Test |
|---|---------|------|
| [20](20-two-node-dag.md) | Two-node dependency DAG (A → B) | `test_cookbook_20_two_node_dag` |
| [21](21-add-downstream-node.md) | Add a downstream dependent node | `test_cookbook_21_add_downstream_node` |
| [22](22-remove-downstream-node.md) | Remove a downstream node | `test_cookbook_22_remove_downstream_node` |
| [23](23-three-level-chain.md) | Three-level dependency chain | `test_cookbook_23_three_level_chain` |

### Consumer views

| # | Pattern | Test |
|---|---------|------|
| [24](24-create-consumer-view.md) | Create a consumer view | `test_cookbook_24_create_consumer_view` |
| [25](25-drop-consumer-view.md) | Drop a consumer view | `test_cookbook_25_drop_consumer_view` |

### Multi-table and advanced patterns

| # | Pattern | Test |
|---|---------|------|
| [26](26-source-column-cascade.md) | Source column change cascades to stream tables | `test_cookbook_26_source_column_cascade` |
| [27](27-multi-table-schedule-change.md) | Change schedule on multiple tables simultaneously | `test_cookbook_27_multi_table_schedule_change` |
| [28](28-import-roundtrip.md) | Import from live — idempotent round-trip | `test_cookbook_28_import_roundtrip` |
| [29](29-rollback-to-prior-state.md) | Rollback across version boundaries | `test_cookbook_29_rollback_to_prior_state` |
| [30](30-full-dag-lifecycle.md) | Full lifecycle: create → evolve → destroy | `test_cookbook_30_full_dag_lifecycle` |

## Classification Quick Reference

```
Change type                             Migration class   Cost
──────────────────────────────────────────────────────────────
Schedule change                         Free              < 1s
cdc_mode change                         Free              < 1s
refresh_mode DIFF → FULL               Free              < 1s
Add aggregate column (no GROUP BY Δ)   In-place          seconds
Drop column from SELECT                 In-place          seconds
Rename column                           Rebuild           minutes
Change GROUP BY keys                    Rebuild           minutes
Add / remove / change JOIN              Rebuild           minutes
Add / change WHERE predicate           Rebuild           minutes
refresh_mode FULL → DIFF               Rebuild           minutes
Create new stream table                 Create            seconds–minutes
Drop stream table                       Drop              < 1s
Blue/green structural DAG change        Blue/green        background
```
