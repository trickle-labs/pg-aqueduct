# Pattern 28 — Import from Live — Idempotent Round-Trip

**Test:** `test_cookbook_28_import_roundtrip`

## When to use

Bootstrap a `pg_aqueduct` project from an existing live `pg_trickle` deployment: generate canonical migration files that match the current live state, then confirm that running `aqueduct plan` produces an empty (no-op) plan.

## Commands

```bash
# Generate migration files from the live database.
aqueduct import --from prod --output ./my-project/

# Confirm the round-trip: plan should be empty.
cd my-project
aqueduct plan --to prod
```

## What import generates

For each stream table in `pgtrickle.pgt_stream_tables`, `aqueduct import` creates:

- `migrations/streams/{table_name}.sql` — SQL body + front-matter directives
- `migrations/sources/{source_name}.sql` — for referenced base tables (`owned = false`)
- `aqueduct.toml` — skeleton project config

## Idempotency guarantee

Running `aqueduct import` a second time with no live changes produces no file changes. The generated files always round-trip to an empty `aqueduct plan`.

## Notes

- Use `--exclude-pattern '_pg_ripple.*'` and `--exclude-pattern '_pg_eddy.*'` to skip internal extension tables (applied by default).
- After import, commit the generated files and run `aqueduct init` to record the baseline version.
