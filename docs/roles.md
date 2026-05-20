# Role SQL Templates

Copy and customise these templates for your environment. Replace `<schema>` with
each schema that contains stream tables or consumer views.

These roles follow the principle of least privilege. Each role is scoped to
exactly the operations it needs to perform.

## aqueduct_planner

Read-only access: runs `aqueduct plan`, `aqueduct status`, `aqueduct diff`.
Cannot write to any table.

```sql
CREATE ROLE aqueduct_planner LOGIN PASSWORD 'CHANGE_ME' NOSUPERUSER NOCREATEDB NOCREATEROLE;

GRANT USAGE ON SCHEMA aqueduct TO aqueduct_planner;
GRANT SELECT ON ALL TABLES IN SCHEMA aqueduct TO aqueduct_planner;
GRANT SELECT ON ALL TABLES IN SCHEMA pgtrickle TO aqueduct_planner;
GRANT USAGE ON SCHEMA pgtrickle TO aqueduct_planner;

-- Read access to system catalogs (for live-state queries).
GRANT SELECT ON pg_catalog.pg_class TO aqueduct_planner;
GRANT SELECT ON pg_catalog.pg_attribute TO aqueduct_planner;
GRANT SELECT ON pg_catalog.pg_type TO aqueduct_planner;
GRANT SELECT ON pg_catalog.pg_namespace TO aqueduct_planner;

-- Read access to the stream-table schemas (for query validation).
-- Repeat for each schema that contains stream tables:
GRANT USAGE ON SCHEMA <schema> TO aqueduct_planner;
GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO aqueduct_planner;
```

## aqueduct_applier

Write access: runs `aqueduct apply`, `aqueduct rollback`, `aqueduct promote`.
Can call pgtrickle DDL functions and write to the aqueduct catalog.

```sql
CREATE ROLE aqueduct_applier LOGIN PASSWORD 'CHANGE_ME' NOSUPERUSER NOCREATEDB NOCREATEROLE;

GRANT USAGE ON SCHEMA aqueduct TO aqueduct_applier;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA aqueduct TO aqueduct_applier;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA aqueduct TO aqueduct_applier;
GRANT USAGE ON SCHEMA pgtrickle TO aqueduct_applier;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle TO aqueduct_applier;

-- DDL rights on the stream-table schemas.
-- Repeat for each schema that contains stream tables:
GRANT USAGE, CREATE ON SCHEMA <schema> TO aqueduct_applier;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA <schema> TO aqueduct_applier;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA <schema> TO aqueduct_applier;
```

## aqueduct_preview

Preview access: runs `aqueduct preview` in a sandboxed schema.
Can create/drop objects in the preview schema only.

```sql
CREATE ROLE aqueduct_preview LOGIN PASSWORD 'CHANGE_ME' NOSUPERUSER NOCREATEDB NOCREATEROLE;

GRANT USAGE ON SCHEMA aqueduct TO aqueduct_preview;
GRANT SELECT ON ALL TABLES IN SCHEMA aqueduct TO aqueduct_preview;
GRANT USAGE ON SCHEMA pgtrickle TO aqueduct_preview;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle TO aqueduct_preview;

-- Grant full access to the preview schema (created by `aqueduct preview`).
-- Replace <preview_schema> with the actual schema used (default: aqueduct_preview).
GRANT USAGE, CREATE ON SCHEMA <preview_schema> TO aqueduct_preview;
GRANT ALL ON ALL TABLES IN SCHEMA <preview_schema> TO aqueduct_preview;
```

## aqueduct_destroy

Destroy access: runs `aqueduct destroy` to tear down the full project DAG.
This role should be tightly guarded. Consider requiring 2FA or approvals.

```sql
CREATE ROLE aqueduct_destroy LOGIN PASSWORD 'CHANGE_ME' NOSUPERUSER NOCREATEDB NOCREATEROLE;

GRANT USAGE ON SCHEMA aqueduct TO aqueduct_destroy;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA aqueduct TO aqueduct_destroy;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA aqueduct TO aqueduct_destroy;
GRANT USAGE ON SCHEMA pgtrickle TO aqueduct_destroy;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle TO aqueduct_destroy;

-- DDL rights on the stream-table schemas (for DROP TABLE / DROP VIEW).
-- Repeat for each schema that contains stream tables:
GRANT USAGE, CREATE ON SCHEMA <schema> TO aqueduct_destroy;
GRANT ALL ON ALL TABLES IN SCHEMA <schema> TO aqueduct_destroy;
```
