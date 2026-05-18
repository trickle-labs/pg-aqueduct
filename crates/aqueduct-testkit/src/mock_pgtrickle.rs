/// Mock pg_trickle schema SQL.
/// This is installed on a vanilla PostgreSQL instance to simulate pg_trickle
/// for integration testing without needing the real pg_trickle extension.
pub const MOCK_PGTRICKLE_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS pgtrickle;

CREATE TABLE IF NOT EXISTS pgtrickle.pgt_stream_tables (
    id              bigserial PRIMARY KEY,
    schema_name     text NOT NULL,
    table_name      text NOT NULL,
    query           text NOT NULL DEFAULT '',
    refresh_mode    text NOT NULL DEFAULT 'DIFFERENTIAL',
    schedule        text NOT NULL DEFAULT '30s',
    cdc_mode        text,
    spec_jsonb      jsonb,
    UNIQUE(schema_name, table_name)
);

CREATE OR REPLACE FUNCTION pgtrickle.pgt_extension_version()
RETURNS text LANGUAGE SQL AS $$
    SELECT '0.1.0-mock'::text;
$$;

CREATE OR REPLACE FUNCTION pgtrickle.create_stream_table(
    p_schema_name text,
    p_table_name  text,
    p_query       text DEFAULT '',
    p_refresh_mode text DEFAULT 'DIFFERENTIAL',
    p_schedule    text DEFAULT '30s',
    p_cdc_mode    text DEFAULT NULL
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    -- Create the schema if needed.
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_schema_name);

    -- Create the table structure.
    IF p_query IS NOT NULL AND p_query != '' THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I AS SELECT * FROM (%s) _q LIMIT 0',
            p_schema_name, p_table_name, p_query
        );
    ELSE
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I (id bigint)',
            p_schema_name, p_table_name
        );
    END IF;

    -- Record in catalog.
    INSERT INTO pgtrickle.pgt_stream_tables
        (schema_name, table_name, query, refresh_mode, schedule, cdc_mode)
    VALUES (p_schema_name, p_table_name, COALESCE(p_query, ''), p_refresh_mode, p_schedule, p_cdc_mode)
    ON CONFLICT (schema_name, table_name) DO UPDATE
    SET query        = EXCLUDED.query,
        refresh_mode = EXCLUDED.refresh_mode,
        schedule     = EXCLUDED.schedule,
        cdc_mode     = EXCLUDED.cdc_mode;
END;
$$;

CREATE OR REPLACE FUNCTION pgtrickle.drop_stream_table(
    p_schema_name text,
    p_table_name  text
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('DROP TABLE IF EXISTS %I.%I', p_schema_name, p_table_name);
    DELETE FROM pgtrickle.pgt_stream_tables
    WHERE schema_name = p_schema_name AND table_name = p_table_name;
END;
$$;

CREATE OR REPLACE FUNCTION pgtrickle.alter_stream_table(
    p_schema_name  text,
    p_table_name   text,
    p_schedule     text DEFAULT NULL,
    p_refresh_mode text DEFAULT NULL,
    p_cdc_mode     text DEFAULT NULL
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE pgtrickle.pgt_stream_tables
    SET
        schedule     = COALESCE(p_schedule, schedule),
        refresh_mode = COALESCE(p_refresh_mode, refresh_mode),
        cdc_mode     = COALESCE(p_cdc_mode, cdc_mode)
    WHERE schema_name = p_schema_name AND table_name = p_table_name;
END;
$$;

CREATE OR REPLACE FUNCTION pgtrickle.pause_scheduler(nodes text[] DEFAULT NULL)
RETURNS void LANGUAGE SQL AS $$
    SELECT 1 WHERE false;
$$;

CREATE OR REPLACE FUNCTION pgtrickle.resume_scheduler(nodes text[] DEFAULT NULL)
RETURNS void LANGUAGE SQL AS $$
    SELECT 1 WHERE false;
$$;
"#;
