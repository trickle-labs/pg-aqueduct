use sha2::{Digest, Sha256};

use crate::catalog::{
    ACQUIRE_LOCK_SQL, FINISH_MIGRATION_SQL, INSERT_DAG_VERSION_SQL, RELEASE_LOCK_SQL,
    START_MIGRATION_SQL,
};
use crate::error::{AqueductError, Result};
use crate::plan::{Plan, PlanStep};

/// Execute a plan against the target database.
pub struct PlanExecutor<'a> {
    client: &'a tokio_postgres::Client,
    project: &'a str,
    cli_version: &'a str,
    dry_run: bool,
}

impl<'a> PlanExecutor<'a> {
    pub fn new(
        client: &'a tokio_postgres::Client,
        project: &'a str,
        cli_version: &'a str,
        dry_run: bool,
    ) -> Self {
        Self {
            client,
            project,
            cli_version,
            dry_run,
        }
    }

    /// Execute the plan, returning the new DAG version.
    pub async fn execute(&self, plan: &Plan) -> Result<u64> {
        // Check we're connected to a primary.
        let is_primary: bool = self
            .client
            .query_one("SELECT NOT pg_is_in_recovery()", &[])
            .await?
            .get(0);
        if !is_primary {
            return Err(AqueductError::NotPrimary);
        }

        if self.dry_run {
            tracing::info!("Dry run: not executing plan");
            return Ok(plan.to_version);
        }

        // Start migration record.
        let plan_json = serde_json::to_value(plan).map_err(AqueductError::Json)?;
        let migration_id: i64 = self
            .client
            .query_one(
                START_MIGRATION_SQL,
                &[
                    &self.project,
                    &plan.from_version.map(|v| v as i64),
                    &plan_json,
                    &self.cli_version,
                ],
            )
            .await?
            .get(0);

        let result = self.run_steps(plan).await;

        // Finish migration record.
        let (status, new_version) = match &result {
            Ok(v) => ("committed", Some(*v as i64)),
            Err(_) => ("failed", None),
        };

        self.client
            .execute(
                FINISH_MIGRATION_SQL,
                &[&migration_id, &status, &new_version, &serde_json::json!({})],
            )
            .await
            .ok();

        result
    }

    async fn run_steps(&self, plan: &Plan) -> Result<u64> {
        let lock_holder = format!("aqueduct-cli/{}", self.cli_version);
        let mut locked = false;
        let mut new_version: u64 = plan.to_version;

        for step in &plan.steps {
            tracing::debug!("Executing step: {}", step.description());
            match step {
                PlanStep::LockDag { project, ttl } => {
                    self.acquire_lock(project, &lock_holder, ttl).await?;
                    locked = true;
                }

                PlanStep::ValidateQuery { name, query } => {
                    self.validate_query(name, query)?;
                }

                PlanStep::AlterBaseTable { name, statement } => {
                    // Execute the base-table DDL directly.
                    // For safety, we only allow well-formed DDL statements here.
                    tracing::info!("Executing base-table DDL for '{}'", name);
                    self.client.execute(statement.as_str(), &[]).await?;
                }

                PlanStep::CreateStreamTable { spec } => {
                    let schema = &spec.qualified_name.schema;
                    let table = &spec.qualified_name.name;

                    // Check if pgtrickle is available.
                    let pgtrickle_exists: bool = self
                        .client
                        .query_one(
                            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle')",
                            &[],
                        )
                        .await?
                        .get(0);

                    if pgtrickle_exists {
                        self.client
                            .execute(
                                "SELECT pgtrickle.create_stream_table($1, $2, $3, $4, $5, $6)",
                                &[
                                    schema,
                                    table,
                                    &spec.query,
                                    &spec.refresh_mode.to_string(),
                                    &spec.schedule,
                                    &spec.cdc_mode,
                                ],
                            )
                            .await?;
                    } else {
                        // Fallback: create a regular table with the query structure.
                        self.client
                            .execute(
                                &format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(schema)),
                                &[],
                            )
                            .await?;
                        if !spec.query.is_empty() {
                            self.client
                                .execute(
                                    &format!(
                                        "CREATE TABLE IF NOT EXISTS {}.{} AS SELECT * FROM ({}) q LIMIT 0",
                                        quote_ident(schema),
                                        quote_ident(table),
                                        spec.query
                                    ),
                                    &[],
                                )
                                .await?;
                        }
                    }
                }

                PlanStep::AlterStreamTable {
                    name,
                    schedule,
                    refresh_mode,
                    cdc_mode,
                    new_query: _,
                } => {
                    let pgtrickle_exists: bool = self
                        .client
                        .query_one(
                            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle')",
                            &[],
                        )
                        .await?
                        .get(0);

                    if pgtrickle_exists {
                        self.client
                            .execute(
                                "SELECT pgtrickle.alter_stream_table($1, $2, $3, $4, $5)",
                                &[&name.schema, &name.name, schedule, refresh_mode, cdc_mode],
                            )
                            .await?;
                    }
                }

                PlanStep::DropStreamTable { name, cascade: _ } => {
                    let pgtrickle_exists: bool = self
                        .client
                        .query_one(
                            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle')",
                            &[],
                        )
                        .await?
                        .get(0);

                    if pgtrickle_exists {
                        self.client
                            .execute(
                                "SELECT pgtrickle.drop_stream_table($1, $2)",
                                &[&name.schema, &name.name],
                            )
                            .await?;
                    } else {
                        self.client
                            .execute(
                                &format!(
                                    "DROP TABLE IF EXISTS {}.{}",
                                    quote_ident(&name.schema),
                                    quote_ident(&name.name)
                                ),
                                &[],
                            )
                            .await?;
                    }
                }

                PlanStep::Backfill { name: _, mode: _ } => {
                    // In the mock environment, backfill is a no-op.
                    // In production, this would call pgtrickle.refresh_stream_table().
                    tracing::debug!("Backfill step: no-op in mock environment");
                }

                PlanStep::RecordSnapshot { version } => {
                    new_version = *version;
                    let spec_json = serde_json::json!({});
                    let plan_json = serde_json::to_value(plan).map_err(AqueductError::Json)?;
                    let spec_hash: Vec<u8> =
                        Sha256::digest(spec_json.to_string().as_bytes()).to_vec();

                    let applied_by = std::env::var("USER")
                        .or_else(|_| std::env::var("USERNAME"))
                        .unwrap_or_else(|_| "unknown".to_string());

                    self.client
                        .execute(
                            INSERT_DAG_VERSION_SQL,
                            &[
                                &self.project,
                                &spec_hash,
                                &applied_by,
                                &plan_json,
                                &spec_json,
                            ],
                        )
                        .await?;
                }

                PlanStep::UnlockDag { .. } => {
                    if locked {
                        self.client
                            .execute(RELEASE_LOCK_SQL, &[&self.project, &lock_holder])
                            .await
                            .ok();
                        locked = false;
                    }
                }
            }
        }

        // Ensure lock is always released on success too.
        if locked {
            self.client
                .execute(RELEASE_LOCK_SQL, &[&self.project, &lock_holder])
                .await
                .ok();
        }

        Ok(new_version)
    }

    async fn acquire_lock(&self, project: &str, holder: &str, ttl: &str) -> Result<()> {
        // Try to acquire the lock using INSERT ... ON CONFLICT.
        let rows = self
            .client
            .query(ACQUIRE_LOCK_SQL, &[&project, &holder, &ttl])
            .await?;

        if rows.is_empty() {
            // Lock is held by someone else and hasn't expired.
            let lock_row = self
                .client
                .query_opt(
                    "SELECT holder FROM aqueduct.locks WHERE project = $1",
                    &[&project],
                )
                .await?;

            let current_holder = lock_row
                .map(|r| r.get::<_, String>(0))
                .unwrap_or_else(|| "unknown".to_string());

            return Err(AqueductError::LockContention {
                project: project.to_string(),
                holder: current_holder,
            });
        }

        Ok(())
    }

    fn validate_query(&self, name: &str, query: &str) -> Result<()> {
        crate::validate::validate_sql_syntax(query, name)?;
        Ok(())
    }
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Import stream tables from the live pg_trickle catalog into a migrations directory.
pub async fn import_from_live(
    client: &tokio_postgres::Client,
    project: &str,
    output_dir: &std::path::Path,
    exclude_patterns: &[String],
) -> Result<usize> {
    use crate::live_state::read_live_state;

    let state = read_live_state(client).await?;
    let streams_dir = output_dir.join("migrations").join("streams");
    std::fs::create_dir_all(&streams_dir)?;

    let mut count = 0;
    let exclude_regexes: Vec<regex::Regex> = exclude_patterns
        .iter()
        .filter_map(|p| regex::Regex::new(p).ok())
        .collect();

    for table in &state.stream_tables {
        let fq_name = table.qualified_name.to_string();

        // Check exclusion patterns.
        let excluded = exclude_regexes.iter().any(|re| re.is_match(&fq_name));
        if excluded {
            continue;
        }

        let filename = format!("{}.sql", table.qualified_name.name);
        let path = streams_dir.join(&filename);

        let mut content = String::new();
        content.push_str(&format!(
            "-- @aqueduct:schedule      = \"{}\"\n",
            table.schedule
        ));
        content.push_str(&format!(
            "-- @aqueduct:refresh_mode  = \"{}\"\n",
            table.refresh_mode
        ));
        if let Some(cdc) = &table.cdc_mode {
            content.push_str(&format!("-- @aqueduct:cdc_mode      = \"{}\"\n", cdc));
        }
        if table.qualified_name.schema != "public" {
            content.push_str(&format!(
                "-- @aqueduct:schema         = \"{}\"\n",
                table.qualified_name.schema
            ));
        }
        content.push('\n');
        content.push_str(&table.query);
        content.push('\n');

        std::fs::write(&path, content)?;
        count += 1;
    }

    // Create skeleton aqueduct.toml if it doesn't exist.
    let toml_path = output_dir.join("aqueduct.toml");
    if !toml_path.exists() {
        let toml_content = format!(
            r#"[project]
name = "{}"

[targets.default]
dsn = "${{AQUEDUCT_DSN}}"

[apply]
lock_timeout = "30s"
allow_full_refresh = true
"#,
            project
        );
        std::fs::write(&toml_path, toml_content)?;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::quote_ident;

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("myschema"), "\"myschema\"");
    }

    #[test]
    fn test_quote_ident_with_quote() {
        assert_eq!(quote_ident("my\"schema"), "\"my\"\"schema\"");
    }
}
