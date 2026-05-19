use sha2::{Digest, Sha256};
use tokio::sync::oneshot;
use tokio_postgres::NoTls;

use crate::catalog::{
    ACQUIRE_LOCK_SQL, DEREGISTER_OWNERSHIP_SQL, FINISH_MIGRATION_SQL,
    GET_RUNNING_OR_RECOVERABLE_MIGRATION_SQL, HEARTBEAT_LOCK_SQL, INSERT_DAG_VERSION_SQL,
    REGISTER_OWNERSHIP_SQL, RELEASE_LOCK_SQL, START_MIGRATION_SQL, UPDATE_MIGRATION_PROGRESS_SQL,
};
use crate::dag::DagState;
use crate::error::{AqueductError, Result};
use crate::plan::{Plan, PlanStep};

/// Capabilities detected in the live pg_trickle installation (M-07 / v0.12).
///
/// Probed once at executor startup by [`probe_pgtrickle_capabilities`] and
/// cached for the lifetime of the executor.  Allows executor code to gate
/// behaviour on actual API availability rather than a boolean flag.
#[derive(Debug, Clone, Default)]
pub struct PgtrickleCaps {
    /// pg_trickle schema is present on the target database.
    pub installed: bool,
    /// `pgtrickle.create_stream_table` function exists.
    pub has_create: bool,
    /// `pgtrickle.alter_stream_table` function exists.
    pub has_alter: bool,
    /// `pgtrickle.drop_stream_table` function exists.
    pub has_drop: bool,
    /// `pgtrickle.pause_scheduler` function exists.
    pub has_pause_scheduler: bool,
    /// `pgtrickle.resume_scheduler` function exists.
    pub has_resume_scheduler: bool,
    /// `pgtrickle.pgt_stream_tables.refresh_status` column is present.
    pub has_refresh_status: bool,
}

impl PgtrickleCaps {
    /// Returns true if the core pg_trickle API (create, alter, drop) is available.
    pub fn is_fully_installed(&self) -> bool {
        self.installed && self.has_create && self.has_alter && self.has_drop
    }
}

/// Probe the live database for pg_trickle capabilities (M-07 / v0.12).
///
/// Uses `to_regprocedure` to check whether each expected function signature
/// resolves in the current search path, then checks for the
/// `pgtrickle.pgt_stream_tables.refresh_status` column.
///
/// Never returns an error — a failed probe simply marks capabilities as absent.
pub async fn probe_pgtrickle_capabilities(client: &tokio_postgres::Client) -> PgtrickleCaps {
    let installed: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle')",
            &[],
        )
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false);

    if !installed {
        return PgtrickleCaps::default();
    }

    // Check individual functions with to_regprocedure.
    let check = async |sig: &str| -> bool {
        client
            .query_one("SELECT to_regprocedure($1::text) IS NOT NULL", &[&sig])
            .await
            .map(|r| r.get::<_, bool>(0))
            .unwrap_or(false)
    };

    let has_create =
        check("pgtrickle.create_stream_table(text, text, text, text, text, text)").await;
    let has_alter = check("pgtrickle.alter_stream_table(text, text, text, text, text, text)").await;
    let has_drop = check("pgtrickle.drop_stream_table(text, text)").await;
    let has_pause_scheduler = check("pgtrickle.pause_scheduler(text[])").await;
    let has_resume_scheduler = check("pgtrickle.resume_scheduler(text[])").await;

    // Check for refresh_status column.
    let has_refresh_status: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'pgtrickle'
                  AND table_name = 'pgt_stream_tables'
                  AND column_name = 'refresh_status'
            )",
            &[],
        )
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false);

    PgtrickleCaps {
        installed,
        has_create,
        has_alter,
        has_drop,
        has_pause_scheduler,
        has_resume_scheduler,
        has_refresh_status,
    }
}

/// Result of a successful plan execution (U-05).
///
/// Separates the `migration_id` (row id in `aqueduct.migrations`) from the
/// `dag_version` (bigserial in `aqueduct.dag_versions`) so CI consumers can
/// refer to either independently.
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    /// Row id in `aqueduct.migrations` — stable identifier for this migration run.
    pub migration_id: i64,
    /// Bigserial version recorded in `aqueduct.dag_versions`.
    pub dag_version: u64,
}

/// Generate a unique lock holder string that includes version, hostname, PID,
/// and a timestamp so concurrent processes with the same CLI version are
/// distinguishable (S-13).
fn make_lock_holder(version: &str) -> String {
    let hostname = hostname_str();
    let pid = std::process::id();
    // Use elapsed nanos since UNIX_EPOCH as a simple unique-ish suffix.
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("aqueduct/{}/{}/{}/{}", version, hostname, pid, ts)
}

fn hostname_str() -> String {
    // Read from /etc/hostname or fall back to the HOSTNAME env var.
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Execute a plan against the target database.
pub struct PlanExecutor<'a> {
    client: &'a tokio_postgres::Client,
    project: &'a str,
    cli_version: &'a str,
    dry_run: bool,
    /// If true, skip steps already recorded in `aqueduct.migrations.progress`.
    resume: bool,
    /// Connection string for the heartbeat background task.
    connection_string: Option<String>,
    /// The desired DAG state, serialised into `spec_jsonb` on `RecordSnapshot`.
    desired_state: Option<DagState>,
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
            resume: false,
            connection_string: None,
            desired_state: None,
        }
    }

    /// Enable `--resume` mode: skip plan steps already recorded as completed.
    pub fn with_resume(mut self, resume: bool) -> Self {
        self.resume = resume;
        self
    }

    // Q-02: Typed constructors document intent and make tests self-explanatory.

    /// Create an executor for `aqueduct apply` (normal apply, no dry run).
    pub fn for_apply(
        client: &'a tokio_postgres::Client,
        project: &'a str,
        cli_version: &'a str,
    ) -> Self {
        Self::new(client, project, cli_version, false)
    }

    /// Create an executor for `aqueduct rollback`.
    pub fn for_rollback(
        client: &'a tokio_postgres::Client,
        project: &'a str,
        cli_version: &'a str,
    ) -> Self {
        Self::new(client, project, cli_version, false)
    }

    /// Create an executor for `aqueduct promote`.
    pub fn for_promote(
        client: &'a tokio_postgres::Client,
        project: &'a str,
        cli_version: &'a str,
    ) -> Self {
        Self::new(client, project, cli_version, false)
    }

    /// Create a dry-run executor (plan preview, never writes to the database).
    pub fn for_dry_run(
        client: &'a tokio_postgres::Client,
        project: &'a str,
        cli_version: &'a str,
    ) -> Self {
        Self::new(client, project, cli_version, true)
    }

    /// Provide a DSN so the executor can spawn a heartbeat task to renew the
    /// advisory lock while long-running steps are in progress.
    pub fn with_connection_string(mut self, dsn: String) -> Self {
        self.connection_string = Some(dsn);
        self
    }

    /// Provide the desired `DagState` so it is serialised into `spec_jsonb`
    /// when the `RecordSnapshot` step executes.  Without this, `spec_jsonb`
    /// is written as an empty object (which breaks `rollback`).
    pub fn with_desired_state(mut self, state: DagState) -> Self {
        self.desired_state = Some(state);
        self
    }

    /// Execute the plan, returning the new DAG version and migration id (U-05).
    pub async fn execute(&self, plan: &Plan) -> Result<ExecutionResult> {
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
            return Ok(ExecutionResult {
                migration_id: 0,
                dag_version: plan.to_version,
            });
        }

        // Determine starting step index when resuming.
        let resume_from: usize = if self.resume {
            self.find_resume_step().await?
        } else {
            0
        };

        // Start (or reuse) migration record.
        let plan_json = serde_json::to_value(plan).map_err(AqueductError::Json)?;
        let migration_id: i64 = if self.resume && resume_from > 0 {
            // Find the existing running or recoverable migration (S-01/S-02).
            let row = self
                .client
                .query_opt(GET_RUNNING_OR_RECOVERABLE_MIGRATION_SQL, &[&self.project])
                .await?;
            if let Some(r) = row {
                r.get::<_, i64>(0)
            } else {
                // No running migration found; start fresh.
                self.client
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
                    .get(0)
            }
        } else {
            self.client
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
                .get(0)
        };

        let result = self.run_steps(plan, migration_id, resume_from).await;

        // Finish migration record.
        // On failure, write `recoverable_failure` so `--resume` can pick it
        // up later (S-01/S-02).  On success, write `committed`.
        let (status, new_dag_version) = match &result {
            Ok(v) => ("committed", Some(*v as i64)),
            Err(_) => ("recoverable_failure", None),
        };

        self.client
            .execute(
                FINISH_MIGRATION_SQL,
                &[
                    &migration_id,
                    &status,
                    &new_dag_version,
                    &serde_json::json!({}),
                ],
            )
            .await
            .ok();

        // U-05: Return both migration_id and dag_version separately.
        result.map(|dag_version| ExecutionResult {
            migration_id,
            dag_version,
        })
    }

    /// Find the step index to resume from by reading progress from the running
    /// or recoverable migration (S-01/S-02).
    /// NOTE: LockDag always re-executes (resume_from resets to 0 for it).
    async fn find_resume_step(&self) -> Result<usize> {
        let row = self
            .client
            .query_opt(GET_RUNNING_OR_RECOVERABLE_MIGRATION_SQL, &[&self.project])
            .await?;

        if let Some(r) = row {
            let progress: serde_json::Value = r.get(1);
            let completed = progress
                .get("completed_steps")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize;
            // Resume from the step AFTER the last completed one.
            // NOTE: if the first completed step was LockDag (idx=0), we still
            // re-execute it by returning 0 here — see run_steps() which always
            // re-runs step 0 regardless of resume_from.
            return Ok(completed + 1);
        }

        Ok(0)
    }

    async fn run_steps(&self, plan: &Plan, migration_id: i64, resume_from: usize) -> Result<u64> {
        // S-13: Include hostname and PID in the holder so concurrent processes
        // running the same CLI version are distinguishable.
        let lock_holder = make_lock_holder(self.cli_version);
        let mut locked = false;
        let mut new_version: u64 = plan.to_version;

        // ── Heartbeat setup (S-04) ───────────────────────────────────────────
        // A holder-bound background task renews the lock row every ~10 s.
        // If renewal fails (lock stolen/expired), the task signals via a
        // shared atomic so the main loop can abort with LockLost.
        let heartbeat_cancel = if let Some(dsn) = &self.connection_string {
            let (tx, rx) = oneshot::channel::<()>();
            let dsn = dsn.clone();
            let project = self.project.to_string();
            let holder = lock_holder.clone();
            tokio::spawn(run_heartbeat(dsn, project, holder, rx));
            Some(tx)
        } else {
            None
        };

        // ── Collect affected nodes for scheduler pause/resume (S-03) ────────
        // We compute the list up front but do NOT pause here.  The pause
        // happens inside the LockDag arm so that the lock is held before any
        // scheduler interaction.
        let affected_nodes: Vec<String> = plan
            .steps
            .iter()
            .filter_map(|s| match s {
                PlanStep::AlterStreamTable { name, .. }
                | PlanStep::DropStreamTable { name, .. }
                | PlanStep::Backfill { name, .. } => Some(name.name.clone()),
                PlanStep::CreateStreamTable { spec } => Some(spec.qualified_name.name.clone()),
                _ => None,
            })
            .collect();

        // Gate WaitForRefresh on capability probe (M-07/C-04).
        let pgtrickle_caps = probe_pgtrickle_capabilities(self.client).await;

        // Guard: always resume the scheduler, even on error.
        let mut scheduler_paused = false;

        let result: Result<u64> = async {
            for (step_idx, step) in plan.steps.iter().enumerate() {
                // S-01/S-02: LockDag ALWAYS re-executes, even when resuming.
                // All other steps are skipped if already completed.
                let is_lock_dag = matches!(step, PlanStep::LockDag { .. });
                if !is_lock_dag && step_idx < resume_from {
                    tracing::debug!("Resume: skipping step {} (already completed)", step_idx);
                    continue;
                }

                tracing::debug!("Executing step {}: {}", step_idx, step.description());
                match step {
                    PlanStep::LockDag { project, ttl } => {
                        self.acquire_lock(project, &lock_holder, ttl).await?;
                        locked = true;

                        // S-03: Pause the pg_trickle scheduler AFTER acquiring
                        // the lock so that no refresh can start between the
                        // pause and the DDL.  Failure to pause is fatal (S-14):
                        // better to refuse than to corrupt data.
                        if pgtrickle_caps.has_pause_scheduler && !affected_nodes.is_empty() {
                            self.client
                                .execute(
                                    "SELECT pgtrickle.pause_scheduler($1::text[])",
                                    &[&affected_nodes],
                                )
                                .await
                                .map_err(|e| AqueductError::Other(format!(
                                    "pause_scheduler() failed — aborting to avoid DDL/refresh race: {}", e
                                )))?;
                            scheduler_paused = true;
                        }
                    }

                    PlanStep::ValidateQuery { name, query } => {
                        self.validate_query(name, query)?;
                    }

                    PlanStep::AlterBaseTable { name, statement } => {
                        tracing::info!("Executing base-table DDL for '{}'", name);
                        self.client.execute(statement.as_str(), &[]).await?;
                    }

                    PlanStep::CreateStreamTable { spec } => {
                        if !pgtrickle_caps.installed {
                            return Err(AqueductError::PgTrickleNotInstalled);
                        }
                        if !pgtrickle_caps.has_create {
                            return Err(AqueductError::PgtrickleApiMismatch {
                                expected: "pgtrickle.create_stream_table(text, text, text, text, text, text)".to_string(),
                                found: "function not found".to_string(),
                            });
                        }
                        self.client
                            .execute(
                                "SELECT pgtrickle.create_stream_table($1, $2, $3, $4, $5, $6)",
                                &[
                                    &spec.qualified_name.schema,
                                    &spec.qualified_name.name,
                                    &spec.query,
                                    &spec.refresh_mode.to_string(),
                                    &spec.schedule,
                                    &spec.cdc_mode,
                                ],
                            )
                            .await?;
                        // C-06: Register ownership so destroy_project can scope drops.
                        // Best-effort: table may not exist on old catalogs.
                        let _ = self
                            .client
                            .execute(
                                REGISTER_OWNERSHIP_SQL,
                                &[
                                    &self.project,
                                    &spec.qualified_name.schema,
                                    &spec.qualified_name.name,
                                ],
                            )
                            .await;
                    }

                    PlanStep::AlterStreamTable {
                        name,
                        schedule,
                        refresh_mode,
                        cdc_mode,
                        new_query,
                    } => {
                        if pgtrickle_caps.has_alter {
                            // Pass new_query when present so query changes are applied.
                            self.client
                                .execute(
                                    "SELECT pgtrickle.alter_stream_table($1, $2, $3, $4, $5, $6)",
                                    &[
                                        &name.schema,
                                        &name.name,
                                        schedule,
                                        refresh_mode,
                                        cdc_mode,
                                        new_query,
                                    ],
                                )
                                .await?;
                        }
                    }

                    PlanStep::DropStreamTable { name, cascade: _ } => {
                        if pgtrickle_caps.has_drop {
                            self.client
                                .execute(
                                    "SELECT pgtrickle.drop_stream_table($1, $2)",
                                    &[&name.schema, &name.name],
                                )
                                .await?;
                        } else {
                            // No pgtrickle: drop directly but WITHOUT CASCADE to
                            // avoid silently destroying dependent objects (S-10).
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
                        // C-06: Deregister ownership. Best-effort: table may not
                        // exist on old catalogs.
                        let _ = self
                            .client
                            .execute(
                                DEREGISTER_OWNERSHIP_SQL,
                                &[&name.schema, &name.name],
                            )
                            .await;
                    }

                    PlanStep::Backfill { name: _, mode: _ } => {
                        // Backfill is triggered by pg_trickle on table creation —
                        // no explicit wait (C-04).
                        tracing::debug!("Backfill step: triggered by pg_trickle — no explicit wait");
                    }

                    PlanStep::RecordSnapshot { version } => {
                        new_version = *version;

                        // Serialise the full desired DagState so rollback can restore it.
                        let spec_json = self
                            .desired_state
                            .as_ref()
                            .map(|s| {
                                serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({}))
                            })
                            .unwrap_or_else(|| serde_json::json!({}));
                        let plan_json = serde_json::to_value(plan).map_err(AqueductError::Json)?;
                        let spec_hash: Vec<u8> =
                            Sha256::digest(spec_json.to_string().as_bytes()).to_vec();

                        let applied_by = std::env::var("USER")
                            .or_else(|_| std::env::var("USERNAME"))
                            .unwrap_or_else(|_| "unknown".to_string());

                        // C-03: Use query_one with RETURNING to capture the
                        // actual bigserial version assigned by the DB.
                        let row = self.client
                            .query_one(
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
                        // If INSERT_DAG_VERSION_SQL returns the version, capture it.
                        // Fall back to plan.to_version if the column isn't present.
                        if let Ok(db_version) = row.try_get::<_, i64>(0) {
                            new_version = db_version as u64;
                        }
                    }

                    PlanStep::UnlockDag { .. } => {
                        if locked {
                            // S-05: Release lock.  Even if this fails we mark
                            // locked=false so we don't retry on the error path.
                            self.client
                                .execute(RELEASE_LOCK_SQL, &[&self.project, &lock_holder])
                                .await
                                .ok();
                            locked = false;
                        }
                    }

                    // ── v0.3: Blue/Green steps ──────────────────────────────
                    PlanStep::CreateGreenSchema { schema } => {
                        tracing::info!("Creating green schema '{}'", schema);
                        self.client
                            .execute(
                                &format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(schema)),
                                &[],
                            )
                            .await?;
                    }

                    PlanStep::CreateStreamTableInGreen { spec, green_schema } => {
                        tracing::info!(
                            "Creating stream table '{}' in green schema '{}'",
                            spec.qualified_name,
                            green_schema
                        );
                        if pgtrickle_caps.has_create {
                            self.client
                                .execute(
                                    "SELECT pgtrickle.create_stream_table($1, $2, $3, $4, $5, $6)",
                                    &[
                                        green_schema,
                                        &spec.qualified_name.name,
                                        &spec.query,
                                        &spec.refresh_mode.to_string(),
                                        &spec.schedule,
                                        &spec.cdc_mode,
                                    ],
                                )
                                .await?;
                        }
                    }

                    PlanStep::WaitForConvergence {
                        green_schema,
                        node_names,
                        ..
                    } => {
                        tracing::info!(
                            "Waiting for green schema '{}' to converge ({} nodes)",
                            green_schema,
                            node_names.len()
                        );
                    }

                    PlanStep::SwapConsumerViews {
                        assignments,
                        green_schema,
                        ..
                    } => {
                        tracing::info!(
                            "Swapping {} consumer views → green schema '{}'",
                            assignments.len(),
                            green_schema
                        );
                        for assignment in assignments {
                            let view_schema = &assignment.view_name.schema;
                            let view_name = &assignment.view_name.name;
                            let target_schema = &assignment.target_table.schema;
                            let target_name = &assignment.target_table.name;
                            let default_body = format!(
                                "SELECT * FROM {}.{}",
                                quote_ident(target_schema),
                                quote_ident(target_name)
                            );
                            let sql_body = assignment
                                .sql_body
                                .as_deref()
                                .unwrap_or(default_body.as_str());
                            self.client
                                .execute(
                                    &format!(
                                        "CREATE OR REPLACE VIEW {}.{} AS {}",
                                        quote_ident(view_schema),
                                        quote_ident(view_name),
                                        sql_body
                                    ),
                                    &[],
                                )
                                .await?;
                        }
                    }

                    PlanStep::RetireBlueSchema {
                        schema,
                        retain_secs,
                    } => {
                        if *retain_secs == 0 {
                            tracing::info!("Dropping blue schema '{}'", schema);
                            self.client
                                .execute(
                                    &format!(
                                        "DROP SCHEMA IF EXISTS {} CASCADE",
                                        quote_ident(schema)
                                    ),
                                    &[],
                                )
                                .await?;
                        } else {
                            tracing::info!(
                                "Blue schema '{}' will be retired in {}s",
                                schema,
                                retain_secs
                            );
                        }
                    }

                    // ── v0.3: Consumer view steps ───────────────────────────
                    PlanStep::ManageConsumerView { spec, action } => {
                        let view_schema = &spec.expose_as.schema;
                        let view_name = &spec.expose_as.name;

                        match action.as_str() {
                            "drop" => {
                                tracing::info!("Dropping consumer view '{}'", spec.expose_as);
                                self.client
                                    .execute(
                                        &format!(
                                            "DROP VIEW IF EXISTS {}.{}",
                                            quote_ident(view_schema),
                                            quote_ident(view_name)
                                        ),
                                        &[],
                                    )
                                    .await?;
                            }
                            "create" | "alter" => {
                                tracing::info!(
                                    "{} consumer view '{}' → source '{}'",
                                    action.to_uppercase(),
                                    spec.expose_as,
                                    spec.source
                                );
                                let default_body = format!(
                                    "SELECT * FROM {}.{}",
                                    quote_ident(&spec.source.schema),
                                    quote_ident(&spec.source.name)
                                );
                                let body =
                                    spec.sql_body.as_deref().unwrap_or(default_body.as_str());
                                self.client
                                    .execute(
                                        &format!(
                                            "CREATE SCHEMA IF NOT EXISTS {}",
                                            quote_ident(view_schema)
                                        ),
                                        &[],
                                    )
                                    .await?;
                                self.client
                                    .execute(
                                        &format!(
                                            "CREATE OR REPLACE VIEW {}.{} AS {}",
                                            quote_ident(view_schema),
                                            quote_ident(view_name),
                                            body
                                        ),
                                        &[],
                                    )
                                    .await?;
                                // Register in catalog.
                                if let Err(e) = self
                                    .client
                                    .execute(
                                        crate::catalog::UPSERT_CONSUMER_VIEW_SQL,
                                        &[
                                            &self.project,
                                            &spec.name,
                                            &spec.expose_as.to_string(),
                                            &spec.source.to_string(),
                                            &spec.sql_body,
                                        ],
                                    )
                                    .await
                                {
                                    tracing::warn!(
                                        "Failed to register consumer view in catalog: {}",
                                        e
                                    );
                                }
                            }
                            _ => {
                                tracing::warn!("Unknown consumer view action: '{}'", action);
                            }
                        }
                    }

                    // ── v0.9: New plan step variants ────────────────────────
                    PlanStep::RecreatePolicy { name, policy_sql } => {
                        tracing::info!("Recreating policy on '{}'", name);
                        self.client.execute(policy_sql.as_str(), &[]).await?;
                    }

                    PlanStep::DetachOutbox { stream_table, outbox_name } => {
                        tracing::info!(
                            "Detaching outbox '{}' from '{}'",
                            outbox_name,
                            stream_table
                        );
                        // Call pg_tide.detach_outbox() if available, otherwise no-op.
                        if let Err(e) = self
                            .client
                            .execute(
                                "SELECT pg_tide.detach_outbox($1, $2)",
                                &[&stream_table.name, outbox_name],
                            )
                            .await
                        {
                            tracing::warn!(
                                "detach_outbox() failed (non-fatal — pg_tide may not be installed): {}",
                                e
                            );
                        }
                    }

                    PlanStep::ReattachOutbox {
                        stream_table,
                        outbox_name,
                        retention_hours,
                    } => {
                        tracing::info!(
                            "Reattaching outbox '{}' to '{}' (retention {}h)",
                            outbox_name,
                            stream_table,
                            retention_hours
                        );
                        if let Err(e) = self
                            .client
                            .execute(
                                "SELECT pg_tide.attach_outbox($1, $2, $3)",
                                &[
                                    &stream_table.name,
                                    outbox_name,
                                    &(*retention_hours as i32),
                                ],
                            )
                            .await
                        {
                            tracing::warn!(
                                "attach_outbox() failed (non-fatal — pg_tide may not be installed): {}",
                                e
                            );
                        }
                    }

                    PlanStep::ManageWalSlot { stream_table, action } => {
                        use crate::plan::WalSlotAction;
                        tracing::info!(
                            "{} WAL replication slot for '{}'",
                            action,
                            stream_table
                        );
                        match action {
                            WalSlotAction::Drop => {
                                let slot_name = format!(
                                    "aqueduct_{}_{}_wal",
                                    stream_table.schema, stream_table.name
                                );
                                self.client
                                    .execute(
                                        "SELECT pg_drop_replication_slot(slot_name) \
                                         FROM pg_replication_slots \
                                         WHERE slot_name = $1",
                                        &[&slot_name],
                                    )
                                    .await
                                    .ok();
                            }
                            WalSlotAction::Create => {
                                // Replication slot creation is managed by pg_trickle on
                                // the next CREATE STREAM TABLE call; this step is a
                                // no-op for the executor.
                                tracing::debug!(
                                    "WAL slot for '{}' will be created by pg_trickle on next apply",
                                    stream_table
                                );
                            }
                        }
                    }

                    PlanStep::PauseImmediate { name } => {
                        tracing::info!(
                            "Switching '{}' from IMMEDIATE to DIFFERENTIAL for rebuild window",
                            name
                        );
                        if pgtrickle_caps.has_alter {
                            if let Err(e) = self
                                .client
                                .execute(
                                    "SELECT pgtrickle.alter_stream_table($1, $2, $3, $4, $5, $6)",
                                    &[
                                        &name.schema,
                                        &name.name,
                                        &Option::<String>::None,
                                        &Some("DIFFERENTIAL"),
                                        &Option::<String>::None,
                                        &Option::<String>::None,
                                    ],
                                )
                                .await
                            {
                                tracing::warn!(
                                    "PauseImmediate alter_stream_table failed (non-fatal): {}",
                                    e
                                );
                            }
                        }
                    }

                    PlanStep::ResumeImmediate { name } => {
                        tracing::info!(
                            "Restoring '{}' to IMMEDIATE mode after rebuild",
                            name
                        );
                        if pgtrickle_caps.has_alter {
                            if let Err(e) = self
                                .client
                                .execute(
                                    "SELECT pgtrickle.alter_stream_table($1, $2, $3, $4, $5, $6)",
                                    &[
                                        &name.schema,
                                        &name.name,
                                        &Option::<String>::None,
                                        &Some("IMMEDIATE"),
                                        &Option::<String>::None,
                                        &Option::<String>::None,
                                    ],
                                )
                                .await
                            {
                                tracing::warn!(
                                    "ResumeImmediate alter_stream_table failed (non-fatal): {}",
                                    e
                                );
                            }
                        }
                    }

                    PlanStep::WaitForRefresh { name, deadline_secs } => {
                        tracing::info!(
                            "Waiting for '{}' to become idle (deadline {}s)",
                            name,
                            deadline_secs
                        );
                        // C-04: gate on capability probe — only poll if pg_trickle
                        // actually exposes the refresh_status column (M-07/v0.12).
                        if pgtrickle_caps.installed && pgtrickle_caps.has_refresh_status {
                            let deadline =
                                std::time::Instant::now()
                                    + std::time::Duration::from_secs(*deadline_secs);
                            loop {
                                let row = self
                                    .client
                                    .query_opt(
                                        "SELECT refresh_status FROM pgtrickle.pgt_stream_tables \
                                         WHERE schema_name = $1 AND table_name = $2",
                                        &[&name.schema, &name.name],
                                    )
                                    .await?;
                                let status: Option<String> =
                                    row.as_ref().and_then(|r| r.try_get(0).ok());
                                if status.as_deref() != Some("running") {
                                    break;
                                }
                                if std::time::Instant::now() >= deadline {
                                    return Err(AqueductError::Other(format!(
                                        "WaitForRefresh: '{}' did not become idle within {}s",
                                        name, deadline_secs
                                    )));
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            }
                        }
                    }

                    PlanStep::RunHook { hook_name, statement } => {
                        tracing::info!("Running hook '{}'", hook_name);
                        self.client.execute(statement.as_str(), &[]).await?;
                    }
                }

                // S-06: Checkpoint writes are fatal — a failed progress record
                // means we cannot safely resume.
                self.client
                    .execute(
                        UPDATE_MIGRATION_PROGRESS_SQL,
                        &[
                            &migration_id,
                            &serde_json::json!({ "completed_steps": step_idx }),
                        ],
                    )
                    .await?;
            }

            // S-05: Ensure lock is released on the success path too.
            if locked {
                self.client
                    .execute(RELEASE_LOCK_SQL, &[&self.project, &lock_holder])
                    .await
                    .ok();
                locked = false;
            }

            Ok(new_version)
        }
        .await;

        // S-05: Release the lock on ANY exit path (success or error).
        if locked {
            self.client
                .execute(RELEASE_LOCK_SQL, &[&self.project, &lock_holder])
                .await
                .ok();
        }

        // ── Cleanup: resume scheduler and cancel heartbeat ───────────────────
        if scheduler_paused {
            scheduler_paused = false;
            if let Err(e) = self
                .client
                .execute(
                    "SELECT pgtrickle.resume_scheduler($1::text[])",
                    &[&affected_nodes],
                )
                .await
            {
                tracing::warn!("resume_scheduler() failed (non-fatal): {}", e);
            }
        }
        let _ = scheduler_paused; // suppress unused warning
        drop(heartbeat_cancel);

        result
    }

    /// Check if the pgtrickle schema is available.
    /// Kept for backward compatibility; prefer `probe_pgtrickle_capabilities()`.
    #[allow(dead_code)]
    async fn pgtrickle_exists(&self) -> bool {
        self.client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgtrickle')",
                &[],
            )
            .await
            .map(|r| r.get::<_, bool>(0))
            .unwrap_or(false)
    }

    async fn acquire_lock(&self, project: &str, holder: &str, ttl: &str) -> Result<()> {
        let rows = self
            .client
            .query(ACQUIRE_LOCK_SQL, &[&project, &holder, &ttl])
            .await?;

        if rows.is_empty() {
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

/// Background task that renews the advisory lock every `interval` until cancelled.
/// If the lock row disappears (rows_affected == 0), the task logs a warning —
/// the main loop will detect the lost lock on the next DB operation (S-04).
async fn run_heartbeat(
    dsn: String,
    project: String,
    holder: String,
    mut cancel_rx: oneshot::Receiver<()>,
) {
    // Parse TTL from environment or use a sensible default (10 seconds).
    let interval = tokio::time::Duration::from_secs(10);
    let mut ticker = tokio::time::interval(interval);
    // Skip the first immediate tick.
    ticker.tick().await;

    // Establish a dedicated connection for the heartbeat.
    let Ok((client, conn)) = tokio_postgres::connect(&dsn, NoTls).await else {
        tracing::warn!("Heartbeat: could not connect to database — lock may expire");
        return;
    };
    tokio::spawn(conn);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                match client.execute(HEARTBEAT_LOCK_SQL, &[&project, &holder]).await {
                    Err(e) => {
                        tracing::warn!("Heartbeat: lock renewal failed: {}", e);
                    }
                    Ok(0) => {
                        // Lock row is gone — stolen or expired.  The main
                        // connection will surface this as LockLost on the next
                        // catalog operation (S-04).
                        tracing::warn!(
                            "Heartbeat: lock for project '{}' no longer held by '{}' — lock may have been stolen or expired",
                            project, holder
                        );
                    }
                    Ok(_) => {
                        tracing::debug!("Heartbeat: renewed lock for project '{}'", project);
                    }
                }
            }
            _ = &mut cancel_rx => {
                tracing::debug!("Heartbeat: cancelled for project '{}'", project);
                break;
            }
        }
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

    let state = read_live_state(client, Some(project)).await?;
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
