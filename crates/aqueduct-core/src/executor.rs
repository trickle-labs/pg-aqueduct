use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, watch};
use tokio_postgres::NoTls;

use crate::catalog::CatalogSchema;
use crate::catalog::{
    for_schema, ACQUIRE_LOCK_SQL, COMPLETE_COMPENSATING_STEP_SQL, DELETE_CONSUMER_VIEW_SQL,
    DEREGISTER_OWNERSHIP_SQL, FAIL_MIGRATION_STEP_SQL, FINISH_MIGRATION_RECOVERABLE_SQL,
    FINISH_MIGRATION_SQL, FINISH_MIGRATION_STEP_SQL, GET_PENDING_COMPENSATING_STEPS_SQL,
    GET_RUNNING_OR_RECOVERABLE_MIGRATION_SQL, HEARTBEAT_LOCK_SQL, INSERT_COMPENSATING_STEP_SQL,
    INSERT_DAG_VERSION_SQL, MARK_COMPENSATING_APPLIED_SQL, MARK_MIGRATION_INTERRUPTED_SQL,
    REGISTER_OWNERSHIP_SQL, RELEASE_LOCK_SQL, RETIRE_BLUE_GREEN_SQL, START_BLUE_GREEN_SQL,
    START_MIGRATION_SQL, START_MIGRATION_STEP_SQL, SWAP_BLUE_GREEN_SQL,
    UPDATE_MIGRATION_PROGRESS_SQL,
};
use crate::dag::DagState;
use crate::error::{AqueductError, Result};
use crate::plan::{Plan, PlanStep};
use crate::validate::validate_consumer_sql_is_single_select;

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

/// Typed execution context for live `PlanExecutor` constructors (ARCH-2 / v0.20).
///
/// Both `desired_state` and `connection_string` are required non-optional fields.
/// Passing this struct to `for_apply`, `for_rollback`, or `for_promote` is a
/// compile-time guarantee that the executor has the information it needs to
/// renew the advisory lock via heartbeat and to record `spec_jsonb` on apply.
///
/// Dry-run mode does **not** require an `ExecutionContext`; use `for_dry_run`
/// which accepts no context.
///
/// # Compile-fail example
/// ```compile_fail
/// use aqueduct_core::executor::PlanExecutor;
/// // Calling for_apply with wrong arity is a compile error — ExecutionContext is required.
/// let _ = PlanExecutor::for_apply(todo!(), "project", "0.20.0");
/// ```
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Target DAG state, serialised into `spec_jsonb` on `RecordSnapshot`.
    pub desired_state: DagState,
    /// DSN for the heartbeat background task and any reconnect on failover.
    pub connection_string: String,
    /// Optional Patroni REST endpoint for HA primary checks between steps.
    pub patroni_endpoint: Option<String>,
    /// Resume an interrupted migration (skip already-completed steps).
    pub resume: bool,
    /// Force-retry the step at this index (bypasses resume checkpoint).
    pub force_retry: Option<usize>,
    /// Force-skip the step at this index (marks it complete without executing).
    pub force_skip: Option<usize>,
    /// Catalog schema to use for all catalog SQL (ARCH-1 / v0.20).
    pub catalog_schema: CatalogSchema,
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
    /// CORR-2 / v0.15: Force-retry a specific step index (ignores resume).
    force_retry_step: Option<usize>,
    /// CORR-2 / v0.15: Force-skip a specific step index (marks it complete, skips execution).
    force_skip_step: Option<usize>,
    /// DOC-2 / v0.17: Optional Patroni endpoint for between-step primary checks.
    patroni_endpoint: Option<String>,
    /// ARCH-1 / v0.20: Catalog schema for all catalog SQL operations.
    catalog_schema: CatalogSchema,
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
            force_retry_step: None,
            force_skip_step: None,
            patroni_endpoint: None,
            catalog_schema: CatalogSchema::default(),
        }
    }

    /// Enable `--resume` mode: skip plan steps already recorded as completed.
    pub fn with_resume(mut self, resume: bool) -> Self {
        self.resume = resume;
        self
    }

    /// Set the step index to force-retry (CORR-2 / v0.15).
    pub fn with_force_retry(mut self, step: Option<usize>) -> Self {
        self.force_retry_step = step;
        self
    }

    /// Set the step index to force-skip (CORR-2 / v0.15).
    pub fn with_force_skip(mut self, step: Option<usize>) -> Self {
        self.force_skip_step = step;
        self
    }

    /// Set the Patroni endpoint URL for between-step HA primary checks (DOC-2 / v0.17).
    pub fn with_patroni_endpoint(mut self, endpoint: Option<String>) -> Self {
        self.patroni_endpoint = endpoint;
        self
    }

    /// Set the catalog schema for all catalog SQL operations (ARCH-1 / v0.20).
    pub fn with_catalog_schema(mut self, schema: CatalogSchema) -> Self {
        self.catalog_schema = schema;
        self
    }

    // Q-02: Typed constructors document intent and make tests self-explanatory.

    /// Create an executor for `aqueduct apply` (ARCH-2 / v0.20).
    ///
    /// Requires an `ExecutionContext` with both `desired_state` and
    /// `connection_string` set — omitting either is a **compile error**.
    pub fn for_apply(
        client: &'a tokio_postgres::Client,
        project: &'a str,
        cli_version: &'a str,
        ctx: ExecutionContext,
    ) -> Self {
        Self {
            client,
            project,
            cli_version,
            dry_run: false,
            resume: ctx.resume,
            connection_string: Some(ctx.connection_string),
            desired_state: Some(ctx.desired_state),
            force_retry_step: ctx.force_retry,
            force_skip_step: ctx.force_skip,
            patroni_endpoint: ctx.patroni_endpoint,
            catalog_schema: ctx.catalog_schema,
        }
    }

    /// Create an executor for `aqueduct rollback` (ARCH-2 / v0.20).
    ///
    /// Requires an `ExecutionContext` with both `desired_state` and
    /// `connection_string` set — omitting either is a **compile error**.
    pub fn for_rollback(
        client: &'a tokio_postgres::Client,
        project: &'a str,
        cli_version: &'a str,
        ctx: ExecutionContext,
    ) -> Self {
        Self {
            client,
            project,
            cli_version,
            dry_run: false,
            resume: ctx.resume,
            connection_string: Some(ctx.connection_string),
            desired_state: Some(ctx.desired_state),
            force_retry_step: ctx.force_retry,
            force_skip_step: ctx.force_skip,
            patroni_endpoint: ctx.patroni_endpoint,
            catalog_schema: ctx.catalog_schema,
        }
    }

    /// Create an executor for `aqueduct promote` (ARCH-2 / v0.20).
    ///
    /// Requires an `ExecutionContext` with both `desired_state` and
    /// `connection_string` set — omitting either is a **compile error**.
    pub fn for_promote(
        client: &'a tokio_postgres::Client,
        project: &'a str,
        cli_version: &'a str,
        ctx: ExecutionContext,
    ) -> Self {
        Self {
            client,
            project,
            cli_version,
            dry_run: false,
            resume: ctx.resume,
            connection_string: Some(ctx.connection_string),
            desired_state: Some(ctx.desired_state),
            force_retry_step: ctx.force_retry,
            force_skip_step: ctx.force_skip,
            patroni_endpoint: ctx.patroni_endpoint,
            catalog_schema: ctx.catalog_schema,
        }
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
    ///
    /// Prefer constructing via `for_apply` with an `ExecutionContext` in new code.
    pub fn with_connection_string(mut self, dsn: String) -> Self {
        self.connection_string = Some(dsn);
        self
    }

    /// Provide the desired `DagState` so it is serialised into `spec_jsonb`
    /// when the `RecordSnapshot` step executes.
    ///
    /// Prefer constructing via `for_apply` with an `ExecutionContext` in new code.
    pub fn with_desired_state(mut self, state: DagState) -> Self {
        self.desired_state = Some(state);
        self
    }

    /// Returns a schema-parameterized version of a SQL constant (ARCH-1 / v0.20).
    ///
    /// Replaces all `aqueduct.` occurrences in the template with the configured
    /// catalog schema prefix. When the schema is the default (`"aqueduct"`),
    /// this is a no-op string copy preserving backward compatibility.
    #[inline]
    fn schema_sql(&self, template: &'static str) -> String {
        for_schema(template, &self.catalog_schema)
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
                .query_opt(
                    &self.schema_sql(GET_RUNNING_OR_RECOVERABLE_MIGRATION_SQL),
                    &[&self.project],
                )
                .await?;
            if let Some(r) = row {
                r.get::<_, i64>(0)
            } else {
                // No running migration found; start fresh.
                self.client
                    .query_one(
                        &self.schema_sql(START_MIGRATION_SQL),
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
                    &self.schema_sql(START_MIGRATION_SQL),
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
        // On failure, write `recoverable_failure` WITHOUT resetting progress so that
        // `--resume` can re-use the completed_steps checkpoint (CORR-1 / v0.14).
        // On success, write `committed` and clear progress.
        // DOC-2 / v0.17: HA failover errors mark the migration as 'interrupted'.
        match &result {
            Ok(v) => {
                if let Err(e) = self
                    .client
                    .execute(
                        &self.schema_sql(FINISH_MIGRATION_SQL),
                        &[
                            &migration_id,
                            &"committed",
                            &Some(*v as i64),
                            &serde_json::json!({}),
                        ],
                    )
                    .await
                {
                    // Non-silenceable: a failed status update means the catalog
                    // may be stale. Log as error so operators see it (CORR-1).
                    tracing::error!(
                        migration_id = migration_id,
                        "CRITICAL: failed to mark migration as committed in catalog: {}. \
                         The migration completed successfully but the catalog state may be stale.",
                        e
                    );
                }
            }
            Err(e) => {
                // DOC-2 / v0.17: If the failure is an HA failover (pg_is_in_recovery or
                // Patroni check), mark as 'interrupted' so operators know to resume on
                // the new primary.  All other failures use 'recoverable_failure'.
                let is_ha_failover = is_ha_failover_error(e);
                if is_ha_failover {
                    if let Err(mark_err) = self
                        .client
                        .execute(
                            &self.schema_sql(MARK_MIGRATION_INTERRUPTED_SQL),
                            &[&migration_id],
                        )
                        .await
                    {
                        tracing::error!(
                            migration_id = migration_id,
                            "Failed to mark migration as interrupted: {}",
                            mark_err
                        );
                    }
                } else {
                    // Preserve progress — do NOT clear it (CORR-1 / v0.14).
                    if let Err(mark_err) = self
                        .client
                        .execute(
                            &self.schema_sql(FINISH_MIGRATION_RECOVERABLE_SQL),
                            &[&migration_id],
                        )
                        .await
                    {
                        tracing::error!(
                            migration_id = migration_id,
                            "Failed to mark migration as recoverable_failure: {}",
                            mark_err
                        );
                    }
                }
            }
        }

        // U-05: Return both migration_id and dag_version separately.
        result.map(|dag_version| ExecutionResult {
            migration_id,
            dag_version,
        })
    }

    /// Find the step index to resume from by reading progress from the running
    /// or recoverable migration (S-01/S-02).
    /// CORR-2 / v0.20: Also executes any pending compensating steps (status='running'
    /// in ddl_log) before returning the resume index, so that partial DDL from the
    /// crashed migration is rolled back before retrying.
    /// NOTE: LockDag always re-executes (resume_from resets to 0 for it).
    async fn find_resume_step(&self) -> Result<usize> {
        let row = self
            .client
            .query_opt(
                &self.schema_sql(GET_RUNNING_OR_RECOVERABLE_MIGRATION_SQL),
                &[&self.project],
            )
            .await?;

        if let Some(r) = row {
            let migration_id: i64 = r.get(0);
            let progress: serde_json::Value = r.get(1);

            // CORR-2 / v0.20: Execute pending compensating steps for any DDL
            // that started but did not complete before the crash.
            let pending = self
                .client
                .query(
                    &self.schema_sql(GET_PENDING_COMPENSATING_STEPS_SQL),
                    &[&migration_id],
                )
                .await?;

            for comp_row in &pending {
                let comp_id: i64 = comp_row.get(0);
                let comp_sql: Option<String> = comp_row.get(1);
                let cmd_tag: Option<String> = comp_row.get(2);
                let obj_name: Option<String> = comp_row.get(3);

                if let Some(sql) = comp_sql {
                    tracing::info!(
                        migration_id = migration_id,
                        command_tag = cmd_tag.as_deref().unwrap_or("unknown"),
                        object = obj_name.as_deref().unwrap_or("unknown"),
                        "CORR-2: executing compensating step to undo partial DDL"
                    );
                    // Best-effort: log but don't abort if compensating SQL fails.
                    if let Err(e) = self.client.batch_execute(&sql).await {
                        tracing::warn!(
                            migration_id = migration_id,
                            comp_id = comp_id,
                            "CORR-2: compensating step failed (may have been partially applied): {}",
                            e
                        );
                    }
                    // Mark as compensated regardless — we've done our best.
                    let _ = self
                        .client
                        .execute(&self.schema_sql(MARK_COMPENSATING_APPLIED_SQL), &[&comp_id])
                        .await;
                }
            }

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

        // ── Heartbeat setup (S-04 / CORR-3) ─────────────────────────────────
        // A holder-bound background task renews the lock row every ~10 s.
        // CORR-3: A `watch` channel carries a lock-lost signal from the
        // heartbeat to the main executor loop so it can abort with LockLost
        // instead of silently continuing after a stolen lock.
        let (lock_lost_tx, lock_lost_rx) = watch::channel(false);
        let heartbeat_cancel = if let Some(dsn) = &self.connection_string {
            let (tx, rx) = oneshot::channel::<()>();
            let dsn = dsn.clone();
            let project = self.project.to_string();
            let holder = lock_holder.clone();
            // CORR-3 (v0.19): Clone lock_lost_tx so the panic supervisor can
            // signal lock loss independently of the heartbeat task itself.
            let lock_lost_for_supervisor = lock_lost_tx.clone();
            let heartbeat_sql = for_schema(HEARTBEAT_LOCK_SQL, &self.catalog_schema);
            let heartbeat_handle = tokio::spawn(run_heartbeat(
                dsn,
                project,
                holder,
                heartbeat_sql,
                rx,
                lock_lost_tx,
            ));
            // CORR-3 (v0.19): Supervisor task catches panics in the heartbeat.
            // If the heartbeat task panics (JoinError::is_panic() == true),
            // the supervisor signals LockLost so the main executor loop aborts
            // at the next step boundary rather than continuing with a dead heartbeat.
            tokio::spawn(async move {
                if let Err(join_err) = heartbeat_handle.await {
                    if join_err.is_panic() {
                        tracing::error!(
                            "Heartbeat task panicked — signalling LockLost to abort executor"
                        );
                        let _ = lock_lost_for_supervisor.send(true);
                    }
                }
            });
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

        // ARCH-3 / v0.15: Track the active blue/green deployment id so that
        // SwapConsumerViews and RetireBlueSchema can write status transitions.
        let mut active_bg_deployment_id: Option<i64> = None;

        // CORR-6 / v0.15: RLS policy cache — populated before DropStreamTable,
        // consumed by RecreatePolicy. Keyed by "{schema}.{table}".
        let mut rls_policy_cache: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        let result: Result<u64> = async {
            for (step_idx, step) in plan.steps.iter().enumerate() {
                // CORR-2 / v0.15: force-skip marks the step complete and skips execution.
                if self.force_skip_step == Some(step_idx) {
                    tracing::info!("Force-skip: skipping step {} ({})", step_idx, step.description());
                    continue;
                }

                // CORR-2 / v0.15: force-retry overrides resume — don't skip the target step.
                let is_force_retry = self.force_retry_step == Some(step_idx);

                // S-01/S-02: LockDag ALWAYS re-executes, even when resuming.
                // All other steps are skipped if already completed.
                let is_lock_dag = matches!(step, PlanStep::LockDag { .. });
                if !is_lock_dag && !is_force_retry && step_idx < resume_from {
                    tracing::debug!("Resume: skipping step {} (already completed)", step_idx);
                    continue;
                }

                // CORR-3: Check if the heartbeat has detected lock loss before
                // executing each step. Abort immediately if the lock is gone.
                if *lock_lost_rx.borrow() {
                    return Err(AqueductError::LockLost);
                }

                tracing::debug!("Executing step {}: {}", step_idx, step.description());

                // M-08: Record migration step start (best-effort).
                let step_type = step_type_name(step);
                let step_hash = step_hash_hex(step_idx, step_type);
                let _ = self
                    .client
                    .execute(
                        &self.schema_sql(START_MIGRATION_STEP_SQL),
                        &[&migration_id, &(step_idx as i32), &step_type, &step_hash],
                    )
                    .await;

                // v0.17: Emit per-step JSON event to stderr (structured observability).
                let step_start_ts = std::time::Instant::now();
                let step_start_event = serde_json::json!({
                    "schema_version": 1,
                    "event": "step_start",
                    "step_index": step_idx,
                    "step_type": step_type,
                    "migration_id": migration_id,
                    "project": self.project,
                });
                eprintln!("{}", serde_json::to_string(&step_start_event).unwrap_or_default());

                let step_result: Result<()> = async {
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
                        // CORR-2 / v0.15: Record compensating action before DDL executes.
                        // The compensating action for CreateStreamTable is to drop the table.
                        let compensating = format!(
                            "SELECT pgtrickle.drop_stream_table('{}', '{}')",
                            spec.qualified_name.schema.replace('\'', "''"),
                            spec.qualified_name.name.replace('\'', "''")
                        );
                        let _ = self
                            .client
                            .execute(
                                &self.schema_sql(INSERT_COMPENSATING_STEP_SQL),
                                &[
                                    &migration_id,
                                    &"stream_table",
                                    &spec.qualified_name.schema,
                                    &spec.qualified_name.name,
                                    &"CREATE_STREAM_TABLE",
                                    &compensating,
                                ],
                            )
                            .await;
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
                        // CORR-2 / v0.20: Mark compensating step as 'completed' — DDL succeeded.
                        let _ = self
                            .client
                            .execute(
                                &self.schema_sql(COMPLETE_COMPENSATING_STEP_SQL),
                                &[&migration_id, &spec.qualified_name.name],
                            )
                            .await;
                        // C-06: Register ownership so destroy_project can scope drops.
                        // Best-effort: table may not exist on old catalogs.
                        let _ = self
                            .client
                            .execute(
                                &self.schema_sql(REGISTER_OWNERSHIP_SQL),
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
                        // CORR-6 / v0.15: Capture RLS policies before dropping the table.
                        // Store them in rls_policy_cache so RecreatePolicy can execute them.
                        let table_key = format!("{}.{}", name.schema, name.name);
                        let policies = capture_rls_policies(self.client, &name.schema, &name.name).await;
                        if !policies.is_empty() {
                            tracing::info!(
                                "Captured {} RLS polic(ies) for '{}'",
                                policies.len(),
                                table_key
                            );
                            rls_policy_cache.insert(table_key.clone(), policies);
                        }

                        // CORR-2 / v0.15: Record compensating action (recreate) before drop.
                        let compensating = format!(
                            "-- pgtrickle.create_stream_table() call for '{}.{}' would go here",
                            name.schema.replace('\'', "''"),
                            name.name.replace('\'', "''")
                        );
                        let _ = self
                            .client
                            .execute(
                                &self.schema_sql(INSERT_COMPENSATING_STEP_SQL),
                                &[
                                    &migration_id,
                                    &"stream_table",
                                    &name.schema,
                                    &name.name,
                                    &"DROP_STREAM_TABLE",
                                    &compensating,
                                ],
                            )
                            .await;

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
                        // CORR-2 / v0.20: Mark compensating step as 'completed' — DDL succeeded.
                        let _ = self
                            .client
                            .execute(
                                &self.schema_sql(COMPLETE_COMPENSATING_STEP_SQL),
                                &[&migration_id, &name.name],
                            )
                            .await;
                        // C-06: Deregister ownership. Best-effort: table may not
                        // exist on old catalogs.
                        let _ = self
                            .client
                            .execute(
                                &self.schema_sql(DEREGISTER_OWNERSHIP_SQL),
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
                                &self.schema_sql(INSERT_DAG_VERSION_SQL),
                                &[
                                    &self.project,
                                    &spec_hash,
                                    &applied_by,
                                    &plan_json,
                                    &spec_json,
                                ],
                            )
                            .await?;
                        // If &self.schema_sql(INSERT_DAG_VERSION_SQL) returns the version, capture it.
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
                                .execute(&self.schema_sql(RELEASE_LOCK_SQL), &[&self.project, &lock_holder])
                                .await
                                .ok();
                            locked = false;
                        }
                    }

                    // ── v0.3: Blue/Green steps ──────────────────────────────

                    // ARCH-3 / v0.15: Write deployment start row and capture id.
                    PlanStep::StartBlueGreenDeployment { project, green_schema, blue_schema } => {
                        tracing::info!(
                            "Starting blue/green deployment for '{}': {} → {}",
                            project, blue_schema, green_schema
                        );
                        let from_v = plan.from_version.map(|v| v as i64);
                        let row = self
                            .client
                            .query_opt(
                                &self.schema_sql(START_BLUE_GREEN_SQL),
                                &[project, &from_v, blue_schema, green_schema],
                            )
                            .await?;
                        if let Some(r) = row {
                            active_bg_deployment_id = Some(r.get::<_, i64>(0));
                        }
                    }

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
                        max_wait_secs,
                        poll_interval_ms,
                    } => {
                        tracing::info!(
                            "Waiting for green schema '{}' to converge ({} nodes, max {}s)",
                            green_schema,
                            node_names.len(),
                            max_wait_secs
                        );
                        // PERF-1 / v0.15: Replace per-node polling with a single
                        // ANY($2) batch query, comparing in memory.
                        // M-9 (v0.19): Wrap the poll query in a statement_timeout so
                        // a hung pg_trickle query cannot hold the loop open past the
                        // configured deadline. The deadline check is AFTER the sleep
                        // to avoid the zero-deadline race.
                        if pgtrickle_caps.installed && pgtrickle_caps.has_refresh_status && !node_names.is_empty() {
                            let deadline = std::time::Instant::now()
                                + std::time::Duration::from_secs(*max_wait_secs);
                            let poll_ms = *poll_interval_ms;
                            // Per-query timeout: poll_interval + 50% headroom, minimum 500ms.
                            let query_timeout_ms = poll_ms.saturating_add(poll_ms / 2).max(500);
                            loop {
                                // M-9: Run poll query inside a read-only transaction with
                                // statement_timeout so a slow pg_trickle query cannot stall
                                // the loop past the configured deadline.
                                let setup = self
                                    .client
                                    .batch_execute(&format!(
                                        "BEGIN READ ONLY; \
                                         SET LOCAL statement_timeout = '{query_timeout_ms}ms'"
                                    ))
                                    .await;
                                let still_running = if setup.is_err() {
                                    // Cannot set up read-only transaction — treat as running.
                                    true
                                } else {
                                    let rows = self
                                        .client
                                        .query(
                                            "SELECT table_name, refresh_status \
                                             FROM pgtrickle.pgt_stream_tables \
                                             WHERE schema_name = $1 AND table_name = ANY($2)",
                                            &[green_schema, node_names],
                                        )
                                        .await;
                                    let _ = self.client.batch_execute("COMMIT").await;
                                    match rows {
                                        Ok(rows) => rows.iter().any(|r| {
                                            let status: Option<String> =
                                                r.try_get(1).ok().flatten();
                                            status.as_deref() == Some("running")
                                        }),
                                        // On statement_timeout or error, treat as still running.
                                        Err(_) => true,
                                    }
                                };
                                if !still_running {
                                    break;
                                }
                                // M-9: Sleep first, then check deadline — avoids the
                                // zero-deadline race where a 0s deadline would pass
                                // immediately without ever polling.
                                tokio::time::sleep(
                                    std::time::Duration::from_millis(poll_ms)
                                ).await;
                                if std::time::Instant::now() >= deadline {
                                    return Err(AqueductError::Other(format!(
                                        "WaitForConvergence: green schema '{}' did not converge within {}s",
                                        green_schema, max_wait_secs
                                    )));
                                }
                            }
                        }
                    }

                    PlanStep::SwapConsumerViews {
                        assignments,
                        green_schema,
                        ..
                    } => {
                        tracing::info!(
                            "Swapping {} consumer views → green schema '{}' (atomic transaction)",
                            assignments.len(),
                            green_schema
                        );
                        // ARCH-3 / v0.15: All consumer view swaps execute in a single
                        // transaction so the swap is all-or-nothing. On failure the
                        // transaction rolls back, leaving all views pointing to the
                        // previous schema.
                        self.client.execute("BEGIN", &[]).await?;
                        let swap_result: Result<()> = async {
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
                                // SEC-1 (v0.18): Validate sql_body is a single SELECT
                                // before any database call is made.
                                validate_consumer_sql_is_single_select(
                                    sql_body,
                                    &format!("{}.{}", view_schema, view_name),
                                )?;
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
                            // ARCH-3 (v0.19): Write 'swapped' status to deployment row
                            // INSIDE the transaction so the view swap and the status
                            // update are atomic. Previously this was after COMMIT, which
                            // created a window where views pointed to green but the
                            // deployment row still showed 'active'.
                            if let Some(deploy_id) = active_bg_deployment_id {
                                let retain = 3600i64;
                                let _ = self
                                    .client
                                    .execute(
                                        &self.schema_sql(SWAP_BLUE_GREEN_SQL),
                                        &[&deploy_id, &Option::<i64>::None, &retain.to_string()],
                                    )
                                    .await;
                            }
                            Ok(())
                        }
                        .await;
                        match swap_result {
                            Ok(()) => {
                                self.client.execute("COMMIT", &[]).await?;
                            }
                            Err(e) => {
                                self.client.execute("ROLLBACK", &[]).await.ok();
                                return Err(e);
                            }
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
                        // ARCH-3 / v0.15: Write 'retired' status to deployment row.
                        if let Some(deploy_id) = active_bg_deployment_id {
                            let _ = self
                                .client
                                .execute(&self.schema_sql(RETIRE_BLUE_GREEN_SQL), &[&deploy_id])
                                .await;
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
                                // CORR-7: Delete catalog row so status drift counts are accurate.
                                let _ = self
                                    .client
                                    .execute(
                                        &self.schema_sql(DELETE_CONSUMER_VIEW_SQL),
                                        &[&self.project, &spec.name],
                                    )
                                    .await;
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
                                // SEC-1 (v0.18): Validate sql_body is a single SELECT
                                // before any database call is made.
                                validate_consumer_sql_is_single_select(
                                    body,
                                    &format!("{}.{}", view_schema, view_name),
                                )?;
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
                                        &self.schema_sql(crate::catalog::UPSERT_CONSUMER_VIEW_SQL),
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
                        tracing::info!("Recreating RLS policy on '{}'", name);
                        // CORR-6 / v0.15: Use captured RLS policies from the cache.
                        // If the cache has entries for this table, execute them.
                        // Otherwise fall back to the policy_sql from the plan step.
                        let table_key = format!("{}.{}", name.schema, name.name);
                        let stmts: Vec<String> = rls_policy_cache
                            .get(&table_key)
                            .cloned()
                            .unwrap_or_else(|| {
                                if policy_sql.starts_with("--") {
                                    // Placeholder — no policies were captured; skip.
                                    vec![]
                                } else {
                                    vec![policy_sql.clone()]
                                }
                            });
                        for stmt in &stmts {
                            if let Err(e) = self.client.execute(stmt.as_str(), &[]).await {
                                // Non-fatal: log to ddl_log and continue.
                                tracing::warn!(
                                    "RecreatePolicy: failed to execute '{}' on '{}': {} (non-fatal)",
                                    stmt,
                                    table_key,
                                    e
                                );
                                let _ = self
                                    .client
                                    .execute(
                                        &self.schema_sql(INSERT_COMPENSATING_STEP_SQL),
                                        &[
                                            &migration_id,
                                            &"rls_policy",
                                            &name.schema,
                                            &name.name,
                                            &"RECREATE_POLICY_FAILED",
                                            &format!("-- failed: {}: {}", stmt, e),
                                        ],
                                    )
                                    .await;
                            }
                        }
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
                Ok(())
                }.await;

                // Compute duration and extract error message before consuming step_result.
                let duration_ms = step_start_ts.elapsed().as_millis() as u64;
                let step_err_msg: Option<String> =
                    step_result.as_ref().err().map(|e| e.to_string());

                // v0.17: Emit step_complete / step_failed event to stderr, update DB.
                if let Some(ref err_str) = step_err_msg {
                    let step_failed_event = serde_json::json!({
                        "schema_version": 1,
                        "event": "step_failed",
                        "step_index": step_idx,
                        "step_type": step_type,
                        "migration_id": migration_id,
                        "project": self.project,
                        "duration_ms": duration_ms,
                        "error": err_str,
                    });
                    eprintln!(
                        "{}",
                        serde_json::to_string(&step_failed_event).unwrap_or_default()
                    );
                    // M-08: Mark step as failed with error message (best-effort).
                    let _ = self
                        .client
                        .execute(
                            &self.schema_sql(FAIL_MIGRATION_STEP_SQL),
                            &[&migration_id, &(step_idx as i32), err_str],
                        )
                        .await;
                } else {
                    let step_done_event = serde_json::json!({
                        "schema_version": 1,
                        "event": "step_complete",
                        "step_index": step_idx,
                        "step_type": step_type,
                        "migration_id": migration_id,
                        "project": self.project,
                        "duration_ms": duration_ms,
                    });
                    eprintln!(
                        "{}",
                        serde_json::to_string(&step_done_event).unwrap_or_default()
                    );
                    // M-08: Mark step completed (best-effort).
                    let _ = self
                        .client
                        .execute(
                            &self.schema_sql(FINISH_MIGRATION_STEP_SQL),
                            &[&migration_id, &(step_idx as i32)],
                        )
                        .await;
                }

                // Propagate error (consumes step_result, no Clone needed).
                step_result?;

                // S-06: Checkpoint writes are fatal — a failed progress record
                // means we cannot safely resume.
                self.client
                    .execute(
                        &self.schema_sql(UPDATE_MIGRATION_PROGRESS_SQL),
                        &[
                            &migration_id,
                            &serde_json::json!({ "completed_steps": step_idx }),
                        ],
                    )
                    .await?;

                // CORR-3: Check lock-loss again after the step completes.
                if *lock_lost_rx.borrow() {
                    return Err(AqueductError::LockLost);
                }

                // DOC-2 / v0.17: Between-step primary re-check via pg_is_in_recovery()
                // and optional Patroni endpoint check.
                crate::ha::check_still_primary(self.client).await?;
                if let Some(ref endpoint) = self.patroni_endpoint {
                    crate::ha::check_patroni_primary(endpoint).await?;
                }
            }

            // S-05: Ensure lock is released on the success path too.
            if locked {
                self.client
                    .execute(&self.schema_sql(RELEASE_LOCK_SQL), &[&self.project, &lock_holder])
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
                .execute(
                    &self.schema_sql(RELEASE_LOCK_SQL),
                    &[&self.project, &lock_holder],
                )
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
        // Suppress unused warning for lock_lost_rx when no heartbeat was started.
        drop(lock_lost_rx);

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
            .query(
                &self.schema_sql(ACQUIRE_LOCK_SQL),
                &[&project, &holder, &ttl],
            )
            .await?;

        if rows.is_empty() {
            let lock_row = self
                .client
                .query_opt(
                    &for_schema(
                        "SELECT holder FROM aqueduct.locks WHERE project = $1",
                        &self.catalog_schema,
                    ),
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
/// CORR-3: When the lock row disappears (rows_affected == 0), the task sets
/// `lock_lost_tx` to `true` so the main executor loop can abort with LockLost
/// at the next step boundary instead of silently continuing.
async fn run_heartbeat(
    dsn: String,
    project: String,
    holder: String,
    heartbeat_sql: String,
    mut cancel_rx: oneshot::Receiver<()>,
    lock_lost_tx: watch::Sender<bool>,
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
                match client.execute(&heartbeat_sql, &[&project, &holder]).await {
                    Err(e) => {
                        tracing::warn!("Heartbeat: lock renewal failed: {}", e);
                    }
                    Ok(0) => {
                        // Lock row is gone — stolen or expired. Signal the main
                        // executor loop to abort with LockLost (CORR-3 / v0.14).
                        tracing::warn!(
                            "Heartbeat: lock for project '{}' no longer held by '{}' — signalling LockLost",
                            project, holder
                        );
                        let _ = lock_lost_tx.send(true);
                        return;
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

/// Capture RLS policies for a table before dropping it (CORR-6 / v0.15).
///
/// Queries `pg_policies` and returns a list of `CREATE POLICY` and
/// `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` SQL statements. Returns an empty
/// vec if the table has no RLS policies or if `pg_policies` is not accessible.
async fn capture_rls_policies(
    client: &tokio_postgres::Client,
    schema_name: &str,
    table_name: &str,
) -> Vec<String> {
    let mut stmts: Vec<String> = Vec::new();

    // Check if the table has RLS enabled.
    let rls_enabled: bool = client
        .query_one(
            "SELECT c.relrowsecurity
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema_name, &table_name],
        )
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false);

    if rls_enabled {
        stmts.push(format!(
            "ALTER TABLE {}.{} ENABLE ROW LEVEL SECURITY",
            quote_ident(schema_name),
            quote_ident(table_name)
        ));
    }

    // Query pg_policies for all policies on this table.
    let policies = client
        .query(
            "SELECT policyname, cmd, qual, with_check, roles
             FROM pg_policies
             WHERE schemaname = $1 AND tablename = $2",
            &[&schema_name, &table_name],
        )
        .await
        .unwrap_or_default();

    for row in policies {
        let policy_name: String = row.get(0);
        let cmd: String = row.try_get(1).unwrap_or_else(|_| "ALL".to_string());
        let qual: Option<String> = row.try_get(2).ok().flatten();
        let with_check: Option<String> = row.try_get(3).ok().flatten();
        let roles: Vec<String> = row
            .try_get::<_, Vec<String>>(4)
            .unwrap_or_else(|_| vec!["PUBLIC".to_string()]);

        let mut policy_sql = format!(
            "CREATE POLICY {} ON {}.{} FOR {} TO {}",
            quote_ident(&policy_name),
            quote_ident(schema_name),
            quote_ident(table_name),
            cmd,
            roles.join(", ")
        );
        if let Some(q) = qual {
            policy_sql.push_str(&format!(" USING ({})", q));
        }
        if let Some(wc) = with_check {
            policy_sql.push_str(&format!(" WITH CHECK ({})", wc));
        }
        stmts.push(policy_sql);
    }

    stmts
}

/// Determine whether an error represents an HA failover (DOC-2 / v0.17).
///
/// Returns `true` when the error message indicates that the connected host
/// is no longer the primary — either via `pg_is_in_recovery()` returning true
/// (NotPrimary / check_still_primary) or a failed Patroni `/master` HTTP check.
fn is_ha_failover_error(e: &AqueductError) -> bool {
    match e {
        AqueductError::NotPrimary => true,
        AqueductError::Other(msg) => msg.contains("pg_is_in_recovery") || msg.contains("failover"),
        _ => false,
    }
}

/// Return a short string tag for a plan step (used in migration_steps tracking).
fn step_type_name(step: &PlanStep) -> &'static str {
    match step {
        PlanStep::LockDag { .. } => "LockDag",
        PlanStep::UnlockDag { .. } => "UnlockDag",
        PlanStep::ValidateQuery { .. } => "ValidateQuery",
        PlanStep::AlterBaseTable { .. } => "AlterBaseTable",
        PlanStep::CreateStreamTable { .. } => "CreateStreamTable",
        PlanStep::AlterStreamTable { .. } => "AlterStreamTable",
        PlanStep::DropStreamTable { .. } => "DropStreamTable",
        PlanStep::Backfill { .. } => "Backfill",
        PlanStep::RecordSnapshot { .. } => "RecordSnapshot",
        PlanStep::CreateGreenSchema { .. } => "CreateGreenSchema",
        PlanStep::CreateStreamTableInGreen { .. } => "CreateStreamTableInGreen",
        PlanStep::WaitForConvergence { .. } => "WaitForConvergence",
        PlanStep::SwapConsumerViews { .. } => "SwapConsumerViews",
        PlanStep::RetireBlueSchema { .. } => "RetireBlueSchema",
        PlanStep::StartBlueGreenDeployment { .. } => "StartBlueGreenDeployment",
        PlanStep::ManageConsumerView { .. } => "ManageConsumerView",
        PlanStep::RecreatePolicy { .. } => "RecreatePolicy",
        PlanStep::DetachOutbox { .. } => "DetachOutbox",
        PlanStep::ReattachOutbox { .. } => "ReattachOutbox",
        PlanStep::ManageWalSlot { .. } => "ManageWalSlot",
        PlanStep::WaitForRefresh { .. } => "WaitForRefresh",
        PlanStep::PauseImmediate { .. } => "PauseImmediate",
        PlanStep::ResumeImmediate { .. } => "ResumeImmediate",
        PlanStep::RunHook { .. } => "RunHook",
    }
}

/// Compute a simple hex hash for a step (used as an idempotency key).
fn step_hash_hex(step_idx: usize, step_type: &str) -> String {
    use sha2::Digest;
    let input = format!("{}:{}", step_idx, step_type);
    format!("{:x}", sha2::Sha256::digest(input.as_bytes()))
}
/// Import stream tables from the live pg_trickle catalog into a migrations directory.
pub async fn import_from_live(
    client: &tokio_postgres::Client,
    project: &str,
    output_dir: &std::path::Path,
    exclude_patterns: &[String],
    catalog_schema: &crate::catalog::CatalogSchema,
) -> Result<usize> {
    use crate::live_state::read_live_state;

    // Import reads ALL stream tables visible on the database — no ownership
    // filtering at this stage since we are bootstrapping a new project.
    let state = read_live_state(client, None, catalog_schema).await?;
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

    // M-06: Record an initial baseline snapshot (version 1) in the catalog so
    // that a subsequent `aqueduct plan` returns an empty plan.
    // Best-effort: if the catalog tables don't exist yet, skip silently.
    if let Err(e) = record_import_baseline(client, project, &state, catalog_schema).await {
        tracing::warn!(
            "Could not record import baseline snapshot (non-fatal): {}",
            e
        );
    }

    Ok(count)
}

/// M-06: Write an initial baseline snapshot (version 1) into aqueduct.dag_versions
/// so that `aqueduct plan` returns an empty plan immediately after `import`.
async fn record_import_baseline(
    client: &tokio_postgres::Client,
    project: &str,
    state: &crate::dag::DagState,
    catalog_schema: &crate::catalog::CatalogSchema,
) -> Result<()> {
    use sha2::Digest;

    // Ensure the catalog schema is bootstrapped.
    crate::catalog::ensure_catalog_current(client, catalog_schema).await?;

    // Serialize the live state as our desired spec snapshot.
    let spec_json = serde_json::to_value(state).map_err(crate::error::AqueductError::Json)?;
    let spec_hash: Vec<u8> = sha2::Sha256::digest(spec_json.to_string().as_bytes()).to_vec();
    let applied_by = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "aqueduct-import".to_string());
    let plan_json = serde_json::json!({
        "source": "import",
        "steps": []
    });

    client
        .execute(
            &for_schema(INSERT_DAG_VERSION_SQL, catalog_schema),
            &[&project, &spec_hash, &applied_by, &plan_json, &spec_json],
        )
        .await?;

    tracing::info!(
        "Recorded import baseline snapshot for project '{}' in aqueduct.dag_versions",
        project
    );
    Ok(())
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
