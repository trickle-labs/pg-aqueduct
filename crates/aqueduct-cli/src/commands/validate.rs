use aqueduct_core::{
    dag::build_dag_state,
    diagnostic::DiagnosticSet,
    parser::load_migrations,
    validate::{validate_dag_diagnostic, validate_migration_files_diagnostic},
};
use clap::Args;
use std::collections::HashMap;

#[derive(Debug, Args)]
pub struct ValidateArgs {
    /// Project directory.
    #[arg(long, default_value = ".")]
    pub project_dir: std::path::PathBuf,

    /// Output format: text or json.
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Treat warnings as errors and exit non-zero if any are present.
    #[arg(long)]
    pub strict: bool,
}

pub async fn run(args: ValidateArgs) -> anyhow::Result<()> {
    let files = load_migrations(&args.project_dir, &HashMap::new())?;

    // File-level validation — returns structured DiagnosticSet (M6 / v0.15).
    let mut diagnostics: DiagnosticSet = validate_migration_files_diagnostic(&files);

    // DAG-level validation — also returns DiagnosticSet.
    let state = build_dag_state(&files, true)?;
    let dag_diagnostics = validate_dag_diagnostic(&state);
    for d in dag_diagnostics.diagnostics {
        diagnostics.push(d);
    }

    let total_files = files.len();
    let errors: Vec<_> = diagnostics.errors().collect();
    let warnings: Vec<_> = diagnostics.warnings().collect();

    match args.format.as_str() {
        "json" => {
            // M6 / v0.15: Emit structured diagnostics matching the lint JSON schema.
            let diag_json: Vec<serde_json::Value> = diagnostics
                .diagnostics
                .iter()
                .map(|d| {
                    serde_json::json!({
                        "severity": d.severity.to_string(),
                        "code": d.code,
                        "message": d.message,
                        "file": d.file.as_ref().map(|f| f.display().to_string()),
                        "line": d.line,
                        "hint": d.hint,
                    })
                })
                .collect();
            let output = serde_json::json!({
                "schema_version": 1,
                "files": total_files,
                "diagnostics": diag_json,
                "errors": errors.len(),
                "warnings": warnings.len(),
                "ok": errors.is_empty(),
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        _ => {
            println!(
                "Validated {} migration file{}.",
                total_files,
                if total_files == 1 { "" } else { "s" }
            );
            for d in &diagnostics.diagnostics {
                println!("  {}", d.render());
            }
            if errors.is_empty() && (!args.strict || warnings.is_empty()) {
                println!("✓ All checks passed.");
            }
        }
    }

    if !errors.is_empty() {
        anyhow::bail!(
            "Validation failed with {} error{}.",
            errors.len(),
            if errors.len() == 1 { "" } else { "s" }
        );
    }

    // In --strict mode, warnings are treated as errors.
    if args.strict && !warnings.is_empty() {
        anyhow::bail!(
            "Strict mode: validation failed with {} warning{} (treated as errors).",
            warnings.len(),
            if warnings.len() == 1 { "" } else { "s" }
        );
    }

    Ok(())
}
