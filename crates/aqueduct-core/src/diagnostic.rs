//! Unified diagnostic type used across parse, validate, lint, and plan preflight (Q-08).
//!
//! All four subsystems (parser, validate, lint, plan) emit diagnostics through this
//! single type so that the CLI can render a consistent, location-aware diagnostic list
//! regardless of which phase produced the message.

use std::path::PathBuf;

/// Severity of a diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
}

impl std::fmt::Display for DiagnosticSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiagnosticSeverity::Error => write!(f, "error"),
            DiagnosticSeverity::Warning => write!(f, "warning"),
            DiagnosticSeverity::Info => write!(f, "info"),
        }
    }
}

/// A single diagnostic message with optional source location.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    /// Severity level.
    pub severity: DiagnosticSeverity,
    /// Source file that triggered the diagnostic (if applicable).
    pub file: Option<PathBuf>,
    /// 1-based line number within the file (if known).
    pub line: Option<u32>,
    /// 1-based column number within the line (if known).
    pub column: Option<u32>,
    /// Machine-readable diagnostic code (e.g. "E001", "W042", "L001").
    pub code: String,
    /// Human-readable description.
    pub message: String,
    /// Optional additional context or remediation hint.
    pub hint: Option<String>,
}

impl Diagnostic {
    /// Create an error-severity diagnostic.
    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: DiagnosticSeverity::Error,
            file: None,
            line: None,
            column: None,
            code: code.into(),
            message: message.into(),
            hint: None,
        }
    }

    /// Create a warning-severity diagnostic.
    pub fn warning(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: DiagnosticSeverity::Warning,
            file: None,
            line: None,
            column: None,
            code: code.into(),
            message: message.into(),
            hint: None,
        }
    }

    /// Create an info-severity diagnostic.
    pub fn info(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            severity: DiagnosticSeverity::Info,
            file: None,
            line: None,
            column: None,
            code: code.into(),
            message: message.into(),
            hint: None,
        }
    }

    /// Attach a file location to this diagnostic.
    pub fn with_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.file = Some(path.into());
        self
    }

    /// Attach line and column to this diagnostic.
    pub fn with_location(mut self, line: u32, column: u32) -> Self {
        self.line = Some(line);
        self.column = Some(column);
        self
    }

    /// Attach only a line number (column unknown).
    pub fn with_line(mut self, line: u32) -> Self {
        self.line = Some(line);
        self
    }

    /// Attach a remediation hint.
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Format this diagnostic in the standard `file:line:col: severity[code]: message` format.
    pub fn render(&self) -> String {
        let mut out = String::new();

        if let Some(ref f) = self.file {
            out.push_str(&f.display().to_string());
            if let Some(l) = self.line {
                out.push_str(&format!(":{}", l));
                if let Some(c) = self.column {
                    out.push_str(&format!(":{}", c));
                }
            }
            out.push_str(": ");
        }

        out.push_str(&format!(
            "{}[{}]: {}",
            self.severity, self.code, self.message
        ));

        if let Some(ref h) = self.hint {
            out.push_str(&format!("\n  hint: {}", h));
        }

        out
    }
}

impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.render())
    }
}

/// A collection of diagnostics emitted by a subsystem.
#[derive(Debug, Default)]
pub struct DiagnosticSet {
    pub diagnostics: Vec<Diagnostic>,
}

impl DiagnosticSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }

    pub fn error(&mut self, code: impl Into<String>, message: impl Into<String>) {
        self.push(Diagnostic::error(code, message));
    }

    pub fn warning(&mut self, code: impl Into<String>, message: impl Into<String>) {
        self.push(Diagnostic::warning(code, message));
    }

    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity == DiagnosticSeverity::Error)
    }

    pub fn has_warnings(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity == DiagnosticSeverity::Warning)
    }

    pub fn is_clean(&self) -> bool {
        self.diagnostics.is_empty()
    }

    pub fn errors(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == DiagnosticSeverity::Error)
    }

    pub fn warnings(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics
            .iter()
            .filter(|d| d.severity == DiagnosticSeverity::Warning)
    }

    /// Render all diagnostics sorted by severity (errors first).
    pub fn render_all(&self) -> String {
        let mut sorted = self.diagnostics.clone();
        sorted.sort_by(|a, b| a.severity.cmp(&b.severity));
        sorted
            .iter()
            .map(|d| d.render())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diagnostic_render_with_location() {
        let d = Diagnostic::error("E001", "syntax error near 'FROM'")
            .with_file("migrations/streams/orders.sql")
            .with_location(12, 5)
            .with_hint("Check the SQL syntax at this position");
        let rendered = d.render();
        assert!(rendered.contains("migrations/streams/orders.sql:12:5"));
        assert!(rendered.contains("error[E001]"));
        assert!(rendered.contains("syntax error near 'FROM'"));
    }

    #[test]
    fn test_diagnostic_set_has_errors() {
        let mut set = DiagnosticSet::new();
        set.warning("W001", "aggressive schedule");
        assert!(!set.has_errors());
        set.error("E001", "cycle detected");
        assert!(set.has_errors());
    }
}
