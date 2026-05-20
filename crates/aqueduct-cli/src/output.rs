//! `OutputEmitter` — centralised output mode for the aqueduct CLI.
//!
//! Replaces direct `println!`/`eprintln!` calls with a typed emitter so that
//! `--quiet`, `--porcelain`, and `--log-format json` behave consistently
//! across every subcommand (ERG-1, M11).
//!
//! A process-wide singleton is stored in `EMITTER` via [`std::sync::OnceLock`].
//! Command handlers obtain the global instance via [`emitter()`].

use std::sync::OnceLock;

/// Process-wide singleton emitter (M-1 / v0.20).
static EMITTER: OnceLock<OutputEmitter> = OnceLock::new();

/// Initialise the global emitter.  Must be called once in `main` before any
/// command handler runs.  Subsequent calls are silently ignored (OnceLock
/// semantics).
pub fn init_emitter(mode: OutputMode) {
    let _ = EMITTER.set(OutputEmitter::new(mode));
}

/// Return a reference to the global emitter.  Falls back to a [`OutputMode::Human`]
/// emitter if `init_emitter` was never called (e.g. in unit tests).
pub fn emitter() -> &'static OutputEmitter {
    EMITTER.get_or_init(|| OutputEmitter::new(OutputMode::Human))
}

/// Output mode selected by global CLI flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(dead_code)]
pub enum OutputMode {
    /// Human-readable text (default).
    #[default]
    Human,
    /// Every output line is a JSON object matching the published schema.
    Json,
    /// Structured YAML output.
    Yaml,
    /// Suppress all info-level output; only errors are shown.
    Quiet,
    /// Only emit `key=value` lines on stdout; decorative output is suppressed.
    Porcelain,
}

/// A lightweight wrapper that routes structured output through the configured mode.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OutputEmitter {
    pub mode: OutputMode,
}

#[allow(dead_code)]
impl OutputEmitter {
    /// Construct an emitter with the given mode.
    pub fn new(mode: OutputMode) -> Self {
        Self { mode }
    }

    /// Print an info-level message to stdout.
    ///
    /// In `Quiet` mode this is suppressed. In `Porcelain` mode use
    /// [`emit_kv`] instead for structured data.
    pub fn info(&self, msg: &str) {
        if self.mode != OutputMode::Quiet && self.mode != OutputMode::Porcelain {
            println!("{}", msg);
        }
    }

    /// Print a warning to stderr (never suppressed).
    pub fn warn(&self, msg: &str) {
        eprintln!("warning: {}", msg);
    }

    /// Print an error to stderr (never suppressed).
    pub fn error(&self, msg: &str) {
        eprintln!("error: {}", msg);
    }

    /// Emit a `key=value` line on stdout.
    ///
    /// In `Porcelain` mode this is the primary output channel.
    /// In other modes it is emitted as normal info output.
    pub fn emit_kv(&self, key: &str, value: &str) {
        match self.mode {
            OutputMode::Quiet => {}
            OutputMode::Porcelain | OutputMode::Human | OutputMode::Json | OutputMode::Yaml => {
                println!("{}={}", key, value);
            }
        }
    }

    /// Emit raw text directly to stdout without any transformation.
    ///
    /// Used when the caller has already formatted the output (e.g. rendered JSON).
    pub fn raw(&self, text: &str) {
        if self.mode != OutputMode::Quiet {
            println!("{}", text);
        }
    }

    /// Returns `true` if this emitter suppresses decorative (non-error) output.
    pub fn is_quiet(&self) -> bool {
        self.mode == OutputMode::Quiet || self.mode == OutputMode::Porcelain
    }

    /// Returns `true` if only `key=value` structured lines should be emitted.
    pub fn is_porcelain(&self) -> bool {
        self.mode == OutputMode::Porcelain
    }
}
