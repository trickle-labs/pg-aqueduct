pub mod catalog;
pub mod classifier;
pub mod config;
pub mod cost;
pub mod dag;
pub mod destroy;
pub mod diff;
pub mod error;
pub mod executor;
pub mod fmt;
pub mod ingest;
pub mod lint;
pub mod live_state;
pub mod parser;
pub mod plan;
pub mod preview;
pub mod promote;
pub mod renderer;
pub mod secrets;
pub mod validate;

pub use error::{AqueductError, Result};

/// Current CLI version string, used in migration records.
pub const CLI_VERSION: &str = env!("CARGO_PKG_VERSION");
