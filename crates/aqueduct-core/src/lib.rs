pub mod catalog;
pub mod classifier;
pub mod config;
pub mod dag;
pub mod diff;
pub mod error;
pub mod executor;
pub mod live_state;
pub mod parser;
pub mod plan;
pub mod renderer;
pub mod validate;

pub use error::{AqueductError, Result};

/// Current CLI version string, used in migration records.
pub const CLI_VERSION: &str = env!("CARGO_PKG_VERSION");
