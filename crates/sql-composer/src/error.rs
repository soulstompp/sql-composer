//! Error types for sql-composer.

use std::path::PathBuf;

/// The error type for sql-composer operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A referenced template file was not found on any search path.
    #[error("template not found: {path}")]
    TemplateNotFound {
        /// The path that was searched for.
        path: PathBuf,
    },

    /// A parse error occurred while processing a template.
    #[error("parse error at {location}: {message}")]
    Parse {
        /// Human-readable location description (e.g. line:col or offset).
        location: String,
        /// Description of the parse failure.
        message: String,
    },

    /// A binding referenced in the template was not provided values.
    #[error("binding '{name}' has no values")]
    MissingBinding {
        /// The name of the missing binding.
        name: String,
    },

    /// A compose reference could not be resolved.
    #[error("compose reference not found: {path}")]
    ComposeNotFound {
        /// The path of the unresolved compose reference.
        path: PathBuf,
    },

    /// A command references sources that could not be resolved.
    #[error("command source not found: {path}")]
    CommandSourceNotFound {
        /// The path of the unresolved source.
        path: PathBuf,
    },

    /// An I/O error occurred while reading a template file.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// A mock table was referenced but not registered.
    #[error("mock table '{name}' not registered")]
    MockNotFound {
        /// The name of the missing mock table.
        name: String,
    },

    /// A circular compose reference was detected.
    #[error("circular compose reference detected: {path}")]
    CircularReference {
        /// The path where the cycle was detected.
        path: PathBuf,
    },
}

/// A specialized `Result` type for sql-composer operations.
pub type Result<T> = std::result::Result<T, Error>;
