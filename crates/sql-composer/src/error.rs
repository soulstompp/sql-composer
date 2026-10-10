//! Error types for sql-composer.

use std::path::PathBuf;

use crate::types::Dialect;

/// The error type for sql-composer operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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

    /// A binding was given a number of values outside what its `EXPECTING` says.
    #[error("binding '{name}' was given {given} value(s), and expects {expected}")]
    ValueCount {
        /// The name of the binding.
        name: String,
        /// How many values it was given.
        given: usize,
        /// What its `EXPECTING` says: `2..3`, or `at least 2`.
        expected: String,
    },

    /// A binding written without `NULL` was given a NULL.
    #[error("binding '{name}' was given a NULL, and its :bind does not say NULL")]
    NullValue {
        /// The name of the binding.
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

    /// A slot referenced in a template was not provided by the caller.
    #[error("missing slot '@{name}' in {template} — not provided by caller")]
    MissingSlot {
        /// The name of the missing slot.
        name: String,
        /// The template the slot stands in: its path, or the name a literal template was given.
        template: String,
    },

    /// A template registered as a view takes an argument, which a view cannot.
    #[error("{path} is registered as the view {view} and {reason}")]
    ViewTakesArguments {
        /// The registered template.
        path: PathBuf,
        /// The view it was registered as.
        view: String,
        /// What it takes: a bound parameter or an open slot.
        reason: String,
    },

    /// A relation's name, taken from its file, is a word the database reserves.
    #[error("{path} would be named {name}, which is a reserved word")]
    ReservedName {
        /// The template the name was taken from.
        path: PathBuf,
        /// The name.
        name: String,
    },

    /// Two templates a statement reads take the same name from their files.
    #[error("{first} and {second} would both be named {name}")]
    NameTaken {
        /// The name.
        name: String,
        /// The template that took it first.
        first: PathBuf,
        /// The template that would take it again.
        second: PathBuf,
    },

    /// A `:define` met while the composer names every relation itself.
    #[error("{path} is composed with :define, and the composer is naming every relation itself")]
    DefinitionWhileNaming {
        /// The template the `:define` puts in place.
        path: PathBuf,
    },

    /// A template composed for a dialect its `:dialect(…)` does not name.
    #[error("{template} is written for {written}, and is composed for {composed}", written = crate::types::listed(.declared.iter().map(Dialect::name)), composed = .dialect.name())]
    NotWrittenFor {
        /// The template: its path, or the name a literal template was given.
        template: String,
        /// The dialects it declares.
        declared: Vec<Dialect>,
        /// The dialect it was composed for.
        dialect: Dialect,
    },

    /// A manifest that could not be written or read as JSON.
    #[cfg(feature = "serde")]
    #[error("manifest: {0}")]
    Manifest(#[from] serde_json::Error),

    /// A name that is no dialect's.
    #[error("{name} is not a dialect: postgres, mysql, mariadb, sqlite or duckdb")]
    UnknownDialect {
        /// The name given.
        name: String,
    },

    /// A `:bind` composed into a dollar quote, where no value reaches it.
    #[error("binding '{name}' would stand inside a dollar quote, where no value reaches it")]
    BindInDollarQuote {
        /// The name of the binding.
        name: String,
    },
}

/// A specialized `Result` type for sql-composer operations.
pub type Result<T> = std::result::Result<T, Error>;
