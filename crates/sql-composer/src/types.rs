//! Core types for the sql-composer template AST.

use std::path::PathBuf;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// A parsed template consisting of a sequence of literal SQL and macro invocations.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Template {
    /// The ordered elements that make up this template.
    pub elements: Vec<Element>,
    /// Where this template originated from.
    pub source: TemplateSource,
}

/// The origin of a template.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TemplateSource {
    /// Loaded from a file at the given path.
    File(PathBuf),
    /// Parsed from an inline string literal.
    Literal(String),
}

/// A single element in a template.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Element {
    /// Raw SQL text passed through unchanged.
    Sql(String),
    /// `:bind(name ...)` - a parameter placeholder.
    Bind(Binding),
    /// `:compose(path)` - include another template.
    Compose(ComposeRef),
    /// `:count(...)` or `:union(...)` - an aggregate command.
    Command(Command),
}

/// A parameter binding parsed from `:bind(name ...)`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Binding {
    /// The name of the bind parameter.
    pub name: String,
    /// Minimum number of values expected (from `EXPECTING min`).
    pub min_values: Option<u32>,
    /// Maximum number of values expected (from `EXPECTING min..max`).
    pub max_values: Option<u32>,
    /// Whether this binding accepts NULL (from `NULL` keyword).
    pub nullable: bool,
}

/// A compose reference parsed from `:compose(path)`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ComposeRef {
    /// The path to the template to include.
    pub path: PathBuf,
}

/// An aggregate command parsed from `:count(...)` or `:union(...)`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Command {
    /// The kind of command (count or union).
    pub kind: CommandKind,
    /// Whether the DISTINCT modifier is present.
    pub distinct: bool,
    /// Whether the ALL modifier is present.
    pub all: bool,
    /// Optional column list (from `columns OF`).
    pub columns: Option<Vec<String>>,
    /// Source template paths.
    pub sources: Vec<PathBuf>,
}

/// The kind of aggregate command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CommandKind {
    /// COUNT command - wraps in `SELECT COUNT(*) FROM (...)`.
    Count,
    /// UNION command - combines sources with UNION.
    Union,
}

/// Target database dialect for placeholder syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Dialect {
    /// PostgreSQL: `$1`, `$2`, `$3`
    Postgres,
    /// MySQL: `?`, `?`, `?`
    Mysql,
    /// SQLite: `?1`, `?2`, `?3`
    Sqlite,
}

impl Dialect {
    /// Format a placeholder for the given 1-based parameter index.
    pub fn placeholder(&self, index: usize) -> String {
        match self {
            Dialect::Postgres => format!("${index}"),
            Dialect::Mysql => "?".to_string(),
            Dialect::Sqlite => format!("?{index}"),
        }
    }

    /// Whether this dialect uses numbered placeholders ($1, ?1) vs positional (?).
    ///
    /// Numbered dialects (Postgres, SQLite) support alphabetical parameter ordering
    /// and deduplication. Positional dialects (MySQL) use document-order placeholders.
    pub fn supports_numbered_placeholders(&self) -> bool {
        matches!(self, Dialect::Postgres | Dialect::Sqlite)
    }
}
