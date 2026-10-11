//! sql-composer composes SQL templates: each query written once, in a file of plain SQL, and put
//! into every statement that needs it, for PostgreSQL, MySQL, MariaDB, SQLite or DuckDB.
//!
//! A template is SQL with directives in it:
//! - `:compose(path)`: the relation in `path`, where the directive stands;
//! - `:compose(path, @slot = other)`: the same, with its slot filled by `other`, and
//!   `:compose(@slot)`: the relation a caller fills the slot with;
//! - `:define(path)`: the relation's body, the name in front of its `AS` standing for it below;
//! - `:bind(name)`: the dialect's placeholder, one for each value of a name bound to a list;
//! - `:union(a, b, …)`, `:intersect(a, b, …)` and `:except(a, b, …)`: the sources combined as
//!   sets, each a derived table of its own;
//! - `:count(a)` and `:count(DISTINCT col OF a)`: a relation's rows counted;
//! - `:dialect(postgres, duckdb)`: at a template's head, the dialects it is written for;
//! - `# …`: a template comment, to the end of the line, which never reaches the database.
//!
//! SQL text is passed through unchanged. The composer reads where its quotes and comments begin
//! and end, as each dialect's database does, so a `#` or a directive inside one is SQL. Under
//! the feature `respell`, `respell::respell` reads a file composed for PostgreSQL with
//! sqlparser and writes a union of a roster's arms as one join. Under `spellings`, the
//! `spellings` module lays out the other spellings of a template and writes it in the picks.
//!
//! # Example
//!
//! ```
//! use sql_composer::parser::parse_template;
//! use sql_composer::composer::Composer;
//! use sql_composer::types::{Dialect, TemplateSource};
//!
//! let input = "SELECT set_num, name, year FROM lego_sets WHERE year = :bind(year) AND theme_id = :bind(theme_id)";
//! let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();
//!
//! let composer = Composer::new(Dialect::Postgres);
//! let result = composer.compose(&template).unwrap();
//!
//! // Numbered by each name's alphabetical place: theme_id is $1, year is $2.
//! assert_eq!(result.sql, "SELECT set_num, name, year FROM lego_sets WHERE year = $2 AND theme_id = $1");
//! assert_eq!(result.bind_params, vec!["theme_id", "year"]);
//! ```

mod clippy;
pub mod composer;
pub mod driver;
pub mod error;
#[cfg(all(test, any(feature = "respell", feature = "spellings")))]
mod laws;
pub mod mock;
#[cfg(any(feature = "respell", feature = "spellings"))]
mod parsed;
pub mod parser;
mod reserved;
#[cfg(feature = "respell")]
pub mod respell;
mod scan;
pub mod source;
#[cfg(feature = "spellings")]
pub mod spellings;
pub mod types;

pub use composer::{ComposedSql, Composer, Held, Reach};
pub use error::Error;
pub use mock::{MockColumn, MockTable};
pub use types::{
    Binding, Command, CommandKind, ComposeRef, ComposeTarget, Dialect, Element, Placeholders,
    SlotAssignment, Template, TemplateSource,
};
