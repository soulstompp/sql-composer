//! sql-composer: A SQL template engine that composes reusable SQL fragments
//! with parameterized bindings.
//!
//! Templates use a simple macro syntax embedded in SQL:
//! - `:bind(name)` — parameter placeholder
//! - `:compose(path)` — include another template
//! - `:count(sources...)` — count aggregate
//! - `:union(sources...)` — union combinator
//! - `:intersect(sources...)` — the rows every source holds
//! - `:except(first, rest...)` — the rows of the first source that no other source holds
//! - `:dialect(names...)` — the dialects the template is written for, at its head
//!
//! SQL text is passed through unchanged. The composer reads where its quotes and comments begin
//! and end, as each dialect's database does, so a `#` or a directive inside one is SQL.
//!
//! # Example
//!
//! ```
//! use sql_composer::parser::parse_template;
//! use sql_composer::composer::Composer;
//! use sql_composer::types::{Dialect, TemplateSource};
//!
//! let input = "SELECT * FROM users WHERE id = :bind(user_id) AND active = :bind(active);";
//! let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();
//!
//! let composer = Composer::new(Dialect::Postgres);
//! let result = composer.compose(&template).unwrap();
//!
//! // Alphabetical ordering: active=$1, user_id=$2
//! assert_eq!(result.sql, "SELECT * FROM users WHERE id = $2 AND active = $1;");
//! assert_eq!(result.bind_params, vec!["active", "user_id"]);
//! ```

mod clippy;
pub mod composer;
pub mod driver;
pub mod error;
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
