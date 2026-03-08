//! sql-composer: A SQL template engine that composes reusable SQL fragments
//! with parameterized bindings.
//!
//! Templates use a simple macro syntax embedded in SQL:
//! - `:bind(name)` — parameter placeholder
//! - `:compose(path)` — include another template
//! - `:count(sources...)` — count aggregate
//! - `:union(sources...)` — union combinator
//!
//! SQL text is treated as opaque literals and passed through unchanged.
//! Only the macro syntax is parsed.
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
pub mod parser;
pub mod types;

pub use composer::{ComposedSql, Composer};
pub use error::Error;
pub use mock::MockTable;
pub use types::{
    Binding, Command, CommandKind, ComposeRef, Dialect, Element, Template, TemplateSource,
};
