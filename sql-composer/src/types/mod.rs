//! Useful types for parsing SQL (.sql) and Templated SQL (.tql) files
//!

mod parsed_item;
mod parsed_sql_composition;
mod position;
mod span;
mod sql;
mod sql_composition;
mod sql_composition_alias;
pub mod value;

pub struct Null();

pub use parsed_item::ParsedItem;

pub use parsed_sql_composition::ParsedSqlComposition;

pub use position::Position;

pub use span::{GeneratedSpan, LocatedSpan, ParsedSpan, Span};

pub use sql::{Sql, SqlBinding, SqlDbObject, SqlEnding, SqlKeyword, SqlLiteral};

pub use sql_composition::SqlComposition;
pub use sql_composition_alias::SqlCompositionAlias;
