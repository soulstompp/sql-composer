//! Useful types for parsing SQL (.sql) and Templated SQL (.tql) files
//!

mod parsed_item;
mod parsed_sql;
mod parsed_sql_macro;
mod parsed_sql_statement;
mod position;
mod span;
mod sql;
mod sql_composition;
mod sql_composition_alias;
mod sql_macro;
mod sql_statement;
pub mod value;

pub struct Null();

pub use parsed_item::ParsedItem;

pub use parsed_sql::ParsedSql;
pub use parsed_sql_macro::ParsedSqlMacro;
pub use parsed_sql_statement::ParsedSqlStatement;

pub use position::Position;

pub use span::{GeneratedSpan, LocatedSpan, ParsedSpan, Span};

pub use sql::{Sql, SqlBinding, SqlDbObject, SqlEnding, SqlKeyword, SqlLiteral, SqlMacroLiteral};

pub use sql_composition::SqlComposition;
pub use sql_composition_alias::SqlCompositionAlias;
pub use sql_macro::SqlMacro;

pub use sql_statement::SqlStatement;
