use crate::error::{Error, Result};
use crate::parser::statement as parse_statement;
use crate::types::{ParsedItem, Span, SqlStatement, SqlCompositionAlias};

use std::convert::{Into, TryFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Convenience type for consumers.
pub type ParsedSqlStatement = ParsedItem<SqlStatement>;

impl ParsedSqlStatement {
    pub fn parse<T>(a: T) -> Result<Self>
    where
        T: Into<SqlCompositionAlias> + std::fmt::Debug,
    {
        //TODO: make this a ?, doesn't work for some reason, so unwrapping for now
        let alias: SqlCompositionAlias = a.into();
        let stmt = parse_statement(Span::new(&alias.read_raw_sql()?), alias);

        stmt
    }
}

/// Equivalent to `SqlStatement::from_path(path)`.
///
/// # Examples
///
/// TryInto:
/// ```
/// use std::convert::TryInto;
/// use std::path::Path;
/// use sql_composer::{types::ParsedSqlStatement,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = Path::new("src/tests/simple-template.tql");
///   let stmt: ParsedSqlStatement = path.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use std::path::Path;
/// use sql_composer::{types::ParsedSqlStatement,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = Path::new("src/tests/simple-template.tql");
///   let stmt = ParsedSqlStatement::try_from(path)?;
///   Ok(())
/// }
/// ```
impl TryFrom<&Path> for ParsedSqlStatement {
    type Error = Error;
    fn try_from(path: &Path) -> Result<Self> {
        SqlStatement::from_path(path)
    }
}

/// Equivalent to `SqlStatement::from_path(path)`.
///
/// # Examples
///
/// TryInto:
/// ```
/// use std::convert::TryInto;
/// use std::path::PathBuf;
/// use sql_composer::{types::ParsedSqlStatement,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = PathBuf::from("src/tests/simple-template.tql");
///   let stmt: ParsedSqlStatement = path.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use std::path::PathBuf;
/// use sql_composer::{types::ParsedSqlStatement,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = PathBuf::from("src/tests/simple-template.tql");
///   let stmt = ParsedSqlStatement::try_from(path)?;
///   Ok(())
/// }
/// ```
impl TryFrom<PathBuf> for ParsedSqlStatement {
    type Error = Error;
    fn try_from(path: PathBuf) -> Result<Self> {
        SqlStatement::from_path(path)
    }
}

/// Equivalent to `SqlStatement::from_path(path)`.
///
/// # Examples
///
/// TryInto:
/// ```
/// use std::convert::TryInto;
/// use sql_composer::{types::ParsedSqlStatement,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = "src/tests/simple-template.tql";
///   let stmt: ParsedSqlStatement = path.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use sql_composer::{types::ParsedSqlStatement,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = "src/tests/simple-template.tql";
///   let stmt = ParsedSqlStatement::try_from(path)?;
///   Ok(())
/// }
/// ```
impl TryFrom<&str> for ParsedSqlStatement {
    type Error = Error;
    fn try_from(raw_sql: &str) -> Result<Self> {
        ParsedSqlStatement::from_str(raw_sql)
    }
}

/// Equivalent to `SqlStatement::from_path(path)`.
///
/// # Examples
///
/// TryInto:
/// ```
/// use std::convert::TryInto;
/// use sql_composer::{types::{ParsedSqlStatement,
///                            SqlCompositionAlias},
///                    error::Result};
/// fn main() -> Result<()> {
///   let alias: SqlCompositionAlias = "SELECT 1".into();
///   let stmt: ParsedSqlStatement = alias.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use sql_composer::{types::{ParsedSqlStatement,
///                            SqlCompositionAlias},
///                    error::Result};
/// fn main() -> Result<()> {
///   let raw_sql = "SELECT 1".to_string();
///   let alias = SqlCompositionAlias::from(raw_sql);
///   let stmt = ParsedSqlStatement::try_from(alias)?;
///   Ok(())
/// }
/// ```
impl TryFrom<SqlCompositionAlias> for ParsedSqlStatement {
    type Error = Error;
    fn try_from(alias: SqlCompositionAlias) -> Result<Self> {
        SqlStatement::parse(alias)
    }
}

impl FromStr for ParsedSqlStatement {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let alias = SqlCompositionAlias::from_str(s)?;
        Ok(ParsedSqlStatement::try_from(alias)?)
    }
}
