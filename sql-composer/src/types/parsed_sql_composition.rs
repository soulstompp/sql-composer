use crate::error::{Error, Result};
use crate::parser::template;
use crate::types::{ParsedItem, Span, SqlComposition, SqlCompositionAlias};

use std::convert::{Into, TryFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Convenience type for consumers.
pub type ParsedSqlComposition = ParsedItem<SqlComposition>;

impl ParsedSqlComposition {
    pub fn parse<T>(a: T) -> Result<Self>
    where
        T: Into<SqlCompositionAlias> + std::fmt::Debug,
    {
        //TODO: make this a ?, doesn't work for some reason, so unwrapping for now
        let alias:SqlCompositionAlias = a.into();

        let stmt = template(Span::new(&alias.read_raw_sql()?), alias);

        stmt
    }
}

/// Equivalent to `SqlComposition::from_path(path)`.
///
/// # Examples
///
/// TryInto:
/// ```
/// use std::convert::TryInto;
/// use std::path::Path;
/// use sql_composer::{types::ParsedSqlComposition,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = Path::new("src/tests/simple-template.tql");
///   let stmt: ParsedSqlComposition = path.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use std::path::Path;
/// use sql_composer::{types::ParsedSqlComposition,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = Path::new("src/tests/simple-template.tql");
///   let stmt = ParsedSqlComposition::try_from(path)?;
///   Ok(())
/// }
/// ```
impl TryFrom<&Path> for ParsedSqlComposition {
    type Error = Error;
    fn try_from(path: &Path) -> Result<Self> {
        SqlComposition::from_path(path)
    }
}

/// Equivalent to `SqlComposition::from_path(path)`.
///
/// # Examples
///
/// TryInto:
/// ```
/// use std::convert::TryInto;
/// use std::path::PathBuf;
/// use sql_composer::{types::ParsedSqlComposition,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = PathBuf::from("src/tests/simple-template.tql");
///   let stmt: ParsedSqlComposition = path.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use std::path::PathBuf;
/// use sql_composer::{types::ParsedSqlComposition,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = PathBuf::from("src/tests/simple-template.tql");
///   let stmt = ParsedSqlComposition::try_from(path)?;
///   Ok(())
/// }
/// ```
impl TryFrom<PathBuf> for ParsedSqlComposition {
    type Error = Error;
    fn try_from(path: PathBuf) -> Result<Self> {
        SqlComposition::from_path(path)
    }
}

/// Equivalent to `SqlComposition::from_path(path)`.
///
/// # Examples
///
/// TryInto:
/// ```
/// use std::convert::TryInto;
/// use sql_composer::{types::ParsedSqlComposition,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = "src/tests/simple-template.tql";
///   let stmt: ParsedSqlComposition = path.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use sql_composer::{types::ParsedSqlComposition,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = "src/tests/simple-template.tql";
///   let stmt = ParsedSqlComposition::try_from(path)?;
///   Ok(())
/// }
/// ```
impl TryFrom<&str> for ParsedSqlComposition {
    type Error = Error;
    fn try_from(raw_sql: &str) -> Result<Self> {
        ParsedSqlComposition::from_str(raw_sql)
    }
}

/// Equivalent to `SqlComposition::from_path(path)`.
///
/// # Examples
///
/// TryInto:
/// ```
/// use std::convert::TryInto;
/// use sql_composer::{types::{ParsedSqlComposition,
///                            SqlCompositionAlias},
///                    error::Result};
/// fn main() -> Result<()> {
///   let alias: SqlCompositionAlias = "SELECT 1".into();
///   let stmt: ParsedSqlComposition = alias.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use sql_composer::{types::{ParsedSqlComposition,
///                            SqlCompositionAlias},
///                    error::Result};
/// fn main() -> Result<()> {
///   let raw_sql = "SELECT 1".to_string();
///   let alias = SqlCompositionAlias::from(raw_sql);
///   let stmt = ParsedSqlComposition::try_from(alias)?;
///   Ok(())
/// }
/// ```
impl TryFrom<SqlCompositionAlias> for ParsedSqlComposition {
    type Error = Error;
    fn try_from(alias: SqlCompositionAlias) -> Result<Self> {
        SqlComposition::parse(alias)
    }
}

impl FromStr for ParsedSqlComposition {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let alias = SqlCompositionAlias::from_str(s)?;
        Ok(ParsedSqlComposition::try_from(alias)?)
    }
}
