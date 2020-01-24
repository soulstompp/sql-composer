use crate::error::{Error, Result};
use crate::parser::template;
use crate::types::{ParsedItem, Span, SqlComposition, SqlCompositionAlias};

use std::convert::{Into, TryFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Convenience type for consumers.
pub type ParsedItemSql = ParsedItem<SqlComposition>;

impl ParsedItemSql {
    pub fn parse(q: &str, alias: Option<SqlCompositionAlias>) -> Result<Self> {
        let stmt = template(Span::new(q.into()), alias)?;

        Ok(stmt)
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
/// use sql_composer::{types::ParsedItemSql,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = Path::new("src/tests/simple-template.tql");
///   let stmt: ParsedItemSql = path.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use std::path::Path;
/// use sql_composer::{types::ParsedItemSql,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = Path::new("src/tests/simple-template.tql");
///   let stmt = ParsedItemSql::try_from(path)?;
///   Ok(())
/// }
/// ```
impl TryFrom<&Path> for ParsedItemSql {
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
/// use sql_composer::{types::ParsedItemSql,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = PathBuf::from("src/tests/simple-template.tql");
///   let stmt: ParsedItemSql = path.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use std::path::PathBuf;
/// use sql_composer::{types::ParsedItemSql,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = PathBuf::from("src/tests/simple-template.tql");
///   let stmt = ParsedItemSql::try_from(path)?;
///   Ok(())
/// }
/// ```
impl TryFrom<PathBuf> for ParsedItemSql {
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
/// use sql_composer::{types::ParsedItemSql,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = "src/tests/simple-template.tql";
///   let stmt: ParsedItemSql = path.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use sql_composer::{types::ParsedItemSql,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = "src/tests/simple-template.tql";
///   let stmt = ParsedItemSql::try_from(path)?;
///   Ok(())
/// }
/// ```
impl TryFrom<&str> for ParsedItemSql {
    type Error = Error;
    fn try_from(path: &str) -> Result<Self> {
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
/// use sql_composer::{types::ParsedItemSql,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = String::from("src/tests/simple-template.tql");
///   let stmt: ParsedItemSql = path.try_into()?;
///   Ok(())
/// }
/// ```
///
///
/// TryFrom:
/// ```
/// use std::convert::TryFrom;
/// use sql_composer::{types::ParsedItemSql,
///                    error::Result};
/// fn main() -> Result<()> {
///   let path = "src/tests/simple-template.tql".to_string();
///   let stmt = ParsedItemSql::try_from(path)?;
///   Ok(())
/// }
/// ```
impl TryFrom<String> for ParsedItemSql {
    type Error = Error;
    fn try_from(path: String) -> Result<Self> {
        SqlComposition::from_path(path)
    }
}

impl FromStr for ParsedItemSql {
    type Err = Error;
    fn from_str(path: &str) -> Result<Self> {
        SqlComposition::from_path(path)
    }
}
