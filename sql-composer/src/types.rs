//! Useful Types for parsing sql and tql.
//!
//!## Notes
//!### TryFrom trait implementation
//!
//! We have implemented several concrete implementations of `TryFrom`.
//! We would have preferred to implement `TryFrom` with a generic type constraint of `Into<PathBuf>`,
//! but this is blocked by [Bug 50133].
//!
//! The underlying method, `SqlComposition::from_path()` provides a
//! work around for any missing concrete implementations.
//!
//! [Bug 50133]: <https://github.com/rust-lang/rust/issues/50133>
//!
//! ``` ignore
//! // Will not compile due to rust-lang/rust#50133
//! impl SqlComposition {
//!     pub fn try_from<P>(path: P) -> Result<Self>
//!         where P: Into<PathBuf> + Debug {
//!             let path = path.into();
//!             Self::parse(SqlCompositionAlias::from(path.to_path_buf()))
//!         }
//! }
//! ```
pub mod value;

use crate::error::{Error, ErrorKind, Result};

use crate::parser::template;

use std::hash::{Hash, Hasher};
use std::collections::{HashMap};
use std::convert::{From, Into, TryFrom};
use std::fmt;
use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub use nom_locate::LocatedSpan;

pub type Span<'a> = LocatedSpan<&'a str>;

pub struct Null();

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum Position {
    Generated(GeneratedSpan),
    Parsed(ParsedSpan),
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Position::Generated(gs) => write!(
                f,
                "command {}",
                match &gs.command {
                    Some(c) => c.to_string(),
                    None => "<None>".to_string(),
                }
            ),
            Position::Parsed(ps) => {
                match &ps.alias {
                    Some(a) => write!(f, "composition {} ", a)?,
                    None => write!(f, "")?,
                }

                write!(f, "character {} line {}", ps.offset, ps.line)
            }
        }
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Default, Clone)]
pub struct GeneratedSpan {
    pub command: Option<String>,
}

#[derive(Debug, Hash, Eq, PartialEq, Default, Clone)]
pub struct ParsedSpan {
    pub alias:    Option<SqlCompositionAlias>,
    pub line:     u32,
    pub offset:   usize,
    pub fragment: String,
}

impl ParsedSpan {
    pub fn new(span: Span, alias: Option<SqlCompositionAlias>) -> Self {
        Self {
            alias:    alias,
            line:     span.line,
            offset:   span.offset,
            fragment: span.fragment.to_string(),
        }
    }

    pub fn from_span(span: Span) -> Self {
        Self {
            line: span.line,
            offset: span.offset,
            fragment: span.fragment.to_string(),
            ..Default::default()
        }
    }
}

// explicit lifetime for Span is required: Span<'a>
// because "implicit elided lifetime is not allowed here"
impl<'a> From<Span<'a>> for ParsedSpan {
    fn from(span: Span) -> Self {
        Self {
            line: span.line,
            offset: span.offset,
            fragment: span.fragment.to_string(),
            ..Default::default()
        }
    }
}

impl fmt::Display for ParsedSpan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "character {}, line {}", self.line, self.offset)?;

        match &self.alias {
            Some(a) => write!(f, " of {}:", a)?,
            None => write!(f, ":")?,
        };

        write!(f, "{}", self.fragment)
    }
}

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub enum SqlCompositionAlias {
    Path(PathBuf),
    DbObject(SqlDbObject),
    SqlLiteral(SqlLiteral),
}

impl SqlCompositionAlias {
    pub fn from_span(s: Span) -> Result<Self> {
        Self::from_str(s.fragment)
    }

    fn from_str(s: &str) -> Result<Self> {
        let (_is_name, _is_path) = s.chars().fold((true, false), |mut acc, u| {
            let c = u as char;

            match c {
                'a'..='z' => {}
                '0'..='9' => {}
                '-' | '_' => {}
                '.' | '/' | '\\' => acc.1 = true,
                _ => acc = (false, false),
            }

            acc
        });

        Ok(SqlCompositionAlias::Path(PathBuf::from(&s)))
    }

    pub fn from_path<P>(path: P) -> Self
    where
        P: Into<PathBuf> + std::fmt::Debug,
    {
        // TODO: include path in error.
        // TODO: check if path is absolute or relative?
        SqlCompositionAlias::Path(path.into())
    }

    /// Return an owned copy of the PathBuf for SqlCompositionAlias::Path types.
    pub fn path(&self) -> Option<PathBuf> {
        match self {
            // PathBuf doesn't impl Copy, so use to_path_buf for a new one
            SqlCompositionAlias::Path(p) => Some(p.to_path_buf()),
            _ => None,
        }
    }

    pub fn read_raw_sql(&self) -> Result<String> {
        match self {
            SqlCompositionAlias::DbObject(dbo) => Ok(dbo.to_string()),
            SqlCompositionAlias::Path(path) => Ok(fs::read_to_string(&path)?),
            SqlCompositionAlias::SqlLiteral(s) => Ok(s.to_string()),
        }
    }
}

// str and Span will need to be moved to TryFrom
// if the from_str match gets implemented
//impl<P> From<P> for SqlCompositionAlias
//where
//    P: Into<PathBuf> + std::fmt::Debug,
//{
//    fn from(path: P) -> Self {
//        SqlCompositionAlias::Path(path.into())
//    }
//}

impl From<PathBuf> for SqlCompositionAlias {
    fn from(path: PathBuf) -> Self {
        SqlCompositionAlias::Path(path.into())
    }
}

impl From<&PathBuf> for SqlCompositionAlias {
    fn from(path: &PathBuf) -> Self {
        SqlCompositionAlias::Path(path.into())
    }
}

impl From<&str> for SqlCompositionAlias {
    fn from(s: &str) -> Self {
        SqlCompositionAlias::SqlLiteral(SqlLiteral {
            id:        None,
            value:     s.to_string(),
            generated: false,
        })
    }
}

impl From<String> for SqlCompositionAlias {
    fn from(s: String) -> Self {
        SqlCompositionAlias::SqlLiteral(SqlLiteral {
            id:        None,
            value:     s,
            generated: false,
        })
    }
}

/// Destructively convert a SqlCompositionAlias into a PathBuf
impl Into<Option<PathBuf>> for SqlCompositionAlias {
    fn into(self) -> Option<PathBuf> {
        match self {
            SqlCompositionAlias::Path(p) => Some(p),
            _ => None,
        }
    }
}

impl Default for SqlCompositionAlias {
    fn default() -> Self {
        //TODO: better default
        SqlCompositionAlias::DbObject(SqlDbObject {
            id:           None,
            object_name:  "DUAL".to_string(),
            object_alias: None,
        })
    }
}

impl FromStr for SqlCompositionAlias {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(SqlCompositionAlias::SqlLiteral(SqlLiteral {
            id:        None,
            value:     s.to_string(),
            generated: false,
        }))
    }
}

impl fmt::Display for SqlCompositionAlias {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SqlCompositionAlias::Path(p) => write!(f, ", {}", p.to_string_lossy()),
            SqlCompositionAlias::DbObject(dbo) => write!(f, ", {}", dbo),
            SqlCompositionAlias::SqlLiteral(l) => write!(f, "{}", l),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ParsedItem<T>
where
    T: Debug + Default + PartialEq + Clone,
{
    pub item:     T,
    pub position: Position,
}

impl<T> ParsedItem<T>
where
    T: Debug + Default + PartialEq + Clone,
{
    pub fn from_span(item: T, span: Span, alias: Option<SqlCompositionAlias>) -> Result<Self> {
        Ok(Self {
            item:     item,
            position: Position::Parsed(ParsedSpan::new(span, alias)),
        })
    }

    pub fn generated(item: T, command: Option<String>) -> Result<Self> {
        Ok(Self {
            item:     item,
            position: Position::Generated(GeneratedSpan { command }),
        })
    }

    pub fn item(&self) -> T {
        self.item.clone()
    }
}

impl<T: fmt::Display + Debug + Default + PartialEq + Clone> fmt::Display for ParsedItem<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.item)
    }
}

//command - :(command [distinct, all] [column1, column2] of t1.tql, t2.tql)
//----------------------------------|-------------------------------------------
// examples -
//
//            :compose([distinct] [column1, column2 of] t1.sql)
//            :count([distinct] [column1, column2 of] t1.sql)
//            :expand([column1, column2 of] t1.sql)
//            :except([distinct] [column1, column2 of] t1.sql, t2.tql)
//            :intercept([distinct] [column1, column2 of] t1.sql, t2.tql)
//            :union([all|distinct] [column1, column2 of] t1.sql, t2.tql)

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct SqlComposition {
    pub command:  Option<ParsedItem<String>>,
    pub distinct: Option<ParsedItem<bool>>,
    pub all:      Option<ParsedItem<bool>>,
    pub columns:  Option<Vec<ParsedItem<String>>>,
    pub source_alias: SqlCompositionAlias,
    pub of:       Vec<ParsedItem<SqlCompositionAlias>>,
    pub aliases:  HashMap<SqlCompositionAlias, ParsedItem<SqlComposition>>,
    pub sql:      Vec<Sql>,
    pub position: Option<Position>,
}

impl SqlComposition {
    //TODO: properly check remaining along with a few other traits
    pub fn parse(alias: SqlCompositionAlias) -> Result<ParsedItem<Self>> {
        let stmt = template(Span::new(&alias.read_raw_sql()?), alias)?;

        //if remaining.fragment.len() > 0 {
        //panic!("found extra information: {}", remaining.to_string());
        //}

        Ok(stmt)
    }

    /// Reads the file at path and parses with `Self::parse()`
    ///
    /// Relative paths are resolved from the directory where the code is executed.
    ///
    /// Can fail reading the contents of path or while parsing the contents.
    pub fn from_path<P>(path: P) -> Result<ParsedItem<Self>>
    where
        P: AsRef<Path> + Debug,
    {
        let path = path.as_ref();
        Self::parse(SqlCompositionAlias::from(path.to_path_buf()))
    }

    pub fn column_list(&self) -> Result<Option<String>> {
        match &self.columns {
            Some(c) => {
                let s = c
                    .iter()
                    .enumerate()
                    .fold(String::new(), |mut acc, (i, name)| {
                        if i > 0 {
                            acc.push(',');
                        }

                        acc.push_str(&name.item);

                        acc
                    });

                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    pub fn push_sql(&mut self, c: Sql) -> Result<()> {
        self.sql.push(c);

        Ok(())
    }

    pub fn update_aliases(&mut self) -> Result<()> {
        for parsed_alias in &self.of {
            let alias = &parsed_alias.item;

            if let Some(path) = &alias.path() {
                self.aliases
                    .entry(alias.clone())
                    .or_insert(SqlComposition::from_path(path)?);
            }
        }

        Ok(())
    }

    pub fn insert_alias(&mut self, p: &Path) -> Result<()> {
        self.aliases
            .entry(SqlCompositionAlias::from_path(p))
            .or_insert(SqlComposition::from_path(p)?);

        Ok(())
    }

    //TODO: error if path already set to Some(_)
    pub fn set_position(&mut self, new: Position) -> Result<()> {
        match &self.position {
            Some(_existing) => {
                Err(ErrorKind::CompositionAliasConflict("bad posisition".into()).into())
            }
            None => {
                self.position = Some(new);
                Ok(())
            }
        }
    }

    pub fn push_sub_comp(&mut self, value: ParsedItem<SqlComposition>) -> Result<()> {
        self.push_sql(Sql::Composition((value, vec![])))
    }

    pub fn push_generated_sub_comp(&mut self, value: SqlComposition) -> Result<()> {
        self.push_sql(Sql::Composition((
            ParsedItem::generated(value, None)?,
            vec![],
        )))
    }

    pub fn push_generated_literal(&mut self, value: &str, command: Option<String>) -> Result<()> {
        self.push_sql(Sql::Literal(ParsedItem::generated(
            SqlLiteral {
                value: value.into(),
                ..Default::default()
            },
            command,
        )?))
    }

    pub fn push_generated_end(&mut self, command: Option<String>) -> Result<()> {
        self.push_sql(Sql::Ending(ParsedItem::generated(
            SqlEnding { value: ";".into() },
            command,
        )?))
    }

    pub fn end(&mut self, value: &str, span: Span) -> Result<()> {
        //TODO: check if this has already ended
        match self.sql.last() {
            Some(_last) => self.push_sql(Sql::Ending(
                ParsedItem::from_span(
                    SqlEnding {
                        value: value.into(),
                    },
                    span,
                    None,
                )
                .unwrap(),
            )),
            None => Err(ErrorKind::CompositionIncomplete("".into()).into()),
        }
    }
}

impl Hash for SqlComposition {
    fn hash<H: Hasher>(&self, alias: &mut H) {
        self.source_alias.hash(alias);
    }
}

impl fmt::Display for SqlComposition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.command {
            Some(n) => write!(f, ":{}(", n)?,
            None => write!(f, ":expand(")?,
        }

        let mut c = 0;

        for col in &self.columns {
            if c > 0 {
                write!(f, ",")?;
            }

            write!(f, "{:?}", col)?;

            c += 1;
        }

        write!(f, ")")
    }
}

/// Convenience type for consumers.
pub type ParsedSqlComposition = ParsedItem<SqlComposition>;

impl ParsedSqlComposition {
    pub fn parse<T>(a: T) -> Result<Self>
    where
        T: Into<SqlCompositionAlias> + std::fmt::Debug,
    {
        //TODO: make this a ?, doesn't work for some reason, so unwrapping for now
        let alias: SqlCompositionAlias = a.into();
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

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Sql {
    Literal(ParsedItem<SqlLiteral>),
    Binding(ParsedItem<SqlBinding>),
    Composition((ParsedItem<SqlComposition>, Vec<SqlCompositionAlias>)),
    Ending(ParsedItem<SqlEnding>),
    DbObject(ParsedItem<SqlDbObject>),
    Keyword(ParsedItem<SqlKeyword>),
}

impl fmt::Display for Sql {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Sql::Literal(t) => write!(f, "{}", t)?,
            Sql::Binding(b) => write!(f, "{}", b)?,
            Sql::Composition(w) => write!(f, "{:?}", w)?,
            Sql::Ending(e) => write!(f, "{}", e)?,
            Sql::DbObject(ft) => write!(f, "{}", ft)?,
            Sql::Keyword(k) => write!(f, "{}", k)?,
        }

        write!(f, "")
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct SqlEnding {
    pub value: String,
}

impl SqlEnding {
    pub fn new(v: String) -> Result<Self> {
        Ok(Self { value: v })
    }
}

impl fmt::Display for SqlEnding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, Default, Hash, Eq, PartialEq, Clone)]
pub struct SqlDbObject {
    pub id:           Option<String>,
    pub object_name:  String,
    pub object_alias: Option<String>,
}

impl SqlDbObject {
    pub fn new(name: String, alias: Option<String>) -> Result<Self> {
        Ok(Self {
            id:           None,
            object_name:  name,
            object_alias: alias,
        })
    }
}

impl fmt::Display for SqlDbObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.object_name)?;

        if let Some(alias) = &self.object_alias {
            write!(f, " AS {}", alias)
        }
        else {
            write!(f, "")
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct SqlKeyword {
    pub value: String,
}

impl SqlKeyword {
    pub fn new(v: String) -> Result<Self> {
        Ok(Self { value: v })
    }
}

impl fmt::Display for SqlKeyword {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Default, Clone)]
pub struct SqlLiteral {
    pub id:        Option<String>,
    pub value:     String,
    pub generated: bool,
}

impl SqlLiteral {
    pub fn new(v: String) -> Result<Self> {
        Ok(Self {
            value: v,
            ..Default::default()
        })
    }
}

impl fmt::Display for SqlLiteral {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.value.trim_end_matches(" "))
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct SqlBinding {
    pub name:       String,
    pub quoted:     bool,
    pub min_values: Option<u32>,
    pub max_values: Option<u32>,
    pub nullable:   bool,
}

impl SqlBinding {
    pub fn new(
        name: String,
        quoted: bool,
        min_values: Option<u32>,
        max_values: Option<u32>,
        nullable: bool,
    ) -> Result<Self> {
        Ok(Self {
            name,
            min_values,
            max_values,
            quoted,
            nullable,
        })
    }
}

impl fmt::Display for SqlBinding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::{ParsedSpan, Span, SqlCompositionAlias};
    use std::path::PathBuf;

    /// Test conversion: Span -> ParsedSpan
    #[test]
    fn parsed_span_from() {
        let span = Span::new("");
        // Into and From are allowed to consume the input.
        // ParsedSpan does not consume the input.
        let span_into_parsedspan: ParsedSpan = span.into();
        let parsedspan_from_span = ParsedSpan::from(span);
        assert_eq!(parsedspan_from_span, span_into_parsedspan);
    }

    #[test]
    /// Test conversions to SqlCompositionAlias
    fn sql_composition_alias_from() {
        // PathBuf
        {
            let pathbuf = PathBuf::from("src/tests/single-template.tql");
            // consumes the owned pathbuf
            let sca_from_pathbuf = SqlCompositionAlias::from(pathbuf);

            let pathbuf = PathBuf::from("src/tests/single-template.tql");
            let pathbuf_into_sca: SqlCompositionAlias = (pathbuf).into();

            assert_eq!(sca_from_pathbuf, pathbuf_into_sca);
        }

        // &PathBuf
        {
            let pathbuf = PathBuf::from("src/tests/single-template.tql");
            let sca_from_pathbuf_alias = SqlCompositionAlias::from(&pathbuf);
            let pathbuf_alias_into_sca: SqlCompositionAlias = (&pathbuf).into();

            assert_eq!(sca_from_pathbuf_alias, pathbuf_alias_into_sca);
        }

        // &PathBuf
        {
            let pathbuf = PathBuf::from("src/tests/single-template.tql");
            let sca_from_pathbuf_alias = SqlCompositionAlias::from(&pathbuf);
            let pathbuf_alias_into_sca: SqlCompositionAlias = (&pathbuf).into();

            assert_eq!(sca_from_pathbuf_alias, pathbuf_alias_into_sca);
        }

        //let sca_from_aliaspathbuf = SqlCompositionAlias::from(&pathbuf);
        // PathBuf
        // &PathBuf <-- should be &Path?
        // &str
        // String
        // FromStr
    }

    #[test]
    fn parsed_sql_composition_try_from() {
        // &Path
        // PathBuf
        // &str
        // missing: String
        // SqlCompositionAlias
        // FromStr
    }
}
