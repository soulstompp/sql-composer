pub mod value;

use crate::error::{new_alias_conflict_error, new_incomplete_composition_error, Result};

use crate::parser::parse_template;

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

pub use nom::types::CompleteStr;

pub use nom_locate::LocatedSpan;

pub type Span<'a> = LocatedSpan<CompleteStr<'a>>;

#[cfg(feature = "composer-serde")]
use serde_value::Value;

use std::fs::File;
use std::io::prelude::*;

struct Null();

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
            line: span.line,
            offset: span.offset,
            fragment: span.fragment.to_string(),
            alias: alias,
            ..Default::default()
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
}

impl SqlCompositionAlias {
    pub fn from_span(s: Span) -> Result<Self> {
        Self::from_str(*s.fragment)
    }

    fn from_str(s: &str) -> Result<Self> {
        let (is_name, is_path) = s.chars().fold((true, false), |mut acc, u| {
            let c = u as char;

            match c {
                'a'...'z' => {}
                '0'...'9' => {}
                '-' | '_' => {}
                '.' | '/' | '\\' => acc.1 = true,
                _ => acc = (false, false),
            }

            acc
        });

        Ok(SqlCompositionAlias::Path(PathBuf::from(&s)))
    }

    pub fn from_path(p: &Path) -> Self {
        SqlCompositionAlias::Path(p.into())
    }

    pub fn path(&self) -> Option<PathBuf> {
        //! Returns the path as a PathBuf
        if let SqlCompositionAlias::Path(p) = self {
            Some(p.to_path_buf())
        }
        else {
            None
        }
    }
}

impl Default for SqlCompositionAlias {
    fn default() -> Self {
        //TODO: better default
        SqlCompositionAlias::DbObject(SqlDbObject {
            object_name:  "DUAL".to_string(),
            object_alias: None,
        })
    }
}

impl fmt::Display for SqlCompositionAlias {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SqlCompositionAlias::Path(p) => write!(f, ", {}", p.to_string_lossy()),
            SqlCompositionAlias::DbObject(dbo) => write!(f, ", {}", dbo),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ParsedItem<T: Debug + Default + PartialEq + Clone> {
    pub item:     T,
    pub position: Position,
}

impl<T: Debug + Default + PartialEq + Clone> ParsedItem<T> {
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

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SqlComposition {
    pub command:  Option<ParsedItem<String>>,
    pub distinct: Option<ParsedItem<bool>>,
    pub all:      Option<ParsedItem<bool>>,
    pub columns:  Option<Vec<ParsedItem<String>>>,
    pub of:       Vec<ParsedItem<SqlCompositionAlias>>,
    pub aliases:  HashMap<SqlCompositionAlias, ParsedItem<SqlComposition>>,
    pub sql:      Vec<Sql>,
    pub position: Option<Position>,
}

impl SqlComposition {
    pub fn from_str(q: &str) -> ParsedItem<Self> {
        let (remaining, stmt) = parse_template(Span::new(q.into()), None).unwrap();

        if remaining.fragment.len() > 0 {
            panic!("found extra information: {}", remaining.to_string());
        }

        stmt
    }

    pub fn from_path(path: &Path) -> Result<ParsedItem<Self>> {
        let mut f = File::open(path).unwrap();
        let mut s = String::new();

        let _res = f.read_to_string(&mut s);

        let (_remaining, stmt) = parse_template(
            Span::new(s.as_str().into()),
            Some(SqlCompositionAlias::from_path(path.into())),
        )
        .unwrap();

        Ok(stmt)
    }

    pub fn from_path_name(s: &str) -> Result<ParsedItem<SqlComposition>> {
        let p = Path::new(s);

        Self::from_path(p)
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

    //TODO: error if path already set to Some(...)
    pub fn set_position(&mut self, new: Position) -> Result<()> {
        match &self.position {
            Some(existing) => Err(new_alias_conflict_error(existing.clone(), new).into()),
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
        self.push_sql(Sql::Ending(
            ParsedItem::generated(SqlEnding { value: ";".into() }, command).unwrap(),
        ))
    }

    pub fn end(&mut self, value: &str, span: Span) -> Result<()> {
        //TODO: check if this has already ended
        match self.sql.last() {
            Some(last) => self.push_sql(Sql::Ending(
                ParsedItem::from_span(
                    SqlEnding {
                        value: value.into(),
                    },
                    span,
                    None,
                )
                .unwrap(),
            )),
            None => Err(new_incomplete_composition_error(
                Position::Generated(GeneratedSpan { command: None }),
                self.clone(),
                "".into(),
            )
            .into()),
        }
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

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, Default, PartialEq, Clone)]
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
    pub object_name:  String,
    pub object_alias: Option<String>,
}

impl SqlDbObject {
    pub fn new(name: String, alias: Option<String>) -> Result<Self> {
        Ok(Self {
            object_name:  name,
            object_alias: alias,
        })
    }
}

impl fmt::Display for SqlDbObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.object_name);

        if let Some(alias) = &self.object_alias {
            write!(f, " AS {}", alias)
        }
        else {
            write!(f, "")
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Default, Clone)]
pub struct SqlLiteral {
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

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SqlBinding {
    pub name:      String,
    pub quoted:    bool,
    pub generated: bool,
}

impl SqlBinding {
    pub fn new(s: String) -> Result<Self> {
        Ok(Self {
            name:      s,
            quoted:    false,
            generated: false,
        })
    }

    pub fn new_quoted(s: String) -> Result<Self> {
        Ok(Self {
            name:      s,
            quoted:    true,
            generated: false,
        })
    }
}

impl fmt::Display for SqlBinding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(feature = "composer-serde")]
#[derive(Clone, Debug)]
pub struct SerdeValue(pub Value);

#[cfg(feature = "composer-serde")]
impl PartialEq for SerdeValue {
    fn eq(&self, rhs: &Self) -> bool {
        self.0 == rhs.0
    }
}
