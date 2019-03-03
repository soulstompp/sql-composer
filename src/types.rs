pub mod value;

use crate::error::{new_alias_conflict_error, Result};

use crate::parser::parse_template;

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};

pub use nom::types::CompleteStr;

use nom_locate::LocatedSpan;

pub type Span<'a> = LocatedSpan<CompleteStr<'a>>;

struct Token<'a> {
    pub position: Span<'a>,
    pub sql:      Option<Sql>,
    pub notes:    Vec<String>,
}

use std::fs::File;
use std::io::prelude::*;

struct Null();

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct SqlCompositionAlias {
    pub name: Option<String>,
    pub path: Option<PathBuf>,
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

        if is_path {
            Ok(Self {
                name: None,
                path: Some(PathBuf::from(&s)),
            })
        }
        else if is_name {
            Ok(Self {
                name: Some(s.to_string()),
                path: None,
            })
        }
        else {
            //TODO: better error handling
            panic!("invalid path");
        }
    }

    pub fn from_path(p: &Path) -> Self {
        Self {
            path: Some(p.into()),
            name: None,
        }
    }

    pub fn path(&self) -> Option<PathBuf> {
        //! Returns the path as a PathBuf
        match &self.path {
            Some(p) => Some(p.to_path_buf()),
            None => None,
        }
    }
}

impl fmt::Display for SqlCompositionAlias {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let name = match &self.name {
            Some(n) => n,
            None => "<None>",
        };

        write!(f, "name: {}", name)?;

        let path = match &self.path {
            Some(p) => p.to_string_lossy(),
            None => "<None>".into(),
        };

        write!(f, ", path: {}", path)
    }
}

//command - :(command [distinct, all] [column1, column2] of t1.tql, t2.tql)
//-----------------------------------------------------------------------------
// examples - :union([all] [distinct] [column1, column2 of] t1.sql [as ut1], t2.tql as [ut2])
//            :distinct([distinct] [column1, column2 of] t1.sql [as ut1], t2.tql [as ut2])
//            :except([distinct] [column1, column2 of] t1.sql [as ut1], t2.tql [as ut1])
//            :expand([column1, column2 of] t1.sql [as ut1] [alias t3])
//            :count([distinct] [column1, column2 of] t1.sql [as ut1])
//            :checksum([column1, column3 of] t2.sql [as ut1])

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SqlComposition {
    pub command:  Option<String>,
    pub distinct: bool,
    pub all:      bool,
    pub columns:  Option<Vec<String>>,
    pub of:       Vec<SqlCompositionAlias>,
    pub aliases:  HashMap<SqlCompositionAlias, SqlComposition>,
    pub path:     Option<PathBuf>,
    pub sql:      Vec<Sql>,
}

impl SqlComposition {
    pub fn from_str(q: &str) -> Self {
        let (remaining, stmt) = parse_template(Span::new(q.into()), None).unwrap();

        if remaining.fragment.len() > 0 {
            panic!("found extra information: {}", remaining.to_string());
        }

        stmt
    }

    pub fn from_path(path: &Path) -> Result<SqlComposition> {
        let mut f = File::open(path).unwrap();
        let mut s = String::new();

        let _res = f.read_to_string(&mut s);

        let (_remaining, stmt) =
            parse_template(Span::new(s.as_str().into()), Some(path.into())).unwrap();

        Ok(stmt)
    }

    pub fn from_path_name(s: &str) -> Result<SqlComposition> {
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

                        acc.push_str(name);

                        acc
                    });

                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    pub fn push_sql(&mut self, c: Sql) -> () {
        self.sql.push(c)
    }

    pub fn update_aliases(&mut self) -> Result<()> {
        for alias in &self.of {
            let p = alias.path().unwrap();

            self.aliases
                .entry(alias.clone())
                .or_insert(SqlComposition::from_path(&p)?);
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
    pub fn set_path(&mut self, new: &Path) -> Result<()> {
        match &self.path {
            Some(existing) => Err(new_alias_conflict_error(
                SqlCompositionAlias {
                    name: None,
                    path: Some(existing.to_path_buf()),
                },
                SqlCompositionAlias {
                    name: None,
                    path: Some(new.into()),
                },
            )
            .into()),
            None => {
                self.path = Some(new.into());
                Ok(())
            }
        }
    }

    pub fn push_sub_comp(&mut self, value: SqlComposition) {
        self.push_sql(Sql::Composition((value, vec![])));
    }

    pub fn push_literal(&mut self, value: &str) {
        self.push_sql(Sql::Literal(SqlLiteral {
            value:  value.into(),
            quoted: false,
        }))
    }

    pub fn push_quoted_text(&mut self, value: &str) {
        self.push_sql(Sql::Literal(SqlLiteral {
            value:  value.into(),
            quoted: true,
        }))
    }

    pub fn end(&mut self, value: &str) {
        //TODO: check if this has already ended
        self.push_sql(Sql::Ending(SqlEnding {
            value: value.into(),
        }));
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
    Literal(SqlLiteral),
    Binding(SqlBinding),
    Composition((SqlComposition, Vec<SqlCompositionAlias>)),
    Ending(SqlEnding),
}

impl fmt::Display for Sql {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Sql::Literal(t) => write!(f, "{}", t)?,
            Sql::Binding(b) => write!(f, "{}", b)?,
            Sql::Composition(w) => write!(f, "{:?}", w)?,
            Sql::Ending(e) => write!(f, "{}", e)?,
        }

        write!(f, "")
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SqlEnding {
    pub value: String,
}

impl SqlEnding {
    pub fn from_span(s: Span) -> Result<Self> {
        let s = s.to_string();

        Ok(Self { value: s })
    }
}

impl fmt::Display for SqlEnding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, PartialEq, Default, Clone)]
pub struct SqlLiteral {
    pub value:  String,
    pub quoted: bool,
}

impl SqlLiteral {
    pub fn from_span(s: Span) -> Result<Self> {
        let s = s.fragment.to_string();

        Ok(Self {
            value: s,
            ..Default::default()
        })
    }
}

impl fmt::Display for SqlLiteral {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SqlBinding {
    pub name:   String,
    pub quoted: bool,
}

impl SqlBinding {
    pub fn from_span(s: Span) -> Result<Self> {
        let s = s.to_string();

        Ok(Self {
            name:   s,
            quoted: false,
        })
    }

    pub fn from_quoted_span(s: Span) -> Result<Self> {
        let s = s.to_string();

        Ok(Self {
            name:   s,
            quoted: false,
        })
    }
}

impl fmt::Display for SqlBinding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}
