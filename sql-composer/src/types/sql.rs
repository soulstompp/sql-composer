// import ParsedItem, SqlComposition SqlCompositionAlias

use std::fmt;

use crate::error::Result;
use crate::types::{ParsedItem, SqlComposition, SqlCompositionAlias};

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
        write!(f, "{}", self.object_name)?;

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
