// import ParsedItem, SqlComposition SqlCompositionAlias

use std::fmt;

use crate::error::Result;
use crate::types::SqlMacro;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Sql {
    Literal(SqlLiteral),
    Binding(SqlBinding),
    Macro(SqlMacro),
    Ending(SqlEnding),
    DbObject(SqlDbObject),
    Keyword(SqlKeyword),
}

impl fmt::Display for Sql {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Sql::Literal(t) => write!(f, "{}", t)?,
            Sql::Binding(b) => write!(f, "{}", b)?,
            Sql::Macro(m) => write!(f, "{:?}", m)?,
            Sql::Ending(e) => write!(f, "{}", e)?,
            Sql::DbObject(ft) => write!(f, "{}", ft)?,
            Sql::Keyword(k) => write!(f, "{}", k)?,
        }

        write!(f, "")
    }
}

impl Default for Sql {
    fn default() -> Self {
        Sql::Literal(SqlLiteral::default())
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

impl From<SqlEnding> for Sql {
    fn from(value: SqlEnding) -> Self {
        Sql::Ending(value)
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

impl From<SqlDbObject> for Sql {
    fn from(value: SqlDbObject) -> Self {
        Sql::DbObject(value)
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

impl From<SqlKeyword> for Sql {
    fn from(value: SqlKeyword) -> Self {
        Sql::Keyword(value)
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Default, Clone)]
pub struct SqlMacroLiteral(SqlLiteral);

impl SqlMacroLiteral {
    pub fn new(m: &SqlMacro) -> Self {
        Self(SqlLiteral {
            value: m.to_string(),
            ..Default::default()
        })
    }
}

impl fmt::Display for SqlMacroLiteral {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.0)
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
        write!(f, "{}", &self.value.trim())
    }
}

impl From<SqlLiteral> for Sql {
    fn from(value: SqlLiteral) -> Self {
        Sql::Literal(value)
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

impl From<SqlBinding> for Sql {
    fn from(value: SqlBinding) -> Self {
        Sql::Binding(value)
    }
}
