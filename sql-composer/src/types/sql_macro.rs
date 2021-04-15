use crate::error::{Error, Result};

use crate::types::{ParsedItem, SqlCompositionAlias};
use std::convert::Into;
use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::string::ToString;

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

pub enum SqlMacroCommand {
    Compose,
    Count,
    Union,
}

impl ToString for SqlMacroCommand {
    fn to_string(&self) -> String {
        match self {
            Self::Compose => "compose",
            Self::Count => "count",
            Self::Union => "union",
        }
        .to_string()
    }
}

impl FromStr for SqlMacroCommand {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let c = match s {
            "compose" => Self::Compose,
            "count" => Self::Count,
            "union" => Self::Union,
            v @ _ => bail!(
                "Unable to determine SqlMacroCommand from unknown value: {}",
                v
            ),
        };

        Ok(c)
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct SqlMacro {
    pub command:      ParsedItem<String>,
    pub distinct:     Option<ParsedItem<bool>>,
    pub all:          Option<ParsedItem<bool>>,
    pub columns:      Option<Vec<ParsedItem<String>>>,
    pub source_alias: SqlCompositionAlias,
    pub of:           Vec<ParsedItem<SqlCompositionAlias>>,
}

impl SqlMacro {
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
}

impl Hash for SqlMacro {
    fn hash<H: Hasher>(&self, alias: &mut H) {
        self.source_alias.hash(alias);
    }
}

impl fmt::Display for SqlMacro {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //write!(f, "{:?}\n\n", self);
        write!(f, ":{}(", self.command)?;

        let mut i = 0;

        if let Some(cols) = &self.columns {
            for col in cols {
                if i > 0 {
                    write!(f, ",")?;
                }

                write!(f, "{}", col)?;

                i += 1;
            }
        }

        if i > 0 {
            write!(f, " of ")?;
        }

        i = 0;

        for o in &self.of {
            if i > 0 {
                write!(f, ", ")?;
            }

            write!(f, "{}", o.item)?;

            i += 1;
        }

        write!(f, ")")
    }
}