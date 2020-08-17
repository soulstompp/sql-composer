use crate::error::{ErrorKind, Result};

use std::collections::HashMap;
use std::convert::Into;
use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use crate::types::{ParsedItem, Position, SqlCompositionAlias};

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
    pub command:      Option<ParsedItem<String>>,
    pub distinct:     Option<ParsedItem<bool>>,
    pub all:          Option<ParsedItem<bool>>,
    pub columns:      Option<Vec<ParsedItem<String>>>,
    pub source_alias: SqlCompositionAlias,
    pub of:           Vec<ParsedItem<SqlCompositionAlias>>,
    pub aliases:      HashMap<SqlCompositionAlias, ParsedItem<SqlComposition>>,
    pub position:     Option<Position>,
}

impl SqlComposition {
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

    pub fn set_position(&mut self, new: Position) -> Result<()> {
        if self.position.is_some() {
            bail!(ErrorKind::CompositionAliasConflict(
                "bad posisition".to_string()
            ))
        }
        self.position = Some(new);
        Ok(())
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
