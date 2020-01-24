use crate::error::{ErrorKind, Result};

use crate::parser::template;

use std::collections::HashMap;
use std::convert::{From, Into};
use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::path::Path;

use crate::types::{ParsedItem, ParsedSqlComposition, Position, Span, Sql, SqlCompositionAlias,
                   SqlEnding, SqlLiteral};

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
    pub sql:          Vec<Sql>,
    pub position:     Option<Position>,
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
                    .or_insert(Self::from_path(path)?);
            }
        }

        Ok(())
    }

    pub fn insert_alias(&mut self, p: &Path) -> Result<()> {
        self.aliases
            .entry(SqlCompositionAlias::from_path(p))
            .or_insert(Self::from_path(p)?);

        Ok(())
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

    pub fn push_sub_comp(&mut self, value: ParsedSqlComposition) -> Result<()> {
        self.push_sql(Sql::Composition((value, vec![])))
    }

    pub fn push_generated_sub_comp(&mut self, value: Self) -> Result<()> {
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
