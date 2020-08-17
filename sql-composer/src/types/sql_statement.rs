use crate::error::{ErrorKind, Result};

use std::convert::{From, Into};
use std::default::Default;
use std::fmt;
use std::fmt::Debug;
use std::path::Path;

use crate::types::{ParsedItem, Span, Sql, SqlComposition, SqlCompositionAlias, SqlEnding,
                   SqlLiteral};

use crate::parser::statement as parse_sql_statement;

#[derive(Default, Debug, Eq, PartialEq, Clone)]
pub struct SqlStatement {
    pub sql:      Vec<ParsedItem<Sql>>,
    pub complete: bool,
}

impl SqlStatement {
    //TODO: properly check remaining along with a few other traits
    pub fn parse(alias: SqlCompositionAlias) -> Result<ParsedItem<Self>> {
        let stmt = parse_sql_statement(Span::new(&alias.read_raw_sql()?), alias)?;

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

    pub fn push_sql(&mut self, s: Sql) -> Result<()> {
        let i = ParsedItem::new(s, None);
        self.sql.push(i);

        Ok(())
    }

    pub fn push_sub_comp(&mut self, value: SqlComposition) -> Result<()> {
        self.push_sql(Sql::Composition((ParsedItem::new(value, None), vec![])))
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
                )
                .unwrap(),
            )),
            None => Err(ErrorKind::CompositionIncomplete("".into()).into()),
        }
    }

    pub fn is_composition(&self) -> bool {
        if self.sql.len() != 1 {
            return false;
        }

        match self.sql.get(0).unwrap().item {
            Sql::Composition(_) => true,
            _ => false,
        }
    }
}

impl fmt::Display for SqlStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for sql in &self.sql {
            write!(f, "{} ", sql)?;
        }

        Ok(())
    }
}
