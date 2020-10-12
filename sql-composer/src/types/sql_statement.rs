use crate::error::{ErrorKind, Result};

use std::convert::{From, Into};
use std::default::Default;
use std::fmt;
use std::fmt::Debug;
use std::path::Path;

use crate::types::{ParsedItem, ParsedSql, Span, Sql, SqlCompositionAlias};

use crate::parser::statement as parse_sql_statement;

#[derive(Default, Debug, Eq, PartialEq, Clone)]
pub struct SqlStatement {
    pub sql:      Vec<ParsedSql>,
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

    pub fn push_sql(&mut self, ps: ParsedSql) -> Result<()> {
        if self.complete {
            return Err(ErrorKind::CompositionIncomplete(
                format!(
                    "invalid attempt to push parsed sql {:?} onto a complete statement: {:?}",
                    ps, self
                )
                .into(),
            )
            .into());
        }

        match ps.item {
            Sql::Ending(_) => self.end(ps)?,
            _ => self.sql.push(ps),
        }

        Ok(())
    }

    pub fn end(&mut self, ps: ParsedSql) -> Result<()> {
        match self.sql.last() {
            Some(_last) => {
                self.sql.push(ps);
                self.complete = true;

                Ok(())
            }
            None => Err(ErrorKind::CompositionIncomplete("".into()).into()),
        }
    }

    pub fn is_composition(&self) -> bool {
        if self.sql.len() != 1 {
            return false;
        }

        match self.sql.get(0).unwrap().item {
            Sql::Macro(_) => true,
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
