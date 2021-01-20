use crate::error::{Error, ErrorKind, Result};
use crate::types::{ParsedItem, ParsedSql, ParsedSqlStatement, Sql, SqlMacro};

use std::convert::TryFrom;

pub type ParsedSqlMacro = ParsedItem<SqlMacro>;

impl From<ParsedSqlMacro> for ParsedSqlStatement {
    fn from(psc: ParsedSqlMacro) -> Self {
        psc.into()
    }
}

impl From<ParsedSqlMacro> for ParsedSql {
    fn from(s: ParsedSqlMacro) -> Self {
        ParsedItem::new(Sql::Macro(s.item()), Some(s.position))
    }
}

impl TryFrom<ParsedSqlStatement> for ParsedSqlMacro {
    type Error = Error;

    fn try_from(pss: ParsedSqlStatement) -> Result<Self> {
        let stmt = pss.item;

        if stmt.sql.len() != 1 {
            bail!(ErrorKind::ParsedSqlStatementIntoParsedSqlMacroInvalidSqlLength("".into()))
        }

        match &stmt.sql[0].item {
            Sql::Macro(c) => {
                let pc = ParsedItem {
                    item:     c.clone(),
                    position: pss.position,
                };
                Ok(pc)
            }
            _ => bail!(ErrorKind::ParsedSqlStatementIntoParsedSqlMacroInvalidVariant("".into())),
        }
    }
}
