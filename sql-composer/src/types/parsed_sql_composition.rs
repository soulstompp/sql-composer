use crate::error::{Error, ErrorKind, Result};
use crate::types::{ParsedItem, ParsedSql, ParsedSqlStatement, Sql, SqlComposition};

use std::convert::TryFrom;

pub type ParsedSqlComposition = ParsedItem<SqlComposition>;

impl From<ParsedSqlComposition> for ParsedSqlStatement {
    fn from(psc: ParsedSqlComposition) -> Self {
        psc.into()
    }
}

impl From<ParsedSqlComposition> for ParsedSql {
    fn from(s: ParsedSqlComposition) -> Self {
        ParsedItem::new(Sql::Composition(s.item()), Some(s.position))
    }
}

impl TryFrom<ParsedSqlStatement> for ParsedSqlComposition {
    type Error = Error;

    fn try_from(pss: ParsedSqlStatement) -> Result<Self> {
        let stmt = pss.item;

        if stmt.sql.len() != 1 {
            bail!(ErrorKind::ParsedSqlStatementIntoParsedSqlCompositionInvalidSqlLength("".into()))
        }

        match &stmt.sql[0].item {
            Sql::Composition(c) => {
                let pc = ParsedItem {
                    item:     c.clone(),
                    position: pss.position,
                };
                Ok(pc)
            }
            _ => bail!(
                ErrorKind::ParsedSqlStatementIntoParsedSqlCompositionInvalidVariant("".into())
            ),
        }
    }
}
