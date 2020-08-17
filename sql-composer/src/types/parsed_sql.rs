use crate::types::{ParsedItem, ParsedSqlStatement, Sql, SqlStatement};

pub type ParsedSql = ParsedItem<Sql>;

impl From<ParsedSql> for ParsedSqlStatement {
    fn from(ps: ParsedSql) -> Self {
        let s = SqlStatement {
            sql:      vec![ps.clone().into()],
            complete: true,
        };

        ParsedItem::new(s, Some(ps.position))
    }
}
