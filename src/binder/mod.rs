pub mod direct;

pub mod rusqlite;
pub mod postgres;
pub mod mysql;

pub use super::parser::{SqlStatement, Sql, parse_template};

pub struct BinderConfig {
    start: usize,
}

pub trait Binder : Sized {
    type Value;

    fn bind(&self, s: SqlStatement) -> (String, Vec<Self::Value>) {
        let mut sql = String::new();

        self.bind_statement(s, 0usize)
    }

    fn bind_statement(&self, s: SqlStatement, offset: usize) -> (String, Vec<Self::Value>) {
        let mut i = offset;

        let mut sql = String::new();

        let mut values:Vec<Self::Value> = vec![];

        for c in s.chunks {
            i += 1;

            let (sub_sql, sub_values) = match c {
                Sql::Text(t) => {
                    (t.to_string(), vec![])
                },
                Sql::Binding(b) => {
                    self.bind_values(b.name, i)
                },
                Sql::SubStatement(s) => {
                    (s.to_string(), vec![])
                }
                Sql::Ending(e) =>
                    (e.to_string(), vec![])
            };

            sql.push_str(&sub_sql);

            for sv in sub_values {
                values.push(sv);
            }

            i = values.len() + offset;
        };

        (sql, values)
    }

    fn bind_var_tag(&self, u: usize, name: String) -> String;

    fn bind_values(&self, name: String, offset: usize) -> (String, Vec<Self::Value>);

    fn config() -> BinderConfig;
}
