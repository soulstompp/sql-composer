pub mod direct;

pub mod rusqlite;
pub mod postgres;
pub mod mysql;

use std::collections::BTreeMap;
pub use super::parser::{SqlStatement, Sql, parse_template};

pub struct BinderConfig {
    start: usize,
}

pub trait Binder : Sized {
    type Value;

    fn bind(&self, s: SqlStatement) -> (String, Vec<Self::Value>) {
        let mut sql = String::new();

        self.bind_statement(s, 1usize, false)
    }

    fn bind_statement(&self, s: SqlStatement, offset: usize, child: bool) -> (String, Vec<Self::Value>) {
        let mut i = offset;

        let mut sql = String::new();

        let mut values:Vec<Self::Value> = vec![];

        for c in s.chunks {
            let (sub_sql, sub_values) = match c {
                Sql::Text(t) => {
                    (t.to_string(), vec![])
                },
                Sql::Binding(b) => {
                    self.bind_values(b.name, i)
                },
                Sql::SubStatement(ss) => {
                    self.bind_statement(ss, i, true)

                }
                Sql::Ending(e) => {
                    if child {
                        ("".to_string(), vec![])
                    }
                    else {
                        (e.to_string(), vec![])
                    }
                }
            };

            sql.push_str(&sub_sql);

            for sv in sub_values {
                values.push(sv);
            }

            i = values.len() + offset;
        };

        (sql, values)
    }

    fn mock_bind(&self, mock_values: Vec<BTreeMap<String, Self::Value>>, offset: usize) -> (String, Vec<Self::Value>) {
        let mut sql = String::new();
        let mut values:Vec<Self::Value> = vec![];

        let mut i = 0;
        let mut r = 0;
        let mut c = 0;

        for row in mock_values {
            if r > 0 {
                sql.push_str(" UNION ");
            }

            sql.push_str("SELECT ");

            for (name, value) in row {
                i += 1;
                c += 1;

                if c > 1 {
                    sql.push_str(", ")
                }

                sql.push_str(&self.bind_var_tag(i + offset, name.to_string()));
                sql.push_str(&format!(" AS {}", &name));

                values.push(value);
            }

            r += 1;
            c = 0;
        }

        (sql, values)
    }

    fn bind_var_tag(&self, u: usize, name: String) -> String;

    fn bind_values(&self, name: String, offset: usize) -> (String, Vec<Self::Value>);

    fn get_values(&self, name: String) -> Option<&Vec<Self::Value>>;

    fn insert_value(&mut self, name: String, values: Vec<Self::Value>) -> ();

    fn config() -> BinderConfig;
}
