pub mod rusqlite;
pub mod postgres;

use std::collections::HashMap;
pub use super::parser::{SQLStatement, SQL, parse_template};

pub struct BinderConfig {
    start: usize,
}

pub trait Binder<T> : Sized {
    fn bind(&self, s: SQLStatement) -> (String, T) {
        let mut sql = String::new();

        let mut i = 0usize;
        let mut names:Vec<String> = vec![];

        for c in s.chunks {
            match c {
                SQL::Text(t) => sql.push_str(&t.to_string()),
                SQL::Binding(b) => {
                    i += 1;
                    names.push(b.name.to_string());
                    sql.push_str(&Self::bind_var(i, b.name.to_string()));
                },
                SQL::SubStatement(s) => sql.push_str(&s.to_string()),
                SQL::Ending(e) => sql.push_str(&e.to_string())
            };
        }

        (sql, self.values(names))
    }

    fn bind_var(u: usize, name: String) -> String;

    fn values(&self, Vec<String>) -> T;

    fn config() -> BinderConfig;
}
