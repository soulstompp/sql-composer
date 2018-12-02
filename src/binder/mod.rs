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

        let mut i = 0usize;
        let mut names:Vec<String> = vec![];

        for c in s.chunks {
            match c {
                Sql::Text(t) => sql.push_str(&t.to_string()),
                Sql::Binding(b) => {
                    i += 1;
                    names.push(b.name.to_string());
                    sql.push_str(&self.bind_var(i, b.name.to_string()));
                },
                Sql::SubStatement(s) => sql.push_str(&s.to_string()),
                Sql::Ending(e) => sql.push_str(&e.to_string())
            };
        }

        (sql, self.values(names))
    }

    fn bind_var(&self, u: usize, name: String) -> String;

    fn values(&self, Vec<String>) -> Vec<Self::Value>;

    fn config() -> BinderConfig;
}
