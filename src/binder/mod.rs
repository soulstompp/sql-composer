pub mod direct;

pub mod rusqlite;
pub mod postgres;
pub mod mysql;

use std::collections::{BTreeMap, HashMap};
pub use super::parser::{SqlStatement, Sql, parse_template};
use std::path::PathBuf;
use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;

pub struct BinderConfig {
    start: usize,
}

pub trait Binder : Sized {
    type Value;

    fn bind(&self, s: &SqlStatement) -> (String, Vec<Rc<Self::Value>>) {
        let mut sql = String::new();

        self.bind_statement(s, &Vec::new(), &HashMap::new(), 1usize, false)
    }

    fn bind_statement(&self, s: &SqlStatement, mock_values: &Vec<BTreeMap<String, Rc<Self::Value>>>, child_mock_values: &HashMap<PathBuf, Vec<BTreeMap<String, Rc<Self::Value>>>>, offset: usize, child: bool) -> (String, Vec<Rc<Self::Value>>) {

        let mut i = offset;

        let mut sql = String::new();

        let mut values:Vec<Rc<Self::Value>> = vec![];

        for c in &s.chunks {
            let (sub_sql, sub_values) = match c {
                Sql::Text(t) => {
                    (t.to_string(), vec![])
                },
                Sql::Binding(b) => {
                    self.bind_values(b.name.to_string(), i)
                },
                Sql::SubStatement(ss) => {
                    match &ss.path {
                        Some(path) => {
                            match child_mock_values.get(path) {
                                Some(e) => self.mock_bind(&ss, e, &HashMap::new(), i),
                                None => self.bind_statement(&ss, &mock_values, &child_mock_values, i, true),
                            }
                        }
                        None => self.bind_statement(&ss, &mock_values, &child_mock_values, i, true)
                    }
                },
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
                values.push(Rc::clone(&sv));
            }

            i = values.len() + offset;
        };

        (sql, values)
    }

    fn mock_bind(&self, stmt: &SqlStatement, mock_values: &Vec<BTreeMap<String, Rc<Self::Value>>>, child_mock_values: &HashMap<PathBuf, Vec<BTreeMap<String, Rc<Self::Value>>>>, offset: usize) -> (String, Vec<Rc<Self::Value>>) {
        let mut sql = String::new();
        let mut values:Vec<Rc<Self::Value>> = vec![];

        let mut i = offset;
        let mut r = 0;
        let mut c = 0;

        if i == 0 {
            i = 1;
        }

        let mut expected_columns:Option<u8> = None;

        if (mock_values.is_empty()) {
            println!("empty, binding statement");
            return self.bind_statement(&stmt, mock_values, child_mock_values, i, false);
        }
        else {
            println!("non-empty, binding union statement directly");
            for row in mock_values.iter() {
                if r > 0 {
                    sql.push_str(" UNION ALL ");
                }

                sql.push_str("SELECT ");

                for (name, value) in row {
                    c += 1;

                    if c > 1 {
                        sql.push_str(", ")
                    }

                    sql.push_str(&self.bind_var_tag(i, name.to_string()));
                    sql.push_str(&format!(" AS {}", &name));

                    values.push(Rc::clone(value));

                    i += 1;
                }

                if let Some(ec) = expected_columns {
                    if c != ec {
                        panic!("expected {} columns found {} for row {}", ec, c, r);
                    }
                }
                else {
                    expected_columns = Some(c);
                }

                r += 1;
                c = 0;
            }
        }

        (sql, values)
    }

    fn bind_var_tag(&self, u: usize, name: String) -> String;

    fn bind_values(&self, name: String, offset: usize) -> (String, Vec<Rc<Self::Value>>);

    fn get_values(&self, name: String) -> Option<&Vec<Rc<Self::Value>>>;

    fn insert_value(&mut self, name: String, values: Vec<Rc<Self::Value>>) -> ();

    fn config() -> BinderConfig;
}
