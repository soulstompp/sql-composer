pub mod direct;

pub mod rusqlite;
pub mod postgres;
pub mod mysql;

use std::collections::{BTreeMap, HashMap};
pub use super::parser::{SqlStatement, SqlComposition, Sql, parse_template};
use std::path::PathBuf;
use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;

use std::error::Error;

pub struct ExpanderConfig {
    start: usize,
}

pub enum ExpanderErrorMessage {
    Declined,
    Error(String),
}

pub trait Expander : Sized {
    type Value;

    fn expand(&self, s: &SqlComposition) -> (String, Vec<Rc<Self::Value>>) {
        let mut sql = String::new();

        self.expand_statement(s, &Vec::new(), &HashMap::new(), 1usize, false)
    }

    fn expand_statement(&self, sc: &SqlComposition, mock_values: &Vec<BTreeMap<String, Rc<Self::Value>>>, child_mock_values: &HashMap<PathBuf, Vec<BTreeMap<String, Rc<Self::Value>>>>, offset: usize, child: bool) -> (String, Vec<Rc<Self::Value>>) {

        let mut i = offset;

        let mut sql = String::new();

        let mut values:Vec<Rc<Self::Value>> = vec![];

        match &sc.stmt {
            Some(s) => {
                for c in &s.chunks {
                    let (sub_sql, sub_values) = match c {
                        Sql::Text(t) => {
                            (t.to_string(), vec![])
                        },
                        Sql::Binding(b) => {
                            self.bind_values(b.name.to_string(), i)
                        },
                        Sql::SubStatement(ss) => {
                            match &ss.stmt {
                                Some(ss_stmt) => {
                                    match &ss.path {
                                        Some(path) => {
                                            match child_mock_values.get(path) {
                                                Some(e) => self.mock_expand(&ss, e, &HashMap::new(), i),
                                                None => self.expand_statement(&ss, &mock_values, &child_mock_values, i, true),
                                            }
                                        }
                                        None => self.expand_statement(&ss, &mock_values, &child_mock_values, i, true)
                                    }
                                },
                                None => {
                                    panic!("missing a stmt!");
                                }
                            }
                        },
                        Sql::Composition(sw) => {
                            self.expand_wrapper(&sw, &mock_values, &child_mock_values, i, true)
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
                }
            },
            None => panic!("missing statement!")
        }

        (sql, values)
    }

    fn expand_wrapper<'c> (&self, wrapper: &SqlComposition, mock_values: &Vec<BTreeMap<String, Rc<Self::Value>>>, child_mock_values: &HashMap<PathBuf, Vec<BTreeMap<String, Rc<Self::Value>>>>, offset: usize, child: bool) -> (String, Vec<Rc<Self::Value>>) {
        self._expand_default_wrapper(wrapper, mock_values, child_mock_values, offset, child).unwrap()
    }

    fn _expand_default_wrapper(&self, composition: &SqlComposition, mock_values: &Vec<BTreeMap<String, Rc<Self::Value>>>, child_mock_values: &HashMap<PathBuf, Vec<BTreeMap<String, Rc<Self::Value>>>>, offset: usize, child: bool) -> Result<(String, Vec<Rc<Self::Value>>), ()> {
        //TODO: only count certain columns, expand the
        //
        if composition.stmt.is_some() {
            return Ok(self.expand_statement(composition, mock_values, child_mock_values, offset, child));
        }

        match &composition.name {
            Some(s) => {
                match s.as_str() {
                    "count" => {
                        let mut stmt = SqlStatement::new("".into());

                        stmt.push_text("SELECT COUNT(");

                        let columns = composition.column_list().unwrap();

                        stmt.push_text(&columns);

                        stmt.push_text(")");

                        stmt.end(";");

                        let c = SqlComposition::new(stmt);

                        Ok(self.expand_statement(composition, mock_values, child_mock_values, offset, child))
                    },
                    // Handle this better
                    _ => panic!("unknown call")
                }
            },
            None => {
                //TODO: better error
                Err(())
            }
        }
    }

    fn mock_expand(&self, stmt: &SqlComposition, mock_values: &Vec<BTreeMap<String, Rc<Self::Value>>>, child_mock_values: &HashMap<PathBuf, Vec<BTreeMap<String, Rc<Self::Value>>>>, offset: usize) -> (String, Vec<Rc<Self::Value>>) {
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
            return self.expand_statement(&stmt, mock_values, child_mock_values, i, false);
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

    fn config() -> ExpanderConfig;
}
