pub mod direct;

pub mod rusqlite;
pub mod postgres;
pub mod mysql;

use std::collections::{BTreeMap, HashMap};
pub use super::parser::{SqlStatement, SqlStatementAlias, SqlComposition, Sql, parse_template};
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
        self.expand_statement(s, &Vec::new(), &HashMap::new(), 1usize, false)
    }

    fn expand_statement(&self, sc: &SqlComposition, mock_values: &Vec<BTreeMap<String, Rc<Self::Value>>>, child_mock_values: &HashMap<PathBuf, Vec<BTreeMap<String, Rc<Self::Value>>>>, offset: usize, child: bool) -> (String, Vec<Rc<Self::Value>>) {
        let mut i = offset;

        let mut sql = String::new();

        let mut values:Vec<Rc<Self::Value>> = vec![];

        match &sc.stmt {
            Some(s) => {
                panic!("statement already cached!")
            }
            None => {
                if sc.command.is_some() {
                    return self.expand_wrapper(&sc, &mock_values, &child_mock_values, i, true).unwrap();
                }

                for c in &sc.sql {
                    let (sub_sql, sub_values) = match c {
                        Sql::Text(t) => {
                            (t.to_string(), vec![])
                        },
                        Sql::Binding(b) => {
                            self.bind_values(b.name.to_string(), i)
                        },
                        Sql::Composition((ss, aliases)) => {
                            match &ss.stmt {
                                Some(ss_stmt) => {
                                    panic!("stmt already cached!");
                                },
                                None => {
                                    self.expand_statement(&ss, &mock_values, &child_mock_values, i, true)
                                },
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
                }
            },
        }

        (sql, values)
    }

    fn expand_wrapper<'c> (&self, composition: &SqlComposition, mock_values: &Vec<BTreeMap<String, Rc<Self::Value>>>, child_mock_values: &HashMap<PathBuf, Vec<BTreeMap<String, Rc<Self::Value>>>>, offset: usize, child: bool) -> Result<(String, Vec<Rc<Self::Value>>), ()> {
        if composition.stmt.is_some() {
            panic!("we already had a stmt!");
            return Ok(self.expand_statement(composition, mock_values, child_mock_values, offset, child));
        }

        match &composition.command {
            Some(s) => {
                match s.to_lowercase().as_str() {
                    "count" => {
                        let mut out = SqlComposition::default();

                        out.push_text("SELECT COUNT(");

                        let columns = composition.column_list().unwrap();

                        if let Some(c) = columns {
                            out.push_text(&c);
                        }
                        else {
                            out.push_text("*");
                        }

                        out.push_text(") FROM ");

                        for alias in composition.of.iter() {
                            out.push_text("(");
                            match composition.aliases.get(&alias)  {
                                Some(sc) =>  {
                                    out.push_sub_comp(sc.clone());
                                },
                                None => {
                                    panic!("no alias found with alias: {:?}", alias);
                                }
                            }

                            out.push_text(") AS count_main");
                        }


                        out.end(";");

                        Ok(self.expand_statement(&out, mock_values, child_mock_values, offset, child))
                    },
                    "expand" => {
                        let mut out = composition.clone();

                        out.command = None;

                        match &out.stmt {
                            Some(out_stmt) => {
                                panic!("stmt already cached!");
                            },
                            None => {
                                match &out.of[0].path() {
                                    Some(path) => {
                                        match child_mock_values.get(path) {
                                            Some(e) => {
                                                println!("mocking child at {:?}", path);

                                                Ok(self.mock_expand(&out.aliases.get(&out.of[0]).unwrap(), e, &HashMap::new(), offset))
                                            },
                                            None => Ok(self.expand_statement(&out.aliases.get(&out.of[0]).unwrap(), &mock_values, &child_mock_values, offset, child)),
                                        }
                                    }
                                    None => Ok(self.expand_statement(&out.aliases.get(&out.of[0]).unwrap(), &mock_values, &child_mock_values, offset, child)),
                                }
                            },
                        }
                    },
                    "union" => {
                        self.union_expand(composition, mock_values, child_mock_values, offset, child)
                    },
                    // TODO: handle this error better
                    _ => panic!("unknown call")
                }
            },
            None => {
                Ok(self.expand_statement(&composition, mock_values, child_mock_values, offset, child))
            }
        }
    }

    fn union_expand(&self, composition: &SqlComposition, mock_values: &Vec<BTreeMap<String, Rc<Self::Value>>>, child_mock_values: &HashMap<PathBuf, Vec<BTreeMap<String, Rc<Self::Value>>>>, offset: usize, child: bool) -> Result<(String, Vec<Rc<Self::Value>>), ()> {
        let mut out = SqlComposition::default();

        // columns in this case would mean an expand on each side of the union literal
        let columns = composition.column_list().unwrap();

        let mut i = 0usize;

        if composition.of.len() < 2 {
            panic!("union requires 2 of arguments");
        }

        for alias in composition.of.iter() {
            if i > 0 {
                out.push_text(" UNION ");
            }

            match composition.aliases.get(&alias)  {
                Some(sc) =>  {
                    out.push_sub_comp(sc.clone());
                },
                None => {
                    panic!("no alias found with alias: {:?}", alias);
                }
            }

            i += 1;
        }

        out.end(";");

        Ok(self.expand_statement(&out, mock_values, child_mock_values, offset, child))
    }

    fn mock_expand(&self, stmt: &SqlComposition, mock_values: &Vec<BTreeMap<String, Rc<Self::Value>>>, child_mock_values: &HashMap<PathBuf, Vec<BTreeMap<String, Rc<Self::Value>>>>, offset: usize) -> (String, Vec<Rc<Self::Value>>) {
        let mut sql = String::new();
        let mut values:Vec<Rc<Self::Value>> = vec![];

        let mut i = offset;
        let mut r = 0;
        let mut c = 0;

        if i == 0 {
            i = 1
        }

        let mut expected_columns:Option<u8> = None;

        if (mock_values.is_empty()) {
            panic!("empty, can't bind statement");
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
