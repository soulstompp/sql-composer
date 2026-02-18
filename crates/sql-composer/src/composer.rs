//! The composer transforms parsed templates into final SQL with dialect-specific
//! placeholders and resolved compose references.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::mock::MockTable;
use crate::parser;
use crate::types::{Command, CommandKind, Dialect, Element, Template, TemplateSource};

/// The result of composing a template: final SQL and ordered bind parameter names.
#[derive(Debug, Clone, PartialEq)]
pub struct ComposedSql {
    /// The final SQL string with dialect-specific placeholders.
    pub sql: String,
    /// Ordered list of bind parameter names corresponding to placeholders.
    pub bind_params: Vec<String>,
}

/// Composes parsed templates into final SQL.
///
/// Handles dialect-specific placeholder generation, compose reference resolution,
/// and mock table substitution.
pub struct Composer {
    /// The target database dialect for placeholder syntax.
    pub dialect: Dialect,
    /// Directories to search for template files referenced by `:compose()`.
    pub search_paths: Vec<PathBuf>,
    /// Mock tables for test data substitution.
    pub mock_tables: HashMap<String, MockTable>,
}

impl Composer {
    /// Create a new composer with the given dialect.
    pub fn new(dialect: Dialect) -> Self {
        Self {
            dialect,
            search_paths: vec![],
            mock_tables: HashMap::new(),
        }
    }

    /// Add a search path for resolving compose references.
    pub fn add_search_path(&mut self, path: PathBuf) {
        self.search_paths.push(path);
    }

    /// Register a mock table for test data substitution.
    pub fn add_mock_table(&mut self, mock: MockTable) {
        self.mock_tables.insert(mock.name.clone(), mock);
    }

    /// Compose a template into final SQL with placeholders.
    pub fn compose(&self, template: &Template) -> Result<ComposedSql> {
        let mut visited = HashSet::new();
        if let TemplateSource::File(ref path) = template.source {
            visited.insert(path.clone());
        }
        self.compose_inner(template, &mut visited)
    }

    fn compose_inner(
        &self,
        template: &Template,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let mut sql = String::new();
        let mut bind_params = Vec::new();

        for element in &template.elements {
            match element {
                Element::Sql(text) => {
                    sql.push_str(text);
                }
                Element::Bind(binding) => {
                    let index = bind_params.len() + 1;
                    sql.push_str(&self.dialect.placeholder(index));
                    bind_params.push(binding.name.clone());
                }
                Element::Compose(compose_ref) => {
                    let composed = self.resolve_compose(&compose_ref.path, visited)?;
                    // Reindex placeholders from the composed SQL
                    let reindexed =
                        self.reindex_sql(&composed.sql, bind_params.len(), composed.bind_params.len());
                    sql.push_str(&reindexed);
                    bind_params.extend(composed.bind_params);
                }
                Element::Command(command) => {
                    let composed = self.compose_command(command, visited)?;
                    let reindexed =
                        self.reindex_sql(&composed.sql, bind_params.len(), composed.bind_params.len());
                    sql.push_str(&reindexed);
                    bind_params.extend(composed.bind_params);
                }
            }
        }

        Ok(ComposedSql { sql, bind_params })
    }

    /// Resolve a compose reference by finding and parsing the template file.
    fn resolve_compose(
        &self,
        path: &Path,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let resolved = self.find_template(path)?;

        if !visited.insert(resolved.clone()) {
            return Err(Error::CircularReference {
                path: path.to_path_buf(),
            });
        }

        let template = parser::parse_template_file(&resolved)?;
        let result = self.compose_inner(&template, visited)?;

        visited.remove(&resolved);

        Ok(result)
    }

    /// Find a template file on the search paths.
    fn find_template(&self, path: &Path) -> Result<PathBuf> {
        // Try the path directly first
        if path.exists() {
            return Ok(path.to_path_buf());
        }

        // Search on each search path
        for search_path in &self.search_paths {
            let candidate = search_path.join(path);
            if candidate.exists() {
                return Ok(candidate);
            }
        }

        Err(Error::TemplateNotFound {
            path: path.to_path_buf(),
        })
    }

    /// Compose a command (count/union) into SQL.
    fn compose_command(
        &self,
        command: &Command,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        match command.kind {
            CommandKind::Union => self.compose_union(command, visited),
            CommandKind::Count => self.compose_count(command, visited),
        }
    }

    /// Compose a UNION command.
    fn compose_union(
        &self,
        command: &Command,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let mut parts = Vec::new();
        let mut all_params = Vec::new();

        for source in &command.sources {
            let resolved = self.find_template(source)?;
            let template = parser::parse_template_file(&resolved)?;
            let composed = self.compose_inner(&template, visited)?;

            let reindexed = self.reindex_sql(
                &composed.sql,
                all_params.len(),
                composed.bind_params.len(),
            );
            parts.push(reindexed);
            all_params.extend(composed.bind_params);
        }

        let union_kw = if command.all {
            "UNION ALL"
        } else if command.distinct {
            "UNION DISTINCT"
        } else {
            "UNION"
        };

        let sql = parts.join(&format!("\n{union_kw}\n"));

        Ok(ComposedSql {
            sql,
            bind_params: all_params,
        })
    }

    /// Compose a COUNT command.
    fn compose_count(
        &self,
        command: &Command,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let columns = match &command.columns {
            Some(cols) => cols.join(", "),
            None => "*".to_string(),
        };

        // If multiple sources, wrap a union first
        let inner = if command.sources.len() > 1 {
            let union_cmd = Command {
                kind: CommandKind::Union,
                distinct: command.distinct,
                all: command.all,
                columns: None,
                sources: command.sources.clone(),
            };
            self.compose_union(&union_cmd, visited)?
        } else {
            let source = &command.sources[0];
            let resolved = self.find_template(source)?;
            let template = parser::parse_template_file(&resolved)?;
            self.compose_inner(&template, visited)?
        };

        let count_expr = if command.distinct {
            format!("COUNT(DISTINCT {columns})")
        } else {
            format!("COUNT({columns})")
        };

        let sql = format!("SELECT {count_expr} FROM (\n{}\n) AS _count_sub", inner.sql);

        Ok(ComposedSql {
            sql,
            bind_params: inner.bind_params,
        })
    }

    /// Reindex placeholders in composed SQL to account for already-accumulated parameters.
    ///
    /// For Postgres ($1, $2...) and SQLite (?1, ?2...), we need to renumber.
    /// For MySQL (?), no reindexing is needed.
    fn reindex_sql(&self, sql: &str, offset: usize, _param_count: usize) -> String {
        if offset == 0 {
            return sql.to_string();
        }

        match self.dialect {
            Dialect::Mysql => sql.to_string(),
            Dialect::Postgres => {
                let mut result = sql.to_string();
                // Reindex from highest to lowest to avoid $1 -> $11 issues
                // We need to find all $N patterns and add the offset
                let mut new_result = String::with_capacity(result.len());
                let mut chars = result.chars().peekable();
                while let Some(ch) = chars.next() {
                    if ch == '$' {
                        let mut num_str = String::new();
                        while let Some(&next) = chars.peek() {
                            if next.is_ascii_digit() {
                                num_str.push(next);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        if let Ok(n) = num_str.parse::<usize>() {
                            new_result.push_str(&format!("${}", n + offset));
                        } else {
                            new_result.push('$');
                            new_result.push_str(&num_str);
                        }
                    } else {
                        new_result.push(ch);
                    }
                }
                result = new_result;
                result
            }
            Dialect::Sqlite => {
                let mut result = String::with_capacity(sql.len());
                let mut chars = sql.chars().peekable();
                while let Some(ch) = chars.next() {
                    if ch == '?' {
                        let mut num_str = String::new();
                        while let Some(&next) = chars.peek() {
                            if next.is_ascii_digit() {
                                num_str.push(next);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        if let Ok(n) = num_str.parse::<usize>() {
                            result.push_str(&format!("?{}", n + offset));
                        } else {
                            result.push('?');
                            result.push_str(&num_str);
                        }
                    } else {
                        result.push(ch);
                    }
                }
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Binding, Element, TemplateSource};

    #[test]
    fn test_compose_plain_sql() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![Element::Sql("SELECT 1".into())],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        assert_eq!(result.sql, "SELECT 1");
        assert!(result.bind_params.is_empty());
    }

    #[test]
    fn test_compose_with_bindings_postgres() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id = ".into()),
                Element::Bind(Binding {
                    name: "user_id".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND active = ".into()),
                Element::Bind(Binding {
                    name: "active".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE id = $1 AND active = $2"
        );
        assert_eq!(result.bind_params, vec!["user_id", "active"]);
    }

    #[test]
    fn test_compose_with_bindings_mysql() {
        let composer = Composer::new(Dialect::Mysql);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id = ".into()),
                Element::Bind(Binding {
                    name: "user_id".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND active = ".into()),
                Element::Bind(Binding {
                    name: "active".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE id = ? AND active = ?"
        );
        assert_eq!(result.bind_params, vec!["user_id", "active"]);
    }

    #[test]
    fn test_compose_with_bindings_sqlite() {
        let composer = Composer::new(Dialect::Sqlite);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id = ".into()),
                Element::Bind(Binding {
                    name: "user_id".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND active = ".into()),
                Element::Bind(Binding {
                    name: "active".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE id = ?1 AND active = ?2"
        );
        assert_eq!(result.bind_params, vec!["user_id", "active"]);
    }

    #[test]
    fn test_reindex_postgres() {
        let composer = Composer::new(Dialect::Postgres);
        let sql = "WHERE id = $1 AND name = $2";
        let result = composer.reindex_sql(sql, 3, 2);
        assert_eq!(result, "WHERE id = $4 AND name = $5");
    }

    #[test]
    fn test_reindex_mysql_noop() {
        let composer = Composer::new(Dialect::Mysql);
        let sql = "WHERE id = ? AND name = ?";
        let result = composer.reindex_sql(sql, 3, 2);
        assert_eq!(result, "WHERE id = ? AND name = ?");
    }

    #[test]
    fn test_reindex_sqlite() {
        let composer = Composer::new(Dialect::Sqlite);
        let sql = "WHERE id = ?1 AND name = ?2";
        let result = composer.reindex_sql(sql, 3, 2);
        assert_eq!(result, "WHERE id = ?4 AND name = ?5");
    }

    #[test]
    fn test_dialect_placeholder() {
        assert_eq!(Dialect::Postgres.placeholder(1), "$1");
        assert_eq!(Dialect::Postgres.placeholder(10), "$10");
        assert_eq!(Dialect::Mysql.placeholder(1), "?");
        assert_eq!(Dialect::Mysql.placeholder(10), "?");
        assert_eq!(Dialect::Sqlite.placeholder(1), "?1");
        assert_eq!(Dialect::Sqlite.placeholder(10), "?10");
    }
}
