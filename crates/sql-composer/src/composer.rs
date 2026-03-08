//! The composer transforms parsed templates into final SQL with dialect-specific
//! placeholders and resolved compose references.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
    ///
    /// For numbered dialects (Postgres, SQLite), names are in alphabetical order
    /// with duplicates removed. For positional dialects (MySQL), names are in
    /// document order.
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

    /// Compose a template with value counts, expanding multi-value bindings
    /// into multiple placeholders.
    ///
    /// When a `:bind(name)` has multiple values in the map, this method emits
    /// one placeholder per value (e.g. `$1, $2, $3` for 3 values), and repeats
    /// the bind name in `bind_params` for each. This enables `IN` clauses:
    ///
    /// ```text
    /// SELECT * FROM users WHERE id IN (:bind(ids))
    /// -- with ids=[10, 20, 30] becomes:
    /// SELECT * FROM users WHERE id IN ($1, $2, $3)
    /// -- bind_params = ["ids", "ids", "ids"]
    /// ```
    ///
    /// For bindings with only one value, behavior is identical to [`Composer::compose()`].
    pub fn compose_with_values<V>(
        &self,
        template: &Template,
        values: &BTreeMap<String, Vec<V>>,
    ) -> Result<ComposedSql> {
        let mut visited = HashSet::new();
        if let TemplateSource::File(ref path) = template.source {
            visited.insert(path.clone());
        }
        self.compose_with_values_inner(template, values, &mut visited)
    }

    // ── Dispatch ──────────────────────────────────────────────────────

    fn compose_inner(
        &self,
        template: &Template,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        if self.dialect.supports_numbered_placeholders() {
            self.compose_inner_numbered(template, visited)
        } else {
            self.compose_inner_positional(template, visited)
        }
    }

    fn compose_with_values_inner<V>(
        &self,
        template: &Template,
        values: &BTreeMap<String, Vec<V>>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        if self.dialect.supports_numbered_placeholders() {
            self.compose_with_values_numbered(template, values, visited)
        } else {
            self.compose_with_values_positional(template, values, visited)
        }
    }

    // ── Numbered path (Postgres, SQLite) ──────────────────────────────
    //
    // Two-pass approach:
    //   Pass 1 — collect all unique bind names (BTreeSet gives alphabetical order)
    //   Allocate — assign 1-based indices from the sorted names
    //   Pass 2 — emit SQL using the global index map (same name → same $N)

    /// Pass 1: Recursively collect unique bind names from a template tree.
    fn collect_bind_names(
        &self,
        template: &Template,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<BTreeSet<String>> {
        let mut names = BTreeSet::new();

        for element in &template.elements {
            match element {
                Element::Sql(_) => {}
                Element::Bind(binding) => {
                    names.insert(binding.name.clone());
                }
                Element::Compose(compose_ref) => {
                    let sub = self.collect_compose_bind_names(&compose_ref.path, visited)?;
                    names.extend(sub);
                }
                Element::Command(command) => {
                    let sub = self.collect_command_bind_names(command, visited)?;
                    names.extend(sub);
                }
            }
        }

        Ok(names)
    }

    /// Collect bind names from a compose reference's resolved template.
    fn collect_compose_bind_names(
        &self,
        path: &Path,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<BTreeSet<String>> {
        let resolved = self.find_template(path)?;

        if !visited.insert(resolved.clone()) {
            return Err(Error::CircularReference {
                path: path.to_path_buf(),
            });
        }

        let template = parser::parse_template_file(&resolved)?;
        let names = self.collect_bind_names(&template, visited)?;

        visited.remove(&resolved);
        Ok(names)
    }

    /// Collect bind names from all sources in a command.
    fn collect_command_bind_names(
        &self,
        command: &Command,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<BTreeSet<String>> {
        let mut names = BTreeSet::new();
        for source in &command.sources {
            let resolved = self.find_template(source)?;
            let template = parser::parse_template_file(&resolved)?;
            let sub = self.collect_bind_names(&template, visited)?;
            names.extend(sub);
        }
        Ok(names)
    }

    /// Build an index map for `compose` (single-value bindings).
    /// Each name maps to `(1-based-index, 1)`.
    fn build_index_map(names: &BTreeSet<String>) -> BTreeMap<String, (usize, usize)> {
        names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), (i + 1, 1)))
            .collect()
    }

    /// Build an index map for `compose_with_values` (multi-value bindings).
    /// Each name maps to `(start_index, count)` where count comes from the
    /// values map (defaults to 1 if absent).
    fn build_index_map_with_values<V>(
        names: &BTreeSet<String>,
        values: &BTreeMap<String, Vec<V>>,
    ) -> BTreeMap<String, (usize, usize)> {
        let mut map = BTreeMap::new();
        let mut index = 1;
        for name in names {
            let count = values.get(name).map(|vs| vs.len()).unwrap_or(1).max(1);
            map.insert(name.clone(), (index, count));
            index += count;
        }
        map
    }

    /// Two-pass compose for numbered dialects (single-value).
    fn compose_inner_numbered(
        &self,
        template: &Template,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        // Pass 1: collect
        let mut collect_visited = visited.clone();
        let names = self.collect_bind_names(template, &mut collect_visited)?;

        // Allocate
        let index_map = Self::build_index_map(&names);
        let bind_params: Vec<String> = names.into_iter().collect();

        // Pass 2: emit
        let mut sql = String::new();
        self.emit_sql_numbered(template, &index_map, &mut sql, visited)?;

        Ok(ComposedSql { sql, bind_params })
    }

    /// Two-pass compose for numbered dialects (multi-value).
    fn compose_with_values_numbered<V>(
        &self,
        template: &Template,
        values: &BTreeMap<String, Vec<V>>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        // Pass 1: collect
        let mut collect_visited = visited.clone();
        let names = self.collect_bind_names(template, &mut collect_visited)?;

        // Allocate with value counts
        let index_map = Self::build_index_map_with_values(&names, values);

        // Build bind_params: each name repeated by its value count, alphabetical
        let mut bind_params = Vec::new();
        for name in &names {
            let count = values
                .get(name.as_str())
                .map(|vs| vs.len())
                .unwrap_or(1)
                .max(1);
            for _ in 0..count {
                bind_params.push(name.clone());
            }
        }

        // Pass 2: emit
        let mut sql = String::new();
        self.emit_sql_numbered(template, &index_map, &mut sql, visited)?;

        Ok(ComposedSql { sql, bind_params })
    }

    /// Pass 2: Emit SQL for a template using the global index map.
    fn emit_sql_numbered(
        &self,
        template: &Template,
        index_map: &BTreeMap<String, (usize, usize)>,
        sql: &mut String,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        for element in &template.elements {
            match element {
                Element::Sql(text) => sql.push_str(text),
                Element::Bind(binding) => {
                    let &(start, count) = &index_map[&binding.name];
                    for i in 0..count {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        sql.push_str(&self.dialect.placeholder(start + i));
                    }
                }
                Element::Compose(compose_ref) => {
                    self.emit_compose_numbered(&compose_ref.path, index_map, sql, visited)?;
                }
                Element::Command(command) => {
                    self.emit_command_numbered(command, index_map, sql, visited)?;
                }
            }
        }
        Ok(())
    }

    /// Emit SQL for a compose reference using the global index map.
    fn emit_compose_numbered(
        &self,
        path: &Path,
        index_map: &BTreeMap<String, (usize, usize)>,
        sql: &mut String,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        let resolved = self.find_template(path)?;

        if !visited.insert(resolved.clone()) {
            return Err(Error::CircularReference {
                path: path.to_path_buf(),
            });
        }

        let template = parser::parse_template_file(&resolved)?;
        self.emit_sql_numbered(&template, index_map, sql, visited)?;

        visited.remove(&resolved);
        Ok(())
    }

    /// Emit SQL for a command (union/count) using the global index map.
    fn emit_command_numbered(
        &self,
        command: &Command,
        index_map: &BTreeMap<String, (usize, usize)>,
        sql: &mut String,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        match command.kind {
            CommandKind::Union => self.emit_union_numbered(command, index_map, sql, visited),
            CommandKind::Count => self.emit_count_numbered(command, index_map, sql, visited),
        }
    }

    /// Emit SQL for a UNION command using the global index map.
    fn emit_union_numbered(
        &self,
        command: &Command,
        index_map: &BTreeMap<String, (usize, usize)>,
        sql: &mut String,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        let union_kw = if command.all {
            "UNION ALL"
        } else if command.distinct {
            "UNION DISTINCT"
        } else {
            "UNION"
        };

        for (i, source) in command.sources.iter().enumerate() {
            if i > 0 {
                sql.push_str(&format!("\n{union_kw}\n"));
            }
            let resolved = self.find_template(source)?;
            let template = parser::parse_template_file(&resolved)?;
            self.emit_sql_numbered(&template, index_map, sql, visited)?;
        }

        Ok(())
    }

    /// Emit SQL for a COUNT command using the global index map.
    fn emit_count_numbered(
        &self,
        command: &Command,
        index_map: &BTreeMap<String, (usize, usize)>,
        sql: &mut String,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        let columns = match &command.columns {
            Some(cols) => cols.join(", "),
            None => "*".to_string(),
        };

        let count_expr = if command.distinct {
            format!("COUNT(DISTINCT {columns})")
        } else {
            format!("COUNT({columns})")
        };

        sql.push_str(&format!("SELECT {count_expr} FROM (\n"));

        if command.sources.len() > 1 {
            let union_cmd = Command {
                kind: CommandKind::Union,
                distinct: command.distinct,
                all: command.all,
                columns: None,
                sources: command.sources.clone(),
            };
            self.emit_union_numbered(&union_cmd, index_map, sql, visited)?;
        } else {
            let source = &command.sources[0];
            let resolved = self.find_template(source)?;
            let template = parser::parse_template_file(&resolved)?;
            self.emit_sql_numbered(&template, index_map, sql, visited)?;
        }

        sql.push_str("\n) AS _count_sub");
        Ok(())
    }

    // ── Positional path (MySQL) ───────────────────────────────────────
    //
    // Document-order placeholders with bare `?`. No reindexing needed
    // since MySQL placeholders carry no index number.

    fn compose_inner_positional(
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
                    sql.push_str(&composed.sql);
                    bind_params.extend(composed.bind_params);
                }
                Element::Command(command) => {
                    let composed = self.compose_command(command, visited)?;
                    sql.push_str(&composed.sql);
                    bind_params.extend(composed.bind_params);
                }
            }
        }

        Ok(ComposedSql { sql, bind_params })
    }

    fn compose_with_values_positional<V>(
        &self,
        template: &Template,
        values: &BTreeMap<String, Vec<V>>,
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
                    let count = values
                        .get(&binding.name)
                        .map(|vs| vs.len())
                        .unwrap_or(1)
                        .max(1);

                    for i in 0..count {
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        let index = bind_params.len() + 1;
                        sql.push_str(&self.dialect.placeholder(index));
                        bind_params.push(binding.name.clone());
                    }
                }
                Element::Compose(compose_ref) => {
                    let composed =
                        self.resolve_compose_with_values(&compose_ref.path, values, visited)?;
                    sql.push_str(&composed.sql);
                    bind_params.extend(composed.bind_params);
                }
                Element::Command(command) => {
                    let composed = self.compose_command(command, visited)?;
                    sql.push_str(&composed.sql);
                    bind_params.extend(composed.bind_params);
                }
            }
        }

        Ok(ComposedSql { sql, bind_params })
    }

    /// Resolve a compose reference by finding and parsing the template file.
    fn resolve_compose(&self, path: &Path, visited: &mut HashSet<PathBuf>) -> Result<ComposedSql> {
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

    /// Resolve a compose reference with value-aware expansion.
    fn resolve_compose_with_values<V>(
        &self,
        path: &Path,
        values: &BTreeMap<String, Vec<V>>,
        visited: &mut HashSet<PathBuf>,
    ) -> Result<ComposedSql> {
        let resolved = self.find_template(path)?;

        if !visited.insert(resolved.clone()) {
            return Err(Error::CircularReference {
                path: path.to_path_buf(),
            });
        }

        let template = parser::parse_template_file(&resolved)?;
        let result = self.compose_with_values_inner(&template, values, visited)?;

        visited.remove(&resolved);
        Ok(result)
    }

    /// Compose a command (count/union) into SQL (positional path).
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

    /// Compose a UNION command (positional path).
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

            parts.push(composed.sql);
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

    /// Compose a COUNT command (positional path).
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

    // ── Shared helpers ────────────────────────────────────────────────

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
        // Alphabetical: active=$1, user_id=$2
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE id = $2 AND active = $1"
        );
        assert_eq!(result.bind_params, vec!["active", "user_id"]);
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
        // MySQL: document order, bare ?
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
        // Alphabetical: active=?1, user_id=?2
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE id = ?2 AND active = ?1"
        );
        assert_eq!(result.bind_params, vec!["active", "user_id"]);
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

    #[test]
    fn test_compose_with_values_single() {
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
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> = BTreeMap::from([("user_id".into(), vec![42])]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        assert_eq!(result.sql, "SELECT * FROM users WHERE id = $1");
        assert_eq!(result.bind_params, vec!["user_id"]);
    }

    #[test]
    fn test_compose_with_values_multi_postgres() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(")".into()),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> = BTreeMap::from([("ids".into(), vec![10, 20, 30])]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        assert_eq!(result.sql, "SELECT * FROM users WHERE id IN ($1, $2, $3)");
        assert_eq!(result.bind_params, vec!["ids", "ids", "ids"]);
    }

    #[test]
    fn test_compose_with_values_multi_mysql() {
        let composer = Composer::new(Dialect::Mysql);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(")".into()),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> = BTreeMap::from([("ids".into(), vec![10, 20, 30])]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        assert_eq!(result.sql, "SELECT * FROM users WHERE id IN (?, ?, ?)");
        assert_eq!(result.bind_params, vec!["ids", "ids", "ids"]);
    }

    #[test]
    fn test_compose_with_values_multi_sqlite() {
        let composer = Composer::new(Dialect::Sqlite);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT * FROM users WHERE id IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(") AND status = ".into()),
                Element::Bind(Binding {
                    name: "status".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> =
            BTreeMap::from([("ids".into(), vec![10, 20]), ("status".into(), vec![1])]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        // Alphabetical: ids=(1,2), status=(3,1) → ids=?1,?2  status=?3
        assert_eq!(
            result.sql,
            "SELECT * FROM users WHERE id IN (?1, ?2) AND status = ?3"
        );
        assert_eq!(result.bind_params, vec!["ids", "ids", "status"]);
    }

    // ── Alphabetical ordering tests ───────────────────────────────────

    #[test]
    fn test_alphabetical_ordering_postgres() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT ".into()),
                Element::Bind(Binding {
                    name: "z_param".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(", ".into()),
                Element::Bind(Binding {
                    name: "a_param".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        // a_param=$1 (alphabetically first), z_param=$2
        assert_eq!(result.sql, "SELECT $2, $1");
        assert_eq!(result.bind_params, vec!["a_param", "z_param"]);
    }

    #[test]
    fn test_alphabetical_ordering_sqlite() {
        let composer = Composer::new(Dialect::Sqlite);
        let template = Template {
            elements: vec![
                Element::Sql("SELECT ".into()),
                Element::Bind(Binding {
                    name: "z_param".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(", ".into()),
                Element::Bind(Binding {
                    name: "a_param".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        assert_eq!(result.sql, "SELECT ?2, ?1");
        assert_eq!(result.bind_params, vec!["a_param", "z_param"]);
    }

    // ── Dedup tests ───────────────────────────────────────────────────

    #[test]
    fn test_dedup_single_value_postgres() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("WHERE a = ".into()),
                Element::Bind(Binding {
                    name: "x".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND b = ".into()),
                Element::Bind(Binding {
                    name: "x".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        // Both :bind(x) emit $1, bind_params has one entry
        assert_eq!(result.sql, "WHERE a = $1 AND b = $1");
        assert_eq!(result.bind_params, vec!["x"]);
    }

    #[test]
    fn test_dedup_multi_value_postgres() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("WHERE a IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(") AND b IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(")".into()),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> = BTreeMap::from([("ids".into(), vec![10, 20, 30])]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        // Both emit $1, $2, $3 — same placeholders
        assert_eq!(result.sql, "WHERE a IN ($1, $2, $3) AND b IN ($1, $2, $3)");
        assert_eq!(result.bind_params, vec!["ids", "ids", "ids"]);
    }

    #[test]
    fn test_mixed_multi_and_single_values() {
        let composer = Composer::new(Dialect::Postgres);
        let template = Template {
            elements: vec![
                Element::Sql("WHERE active = ".into()),
                Element::Bind(Binding {
                    name: "active".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND id IN (".into()),
                Element::Bind(Binding {
                    name: "ids".into(),
                    min_values: Some(1),
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(") AND user_id = ".into()),
                Element::Bind(Binding {
                    name: "user_id".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let values: BTreeMap<String, Vec<i32>> = BTreeMap::from([
            ("active".into(), vec![1]),
            ("ids".into(), vec![10, 20, 30]),
            ("user_id".into(), vec![42]),
        ]);
        let result = composer.compose_with_values(&template, &values).unwrap();
        // Alphabetical: active(1)=$1, ids(3)=$2,$3,$4, user_id(1)=$5
        assert_eq!(
            result.sql,
            "WHERE active = $1 AND id IN ($2, $3, $4) AND user_id = $5"
        );
        assert_eq!(
            result.bind_params,
            vec!["active", "ids", "ids", "ids", "user_id"]
        );
    }

    #[test]
    fn test_mysql_no_dedup() {
        let composer = Composer::new(Dialect::Mysql);
        let template = Template {
            elements: vec![
                Element::Sql("WHERE a = ".into()),
                Element::Bind(Binding {
                    name: "x".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
                Element::Sql(" AND b = ".into()),
                Element::Bind(Binding {
                    name: "x".into(),
                    min_values: None,
                    max_values: None,
                    nullable: false,
                }),
            ],
            source: TemplateSource::Literal("test".into()),
        };
        let result = composer.compose(&template).unwrap();
        // MySQL: document order, no dedup, bare ?
        assert_eq!(result.sql, "WHERE a = ? AND b = ?");
        assert_eq!(result.bind_params, vec!["x", "x"]);
    }

    #[test]
    fn test_supports_numbered_placeholders() {
        assert!(Dialect::Postgres.supports_numbered_placeholders());
        assert!(Dialect::Sqlite.supports_numbered_placeholders());
        assert!(!Dialect::Mysql.supports_numbered_placeholders());
    }
}
