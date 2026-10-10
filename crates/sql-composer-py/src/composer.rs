//! Parsing a template, and composing it for a dialect.

use std::collections::BTreeMap;
use std::path::PathBuf;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use sql_composer::types::{Dialect, TemplateSource};
use sql_composer::{parser, ComposedSql, MockTable};

use crate::errors::raised;
use crate::repr;

/// A parsed template, from `parse` or `parse_file`.
#[pyclass(module = "sql_composer", frozen)]
pub struct Template {
    pub(crate) inner: sql_composer::Template,
}

#[pymethods]
impl Template {
    /// Where the template came from: its file's path, or the name it was parsed under.
    #[getter]
    fn source(&self) -> String {
        match &self.inner.source {
            TemplateSource::File(path) => path.display().to_string(),
            TemplateSource::Literal(name) => name.clone(),
            other => format!("{other:?}"),
        }
    }

    /// The dialects its `:dialect(…)` names, in order; empty when it names none, which is every
    /// dialect.
    #[getter]
    fn dialects(&self) -> Vec<&'static str> {
        self.inner.dialects.iter().map(Dialect::name).collect()
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!("Template({})", repr(py, self.source())?))
    }
}

/// Parse the template `text`. `name` is what an error calls it. Its `:compose` paths are found on
/// the search paths of the composer that composes it.
#[pyfunction]
#[pyo3(signature = (text, name = "template"))]
pub fn parse(text: &str, name: &str) -> PyResult<Template> {
    parser::parse_template(text, TemplateSource::Literal(name.to_string()))
        .map(|inner| Template { inner })
        .map_err(raised)
}

/// Parse the template in the file at `path`. Its `:compose` paths are found on the search paths
/// of the composer that composes it.
#[pyfunction]
pub fn parse_file(path: PathBuf) -> PyResult<Template> {
    parser::parse_template_file(&path)
        .map(|inner| Template { inner })
        .map_err(raised)
}

/// A composed template: its SQL, its bind names in placeholder order, and where each statement's
/// names begin.
#[pyclass(module = "sql_composer", frozen, eq, skip_from_py_object)]
#[derive(Clone, PartialEq)]
pub struct Composed {
    /// The composed SQL, with the dialect's placeholders.
    #[pyo3(get)]
    sql: String,
    /// The bind names, one for each placeholder, in order. On PostgreSQL, SQLite and DuckDB a
    /// statement's names are in alphabetical order, each once; on MySQL and MariaDB they are in
    /// the order the statement uses them.
    #[pyo3(get)]
    bind_params: Vec<String>,
    /// Where each statement's names begin in `bind_params`, one for each statement, the first 0.
    /// Each statement numbers its placeholders from the first.
    #[pyo3(get)]
    statement_starts: Vec<usize>,
}

impl From<ComposedSql> for Composed {
    fn from(composed: ComposedSql) -> Self {
        Composed {
            sql: composed.sql,
            bind_params: composed.bind_params,
            statement_starts: composed.statement_starts,
        }
    }
}

#[pymethods]
impl Composed {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Composed(sql={}, bind_params={}, statement_starts={})",
            repr(py, &self.sql)?,
            repr(py, &self.bind_params)?,
            repr(py, &self.statement_starts)?
        ))
    }
}

/// A composer for a dialect: `postgres`, `mysql`, `mariadb`, `sqlite` or `duckdb`.
///
/// `search_paths` are the directories a `:compose` is looked for in, in order. `name_relations`
/// writes every relation a statement reads once, named, at the head of that statement, as
/// `cargo sqlc compose --with` does. `views` is a view registry, found on the search paths: every
/// `CREATE VIEW <name> AS :define(<path>)` in it makes the composed SQL name the view wherever
/// `<path>` is composed.
#[pyclass(module = "sql_composer")]
pub struct Composer {
    inner: sql_composer::Composer,
}

#[pymethods]
impl Composer {
    #[new]
    #[pyo3(signature = (dialect, *, search_paths = Vec::new(), name_relations = false, views = None))]
    fn new(
        dialect: &str,
        search_paths: Vec<PathBuf>,
        name_relations: bool,
        views: Option<PathBuf>,
    ) -> PyResult<Self> {
        let dialect: Dialect = dialect.parse().map_err(raised)?;
        let mut inner = sql_composer::Composer::new(dialect);
        for path in search_paths {
            inner.add_search_path(path);
        }
        inner.name_relations = name_relations;
        if let Some(registry) = views {
            inner.load_view_registry(&registry).map_err(raised)?;
        }
        Ok(Composer { inner })
    }

    /// The dialect, by its name.
    #[getter]
    fn dialect(&self) -> &'static str {
        self.inner.dialect.name()
    }

    /// Whether every relation a statement reads is written once, named, at its head.
    #[getter]
    fn name_relations(&self) -> bool {
        self.inner.name_relations
    }

    #[setter]
    fn set_name_relations(&mut self, name_relations: bool) {
        self.inner.name_relations = name_relations;
    }

    /// Look for the templates a `:compose` names in `path` too, after the directories already
    /// given.
    fn add_search_path(&mut self, path: PathBuf) {
        self.inner.add_search_path(path);
    }

    /// Register every `CREATE VIEW <name> AS :define(<path>)` in the registry template at
    /// `registry`, found on the search paths.
    fn load_view_registry(&mut self, registry: PathBuf) -> PyResult<()> {
        self.inner.load_view_registry(&registry).map_err(raised)
    }

    /// Give the table `name` rows of your own: every statement composed after this that names the
    /// table reads them instead. `columns` are `(name, type)` pairs, the type as the dialect spells
    /// it; each row holds a value for each column, as text, or `None` for SQL's `NULL`. A mock of
    /// a name already added replaces it.
    #[pyo3(signature = (name, columns, rows = Vec::new()))]
    fn add_mock_table(
        &mut self,
        name: &str,
        columns: Vec<(String, String)>,
        rows: Vec<Vec<Option<String>>>,
    ) -> PyResult<()> {
        let mut mock = MockTable::new(name);
        for (column, sql_type) in &columns {
            mock = mock.column(column, sql_type);
        }
        for row in &rows {
            if row.len() != columns.len() {
                return Err(PyValueError::new_err(format!(
                    "a row of the mock {name} holds {} values for {} columns",
                    row.len(),
                    columns.len()
                )));
            }
            mock = mock.row(&row.iter().map(Option::as_deref).collect::<Vec<_>>());
        }
        self.inner.add_mock_table(mock);
        Ok(())
    }

    /// Compose `template`. `counts` gives a binding a number of values: a placeholder for each,
    /// and its name that many times in `bind_params`. A binding it does not name takes one
    /// placeholder, and a count outside what the binding's `EXPECTING` allows is refused.
    #[pyo3(signature = (template, *, counts = None))]
    fn compose(
        &self,
        py: Python<'_>,
        template: &Bound<'_, Template>,
        counts: Option<BTreeMap<String, usize>>,
    ) -> PyResult<Composed> {
        let template = &template.get().inner;
        let composer = &self.inner;
        let composed = py.detach(|| match counts {
            None => composer.compose(template),
            Some(counts) => {
                let values: BTreeMap<String, Vec<()>> = counts
                    .into_iter()
                    .map(|(name, count)| (name, vec![(); count]))
                    .collect();
                composer.compose_with_values(template, &values)
            }
        });
        composed.map(Composed::from).map_err(raised)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!("Composer({})", repr(py, self.dialect())?))
    }
}
