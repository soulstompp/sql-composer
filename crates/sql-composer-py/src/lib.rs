//! Python bindings for [sql-composer](https://docs.rs/sql-composer): the module `sql_composer`,
//! distributed as `sql-composer`.
//!
//! ```text
//! import sql_composer
//!
//! template = sql_composer.parse(
//!     "SELECT name, rgb FROM lego_colors WHERE id IN (:bind(color_ids)) AND is_trans = :bind(is_trans)"
//! )
//! composed = sql_composer.Composer("postgres").compose(template, counts={"color_ids": 3})
//! composed.sql               # ... WHERE id IN ($1, $2, $3) AND is_trans = $4
//! composed.bind_params       # ['color_ids', 'color_ids', 'color_ids', 'is_trans']
//! composed.statement_starts  # [0]
//!
//! tree = sql_composer.compose_all("sqlc", "postgres")
//! tree.files                 # {'sets/select_set_parts.sql': Composed(...), ...}
//! tree.manifest_json         # the text of the manifest.json cargo sqlc writes
//! ```
//!
//! The values stay Python's: a composer takes how many values each name is given, and the caller
//! passes its values to its own database module in the order `bind_params` lists the names.
//!
//! A refusal raises `sql_composer.Error` with the core's own message; a file that cannot be read
//! raises the `OSError` of its kind (a missing one is a `FileNotFoundError`), with the core's
//! message too.

#![warn(missing_docs)]

mod composer;
mod errors;
mod tree;

use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3::BoundObject;

pub use composer::{Composed, Composer, Template};
pub use errors::Error;
pub use tree::{Standing, Tree};

/// Put the module in an embedding interpreter's table of built-in modules, so `import
/// sql_composer` finds it in a process that starts Python itself. Call it before the interpreter
/// starts. An extension module loaded from its file needs none of this.
pub fn append_to_inittab() {
    pyo3::append_to_inittab!(python_module);
}

/// The module `sql_composer`: compose SQL templates for a dialect, and get back the SQL and the
/// names its placeholders stand for.
#[pymodule(name = "sql_composer")]
pub fn python_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add(
        "DIALECTS",
        PyTuple::new(
            py,
            sql_composer::Dialect::ALL
                .iter()
                .map(|dialect| dialect.name()),
        )?,
    )?;
    m.add("MANIFEST", sql_composer::source::MANIFEST)?;
    m.add("Error", py.get_type::<Error>())?;
    m.add_class::<Template>()?;
    m.add_class::<Composer>()?;
    m.add_class::<Composed>()?;
    m.add_class::<Tree>()?;
    m.add_class::<Standing>()?;
    m.add_function(wrap_pyfunction!(composer::parse, m)?)?;
    m.add_function(wrap_pyfunction!(composer::parse_file, m)?)?;
    m.add_function(wrap_pyfunction!(tree::compose_all, m)?)?;
    Ok(())
}

/// `value` as Python's `repr` writes it.
pub(crate) fn repr<'py>(py: Python<'py>, value: impl IntoPyObject<'py>) -> PyResult<String> {
    let value = value.into_pyobject(py).map_err(Into::<PyErr>::into)?;
    Ok(value.into_bound().into_any().repr()?.to_string())
}
