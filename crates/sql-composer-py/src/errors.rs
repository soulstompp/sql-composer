//! The core's errors as Python exceptions.

use pyo3::prelude::*;

pyo3::create_exception!(
    sql_composer,
    Error,
    pyo3::exceptions::PyException,
    "sql-composer refused, and the message is its own."
);

/// The exception `error` raises, with the core's message: the `OSError` of its kind when a file
/// could not be read, and `sql_composer.Error` for every other refusal.
pub(crate) fn raised(error: sql_composer::Error) -> PyErr {
    let message = error.to_string();
    match error {
        sql_composer::Error::Io(io) => PyErr::from(std::io::Error::new(io.kind(), message)),
        _ => Error::new_err(message),
    }
}
