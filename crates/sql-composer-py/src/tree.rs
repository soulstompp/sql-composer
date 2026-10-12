//! Composing a directory of templates as `cargo sqlc compose` does.

use std::path::{Path, PathBuf};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sql_composer::source::{self, Flags, Manifest};
use sql_composer::types::Dialect;

use crate::composer::Composed;
use crate::errors::raised;
use crate::repr;

/// What composing made of one template under the source.
#[pyclass(module = "sql_composer", frozen)]
pub struct Standing {
    /// `"written"`: composed, and given its file; `"shape"`: it leaves slots of its own for a
    /// caller to fill, and gets no file; `"skipped"`: a caller that leaves a slot of a template it
    /// composes open, and gets no file.
    #[pyo3(get)]
    kind: &'static str,
    /// When written: its file's path under the target, its parts joined by `/`.
    #[pyo3(get)]
    file: Option<String>,
    /// When written: what its file holds, and its names.
    #[pyo3(get)]
    composed: Option<Py<Composed>>,
    /// A shape's open slots, in order, or the one slot a skipped template leaves open.
    #[pyo3(get)]
    slots: Vec<String>,
    /// When skipped: the template the open slot stands in, as `:compose` names it.
    #[pyo3(get)]
    slot_template: Option<String>,
}

#[pymethods]
impl Standing {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let shown = match self.kind {
            "written" => format!("file={}", repr(py, &self.file)?),
            "shape" => format!("slots={}", repr(py, &self.slots)?),
            _ => format!(
                "slots={}, slot_template={}",
                repr(py, &self.slots)?,
                repr(py, &self.slot_template)?
            ),
        };
        Ok(format!("Standing({}, {shown})", repr(py, self.kind)?))
    }
}

/// Every template under a source directory, with what composing made of it, and the manifest
/// of the composed files.
#[pyclass(module = "sql_composer", frozen)]
pub struct Tree {
    standings: Vec<(String, Py<Standing>)>,
    /// The text of the `manifest.json` cargo sqlc writes beside the composed files: the flags,
    /// and each composed file's bind names and where its statements' names begin.
    #[pyo3(get)]
    manifest_json: String,
}

#[pymethods]
impl Tree {
    /// Each template by its path under the source, its parts joined by `/`, in the order of those
    /// paths, with what composing made of it.
    #[getter]
    fn standings<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (path, standing) in &self.standings {
            dict.set_item(path, standing)?;
        }
        Ok(dict)
    }

    /// Each composed file by its path under the target, its parts joined by `/`, with what it
    /// holds: one for each template written, and none for the others.
    #[getter]
    fn files<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (_, standing) in &self.standings {
            let standing = standing.get();
            if let (Some(file), Some(composed)) = (&standing.file, &standing.composed) {
                dict.set_item(file, composed)?;
            }
        }
        Ok(dict)
    }

    fn __len__(&self) -> usize {
        self.standings.len()
    }

    fn __repr__(&self) -> String {
        format!("Tree({} templates)", self.standings.len())
    }
}

/// Compose every `.sqlc` file under `source_dir`, at any depth, for `dialect`, as
/// `cargo sqlc compose` does. `name_relations` is its `--with`, `views` its `--views`,
/// `search_paths` its `--search-path`, each looked in after the source, in order, and `respell`
/// its `--respell`: a union whose arms each read one roster, cut to their own rows of it and left
/// joined `ON true` to what each reads, written as one join that reads the roster once, every
/// other byte as composed. PostgreSQL alone is respelled; any other dialect asking for it is
/// refused.
#[pyfunction]
#[pyo3(signature = (
    source_dir,
    dialect,
    *,
    name_relations = false,
    views = None,
    search_paths = Vec::new(),
    respell = false
))]
pub fn compose_all(
    py: Python<'_>,
    source_dir: PathBuf,
    dialect: &str,
    name_relations: bool,
    views: Option<PathBuf>,
    search_paths: Vec<PathBuf>,
    respell: bool,
) -> PyResult<Tree> {
    let mut flags = Flags::new(dialect.parse::<Dialect>().map_err(raised)?);
    flags.with = name_relations;
    flags.views = views;
    flags.search_paths = search_paths;
    flags.respell = respell;

    let (standings, manifest_json) = py
        .detach(|| {
            let standings = source::compose_all(&source_dir, &flags)?;
            let manifest_json = Manifest::new(&flags, &standings).to_json()?;
            Ok::<_, sql_composer::Error>((standings, manifest_json))
        })
        .map_err(raised)?;

    let standings = standings
        .into_iter()
        .map(|(rel_path, standing)| {
            let standing = standing_of(py, &rel_path, standing)?;
            Ok((slashed(&rel_path), Py::new(py, standing)?))
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok(Tree {
        standings,
        manifest_json,
    })
}

/// The standing of the template at `rel_path` under the source, as Python reads it.
fn standing_of(py: Python<'_>, rel_path: &Path, standing: source::Standing) -> PyResult<Standing> {
    Ok(match standing {
        source::Standing::Written(composed) => Standing {
            kind: "written",
            file: Some(source::composed_file(rel_path)),
            composed: Some(Py::new(py, Composed::from(composed))?),
            slots: Vec::new(),
            slot_template: None,
        },
        source::Standing::Shape(slots) => Standing {
            kind: "shape",
            file: None,
            composed: None,
            slots,
            slot_template: None,
        },
        source::Standing::Skipped { slot, template } => Standing {
            kind: "skipped",
            file: None,
            composed: None,
            slots: vec![slot],
            slot_template: Some(template),
        },
        other => {
            return Err(PyRuntimeError::new_err(format!(
                "{}: {other:?} is a standing this module does not know",
                rel_path.display()
            )))
        }
    })
}

/// `path` with its parts joined by `/`.
fn slashed(path: &Path) -> String {
    path.components()
        .map(|part| part.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}
