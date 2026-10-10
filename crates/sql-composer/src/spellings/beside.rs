//! The templates beside the one a spelling reads: how to read each it composes, and which of them
//! other templates take in.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use super::columns::Resolve;
use crate::error::Result;
use crate::parser::{marked, Mark};
use crate::types::{ComposeTarget, Element};

/// The templates beside the one a spelling reads.
///
/// A template another takes in is not offered a WITH query at its own head: composed into a
/// `FROM`, that WITH would stand below the head of the statement it is composed into. Which
/// templates are taken in is read from the trees given to [`Beside::of_trees`]. A template taken
/// in only from outside those trees cannot be seen there, as the templates of a tree that another
/// tree composes over `--search-path` are taken in from that other tree, and is read as taken in
/// by none.
///
/// ```
/// use std::path::Path;
/// use sql_composer::spellings::{lay_out, Axis, Beside};
///
/// let template = "SELECT d.x FROM (SELECT t.x FROM t) d";
/// let mut beside = Beside::new(&|_| None);
/// let naming = |beside: &Beside| {
///     let layout = lay_out(template, Path::new("d.sqlc"), beside);
///     let axis = layout.axes.iter().find(|a| a.axis == Axis::Naming).unwrap();
///     axis.places.iter().map(|p| p.spellings.len()).sum::<usize>()
/// };
/// assert_eq!(naming(&beside), 2);
/// beside.taken.insert("d.sqlc".into());
/// assert_eq!(naming(&beside), 0);
/// ```
#[non_exhaustive]
pub struct Beside<'r> {
    /// Reads a template a template composes, by its path as `:compose` names it.
    pub resolve: Resolve<'r>,
    /// Every template another takes in, by its path as they name it: by `:compose`, a slot's
    /// fill, `:union`, `:count`, `:intersect`, `:except` or `:define`.
    pub taken: BTreeSet<PathBuf>,
}

impl<'r> Beside<'r> {
    /// `resolve`, with no template known to be taken in by another.
    pub fn new(resolve: Resolve<'r>) -> Self {
        Beside {
            resolve,
            taken: BTreeSet::new(),
        }
    }

    /// `resolve`, with every template the templates under `roots` take in, each read as the
    /// template reader reads it. A template the reader refuses takes in none.
    pub fn of_trees(resolve: Resolve<'r>, roots: &[PathBuf]) -> Result<Self> {
        let mut beside = Beside::new(resolve);
        for root in roots {
            for entry in walkdir::WalkDir::new(root) {
                let entry = entry.map_err(std::io::Error::from)?;
                let path = entry.path();
                if !path.is_file() || path.extension().is_none_or(|ext| ext != "sqlc") {
                    continue;
                }
                let text = std::fs::read_to_string(path)?;
                beside.taken.extend(taken_in(&text));
            }
        }
        Ok(beside)
    }

    /// Whether another template takes in `template`, by its path under its tree.
    pub fn takes_in(&self, template: &Path) -> bool {
        self.taken.contains(&plain(template))
    }
}

/// Every template `text` takes in, by its path as it names it.
fn taken_in(text: &str) -> Vec<PathBuf> {
    let Ok((_, marks)) = marked(text) else {
        return vec![];
    };
    let mut taken = Vec::new();
    for (_, _, mark) in marks {
        match mark {
            Mark::Directive(Element::Compose(compose)) => {
                match &compose.target {
                    ComposeTarget::Path(path) | ComposeTarget::Definition(path) => {
                        taken.push(plain(path))
                    }
                    ComposeTarget::Slot(_) => {}
                }
                taken.extend(compose.slots.iter().map(|slot| plain(&slot.path)));
            }
            Mark::Directive(Element::Command(command)) => {
                taken.extend(command.sources.iter().map(|source| plain(source)));
            }
            _ => {}
        }
    }
    taken
}

/// `path` without the `.` parts that name nothing.
fn plain(path: &Path) -> PathBuf {
    path.components()
        .filter(|part| *part != Component::CurDir)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_way_one_template_takes_in_another_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let write = |path: &str, text: &str| {
            let path = dir.path().join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        };
        write(
            "a.sqlc",
            "SELECT x.n FROM (\n    :compose(./shared/b.sqlc, @rows = rows/c.sqlc)\n) x\n",
        );
        write("d.sqlc", ":union(ALL e.sqlc, f.sqlc)\n");
        write("g.sqlc", ":count(DISTINCT n OF h.sqlc)\n");
        write(
            "i.sqlc",
            ":intersect(j.sqlc, k.sqlc)\n:except(l.sqlc, m.sqlc)\n",
        );
        write("views.sqlc", "CREATE VIEW v AS :define(views/v.sqlc);\n");
        write(
            "n.sqlc",
            "SELECT ':compose(o.sqlc)' AS said -- :compose(p.sqlc)\n# :compose(q.sqlc)\n",
        );
        let beside = Beside::of_trees(&|_| None, &[dir.path().to_path_buf()]).unwrap();
        let taken: Vec<String> = beside
            .taken
            .iter()
            .map(|p| p.display().to_string())
            .collect();
        assert_eq!(
            taken,
            [
                "e.sqlc",
                "f.sqlc",
                "h.sqlc",
                "j.sqlc",
                "k.sqlc",
                "l.sqlc",
                "m.sqlc",
                "rows/c.sqlc",
                "shared/b.sqlc",
                "views/v.sqlc",
            ]
        );
        assert!(beside.takes_in(Path::new("shared/b.sqlc")));
        assert!(beside.takes_in(Path::new("./rows/c.sqlc")));
        assert!(!beside.takes_in(Path::new("a.sqlc")));
    }
}
