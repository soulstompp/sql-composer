//! The templates beside the one a spelling reads: how to read each it composes, and which
//! templates take in which.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use super::columns::Resolve;
use crate::error::Result;
use crate::parser::{marked, Mark};
use crate::types::{ComposeTarget, Element};

/// The templates beside the one a spelling reads.
///
/// A template another takes in keeps its head as written. No subquery or template of it moves to
/// a WITH query at its head, and no WITH query at its head is written inline: composed into a
/// `FROM`, a WITH query at its head keeps it a level of its own in the statement it is composed
/// into, and a new one would add a level.
///
/// A `:compose` is written inline only where it is the one place any template takes in the
/// template it names. Composed with `--with`, the templates that take in the same template read
/// one WITH query named for it, which PostgreSQL computes once for all of them; one of them
/// written inline would leave that WITH query read by the others alone, and PostgreSQL then
/// writes it into the plan where it is read.
///
/// Which templates take in which is read from the trees given to [`Beside::of_trees`]. A template
/// taken in only from outside those trees cannot be seen there, as the templates of a tree that
/// another tree composes over `--search-path` are taken in from that other tree, and is read as
/// taken in by none.
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
/// beside.take_in("report.sqlc", "d.sqlc");
/// assert_eq!(naming(&beside), 0);
/// ```
#[non_exhaustive]
pub struct Beside<'r> {
    /// Reads a template a template composes, by its path as `:compose` names it.
    pub resolve: Resolve<'r>,
    /// Each template another takes in, by its path as they name it, with each template that
    /// takes it in, by its path under its tree, once for each time it does: by `:compose`, a
    /// slot's fill, `:union`, `:count`, `:intersect`, `:except` or `:define`.
    pub takers: BTreeMap<PathBuf, Vec<PathBuf>>,
}

impl<'r> Beside<'r> {
    /// `resolve`, with no template known to be taken in by another.
    pub fn new(resolve: Resolve<'r>) -> Self {
        Beside {
            resolve,
            takers: BTreeMap::new(),
        }
    }

    /// `resolve`, with each template the templates under `roots` take in, and who takes it in,
    /// each read as the template reader reads it. A template the reader refuses takes in none.
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
                let taker = path.strip_prefix(root).unwrap_or(path);
                for template in taken_in(&text) {
                    beside.take_in(taker, template);
                }
            }
        }
        Ok(beside)
    }

    /// Record that `taker` takes in `template` once more, each by its path under its tree.
    pub fn take_in(&mut self, taker: impl AsRef<Path>, template: impl AsRef<Path>) {
        self.takers
            .entry(plain(template.as_ref()))
            .or_default()
            .push(plain(taker.as_ref()));
    }

    /// Whether another template takes in `template`, by its path under its tree.
    pub fn takes_in(&self, template: &Path) -> bool {
        !self.takers_of(template).is_empty()
    }

    /// Each template that takes in `template`, once for each time it does.
    pub fn takers_of(&self, template: &Path) -> &[PathBuf] {
        self.takers.get(&plain(template)).map_or(&[], Vec::as_slice)
    }
}

/// A template's place among the templates beside it, as the naming axis reads it.
#[derive(Clone, Copy)]
pub(crate) struct Among<'b> {
    pub(crate) beside: &'b Beside<'b>,
    /// The template, by its path under its tree.
    pub(crate) template: &'b Path,
}

impl<'b> Among<'b> {
    /// Reads a template the template composes.
    pub(crate) fn resolve(&self) -> Resolve<'b> {
        self.beside.resolve
    }

    /// Whether the template keeps its head as written: another takes it in.
    pub(crate) fn head_kept(&self) -> bool {
        self.beside.takes_in(self.template)
    }

    /// Whether a `:compose` of the template at `composed` may be written inline: it is the one
    /// place any template takes that template in.
    pub(crate) fn inline(&self, composed: &Path) -> bool {
        self.beside.takers_of(composed).len() <= 1
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
            .takers
            .keys()
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
        assert_eq!(
            beside.takers_of(Path::new("e.sqlc")),
            [PathBuf::from("d.sqlc")]
        );
        assert_eq!(
            beside.takers_of(Path::new("rows/c.sqlc")),
            [PathBuf::from("a.sqlc")]
        );
        assert!(beside.takes_in(Path::new("./rows/c.sqlc")));
        assert!(!beside.takes_in(Path::new("a.sqlc")));
    }
}
