//! A directory of templates composed as `cargo sqlc compose` composes it: every `.sqlc` file under
//! it, at any depth, each with what composing it makes of it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::composer::{ComposedSql, Composer};
use crate::error::{Error, Result};
use crate::parser;
use crate::types::{ComposeRef, ComposeTarget, Dialect, Element, Template, TemplateSource};

/// How a directory of templates is composed: the flags `cargo sqlc compose` takes for it.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Flags {
    /// The dialect composed for.
    pub dialect: Dialect,
    /// Every relation a statement reads named once at its head, as `--with` does.
    pub with: bool,
    /// The view registry, as `--views` gives it.
    pub views: Option<PathBuf>,
    /// The directories looked in after the source, in order, as `--search-path` gives them.
    pub search_paths: Vec<PathBuf>,
}

impl Flags {
    /// Composing for `dialect`, and nothing else.
    pub fn new(dialect: Dialect) -> Self {
        Self {
            dialect,
            with: false,
            views: None,
            search_paths: vec![],
        }
    }
}

/// What composing makes of one template under the source.
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum Standing {
    /// Composed: what its file holds.
    Written(ComposedSql),
    /// A shape: the slots of its own a caller fills. It is composed only inside that caller.
    Shape(Vec<String>),
    /// A caller that leaves a slot open: the slot, and the template it stands in, as `:compose`
    /// names it.
    Skipped {
        /// The slot left open.
        slot: String,
        /// The template the slot stands in.
        template: String,
    },
}

/// The composer that composes the templates under `source_dir` with `flags`: it reads them
/// under `source_dir`, then under each search path.
pub fn composer(source_dir: &Path, flags: &Flags) -> Result<Composer> {
    let mut composer = Composer::new(flags.dialect);
    composer.add_search_path(source_dir.to_path_buf());
    for path in &flags.search_paths {
        composer.add_search_path(path.clone());
    }
    composer.name_relations = flags.with;
    if let Some(registry) = &flags.views {
        composer.load_view_registry(registry)?;
    }
    Ok(composer)
}

/// Every `.sqlc` file under `source_dir`, at any depth, by its path under `source_dir`, with
/// what composing it with `flags` makes of it.
pub fn compose_all(source_dir: &Path, flags: &Flags) -> Result<BTreeMap<PathBuf, Standing>> {
    let composer = composer(source_dir, flags)?;

    let mut results = BTreeMap::new();

    for entry in walkdir::WalkDir::new(source_dir) {
        let entry = entry.map_err(std::io::Error::from)?;
        let path = entry.path();

        if !path.is_file() || !path.extension().is_some_and(|ext| ext == "sqlc") {
            continue;
        }

        let rel_path = path
            .strip_prefix(source_dir)
            .expect("walkdir entry must be under source_dir");

        let content = std::fs::read_to_string(path)?;
        let template = parser::parse_template(&content, TemplateSource::File(path.to_path_buf()))?;
        let standing = match composer.compose(&template) {
            Ok(composed) => Standing::Written(composed),
            Err(Error::MissingSlot {
                name,
                template: within,
            }) => {
                let own = open_slots(&template);
                if own.is_empty() {
                    let roots: Vec<&Path> = std::iter::once(source_dir)
                        .chain(flags.search_paths.iter().map(PathBuf::as_path))
                        .collect();
                    Standing::Skipped {
                        slot: name,
                        template: as_composed(Path::new(&within), &roots),
                    }
                } else {
                    Standing::Shape(own)
                }
            }
            Err(e) => return Err(e),
        };

        results.insert(rel_path.to_path_buf(), standing);
    }

    Ok(results)
}

/// The file at the root of a target that records how its files were composed.
#[cfg(feature = "serde")]
pub const MANIFEST: &str = "manifest.json";

/// What `manifest.json` holds: the flags a target's files were composed with, and each composed
/// file's names.
#[cfg(feature = "serde")]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct Manifest {
    /// The dialect, by the name `--dialect` takes.
    pub dialect: String,
    /// `--with`.
    pub with: bool,
    /// `--views`, as it was given.
    pub views: Option<PathBuf>,
    /// Each `--search-path`, as it was given, in order.
    pub search_path: Vec<PathBuf>,
    /// Each composed file, by its path under the target, its parts joined by `/`.
    pub files: BTreeMap<String, Names>,
}

/// The names a composed file's placeholders stand for.
#[cfg(feature = "serde")]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct Names {
    /// The bind names, in placeholder order.
    pub bind_params: Vec<String>,
    /// Where each statement's names begin in `bind_params`, one entry per statement.
    pub statement_starts: Vec<usize>,
}

#[cfg(feature = "serde")]
impl Manifest {
    /// The manifest of composing with `flags`: an entry for each template written, its file
    /// named by its path under the target.
    pub fn new(flags: &Flags, standings: &BTreeMap<PathBuf, Standing>) -> Self {
        Manifest {
            dialect: flags.dialect.name().to_string(),
            with: flags.with,
            views: flags.views.clone(),
            search_path: flags.search_paths.clone(),
            files: standings
                .iter()
                .filter_map(|(rel_path, standing)| match standing {
                    Standing::Written(composed) => Some((
                        composed_file(rel_path),
                        Names {
                            bind_params: composed.bind_params.clone(),
                            statement_starts: composed.statement_starts.clone(),
                        },
                    )),
                    _ => None,
                })
                .collect(),
        }
    }

    /// The manifest as its file holds it.
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)? + "\n")
    }

    /// The manifest a file holds.
    pub fn from_json(text: &str) -> Result<Self> {
        Ok(serde_json::from_str(text)?)
    }
}

/// The composed file of the template at `rel_path` under the source, by its path under the
/// target, its parts joined by `/`.
pub fn composed_file(rel_path: &Path) -> String {
    rel_path
        .with_extension("sql")
        .components()
        .map(|part| part.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// The slots a template leaves for its caller to fill, each named once, in order.
fn open_slots(template: &Template) -> Vec<String> {
    let mut slots = BTreeSet::new();
    for element in &template.elements {
        if let Element::Compose(ComposeRef {
            target: ComposeTarget::Slot(name),
            ..
        }) = element
        {
            slots.insert(name.clone());
        }
    }
    slots.into_iter().collect()
}

/// `template` as a `:compose` names it: its path under the first of `roots` that holds it.
fn as_composed(template: &Path, roots: &[&Path]) -> String {
    let resolved = std::fs::canonicalize(template).unwrap_or_else(|_| template.to_path_buf());
    roots
        .iter()
        .filter_map(|root| std::fs::canonicalize(root).ok())
        .find_map(|root| resolved.strip_prefix(&root).ok().map(Path::to_path_buf))
        .unwrap_or_else(|| template.to_path_buf())
        .display()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::{Held, Reach};

    /// The brick example's templates, copied into a directory of their own, each opening with a
    /// line that names it.
    fn marked_brick() -> (tempfile::TempDir, PathBuf) {
        let from = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/brick/sqlc");
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("sqlc");
        for entry in walkdir::WalkDir::new(&from) {
            let entry = entry.unwrap();
            let rel_path = entry.path().strip_prefix(&from).unwrap();
            let to = source.join(rel_path);
            if entry.file_type().is_dir() {
                std::fs::create_dir_all(&to).unwrap();
            } else {
                let text = std::fs::read_to_string(entry.path()).unwrap();
                std::fs::write(&to, format!("{}\n{text}", marker(rel_path))).unwrap();
            }
        }
        (dir, source)
    }

    /// The line a template opens with in `marked_brick`.
    fn marker(rel_path: &Path) -> String {
        format!("-- held {}", rel_path.display())
    }

    /// `flags` for `dialect`, `with` or not.
    fn flags(dialect: Dialect, with: bool) -> Flags {
        let mut flags = Flags::new(dialect);
        flags.with = with;
        flags
    }

    #[test]
    fn each_template_a_composed_file_holds_is_written_as_often_as_its_tree_says() {
        let (_dir, source) = marked_brick();
        for &dialect in Dialect::ALL {
            for with in [false, true] {
                let flags = flags(dialect, with);
                let composer = composer(&source, &flags).unwrap();
                let standings = compose_all(&source, &flags).unwrap();
                let mut written = 0;
                for (rel_path, standing) in &standings {
                    let Standing::Written(composed) = standing else {
                        continue;
                    };
                    let text = std::fs::read_to_string(source.join(rel_path)).unwrap();
                    let template =
                        parser::parse_template(&text, TemplateSource::File(source.join(rel_path)))
                            .unwrap();
                    let copies = composer.tree(&template).unwrap().copies();
                    for (template, count) in &copies {
                        let held = composed
                            .sql
                            .matches(&format!("{}\n", marker(template)))
                            .count();
                        assert_eq!(
                            held,
                            *count,
                            "{} holds {} {held} times, its tree {count} ({dialect:?}, with: {with})",
                            rel_path.display(),
                            template.display()
                        );
                    }
                    let marked = composed.sql.matches("-- held ").count();
                    assert_eq!(
                        marked,
                        copies.values().sum::<usize>(),
                        "{}",
                        rel_path.display()
                    );
                    written += 1;
                }
                assert!(written > 40, "{written}");
            }
        }
    }

    #[test]
    fn the_shared_moulds_hold_the_theme_closure_twice_and_once_named() {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/brick/sqlc");
        let path = source.join("reports/shared_moulds.sqlc");
        let text = std::fs::read_to_string(&path).unwrap();
        let template = parser::parse_template(&text, TemplateSource::File(path)).unwrap();
        let closure = Path::new("shared/theme_closure.sqlc");

        let plain = composer(&source, &flags(Dialect::Postgres, false)).unwrap();
        let tree = plain.tree(&template).unwrap();
        assert_eq!(tree.template, Path::new("reports/shared_moulds.sqlc"));
        assert_eq!(tree.copies()[closure], 2);

        let named = composer(&source, &flags(Dialect::Postgres, true)).unwrap();
        let tree = named.tree(&template).unwrap();
        assert_eq!(tree.copies()[closure], 1);
        let head: Vec<&Held> = tree
            .holds
            .iter()
            .filter(|h| h.reach == Reach::Head)
            .collect();
        assert!(
            head.iter().any(|h| h.template == closure
                && h.name.as_deref() == Some("shared_theme_closure")),
            "{tree:?}"
        );
    }

    #[test]
    fn a_slot_is_held_as_the_template_that_fills_it() {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/brick/sqlc");
        let path = source.join("sets/select_colored_parts.sqlc");
        let text = std::fs::read_to_string(&path).unwrap();
        let template = parser::parse_template(&text, TemplateSource::File(path)).unwrap();
        let tree = composer(&source, &flags(Dialect::Postgres, false))
            .unwrap()
            .tree(&template)
            .unwrap();
        let [shape] = &tree.holds[..] else {
            panic!("{tree:?}");
        };
        assert_eq!(shape.template, Path::new("shared/filtered_set_parts.sqlc"));
        assert!(
            shape
                .holds
                .iter()
                .any(|h| h.reach == Reach::Slot("filter".into())
                    && h.template == Path::new("filters/by_color.sqlc")),
            "{shape:?}"
        );
    }
}
