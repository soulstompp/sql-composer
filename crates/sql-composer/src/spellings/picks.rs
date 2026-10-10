//! The picks: `spellings.toml` at the root of a source of templates, one key per axis, read with
//! serde, and the templates written in them.

use std::path::{Path, PathBuf};

use sqlparser::ast::Visit;

use super::beside::Beside;
use super::columns::Resolve;
use super::join_order::{self, Selects};
use super::read::Read;
use super::{written_in, Axis};
use crate::error::{Error, Result};

/// The file the picks are kept in, at the root of the source.
pub const FILE: &str = "spellings.toml";

/// The spelling picked on each axis, as `spellings.toml` holds it. An axis left out is written as
/// each template writes it.
///
/// ```
/// use sql_composer::spellings::Picks;
///
/// let picks = Picks::from_toml(
///     "conditions = \"where\"\nnaming = \"with\"\n\n[join_order]\ntemplate = \"shared/scope_sets.sqlc\"\norder = [\"s\", \"sc\", \"scope\"]\n",
/// )
/// .unwrap();
/// assert_eq!(picks.conditions.as_deref(), Some("where"));
/// assert_eq!(picks.join_order.unwrap().order, ["s", "sc", "scope"]);
/// assert!(Picks::from_toml("negation = \"sideways\"").is_err());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Picks {
    /// Where a condition sits: `on`, in the first inner join's `ON` where every relation it
    /// names is joined, or `where`.
    pub conditions: Option<String>,
    /// Negation: `pushed`, onto each operand, or `over`, one `NOT` over the `AND` or `OR`.
    pub negation: Option<String>,
    /// How a relation read once is named: `inline`, `with`, at the statement's head, or
    /// `template`, a template of its own where it is one.
    pub naming: Option<String>,
    /// A semi-join's form: `in` or `exists`.
    pub semi_join: Option<String>,
    /// Join order: one template's join list in the order picked, which takes effect once
    /// outermost first gives that order.
    pub join_order: Option<JoinOrderPick>,
}

/// Join order as picked on one template.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct JoinOrderPick {
    /// The template, by its path under the source.
    pub template: PathBuf,
    /// The relations of one of its join lists, by the names they are read by, in the order
    /// picked.
    pub order: Vec<String>,
}

impl Picks {
    /// The picks `text`, a `spellings.toml`, holds; refused where a key is no axis's or a value
    /// no pick of its axis.
    pub fn from_toml(text: &str) -> Result<Picks> {
        Self::read(text, Path::new(FILE))
    }

    /// The picks the `spellings.toml` at the root of `source` holds; none picked where there is
    /// none.
    pub fn of_source(source: &Path) -> Result<Picks> {
        let path = source.join(FILE);
        match std::fs::read_to_string(&path) {
            Ok(text) => Self::read(&text, &path),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Picks::default()),
            Err(e) => Err(e.into()),
        }
    }

    fn read(text: &str, path: &Path) -> Result<Picks> {
        let refused = |message: String| Error::Picks {
            path: path.display().to_string(),
            message,
        };
        let picks: Picks = toml::from_str(text).map_err(|e| refused(e.message().to_string()))?;
        for axis in Axis::ALL {
            if let Some(pick) = picks.pick(*axis) {
                if !axis.picks().contains(&pick) {
                    return Err(refused(format!(
                        "{} = \"{pick}\" is no pick of {}: {}",
                        axis.key(),
                        axis.words(),
                        crate::types::listed(axis.picks().iter().map(|p| format!("\"{p}\"")))
                            .replace(" and ", " or ")
                    )));
                }
            }
        }
        Ok(picks)
    }

    /// The pick of `axis`, for every axis but join order, whose pick is a template's order.
    pub fn pick(&self, axis: Axis) -> Option<&str> {
        match axis {
            Axis::Conditions => self.conditions.as_deref(),
            Axis::Negation => self.negation.as_deref(),
            Axis::Naming => self.naming.as_deref(),
            Axis::SemiJoin => self.semi_join.as_deref(),
            Axis::JoinOrder => None,
        }
    }
}

/// What a join order picked on one template says of outermost first.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum JoinRule {
    /// Outermost first gives the order picked: the rule takes effect.
    Confirmed {
        /// The template it was picked on.
        template: PathBuf,
        /// The order picked, which outermost first gives.
        order: Vec<String>,
    },
    /// Outermost first gives another order, or none: join order stays as written.
    Parted {
        /// The template it was picked on.
        template: PathBuf,
        /// Where the rule and the pick part, in words.
        at: String,
    },
}

/// Whether outermost first gives the join order `pick` picks, read from its template's text,
/// which `resolve` reads by its path under the source.
pub fn confirm(pick: &JoinOrderPick, resolve: Resolve) -> JoinRule {
    let parted = |at: String| JoinRule::Parted {
        template: pick.template.clone(),
        at,
    };
    let Some(text) = resolve(&pick.template) else {
        return parted(format!(
            "{} is no template under the source",
            pick.template.display()
        ));
    };
    let Ok(read) = Read::new(&text) else {
        return parted(format!("{} is not read", pick.template.display()));
    };
    let order: Vec<String> = pick.order.iter().map(|name| name.to_lowercase()).collect();
    let mut wanted = order.clone();
    wanted.sort();
    for (_, statement) in read.parsed() {
        let mut selects = Selects::default();
        let _ = statement.ast.visit(&mut selects);
        for select in &selects.0 {
            for item in 0..select.from.len() {
                if select.from[item].joins.is_empty() {
                    continue;
                }
                let names: Vec<String> = std::iter::once(&select.from[item].relation)
                    .chain(select.from[item].joins.iter().map(|j| &j.relation))
                    .filter_map(super::conditions::range_name)
                    .collect();
                let mut sorted = names.clone();
                sorted.sort();
                if sorted != wanted {
                    continue;
                }
                let model = match join_order::model(&read, statement, select, item, resolve) {
                    Ok(model) => model,
                    Err(why) => {
                        return parted(format!(
                            "its join of {} stays as written: {why}",
                            names.join(", ")
                        ))
                    }
                };
                let picked: Vec<usize> = order
                    .iter()
                    .map(|name| model.position(name).expect("a relation of the list"))
                    .collect();
                if let Err(why) = join_order::joined(&model, &picked) {
                    return parted(format!(
                        "the order picked is not one its join can take: {why}"
                    ));
                }
                let rule = join_order::outermost_first(&model);
                if rule == picked {
                    return JoinRule::Confirmed {
                        template: pick.template.clone(),
                        order: pick.order.clone(),
                    };
                }
                let widths = join_order::widths(&model);
                let at = (0..rule.len()).find(|&i| rule[i] != picked[i]).unwrap_or(0);
                let words = |r: usize| {
                    format!(
                        "{}, joined by {} column{}",
                        model.names(&[r])[0],
                        widths[r],
                        if widths[r] == 1 { "" } else { "s" }
                    )
                };
                return parted(format!(
                    "outermost first orders it {}: at place {} it puts {} where the pick puts {}",
                    model.names(&rule).join(", "),
                    at + 1,
                    words(rule[at]),
                    words(picked[at]),
                ));
            }
        }
    }
    parted(format!(
        "no join list of it joins {}",
        pick.order.join(", ")
    ))
}

/// Each join list of `text` outermost first reorders, with its order as written and the rule's.
pub fn outermost_first(text: &str, resolve: Resolve) -> Vec<(Vec<String>, Vec<String>)> {
    let Ok(read) = Read::new(text) else {
        return vec![];
    };
    let mut found = Vec::new();
    for (_, statement) in read.parsed() {
        let mut selects = Selects::default();
        let _ = statement.ast.visit(&mut selects);
        for select in &selects.0 {
            for item in 0..select.from.len() {
                if select.from[item].joins.is_empty() {
                    continue;
                }
                let Ok(model) = join_order::model(&read, statement, select, item, resolve) else {
                    continue;
                };
                let written: Vec<usize> = (0..model.len()).collect();
                let rule = join_order::outermost_first(&model);
                if rule != written && join_order::joined(&model, &rule).is_ok() {
                    found.push((model.names(&written), model.names(&rule)));
                }
            }
        }
    }
    found
}

/// How many rounds over the axes a template is written in before it is left as far as it got.
const ROUNDS: usize = 8;

/// `text`, the template at `template` under its tree, written in `picks`: each axis picked, in
/// the order [`Axis::ALL`] lists them, until a round changes nothing, and join order outermost
/// first where its pick confirms the rule. Every place no spelling of a pick reads back at stays
/// as written; `beside` reads the templates it composes, and a template it says another takes in
/// is written no WITH query at its head.
pub fn spell(text: &str, template: &Path, picks: &Picks, beside: &Beside) -> String {
    let resolve = beside.resolve;
    let head = !beside.takes_in(template);
    let join_order = match &picks.join_order {
        Some(pick) => matches!(confirm(pick, resolve), JoinRule::Confirmed { .. }),
        None => false,
    };
    let mut text = text.to_string();
    for _ in 0..ROUNDS {
        let before = text.clone();
        for &axis in Axis::ALL {
            let pick = match axis {
                Axis::JoinOrder if join_order => Some(join_order::OUTERMOST),
                axis => picks.pick(axis),
            };
            if let Some(pick) = pick {
                text = written_in(&text, axis, pick, resolve, head);
            }
        }
        if text == before {
            break;
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::Composer;
    use crate::parser::parse_template;
    use crate::types::{Dialect, TemplateSource};

    fn source() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/brick/sqlc")
    }

    fn brick(path: &Path) -> Option<String> {
        std::fs::read_to_string(source().join(path)).ok()
    }

    /// Every brick template, by its path under the source, with its text.
    fn templates() -> Vec<(PathBuf, String)> {
        let mut all: Vec<(PathBuf, String)> = walkdir::WalkDir::new(source())
            .into_iter()
            .map(|e| e.unwrap().into_path())
            .filter(|p| p.extension().is_some_and(|e| e == "sqlc"))
            .map(|p| {
                let text = std::fs::read_to_string(&p).unwrap();
                (p.strip_prefix(source()).unwrap().to_path_buf(), text)
            })
            .collect();
        all.sort();
        all
    }

    #[test]
    fn the_picks_are_read_one_key_per_axis() {
        let picks = Picks::from_toml(
            "conditions = \"on\"\nnegation = \"over\"\nnaming = \"template\"\nsemi_join = \"in\"\n\n[join_order]\ntemplate = \"a.sqlc\"\norder = [\"b\", \"a\"]\n",
        )
        .unwrap();
        assert_eq!(picks.pick(Axis::Conditions), Some("on"));
        assert_eq!(picks.pick(Axis::Negation), Some("over"));
        assert_eq!(picks.pick(Axis::Naming), Some("template"));
        assert_eq!(picks.pick(Axis::SemiJoin), Some("in"));
        assert_eq!(picks.join_order.unwrap().template, Path::new("a.sqlc"));
        assert_eq!(Picks::from_toml("").unwrap(), Picks::default());
        let refused = |text: &str| Picks::from_toml(text).unwrap_err().to_string();
        assert_eq!(
            refused("naming = \"withh\""),
            "spellings.toml: naming = \"withh\" is no pick of how a relation read once is named: \"inline\", \"with\" or \"template\""
        );
        assert!(
            refused("spelling = \"on\"").contains("unknown field `spelling`"),
            "{}",
            refused("spelling = \"on\"")
        );
        assert!(refused("[join_order]\ntemplate = \"a.sqlc\"").contains("order"));
    }

    #[test]
    fn with_no_pick_every_template_is_written_byte_for_byte() {
        let beside = Beside::of_trees(&brick, &[source()]).unwrap();
        for (path, text) in templates() {
            assert_eq!(
                spell(&text, &path, &Picks::default(), &beside),
                text,
                "{}",
                path.display()
            );
        }
    }

    #[test]
    fn every_template_written_in_every_pick_reads_composes_and_stays_written() {
        let composer = {
            let mut composer = Composer::new(Dialect::Postgres);
            composer.add_search_path(source());
            composer
        };
        for (axes, picks) in [
            ("on, pushed, inline, exists", "conditions = \"on\"\nnegation = \"pushed\"\nnaming = \"inline\"\nsemi_join = \"exists\""),
            ("where, over, with, in", "conditions = \"where\"\nnegation = \"over\"\nnaming = \"with\"\nsemi_join = \"in\""),
            ("template", "naming = \"template\""),
        ] {
            let picks = Picks::from_toml(picks).unwrap();
            let beside = Beside::of_trees(&brick, &[source()]).unwrap();
            let mut changed = 0;
            for (path, text) in templates() {
                let spelled = spell(&text, &path, &picks, &beside);
                assert_eq!(spell(&spelled, &path, &picks, &beside), spelled, "{axes}: {}", path.display());
                let reading = super::super::read(&spelled);
                assert!(reading.unparsed.is_empty() && reading.read_back, "{axes}: {spelled}");
                let template = parse_template(&spelled, TemplateSource::File(source().join(&path))).unwrap();
                match composer.compose(&template) {
                    Ok(_) | Err(Error::MissingSlot { .. }) => {}
                    Err(e) => panic!("{axes}: {}: {e}\n{spelled}", path.display()),
                }
                changed += usize::from(spelled != text);
            }
            assert!(changed > 0, "{axes} changed nothing");
        }
    }

    #[test]
    fn join_order_takes_effect_once_its_pick_confirms_outermost_first() {
        let pick = |order: &[&str]| JoinOrderPick {
            template: "shared/scope_sets.sqlc".into(),
            order: order.iter().map(|s| s.to_string()).collect(),
        };
        assert_eq!(
            confirm(&pick(&["s", "sc", "scope"]), &brick),
            JoinRule::Confirmed {
                template: "shared/scope_sets.sqlc".into(),
                order: vec!["s".into(), "sc".into(), "scope".into()],
            }
        );
        assert_eq!(
            confirm(&pick(&["sc", "s", "scope"]), &brick),
            JoinRule::Parted {
                template: "shared/scope_sets.sqlc".into(),
                at: "outermost first orders it s, sc, scope: at place 1 it puts s, joined by 1 column where the pick puts sc, joined by 2 columns".into(),
            }
        );
        let JoinRule::Parted { at, .. } = confirm(&pick(&["s", "x"]), &brick) else {
            panic!("a list nobody joins");
        };
        assert_eq!(at, "no join list of it joins s, x");

        let layers = "SELECT d.filing, n.size, s.name\nFROM (SELECT x.filing, x.layer, x.size FROM demand x) d\nJOIN (SELECT y.filing, y.layer, y.size FROM nameplate y) n USING (filing, layer)\nJOIN (SELECT z.filing, z.name FROM scope z) s USING (filing)";
        let mut picks = Picks {
            join_order: Some(pick(&["s", "sc", "scope"])),
            ..Picks::default()
        };
        assert_eq!(
            spell(layers, Path::new("layers.sqlc"), &picks, &Beside::new(&brick)),
            "SELECT d.filing, n.size, s.name\nFROM (SELECT z.filing, z.name FROM scope z) s\nJOIN (SELECT x.filing, x.layer, x.size FROM demand x) d USING (filing)\nJOIN (SELECT y.filing, y.layer, y.size FROM nameplate y) n USING (filing, layer)"
        );
        picks.join_order = Some(pick(&["sc", "s", "scope"]));
        assert_eq!(
            spell(
                layers,
                Path::new("layers.sqlc"),
                &picks,
                &Beside::new(&brick)
            ),
            layers
        );
        assert_eq!(
            outermost_first(layers, &brick),
            [(
                vec!["d".to_string(), "n".into(), "s".into()],
                vec!["s".to_string(), "d".into(), "n".into()]
            )]
        );
    }
}
