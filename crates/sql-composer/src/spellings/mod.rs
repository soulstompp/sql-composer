//! The spellings of a template: the other ways to write it that return the same rows, laid out
//! an axis at a time, and the template written in the spelling picked on each axis.
//!
//! A template is read for PostgreSQL as sqlparser reads it, each directive standing in as SQL of
//! its own kind: a `:compose`, a slot, a `:define` or a command as a query reading a relation
//! named after the directive, a `:bind` as a parameter. Each `#` comment reaches the parser as a
//! `--` comment, and the `:dialect(…)` declaration as blanks. A spelling is written by moving the
//! template's own text, every byte outside what moves left as written, and each spelling is read
//! back and compared with the statement it must be before it is offered.
//!
//! [`lay_out`] counts a template's spellings at each place an axis applies, and writes each of
//! them with every other place as written: an axis at a time, and never as the product. The
//! picks are kept in `spellings.toml` at the root of the source ([`Picks`]), and [`spell`] writes
//! a template in them. Join order's pick is one template's join list in the order picked, and it
//! takes effect only where outermost first gives that order ([`confirm`]). Nothing reads a
//! catalog: each condition is read from the template's own text. A template, or a statement of
//! it, that the parser cannot read is left as written, and so is every template when no spelling
//! is picked.

mod columns;
mod conditions;
mod extent;
mod join_order;
#[cfg(test)]
mod laws;
mod naming;
mod negation;
mod picks;
mod place;
mod read;
mod semi_join;

pub use columns::Resolve;
pub use picks::{confirm, outermost_first, spell, JoinOrderPick, JoinRule, Picks, FILE};
use place::{apply, checked, made, Place, Want};
use read::Read;

/// How many places a template is written in one axis's pick at, at most, before it is left as
/// far as it got.
const MOVES: usize = 10_000;

/// An axis a template's spellings vary along: each holds the spellings that return the same
/// rows at each place it applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Axis {
    /// Where a condition sits: in an inner join's `ON`, or in the `WHERE`.
    Conditions,
    /// Negation: `NOT` over an `AND` or an `OR`, or pushed onto each operand, and `NOT NOT p` as
    /// `p`.
    Negation,
    /// Join order: the relations of a join of inner joins in another order, each condition at the
    /// first `ON` where every relation it names is joined.
    JoinOrder,
    /// How a relation read once is named: inline, as a WITH query at the statement's head, or as
    /// a template of its own.
    Naming,
    /// A semi-join's form: `x IN (SELECT y …)`, or `EXISTS (SELECT … WHERE x = y)`.
    SemiJoin,
}

impl Axis {
    /// Each axis, in the order a template is written in their picks.
    pub const ALL: &'static [Axis] = &[
        Axis::JoinOrder,
        Axis::Naming,
        Axis::Conditions,
        Axis::Negation,
        Axis::SemiJoin,
    ];

    /// The axis's key in `spellings.toml`.
    pub fn key(&self) -> &'static str {
        match self {
            Axis::Conditions => "conditions",
            Axis::Negation => "negation",
            Axis::JoinOrder => "join_order",
            Axis::Naming => "naming",
            Axis::SemiJoin => "semi_join",
        }
    }

    /// The axis's picks, by their value in `spellings.toml`.
    pub fn picks(&self) -> &'static [&'static str] {
        match self {
            Axis::Conditions => &[conditions::ON, conditions::WHERE],
            Axis::Negation => &[negation::PUSHED, negation::OVER],
            Axis::JoinOrder => &[join_order::OUTERMOST],
            Axis::Naming => &[naming::INLINE, naming::WITH, naming::TEMPLATE],
            Axis::SemiJoin => &[semi_join::IN, semi_join::EXISTS],
        }
    }

    /// The axis in words.
    pub fn words(&self) -> &'static str {
        match self {
            Axis::Conditions => "where a condition sits",
            Axis::Negation => "negation",
            Axis::JoinOrder => "join order",
            Axis::Naming => "how a relation read once is named",
            Axis::SemiJoin => "a semi-join's form",
        }
    }

    /// The places in `read` this axis applies at, each spelling unchecked, or those `want` asks
    /// for; `resolve` reads the templates it composes.
    fn places(&self, read: &Read, resolve: Resolve, want: Option<Want>) -> Vec<Place> {
        match self {
            Axis::JoinOrder => join_order::places(read, resolve, want),
            Axis::Naming => naming::places(read, resolve, want),
            Axis::Conditions => conditions::places(read, want),
            Axis::Negation => negation::places(read, want),
            Axis::SemiJoin => semi_join::places(read, want),
        }
    }
}

/// `text` written in `pick` on `axis` wherever the axis applies and a spelling of the pick reads
/// back as it must; everywhere else as written. `resolve` reads the templates it composes.
fn written_in(text: &str, axis: Axis, pick: &str, resolve: Resolve) -> String {
    let mut text = text.to_string();
    let mut refused = 0;
    for _ in 0..MOVES {
        let Ok(read) = Read::new(&text) else {
            break;
        };
        let want = Want {
            pick,
            enough: refused + 1,
        };
        let Some(place) = axis
            .places(&read, resolve, Some(want))
            .into_iter()
            .nth(refused)
        else {
            break;
        };
        let spelled = place
            .choices
            .iter()
            .find(|c| c.picks.contains(&pick))
            .and_then(|choice| made(&read, place.statement, choice));
        match spelled {
            Some(spelled) => {
                text = spelled;
                refused = 0;
            }
            None => refused += 1,
        }
    }
    text
}

/// A template's spellings laid out: at each place each axis applies, each spelling there, the
/// template written with that one place changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Layout {
    /// What reading the template found.
    pub reading: Reading,
    /// Each axis, in the order [`Axis::ALL`] lists them, with its places.
    pub axes: Vec<AxisLayout>,
}

/// One axis of a [`Layout`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct AxisLayout {
    /// The axis.
    pub axis: Axis,
    /// Each place it applies at where the template has more than one spelling, in the order the
    /// template holds them.
    pub places: Vec<PlaceLayout>,
}

/// One place of an axis, with its spellings.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PlaceLayout {
    /// The statement it is in, by its place among the template's statements from 0.
    pub statement: usize,
    /// What stands there, in words and the template's own text.
    pub what: String,
    /// For a join list, how many relations it joins: its orders are at most the permutations of
    /// that many.
    pub relations: Option<usize>,
    /// Each spelling, the first as written.
    pub spellings: Vec<Spelling>,
}

/// One spelling of a place.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Spelling {
    /// The spelling, in words.
    pub name: String,
    /// The picks of `spellings.toml` it is.
    pub picks: Vec<String>,
    /// The template written in it, every other place as written.
    pub text: String,
}

impl AxisLayout {
    /// How many spellings the axis has: the product of how many each place has.
    pub fn count(&self) -> u128 {
        self.places
            .iter()
            .map(|place| place.spellings.len() as u128)
            .fold(1, u128::saturating_mul)
    }
}

impl Layout {
    /// How many spellings the template has: the product of how many each axis has.
    pub fn count(&self) -> u128 {
        self.axes
            .iter()
            .map(AxisLayout::count)
            .fold(1, u128::saturating_mul)
    }
}

/// `text`, a template, with its spellings laid out and counted; `resolve` reads the templates it
/// composes, by their path as `:compose` names them.
pub fn lay_out(text: &str, resolve: Resolve) -> Layout {
    let reading = read(text);
    let Ok(read) = Read::new(text) else {
        return Layout {
            reading,
            axes: vec![],
        };
    };
    let axes = Axis::ALL
        .iter()
        .map(|&axis| AxisLayout {
            axis,
            places: axis
                .places(&read, resolve, None)
                .into_iter()
                .map(|place| checked(&read, place))
                .filter(|place| place.choices.len() > 1)
                .map(|place| PlaceLayout {
                    statement: place.statement,
                    what: place.what,
                    relations: place.relations,
                    spellings: place
                        .choices
                        .into_iter()
                        .map(|choice| Spelling {
                            text: apply(text, &choice.edits).expect("a checked spelling applies"),
                            name: choice.name,
                            picks: choice.picks.iter().map(|p| p.to_string()).collect(),
                        })
                        .collect(),
                })
                .collect(),
        })
        .collect();
    Layout { reading, axes }
}

/// What reading a template for its spellings finds, before any spelling of it is laid out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Reading {
    /// Why the template is not read at all, where it is not: the template reader refuses it, or
    /// it is written for other dialects than PostgreSQL. It is left as written.
    pub unread: Option<String>,
    /// How many statements it holds.
    pub statements: usize,
    /// The statements the parser cannot read, by their place among its statements from 0, each
    /// with the parser's reason. Each is left as written.
    pub unparsed: Vec<(usize, String)>,
    /// Whether each statement parsed reads back as itself from the template's own text, its
    /// directives stood in again.
    pub read_back: bool,
}

/// `text`, a template, read for its spellings.
pub fn read(text: &str) -> Reading {
    let read = match Read::new(text) {
        Ok(read) => read,
        Err(unread) => {
            return Reading {
                unread: Some(match unread {
                    read::Unread::Refused(why) => why,
                    read::Unread::Dialects(dialects) => format!(
                        "written for {}",
                        crate::types::listed(dialects.iter().map(|d| d.name()))
                    ),
                }),
                read_back: true,
                ..Reading::default()
            }
        }
    };
    let unparsed = read
        .statements
        .iter()
        .enumerate()
        .filter_map(|(at, statement)| statement.as_ref().err().map(|why| (at, why.clone())))
        .collect();
    let read_back = read.parsed().all(|(_, statement)| {
        let last = statement.tokens.len() - 1;
        read::reparse(read.text(statement, 0, last), |p| p.parse_statements()).is_some_and(
            |parsed| {
                parsed.len() == 1 && read::canonical(&parsed[0]) == read::canonical(&statement.ast)
            },
        )
    });
    Reading {
        unread: None,
        statements: read.statements.len(),
        unparsed,
        read_back,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The brick example's templates, by their path under its source.
    fn brick(path: &std::path::Path) -> Option<String> {
        let source =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/brick/sqlc");
        std::fs::read_to_string(source.join(path)).ok()
    }

    #[test]
    fn each_place_is_counted_and_its_count_is_the_spellings_written() {
        let text = brick(std::path::Path::new("shared/scope_sets.sqlc")).unwrap();
        let layout = lay_out(&text, &brick);
        let counts: Vec<(Axis, Vec<usize>)> = layout
            .axes
            .iter()
            .map(|a| (a.axis, a.places.iter().map(|p| p.spellings.len()).collect()))
            .collect();
        assert_eq!(
            counts,
            vec![
                (Axis::JoinOrder, vec![6]),
                (Axis::Naming, vec![2]),
                (Axis::Conditions, vec![3, 3, 2]),
                (Axis::Negation, vec![]),
                (Axis::SemiJoin, vec![]),
            ]
        );
        assert_eq!(layout.count(), 6 * 2 * 18);
        assert_eq!(layout.axes[0].places[0].relations, Some(3));
        for axis in &layout.axes {
            for place in &axis.places {
                assert_eq!(place.spellings[0].text, text, "{}", place.what);
                let mut texts: Vec<&str> =
                    place.spellings.iter().map(|s| s.text.as_str()).collect();
                texts.sort();
                texts.dedup();
                assert_eq!(texts.len(), place.spellings.len(), "{}", place.what);
                for spelling in &place.spellings {
                    let again = read(&spelling.text);
                    assert!(
                        again.unparsed.is_empty() && again.read_back,
                        "{}",
                        spelling.text
                    );
                    assert!(
                        spelling.text.contains(":compose(@scope)"),
                        "{}",
                        spelling.text
                    );
                    assert!(
                        spelling.text.contains(":bind(min_year)"),
                        "{}",
                        spelling.text
                    );
                    for line in text.lines().filter(|l| l.starts_with('#')) {
                        assert!(spelling.text.contains(line), "{line}: {}", spelling.text);
                    }
                }
            }
        }
    }

    #[test]
    fn join_orders_are_the_permutations_fewer_where_a_lateral_relation_follows_what_it_reads() {
        let laid = |text: &str| {
            lay_out(text, &|_| None).axes[0]
                .places
                .iter()
                .map(|p| (p.relations, p.spellings.len()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            laid("SELECT a.x FROM a JOIN b ON b.x = a.x JOIN c ON c.x = b.x JOIN d ON d.x = c.x"),
            [(Some(4), 24)]
        );
        assert_eq!(
            laid("SELECT a.x FROM a JOIN b ON b.x = a.x CROSS JOIN LATERAL (SELECT u.y FROM u WHERE u.x = b.x) l JOIN c ON c.x = a.x"),
            [(Some(4), 12)]
        );
        let nearest = brick(std::path::Path::new("builders/nearest.sqlc")).unwrap();
        assert!(lay_out(&nearest, &brick).axes[0].places.is_empty());
    }

    #[test]
    fn a_template_with_nothing_to_spell_lays_out_nothing() {
        let layout = lay_out("SELECT 1;\nSELEC 2;\n", &|_| None);
        assert_eq!(layout.count(), 1);
        assert_eq!(layout.reading.unparsed.len(), 1);
        assert!(layout.axes.iter().all(|a| a.places.is_empty()));
    }

    #[test]
    fn each_directive_stands_in_as_sql_of_its_own_kind() {
        let text = "# the sets\nSELECT s.set_num\nFROM (\n    :compose(shared/x.sqlc, @scope = s.sqlc)\n) s\nWHERE s.year >= :bind(min_year EXPECTING 1);\n:union(a.sqlc, b.sqlc);\n";
        let reading = read(text);
        assert_eq!(reading.unread, None);
        assert_eq!(reading.statements, 2);
        assert!(reading.unparsed.is_empty(), "{reading:?}");
        assert!(reading.read_back);
        let stood = read::Stood::new(text).unwrap();
        assert_eq!(
            stood.parsed,
            "-- the sets\nSELECT s.set_num\nFROM (\n    SELECT * FROM \":compose(shared/x.sqlc, @scope = s.sqlc)\"\n) s\nWHERE s.year >= $min_year;\nSELECT * FROM \":union(a.sqlc, b.sqlc)\";\n"
        );
    }

    #[test]
    fn a_template_the_parser_cannot_read_is_counted_with_why() {
        let reading = read("SELECT 1;\nSELEC 2;\n");
        assert_eq!(reading.statements, 2);
        assert_eq!(reading.unparsed.len(), 1);
        assert_eq!(reading.unparsed[0].0, 1);
        let reading = read(":dialect(mysql)\nSELECT 1");
        assert_eq!(reading.unread.as_deref(), Some("written for mysql"));
    }
}

/// The census of the templates under a directory, as a test reads it.
#[cfg(test)]
mod census {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    use super::join_order::join_lists;
    use super::read::Read;

    /// What reading every template under a directory finds.
    #[derive(Debug, Default)]
    struct Census {
        templates: usize,
        unread: BTreeMap<String, usize>,
        statements: usize,
        unparsed: BTreeMap<String, usize>,
        not_read_back: Vec<PathBuf>,
        not_written_back: Vec<PathBuf>,
        join_lists: usize,
        using: usize,
        kept_by_using: usize,
        kept_for: BTreeMap<String, usize>,
    }

    /// The census of `source`, a composed template read under `roots` in order.
    fn census(source: &Path, roots: &[PathBuf]) -> Census {
        let resolve = |path: &Path| {
            roots
                .iter()
                .find_map(|root| std::fs::read_to_string(root.join(path)).ok())
        };
        let mut census = Census::default();
        for entry in walkdir::WalkDir::new(source) {
            let entry = entry.unwrap();
            if entry.path().extension().is_none_or(|ext| ext != "sqlc") {
                continue;
            }
            census.templates += 1;
            let text = std::fs::read_to_string(entry.path()).unwrap();
            let reading = super::read(&text);
            if let Some(why) = reading.unread {
                *census.unread.entry(why).or_default() += 1;
                continue;
            }
            census.statements += reading.statements;
            for (_, why) in reading.unparsed {
                *census.unparsed.entry(why).or_default() += 1;
            }
            if !reading.read_back {
                census.not_read_back.push(entry.path().to_path_buf());
            }
            let read = Read::new(&text).unwrap();
            if read.write_back() != text
                || super::spell(&text, &super::Picks::default(), &resolve) != text
            {
                census.not_written_back.push(entry.path().to_path_buf());
            }
            for (_, statement) in read.parsed() {
                for list in join_lists(&read, &statement.ast, &resolve) {
                    census.join_lists += 1;
                    if !list.using {
                        continue;
                    }
                    census.using += 1;
                    if !list.unread.is_empty() {
                        census.kept_by_using += 1;
                    }
                    for why in list.unread {
                        let kind = why.rsplit(", ").next().unwrap_or(&why).to_string();
                        *census.kept_for.entry(kind).or_default() += 1;
                    }
                }
            }
        }
        census
    }

    #[test]
    fn every_brick_template_reads_and_is_written_back_byte_for_byte() {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/brick/sqlc");
        let census = census(&source, std::slice::from_ref(&source));
        assert_eq!(census.templates, 51);
        assert!(census.unread.is_empty(), "{census:?}");
        assert_eq!(census.statements, 51);
        assert!(census.unparsed.is_empty(), "{census:?}");
        assert!(census.not_read_back.is_empty(), "{census:?}");
        assert!(census.not_written_back.is_empty(), "{census:?}");
        assert_eq!((census.join_lists, census.using), (21, 0), "{census:?}");
    }

    /// The census of the tree `SQLC_CENSUS_SOURCE` names, its composed templates read under it
    /// and then under each directory `SQLC_CENSUS_SEARCH_PATH` lists, separated by `:`.
    #[test]
    #[ignore = "reads a tree of templates: set SQLC_CENSUS_SOURCE"]
    fn the_census_of_a_tree() {
        let source = PathBuf::from(std::env::var("SQLC_CENSUS_SOURCE").unwrap());
        let mut roots = vec![source.clone()];
        if let Ok(paths) = std::env::var("SQLC_CENSUS_SEARCH_PATH") {
            roots.extend(paths.split(':').map(PathBuf::from));
        }
        let census = census(&source, &roots);
        println!("{census:#?}");
        assert!(census.not_read_back.is_empty() && census.not_written_back.is_empty());
    }
}
