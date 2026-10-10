//! The spellings of a template: the other ways to write it that return the same rows, laid out
//! an axis at a time, and the template written in the spelling picked on each axis.
//!
//! A template is read for PostgreSQL as sqlparser reads it, each directive standing in as SQL of
//! its own kind: a `:compose`, a slot, a `:define` or a command as a query reading a relation
//! named after the directive, a `:bind` as a parameter. Each `#` comment and the `:dialect(…)`
//! declaration are blanks to the parser. A spelling is written by moving the template's own text,
//! every byte outside what moves left as written, and each spelling is read back and compared with
//! the statement it must be before it is offered. Nothing reads a catalog: each condition is read
//! from the template's own text. A template, or a statement of it, that the parser cannot read is
//! left as written, and so is every template when no spelling is picked.

// The identities are read by their tests until the picks write templates in them.
#![allow(dead_code)]

mod columns;
mod conditions;
mod extent;
mod join_order;
#[cfg(test)]
mod laws;
mod negation;
mod place;
mod read;
mod semi_join;

use place::{made, Place};
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
    /// A semi-join's form: `x IN (SELECT y …)`, or `EXISTS (SELECT … WHERE x = y)`.
    SemiJoin,
}

impl Axis {
    /// Each axis, in the order a template is written in their picks.
    pub const ALL: &'static [Axis] = &[Axis::Conditions, Axis::Negation, Axis::SemiJoin];

    /// The axis's key in `spellings.toml`.
    pub fn key(&self) -> &'static str {
        match self {
            Axis::Conditions => "conditions",
            Axis::Negation => "negation",
            Axis::SemiJoin => "semi_join",
        }
    }

    /// The axis in words.
    pub fn words(&self) -> &'static str {
        match self {
            Axis::Conditions => "where a condition sits",
            Axis::Negation => "negation",
            Axis::SemiJoin => "a semi-join's form",
        }
    }

    /// The places in `read` this axis applies at, each spelling unchecked.
    fn places(&self, read: &Read) -> Vec<Place> {
        match self {
            Axis::Conditions => conditions::places(read),
            Axis::Negation => negation::places(read),
            Axis::SemiJoin => semi_join::places(read),
        }
    }
}

/// `text` written in `pick` on `axis` wherever the axis applies and a spelling of the pick reads
/// back as it must; everywhere else as written.
fn written_in(text: &str, axis: Axis, pick: &str) -> String {
    let mut text = text.to_string();
    for _ in 0..MOVES {
        let Ok(read) = Read::new(&text) else {
            break;
        };
        let next = axis.places(&read).into_iter().find_map(|place| {
            if place.choices[0].picks.contains(&pick) {
                return None;
            }
            let choice = place.choices.iter().find(|c| c.picks.contains(&pick))?;
            made(&read, place.statement, choice)
        });
        match next {
            Some(spelled) => text = spelled,
            None => break,
        }
    }
    text
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
            "          \nSELECT s.set_num\nFROM (\n    SELECT * FROM \":compose(shared/x.sqlc, @scope = s.sqlc)\"\n) s\nWHERE s.year >= $min_year;\nSELECT * FROM \":union(a.sqlc, b.sqlc)\";\n"
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
            if read.write_back() != text {
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
