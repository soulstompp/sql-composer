//! The template reader: SQL passed through as written, the directives parsed where they stand in
//! SQL itself, and `#` comments stripped.
//!
//! The reader reads the SQL around a directive as each dialect's database reads its quotes and
//! comments. Inside a string, a quoted name, a comment or a psql meta-command, a `#` and a
//! directive are SQL like any other. Inside a dollar quote a directive composes, and a `:bind` is
//! refused, since no value reaches it there. `#>`, `#>>` and `#-` are SQL wherever they stand.
//!
//! A `#` comment in SQL itself runs to the end of the line, the newline with it, and never
//! reaches the composed SQL. A template that two databases read differently at a `#`, a
//! directive, a `;` or its end is refused, with both readings.
//!
//! A template that names the dialects it is written for, by `:dialect(…)` at its head, is read
//! by their databases alone; one that names none, by every dialect's.

use winnow::combinator::{alt, trace};
use winnow::error::{ContextError, ParserError};
use winnow::stream::{AsBStr, AsChar, Compare, Stream, StreamIsPartial};
use winnow::token::literal;
use winnow::Parser;

use crate::error::{Error, Result};
use crate::scan::{self, Scanner, Stand};
use crate::types::{listed, Dialect, Element};

use super::bind::bind;
use super::command::{command_body, command_kind};
use super::compose::{compose, define};

/// The directives, each by the name between its `:` and its `(`.
const DIRECTIVES: [&str; 8] = [
    "bind",
    "compose",
    "define",
    "count",
    "union",
    "intersect",
    "except",
    "dialect",
];

/// Parse a single macro invocation after the `:` prefix.
///
/// Tries `bind(`, `compose(`, `define(`, then the commands `count(`, `union(`, `intersect(` and
/// `except(`, in order.
fn macro_invocation<'i, Input, Error>(input: &mut Input) -> std::result::Result<Element, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'i str>,
    <Input as Stream>::Slice: AsBStr,
    <Input as Stream>::Token: AsChar + Clone,
    Error: ParserError<Input>,
{
    trace("macro_invocation", move |input: &mut Input| {
        literal(":").parse_next(input)?;

        alt((
            literal("bind(").flat_map(|_| bind).map(Element::Bind),
            literal("compose(")
                .flat_map(|_| compose)
                .map(Element::Compose),
            literal("define(")
                .flat_map(|_| define)
                .map(Element::Compose),
            |input: &mut Input| {
                let kind = command_kind(input)?;
                let cmd = command_body(input, kind)?;
                Ok(Element::Command(cmd))
            },
        ))
        .parse_next(input)
    })
    .parse_next(input)
}

/// Read a template into the dialects it declares and its elements: SQL, directives, and nothing
/// for its `#` comments.
pub(crate) fn template(input: &str) -> Result<(Vec<Dialect>, Vec<Element>)> {
    Reader::new(input).read()
}

/// What the reader reads as the template's own, by where it stands in the text.
#[cfg(feature = "spellings")]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Mark {
    /// A directive, from its `:` to its closing `)`.
    Directive(Element),
    /// A `#` comment, from its `#` to the end of its line, the newline left out.
    Comment,
    /// The `:dialect(…)` declaration, from its `:` to its closing `)`.
    Declaration,
}

/// Read a template as [`template`] does, giving the dialects it declares and each byte range the
/// reader reads as the template's own, in the order they stand.
#[cfg(feature = "spellings")]
pub(crate) fn marked(input: &str) -> Result<(Vec<Dialect>, Marks)> {
    Reader::new(input)
        .read_marked()
        .map(|(dialects, _, marks)| (dialects, marks))
}

/// Each byte range the reader reads as the template's own, with what it read there.
#[cfg(feature = "spellings")]
pub(crate) type Marks = Vec<(usize, usize, Mark)>;
#[cfg(not(feature = "spellings"))]
type Marks = ();

/// What the reader does at a byte one database's reading of the template decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Act {
    /// Keep it as SQL.
    Keep,
    /// Read it as the template's own: a directive, a `#` comment, the end of a statement.
    Read,
    /// Refuse it: a `:bind` where no value reaches.
    Refuse,
}

struct Reader<'i> {
    input: &'i str,
    pos: usize,
    /// Each database's reading of the SQL read so far: one for the dialects that read alike.
    readings: Vec<(Vec<Dialect>, Scanner)>,
    elements: Vec<Element>,
    sql: String,
    /// Whether a `#` comment was stripped since the last element.
    stripped: bool,
    /// The dialects the template declares, and whether it has declared them.
    dialects: Vec<Dialect>,
    declared: bool,
    marks: Marks,
}

impl<'i> Reader<'i> {
    fn new(input: &'i str) -> Self {
        Reader {
            input,
            pos: 0,
            readings: Dialect::ALL
                .iter()
                .fold(Vec::new(), |mut readings, dialect| {
                    let regime = dialect.regime();
                    match readings
                        .iter_mut()
                        .find(|(_, scanner): &&mut (Vec<Dialect>, Scanner)| scanner.reads(regime))
                    {
                        Some((dialects, _)) => dialects.push(*dialect),
                        None => readings.push((vec![*dialect], Scanner::new(regime))),
                    }
                    readings
                }),
            elements: Vec::new(),
            sql: String::new(),
            stripped: false,
            dialects: Vec::new(),
            declared: false,
            marks: Marks::default(),
        }
    }

    fn read(self) -> Result<(Vec<Dialect>, Vec<Element>)> {
        self.read_marked()
            .map(|(dialects, elements, _)| (dialects, elements))
    }

    fn read_marked(mut self) -> Result<(Vec<Dialect>, Vec<Element>, Marks)> {
        while self.pos < self.input.len() {
            let input = self.input;
            let rest = &input[self.pos..];
            let run = rest
                .bytes()
                .position(|b| matches!(b, b':' | b'#' | b';'))
                .unwrap_or(rest.len());
            if run > 0 {
                self.keep(&rest[..run]);
                continue;
            }
            let read = match rest.as_bytes()[0] {
                b':' => self.directive()?,
                b'#' => self.comment()?,
                _ => {
                    self.agree("this ;", |stand| match stand {
                        Stand::Code => (Act::Read, "as the end of a statement"),
                        _ => (Act::Keep, ""),
                    })?;
                    false
                }
            };
            if !read {
                self.keep(&rest[..1]);
            }
        }
        self.end_element();
        self.agree("the template's end", |stand| match stand {
            Stand::Code => (Act::Read, "in SQL"),
            _ => (Act::Keep, ""),
        })?;
        Ok((self.dialects, self.elements, self.marks))
    }

    /// The directive at the reader's place, read where every database reads it in SQL itself
    /// or in a dollar quote. Returns whether it was read.
    fn directive(&mut self) -> Result<bool> {
        let rest = &self.input[self.pos + 1..];
        let Some(name) = DIRECTIVES.iter().find(|name| {
            rest.strip_prefix(**name)
                .is_some_and(|r| r.starts_with('('))
        }) else {
            return Ok(false);
        };
        let bind = *name == "bind";
        let what = format!("this :{name}");
        let act = self.agree(&what, |stand| match stand {
            Stand::Code => (Act::Read, "as a directive"),
            Stand::Dollar if bind => (Act::Refuse, ""),
            Stand::Dollar => (Act::Read, "as a directive"),
            Stand::Inside => (Act::Keep, ""),
        })?;
        match act {
            Act::Keep => Ok(false),
            Act::Refuse => Err(self.refusal(format!(
                ":{name} inside a dollar quote: no value reaches it there"
            ))),
            Act::Read if *name == "dialect" => self.declaration(),
            Act::Read => {
                let input = self.input;
                let mut rest = &input[self.pos..];
                let element = macro_invocation::<_, ContextError>
                    .parse_next(&mut rest)
                    .map_err(|error| {
                        let error = error.to_string();
                        self.refusal(match error.as_str() {
                            "" => format!(":{name} does not parse"),
                            _ => format!(":{name} does not parse: {error}"),
                        })
                    })?;
                self.end_element();
                #[cfg(feature = "spellings")]
                self.marks.push((
                    self.pos,
                    self.input.len() - rest.len(),
                    Mark::Directive(element.clone()),
                ));
                self.elements.push(element);
                self.pos = self.input.len() - rest.len();
                Ok(true)
            }
        }
    }

    /// The `#` at the reader's place, stripped to the end of its line, newline and all, where
    /// every database reads it in SQL itself. `#>`, `#>>` and `#-` are SQL, and `#--` is a
    /// comment. Returns whether it was stripped.
    fn comment(&mut self) -> Result<bool> {
        let after = &self.input[self.pos + 1..];
        let operator =
            after.starts_with('>') || (after.starts_with('-') && !after.starts_with("--"));
        let act = self.agree("this #", |stand| match stand {
            Stand::Code if !operator => (Act::Read, "as a template comment"),
            _ => (Act::Keep, ""),
        })?;
        if act != Act::Read {
            return Ok(false);
        }
        #[cfg(feature = "spellings")]
        self.marks.push((
            self.pos,
            self.pos + 1 + after.find('\n').unwrap_or(after.len()),
            Mark::Comment,
        ));
        self.pos = match after.find('\n') {
            Some(newline) => self.pos + 1 + newline + 1,
            None => self.input.len(),
        };
        self.stripped = true;
        Ok(true)
    }

    /// The `:dialect(…)` at the reader's place: the dialects the template is written for, named
    /// once, at its head, before any SQL. From here on only their databases read it. The
    /// declaration leaves the template as a comment would, with the spaces after it and its
    /// newline.
    fn declaration(&mut self) -> Result<bool> {
        if self.declared || !self.elements.is_empty() || !self.sql.trim().is_empty() {
            return Err(self.refusal(
                ":dialect declares the template's dialects once, at its head, before any SQL"
                    .into(),
            ));
        }
        let open = self.pos + ":dialect(".len();
        let Some(close) = self.input[open..].find(')').map(|at| open + at) else {
            return Err(self.refusal(":dialect( is never closed".into()));
        };
        let mut dialects = Vec::new();
        for name in self.input[open..close].split(',').map(str::trim) {
            let dialect = match Dialect::ALL.iter().find(|dialect| dialect.name() == name) {
                Some(dialect) => *dialect,
                None if name.is_empty() => {
                    return Err(self.refusal(":dialect names no dialect".into()))
                }
                None => {
                    return Err(self.refusal(format!(
                        "{name} is not a dialect: postgres, mysql, mariadb, sqlite or duckdb"
                    )))
                }
            };
            if !dialects.contains(&dialect) {
                dialects.push(dialect);
            }
        }
        #[cfg(feature = "spellings")]
        self.marks.push((self.pos, close + 1, Mark::Declaration));
        let rest = self.input[close + 1..].trim_start_matches([' ', '\t']);
        let rest = rest
            .strip_prefix("\r\n")
            .or_else(|| rest.strip_prefix('\n'))
            .unwrap_or(rest);
        self.pos = self.input.len() - rest.len();
        for (reading, _) in &mut self.readings {
            reading.retain(|dialect| dialects.contains(dialect));
        }
        self.readings.retain(|(reading, _)| !reading.is_empty());
        self.dialects = dialects;
        self.declared = true;
        self.stripped = true;
        Ok(true)
    }

    /// `run` kept as SQL, and read by every database: each plain run found once for all of them,
    /// and passed whole by each that stands in SQL itself.
    fn keep(&mut self, run: &str) {
        let at = self.sql.len();
        let bytes = run.as_bytes();
        let mut start = 0;
        while start < bytes.len() {
            let plain = scan::plain(&bytes[start..]);
            let end = start + plain.max(1);
            let piece = &bytes[start..end];
            for (_, scanner) in &mut self.readings {
                if plain == 0 || !scanner.pass(piece) {
                    scanner.skim(at + start, piece);
                }
            }
            start = end;
        }
        self.sql.push_str(run);
        self.pos += run.len();
    }

    /// The SQL read since the last element, as an element of its own, and a break for every
    /// database's reading, as the composer's split takes it.
    fn end_element(&mut self) {
        if !self.sql.is_empty() || self.stripped {
            self.elements
                .push(Element::Sql(std::mem::take(&mut self.sql)));
        }
        self.stripped = false;
        for (_, scanner) in &mut self.readings {
            scanner.settle(&mut |_, _| {});
        }
    }

    /// What every database does at the reader's place, as `act` says from where it stands there;
    /// refused where two differ, naming each reading. `act` gives the words for a stand that
    /// is not the scanner's own: a directive, a comment, the end of a statement.
    fn agree(&self, what: &str, act: impl Fn(Stand) -> (Act, &'static str)) -> Result<Act> {
        let mut done = self
            .readings
            .iter()
            .map(|(_, scanner)| act(scanner.stands()).0);
        let first = done
            .next()
            .expect("a template is read by one dialect at least");
        if done.all(|done| done == first) {
            return Ok(first);
        }
        let acts: Vec<(Dialect, &'static str)> = Dialect::ALL
            .iter()
            .filter_map(|dialect| {
                let (_, scanner) = self
                    .readings
                    .iter()
                    .find(|(reading, _)| reading.contains(dialect))?;
                let words = match act(scanner.stands()).1 {
                    "" => scanner.reading(),
                    words => words,
                };
                Some((*dialect, words))
            })
            .collect();
        let mut groups: Vec<(&'static str, Vec<&'static str>)> = Vec::new();
        for (dialect, words) in &acts {
            match groups.iter_mut().find(|(said, _)| said == words) {
                Some((_, names)) => names.push(dialect.name()),
                None => groups.push((words, vec![dialect.name()])),
            }
        }
        let readings: Vec<String> = groups
            .iter()
            .enumerate()
            .map(|(i, (words, names))| {
                let verb = if i == 0 {
                    format!(" read {what}")
                } else {
                    String::new()
                };
                format!("{}{verb} {words}", listed(names.iter()))
            })
            .collect();
        let (last, rest) = readings.split_last().expect("two readings differ");
        Err(self.refusal(format!(
            "{}, and {last}; write it so they read it alike, or declare the dialects the template \
             is written for with :dialect(…)",
            rest.join(", ")
        )))
    }

    /// A parse error at the reader's place, by line and column.
    fn refusal(&self, message: String) -> Error {
        let before = &self.input[..self.pos];
        let line = before.matches('\n').count() + 1;
        let column = before[before.rfind('\n').map_or(0, |at| at + 1)..]
            .chars()
            .count()
            + 1;
        Error::Parse {
            location: format!("line {line}, column {column}"),
            message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Binding, CommandKind, ComposeRef, ComposeTarget};
    use std::path::PathBuf;

    #[test]
    fn test_plain_sql() {
        let input = "SELECT id, name FROM users";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT id, name FROM users".into()));
    }

    #[test]
    fn test_sql_with_bind() {
        let input = "SELECT * FROM users WHERE id = :bind(user_id)";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            Element::Sql("SELECT * FROM users WHERE id = ".into())
        );
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "user_id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
    }

    #[test]
    fn test_sql_with_compose() {
        let input = "SELECT COUNT(*) FROM (\n  :compose(templates/get_user.tql)\n)";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Element::Sql("SELECT COUNT(*) FROM (\n  ".into()));
        assert_eq!(
            result[1],
            Element::Compose(ComposeRef {
                target: ComposeTarget::Path(PathBuf::from("templates/get_user.tql")),
                slots: vec![],
            })
        );
        assert_eq!(result[2], Element::Sql("\n)".into()));
    }

    #[test]
    fn test_multiple_binds() {
        let input = "WHERE id = :bind(user_id) AND active = :bind(active)";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Element::Sql("WHERE id = ".into()));
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "user_id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
        assert_eq!(result[2], Element::Sql(" AND active = ".into()));
        assert_eq!(
            result[3],
            Element::Bind(Binding {
                name: "active".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
    }

    #[test]
    fn test_colon_not_a_macro() {
        let input = "SELECT '10:30' FROM t";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT '10:30' FROM t".into()));
    }

    #[test]
    fn test_command_in_template() {
        let input = ":count(templates/get_user.tql)";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 1);
        match &result[0] {
            Element::Command(cmd) => {
                assert_eq!(cmd.kind, CommandKind::Count);
                assert_eq!(cmd.sources, vec![PathBuf::from("templates/get_user.tql")]);
            }
            other => panic!("expected Command, got {:?}", other),
        }
    }

    #[test]
    fn test_full_template() {
        let input =
            "SELECT id, name, email\nFROM users\nWHERE id = :bind(user_id)\n  AND active = :bind(active);";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 5);
        assert_eq!(
            result[0],
            Element::Sql("SELECT id, name, email\nFROM users\nWHERE id = ".into())
        );
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "user_id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
        assert_eq!(result[2], Element::Sql("\n  AND active = ".into()));
        assert_eq!(
            result[3],
            Element::Bind(Binding {
                name: "active".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
        assert_eq!(result[4], Element::Sql(";".into()));
    }

    #[test]
    fn test_semicolon_after_bind() {
        let input = "WHERE id = :bind(user_id);";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Element::Sql("WHERE id = ".into()));
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "user_id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
        assert_eq!(result[2], Element::Sql(";".into()));
    }

    #[test]
    fn test_empty_input() {
        let input = "";
        let result = template(input).unwrap().1;
        assert!(result.is_empty());
    }

    #[test]
    fn test_comment_standalone_line() {
        let input = "# comment\nSELECT 1;";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;".into()));
    }

    #[test]
    fn test_comment_inline() {
        let input = "SELECT 1; # comment\nSELECT 2;";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1; SELECT 2;".into()));
    }

    #[test]
    fn test_comment_with_macro_text() {
        let input = "# :bind(x)\nSELECT 1;";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;".into()));
    }

    #[test]
    fn test_comment_before_macro() {
        let input = "# get user\nSELECT * FROM users WHERE id = :bind(id);";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0],
            Element::Sql("SELECT * FROM users WHERE id = ".into())
        );
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
        assert_eq!(result[2], Element::Sql(";".into()));
    }

    #[test]
    fn test_only_comments() {
        let input = "# just a comment";
        let result = template(input).unwrap().1;
        // Comment-only input produces a single empty SQL element (stripped comments).
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql(String::new()));
    }

    #[test]
    fn test_multiple_comment_lines() {
        let input = "# line 1\n# line 2\nSELECT 1;";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;".into()));
    }

    #[test]
    fn test_comment_at_eof_no_newline() {
        let input = "SELECT 1;\n# trailing";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT 1;\n".into()));
    }

    #[test]
    fn test_at_sign_in_sql_literal() {
        let input = "SELECT @variable FROM t";
        let result = template(input).unwrap().1;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Element::Sql("SELECT @variable FROM t".into()));
    }

    #[test]
    fn test_comments_before_macro() {
        let input = "# comment line 1\n# comment line 2\n:bind(id)";
        let result = template(input).unwrap().1;
        // Comments are stripped; the macro is still parsed.
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Element::Sql(String::new()));
        assert_eq!(
            result[1],
            Element::Bind(Binding {
                name: "id".into(),
                min_values: None,
                max_values: None,
                nullable: false,
            })
        );
    }
}
