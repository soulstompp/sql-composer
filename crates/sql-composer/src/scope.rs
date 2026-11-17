//! Which names stand for which templates at each point of a composed statement. A name a `WITH`
//! declares stands from its declaration to the end of the query that holds the `WITH`, in that
//! query's own text and in every template composed inside it, and not where a `WITH` further in
//! declares the same name: where PostgreSQL would find it, and nowhere else.

use std::path::PathBuf;

use crate::scan::{plain, Regime, Scanner, Stand};

/// A name standing at one point of a statement: for a template, or, as an entry of a `WITH`
/// that defines no template, for nothing, hiding any name like it from further out.
#[derive(Debug, Clone)]
pub(crate) struct Named {
    /// The name as the `WITH` writes it.
    pub(crate) name: String,
    /// The template it stands for, by canonical path.
    pub(crate) path: Option<PathBuf>,
    /// The depth of parentheses, in the text of the template being read, of the query whose
    /// `WITH` declares it; `None` for a name declared by a template further out.
    level: Option<usize>,
}

/// One template's text read in the order of its elements: where its quotes and its parentheses
/// stand, and the names that stand there.
pub(crate) struct Reading {
    scanner: Scanner,
    names: Vec<Named>,
    depth: usize,
    /// The SQL of this query level since its last `(`, `)` or `;`, each group in it read as `()`,
    /// each quoted name as written, each string as `''`, each comment as a space.
    recent: Vec<u8>,
    /// `recent` of each level outside this one.
    outer: Vec<Vec<u8>>,
    /// The name and level of the `WITH` entry whose `(` was just read, while nothing but
    /// whitespace and comments have followed it.
    opened: Option<(String, usize)>,
    /// The last element read was a directive.
    directive: bool,
    /// A name stands for some template outside the names read here: an entry of the statement's
    /// own can hide something even where no name read here stands for a template.
    views: bool,
    reads: Reads,
}

/// What a reading reads of its template's text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reads {
    /// Its names, and where its quotes stand.
    Names,
    /// Only where its quotes stand: no name it could declare or hide stands for a template.
    Quotes,
    /// Nothing: no name it could declare or hide stands for a template, and nothing asks where
    /// its quotes stand.
    Nothing,
}

impl Reading {
    /// The text of a template composed where `inherited` stand, with views standing for
    /// templates or not, read for what `reads` says.
    pub(crate) fn new(
        regime: &'static Regime,
        inherited: &[Named],
        views: bool,
        reads: Reads,
    ) -> Self {
        Reading {
            scanner: Scanner::new(regime),
            names: inherited
                .iter()
                .map(|named| Named {
                    level: None,
                    ..named.clone()
                })
                .collect(),
            depth: 0,
            recent: Vec::new(),
            outer: Vec::new(),
            opened: None,
            directive: false,
            views,
            reads,
        }
    }

    /// The names that stand where the reading is.
    pub(crate) fn names(&self) -> &[Named] {
        &self.names
    }

    /// Where the next byte stands.
    pub(crate) fn stands(&self) -> Stand {
        self.scanner.stands()
    }

    /// Read `text`, the template's own SQL up to its next directive or its end, which is a break.
    pub(crate) fn read_sql(&mut self, text: &str) {
        match self.reads {
            Reads::Names => {}
            Reads::Quotes => {
                self.scanner.skim(0, text.as_bytes());
                self.scanner.settle(&mut |_, _| {});
                return;
            }
            Reads::Nothing => return,
        }
        self.past_directive();
        let bytes = text.as_bytes();
        // Bytes the scanner has said are SQL itself and not yet read here, and where the bytes
        // read here end.
        let mut code = Vec::new();
        let mut next = 0;
        let mut at = 0;
        while at < bytes.len() {
            let inside = self.scanner.pass_inside(&bytes[at..]);
            if inside > 0 {
                at += inside;
                continue;
            }
            let run = plain(&bytes[at..]);
            if run > 0 && self.scanner.pass(&bytes[at..at + run]) {
                self.read_code(bytes, &mut code, &mut next);
                if at > next {
                    self.skipped(&bytes[next..at]);
                }
                self.code_run(&bytes[at..at + run]);
                at += run;
                next = at;
                continue;
            }
            self.scanner
                .step(at, bytes[at], &mut |at, byte| code.push((at, byte)));
            at += 1;
        }
        self.scanner.settle(&mut |at, byte| code.push((at, byte)));
        self.read_code(bytes, &mut code, &mut next);
        if next < bytes.len() {
            self.skipped(&bytes[next..]);
        }
    }

    /// Read the bytes of `text` in `code`, which the scanner has said are SQL itself, with what
    /// lies between them.
    fn read_code(&mut self, text: &[u8], code: &mut Vec<(usize, u8)>, next: &mut usize) {
        for (at, byte) in code.drain(..) {
            if at > *next {
                self.skipped(&text[*next..at]);
            }
            self.code_byte(byte);
            *next = at + 1;
        }
    }

    /// Read a directive, which stands where the SQL it writes will.
    pub(crate) fn read_directive(&mut self) {
        if self.reads != Reads::Names {
            return;
        }
        self.past_directive();
        let mut code = Vec::new();
        self.scanner.settle(&mut |_, byte| code.push(byte));
        for byte in code {
            self.code_byte(byte);
        }
        self.directive = true;
    }

    /// Whether the directive just read stands alone inside a `WITH` entry's parentheses.
    pub(crate) fn in_entry(&self) -> bool {
        self.opened.is_some()
    }

    /// Declare the name of the `WITH` entry the directive just read defines, for the template at
    /// `path`.
    pub(crate) fn define(&mut self, path: PathBuf) {
        if let Some((name, level)) = self.opened.take() {
            self.names.push(Named {
                name,
                path: Some(path),
                level: Some(level),
            });
        }
    }

    /// The `WITH` entry just opened, past its `(`: an entry of the statement's own, which hides
    /// any name like it while it stands. Where no name stands for a template, there is nothing
    /// for it to hide.
    pub(crate) fn close_entry(&mut self) {
        if let Some((name, level)) = self.opened.take() {
            if self.views || self.names.iter().any(|named| named.path.is_some()) {
                self.names.push(Named {
                    name,
                    path: None,
                    level: Some(level),
                });
            }
        }
    }

    fn past_directive(&mut self) {
        if std::mem::take(&mut self.directive) {
            self.close_entry();
            self.recent.extend_from_slice(b" _ ");
        }
    }

    /// Bytes that are not SQL itself. A quoted name is kept as written, since a `WITH` entry may
    /// be one; a string is read as `''`; a comment, a meta-command or a dollar quote as a space.
    /// Only a comment leaves a `WITH` entry's `(` standing alone.
    fn skipped(&mut self, bytes: &[u8]) {
        match bytes.first() {
            Some(b'"' | b'`' | b'[') => self.recent.extend_from_slice(bytes.trim_ascii_end()),
            Some(b'\'') => self.recent.extend_from_slice(b"''"),
            _ => self.recent.push(b' '),
        }
        if !matches!(bytes.first(), Some(b'-' | b'/' | b'#')) {
            self.close_entry();
        }
    }

    fn code_byte(&mut self, byte: u8) {
        if !byte.is_ascii_whitespace() {
            self.close_entry();
        }
        match byte {
            b'(' => {
                if let Some(name) = entry_before(&self.recent) {
                    self.opened = Some((name, self.depth));
                }
                self.outer.push(std::mem::take(&mut self.recent));
                self.depth += 1;
            }
            b')' => match self.outer.pop() {
                Some(recent) => {
                    self.recent = recent;
                    self.recent.extend_from_slice(b"()");
                    self.depth -= 1;
                    let depth = self.depth;
                    self.names
                        .retain(|named| named.level.is_none_or(|level| level <= depth));
                }
                // A group this template did not open: what stood where it was composed may
                // not stand past it.
                None => {
                    self.names.clear();
                    self.recent.clear();
                }
            },
            b';' if self.depth == 0 => {
                self.names.retain(|named| named.level.is_none());
                self.recent.clear();
            }
            _ => self.keep(&[byte]),
        }
    }

    /// Read `run`, bytes of SQL itself none of which opens anything: whole between each `(`,
    /// `)` and `;`.
    fn code_run(&mut self, mut run: &[u8]) {
        while !run.is_empty() {
            let cut = run
                .iter()
                .position(|b| matches!(b, b'(' | b')' | b';'))
                .unwrap_or(run.len());
            let (text, rest) = run.split_at(cut);
            if self.opened.is_some() && text.iter().any(|b| !b.is_ascii_whitespace()) {
                self.close_entry();
            }
            self.keep(text);
            let Some((&byte, rest)) = rest.split_first() else {
                break;
            };
            self.code_byte(byte);
            run = rest;
        }
    }

    /// `bytes` kept in `recent`, which keeps no more than its last kilobyte or so.
    fn keep(&mut self, bytes: &[u8]) {
        self.recent.extend_from_slice(bytes);
        if self.recent.len() > 1024 {
            let cut = self.recent.len() - 512;
            self.recent.drain(..cut);
        }
    }
}

/// The name of the `WITH` entry that `recent`, the SQL just before a `(`, opens:
/// `WITH [RECURSIVE] name [(columns)] AS [[NOT] MATERIALIZED]`, or the same after a `,`.
fn entry_before(recent: &[u8]) -> Option<String> {
    let mut end = recent.len();
    let mut token = || {
        let (start, word) = last_token(&recent[..end])?;
        end = start;
        Some(word)
    };
    let mut word = token()?;
    if word.eq_ignore_ascii_case(b"MATERIALIZED") {
        word = token()?;
        if word.eq_ignore_ascii_case(b"NOT") {
            word = token()?;
        }
    }
    if !word.eq_ignore_ascii_case(b"AS") {
        return None;
    }
    let mut name = token()?;
    if name == b"()" {
        name = token()?;
    }
    let named = is_name_byte(name[0]) || matches!(name[0], b'"' | b'`' | b'[');
    let before = token()?;
    let opens = before == b","
        || before.eq_ignore_ascii_case(b"WITH")
        || before.eq_ignore_ascii_case(b"RECURSIVE");
    (named && opens).then(|| String::from_utf8_lossy(name).into_owned())
}

/// The last token of `text`, with where it starts: a name, a quoted name, the `()` a group is
/// read as, or a single byte.
fn last_token(text: &[u8]) -> Option<(usize, &[u8])> {
    let end = text.len()
        - text
            .iter()
            .rev()
            .take_while(|b| b.is_ascii_whitespace())
            .count();
    let last = *text.get(end.checked_sub(1)?)?;
    let start = match last {
        b')' if end >= 2 && text[end - 2] == b'(' => end - 2,
        b'"' | b'`' | b']' => {
            let open = if last == b']' { b'[' } else { last };
            text[..end - 1].iter().rposition(|&b| b == open)?
        }
        byte if is_name_byte(byte) => {
            end - text[..end]
                .iter()
                .rev()
                .take_while(|b| is_name_byte(**b))
                .count()
        }
        _ => end - 1,
    };
    Some((start, &text[start..end]))
}

fn is_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte >= 0x80
}

/// Whether a name of `names` hides `name`: an unqualified name, which a `WITH` entry of the same
/// name, quoted or not, in any case, would stand in front of.
pub(crate) fn hidden(names: &[Named], name: &str) -> bool {
    !name.contains('.') && names.iter().any(|named| same_name(&named.name, name))
}

fn same_name(a: &str, b: &str) -> bool {
    let bare = |name: &str| {
        name.trim_matches(|c| matches!(c, '"' | '`' | '[' | ']'))
            .to_ascii_lowercase()
    };
    bare(a) == bare(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::{MYSQL, POSTGRES};

    fn entry(text: &str) -> Option<String> {
        entry_before(text.as_bytes())
    }

    #[test]
    fn test_an_entry_is_the_name_after_with_or_a_comma() {
        assert_eq!(entry("WITH core AS "), Some("core".into()));
        assert_eq!(entry("with recursive core as"), Some("core".into()));
        assert_eq!(entry("() , core AS NOT MATERIALIZED"), Some("core".into()));
        assert_eq!(entry("WITH core () AS MATERIALIZED "), Some("core".into()));
        assert_eq!(entry("WITH \"Core\" AS "), Some("\"Core\"".into()));
        // A subquery, a table created from a query, and a column's expression are not entries.
        assert_eq!(entry("SELECT * FROM "), None);
        assert_eq!(entry("CREATE TABLE core AS "), None);
        assert_eq!(entry("x int GENERATED ALWAYS AS "), None);
        assert_eq!(entry("WITH core "), None);
    }

    fn names_after(text: &str) -> Vec<String> {
        let mut reading = Reading::new(&POSTGRES, &[], true, Reads::Names);
        reading.read_sql(text);
        reading.names().iter().map(|n| n.name.clone()).collect()
    }

    #[test]
    fn test_a_name_stands_to_the_end_of_its_query() {
        assert_eq!(
            names_after("WITH a AS (SELECT 1), b AS (SELECT 2) SELECT "),
            ["a", "b"]
        );
        assert_eq!(
            names_after("SELECT * FROM (WITH a AS (SELECT 1) SELECT * FROM a) s, "),
            Vec::<String>::new()
        );
        assert_eq!(
            names_after("WITH a AS (SELECT 1) SELECT 1; SELECT "),
            Vec::<String>::new()
        );
        assert_eq!(names_after("WITH a (x) AS (SELECT 1) SELECT ("), ["a"]);
    }

    #[test]
    fn test_quotes_and_comments_are_read_as_the_database_reads_them() {
        assert_eq!(names_after("SELECT 'WITH a AS (' , "), Vec::<String>::new());
        assert_eq!(names_after("-- WITH a AS (\nSELECT "), Vec::<String>::new());
        assert_eq!(
            names_after("WITH /* c */ \"A\" AS (SELECT ')') SELECT "),
            ["\"A\""]
        );
        let mut reading = Reading::new(&MYSQL, &[], true, Reads::Names);
        reading.read_sql("WITH `a` AS (SELECT 1) SELECT ");
        assert_eq!(reading.names()[0].name, "`a`");
    }

    #[test]
    fn test_a_definition_must_stand_alone_in_its_entry() {
        let mut reading = Reading::new(&POSTGRES, &[], true, Reads::Names);
        reading.read_sql("WITH a AS ( -- the core\n  ");
        reading.read_directive();
        assert!(reading.in_entry());
        let mut reading = Reading::new(&POSTGRES, &[], true, Reads::Names);
        reading.read_sql("WITH a AS (SELECT * FROM ");
        reading.read_directive();
        assert!(!reading.in_entry());
    }

    #[test]
    fn test_a_group_the_template_did_not_open_ends_what_it_inherited() {
        let outer = [Named {
            name: "a".into(),
            path: None,
            level: Some(0),
        }];
        let mut reading = Reading::new(&POSTGRES, &outer, true, Reads::Names);
        reading.read_sql("SELECT 1 (x)");
        assert_eq!(reading.names().len(), 1);
        reading.read_sql(") , ");
        assert!(reading.names().is_empty());
    }

    #[test]
    fn test_an_unqualified_name_is_hidden_by_any_spelling_of_it() {
        let names = [Named {
            name: "\"Core\"".into(),
            path: None,
            level: None,
        }];
        assert!(hidden(&names, "core"));
        assert!(hidden(&names, "CORE"));
        assert!(!hidden(&names, "v.core"));
        assert!(!hidden(&names, "other"));
    }
}
