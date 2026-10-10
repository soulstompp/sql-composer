//! A template read for its spellings: each directive stood in as SQL of its own kind, each `#`
//! comment and the `:dialect(…)` declaration blanked, and each statement parsed, with where every
//! token of it stands in the template as written.

use sqlparser::ast::{BinaryOperator, Expr, Spanned, Statement, TableFactor, VisitMut, VisitorMut};
use sqlparser::dialect::{Dialect as _, Precedence};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer, Whitespace};
use std::ops::ControlFlow;

use crate::parsed::{first_code, masked, Postgres};
use crate::parser::{marked, Mark};
use crate::scan::{Scanner, POSTGRES};
use crate::types::{Dialect, Element};

/// What stands in the parser's text for a directive, a comment or the declaration.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Kind {
    /// A `:compose`, a slot, a `:define` or a command: a query reading a relation named after the
    /// directive, `SELECT * FROM ":compose(…)"`.
    Relation(Element),
    /// A `:bind`: a parameter named after it, `$name`.
    Parameter(String),
    /// A comment or the declaration: blanks.
    Blank,
}

/// One stand-in: where it stands in the parser's text and in the template, and what it stands
/// for.
#[derive(Debug, Clone)]
pub(crate) struct StandIn {
    parsed: (usize, usize),
    written: (usize, usize),
    pub(crate) kind: Kind,
}

/// A template with each directive stood in as SQL of its own kind.
#[derive(Debug, Clone)]
pub(crate) struct Stood {
    /// The template as written.
    pub(crate) text: String,
    /// The text the parser reads.
    pub(crate) parsed: String,
    /// Each stand-in, in the order they stand.
    pub(crate) stand_ins: Vec<StandIn>,
}

/// Why a template is read as written, with no spelling of it laid out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Unread {
    /// The template reader refuses it.
    Refused(String),
    /// It declares dialects, and PostgreSQL is not among them.
    Dialects(Vec<Dialect>),
}

impl Stood {
    /// `text` with each directive stood in, each `#` comment and the declaration blanked.
    pub(crate) fn new(text: &str) -> Result<Stood, Unread> {
        let (dialects, marks) = marked(text).map_err(|error| Unread::Refused(error.to_string()))?;
        if !dialects.is_empty() && !dialects.contains(&Dialect::Postgres) {
            return Err(Unread::Dialects(dialects));
        }
        let mut parsed = String::new();
        let mut stand_ins = Vec::new();
        let mut at = 0;
        for (start, end, mark) in marks {
            parsed.push_str(&text[at..start]);
            let written = &text[start..end];
            let (stand_in, kind) = match mark {
                Mark::Directive(Element::Bind(binding)) => {
                    (format!("${}", binding.name), Kind::Parameter(binding.name))
                }
                Mark::Directive(element) => (
                    format!("SELECT * FROM \"{}\"", written.replace('"', "\"\"")),
                    Kind::Relation(element),
                ),
                Mark::Comment | Mark::Declaration => (
                    written
                        .chars()
                        .map(|c| if c == '\n' { '\n' } else { ' ' })
                        .collect(),
                    Kind::Blank,
                ),
            };
            let from = parsed.len();
            parsed.push_str(&stand_in);
            stand_ins.push(StandIn {
                parsed: (from, parsed.len()),
                written: (start, end),
                kind,
            });
            at = end;
        }
        parsed.push_str(&text[at..]);
        Ok(Stood {
            text: text.to_string(),
            parsed: masked(&parsed),
            stand_ins,
        })
    }

    /// Where offset `at` of the parser's text stands in the template: inside a stand-in, at its
    /// start, or at its end where `end` is asked for.
    fn written(&self, at: usize, end: bool) -> usize {
        let mut shift: isize = 0;
        for stand_in in &self.stand_ins {
            let (from, to) = stand_in.parsed;
            if at <= from {
                break;
            }
            if at < to {
                return if end {
                    stand_in.written.1
                } else {
                    stand_in.written.0
                };
            }
            shift += (stand_in.written.1 - stand_in.written.0) as isize - (to - from) as isize;
        }
        (at as isize + shift) as usize
    }

    /// The template's bytes a range of the parser's text covers, a stand-in it cuts through
    /// covered whole.
    pub(crate) fn range(&self, (start, end): (usize, usize)) -> (usize, usize) {
        (self.written(start, false), self.written(end, true))
    }

    /// The directive a relation's stand-in names, by the name the parser read.
    pub(crate) fn relation(&self, name: &str) -> Option<&Element> {
        self.stand_ins
            .iter()
            .find_map(|stand_in| match &stand_in.kind {
                Kind::Relation(element)
                    if self.text[stand_in.written.0..stand_in.written.1] == *name =>
                {
                    Some(element)
                }
                _ => None,
            })
    }
}

/// A template read: its statements, each parsed or not.
#[derive(Debug, Clone)]
pub(crate) struct Read {
    pub(crate) stood: Stood,
    /// Each statement, in order, parsed where the parser reads it.
    pub(crate) statements: Vec<Result<Stmt, String>>,
}

impl Read {
    /// `text` read: stood in, cut into statements, each parsed.
    pub(crate) fn new(text: &str) -> Result<Read, Unread> {
        let stood = Stood::new(text)?;
        let parsed = stood.parsed.clone();
        let mut ends = Scanner::new(&POSTGRES).ends(&parsed);
        if ends.last() != Some(&parsed.len()) {
            ends.push(parsed.len());
        }
        let mut statements = Vec::new();
        let mut start = 0;
        for end in ends {
            let piece = &parsed[start..end];
            if let Some(code) = first_code(piece) {
                statements.push(Stmt::new(start + code, &piece[code..]));
            }
            start = end;
        }
        Ok(Read { stood, statements })
    }

    /// The template written back from its statements: the text between them, and each one's
    /// own text where its tokens stand in the template.
    #[cfg(test)]
    pub(crate) fn write_back(&self) -> String {
        let mut out = String::new();
        let mut at = 0;
        for statement in self.statements.iter().flatten() {
            let last = statement.tokens.len() - 1;
            let (start, end) = self.written(statement, 0, last);
            if start < at {
                return String::new();
            }
            out.push_str(&self.stood.text[at..start]);
            out.push_str(&self.stood.text[start..end]);
            at = end;
        }
        out.push_str(&self.stood.text[at..]);
        out
    }

    /// The statements parsed, by their place among the template's statements.
    pub(crate) fn parsed(&self) -> impl Iterator<Item = (usize, &Stmt)> {
        self.statements
            .iter()
            .enumerate()
            .filter_map(|(at, statement)| statement.as_ref().ok().map(|s| (at, s)))
    }

    /// The template's bytes a statement's tokens `first` to `last` cover.
    pub(crate) fn written(&self, statement: &Stmt, first: usize, last: usize) -> (usize, usize) {
        let (start, end) = statement.bytes(first, last);
        self.stood
            .range((statement.base + start, statement.base + end))
    }

    /// The template's text a statement's tokens `first` to `last` cover.
    pub(crate) fn text(&self, statement: &Stmt, first: usize, last: usize) -> &str {
        let (start, end) = self.written(statement, first, last);
        &self.stood.text[start..end]
    }

    /// Where a statement's byte `at` stands in the template.
    pub(crate) fn written_at(&self, statement: &Stmt, at: usize, end: bool) -> usize {
        self.stood.written(statement.base + at, end)
    }
}

/// One statement of a template, as the parser reads it.
#[derive(Debug, Clone)]
pub(crate) struct Stmt {
    /// Where it starts in the parser's text.
    pub(crate) base: usize,
    /// Its text, as the parser reads it.
    pub(crate) text: String,
    pub(crate) ast: Statement,
    /// Its tokens, whitespace and comments among them.
    pub(crate) tokens: Vec<TokenWithSpan>,
    lines: Vec<usize>,
}

impl Stmt {
    /// The statement at `base`, parsed; why not, where it does not parse as one statement.
    fn new(base: usize, text: &str) -> Result<Stmt, String> {
        let tokens = tokens(text)?;
        let mut statements = Parser::new(&Postgres::default())
            .with_tokens_with_locations(tokens.clone())
            .parse_statements()
            .map_err(|error| error.to_string())?;
        if statements.len() != 1 {
            return Err(format!("{} statements, not one", statements.len()));
        }
        let mut lines = vec![0];
        lines.extend(text.match_indices('\n').map(|(at, _)| at + 1));
        Ok(Stmt {
            base,
            text: text.to_string(),
            ast: statements.remove(0),
            tokens,
            lines,
        })
    }

    /// The byte offset of `location` in the statement's text.
    fn offset(&self, location: Location) -> Option<usize> {
        let start = *self
            .lines
            .get(usize::try_from(location.line).ok()?.checked_sub(1)?)?;
        let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
        let line = &self.text[start..];
        if column == 0 {
            return Some(start);
        }
        line.char_indices()
            .nth(column)
            .map(|(at, _)| start + at)
            .or_else(|| (line.chars().count() == column).then_some(self.text.len()))
    }

    /// The bytes tokens `first` to `last` cover.
    pub(crate) fn bytes(&self, first: usize, last: usize) -> (usize, usize) {
        let start = self.offset(self.tokens[first].span.start).unwrap_or(0);
        let end = self
            .offset(self.tokens[last].span.end)
            .unwrap_or(self.text.len());
        (start, end)
    }

    /// The token that starts where `location` is.
    pub(crate) fn token_at(&self, location: Location) -> Option<usize> {
        self.tokens.iter().position(|t| t.span.start == location)
    }

    /// The first token of `node`'s span.
    pub(crate) fn first_token(&self, node: &impl Spanned) -> Option<usize> {
        let span = node.span();
        if span == sqlparser::tokenizer::Span::empty() {
            return None;
        }
        self.token_at(span.start)
    }

    /// The token before `at` that is neither whitespace nor a comment.
    pub(crate) fn before(&self, at: usize) -> Option<usize> {
        (0..at).rev().find(|&i| !blank(&self.tokens[i].token))
    }

    /// The token after `at` that is neither whitespace nor a comment.
    pub(crate) fn after(&self, at: usize) -> Option<usize> {
        (at + 1..self.tokens.len()).find(|&i| !blank(&self.tokens[i].token))
    }

    /// Whether token `at` is the keyword `keyword`.
    pub(crate) fn is(&self, at: usize, keyword: Keyword) -> bool {
        matches!(&self.tokens[at].token, Token::Word(w) if w.keyword == keyword && w.quote_style.is_none())
    }

    /// Parse with `parse` from token `first`, giving what it parsed and the last token it read.
    pub(crate) fn parse_at<T>(
        &self,
        first: usize,
        parse: impl FnOnce(&mut Parser) -> Result<T, ParserError>,
    ) -> Option<(T, usize)> {
        let dialect = Postgres::default();
        let mut parser =
            Parser::new(&dialect).with_tokens_with_locations(self.tokens[first..].to_vec());
        let parsed = parse(&mut parser).ok()?;
        let read = first + parser.index();
        let last = (first..read)
            .rev()
            .find(|&i| !blank(&self.tokens[i].token))?;
        Some((parsed, last))
    }

    /// The last token of `expr`, read from `first`, its first token, where the tokens read back
    /// as `expr`; `stop` ends the read before an operator that binds no tighter than it, as a
    /// conjunct of a chain is read before its `AND`.
    pub(crate) fn expr_from(
        &self,
        first: usize,
        expr: &Expr,
        stop: Option<Precedence>,
    ) -> Option<usize> {
        let (parsed, last) = self.parse_at(first, |p| match stop {
            Some(stop) => p.parse_subexpr(Postgres::default().prec_value(stop)),
            None => p.parse_expr(),
        })?;
        (canonical_expr(&parsed) == canonical_expr(expr)).then_some(last)
    }

    /// The first token of the clause `expr` follows `keyword` in: `WHERE`, `ON`, `HAVING`. It is
    /// found walking back from the first token of `expr`'s span over what can open an expression.
    pub(crate) fn clause(&self, expr: &Expr, keyword: Keyword) -> Option<usize> {
        let mut at = self.first_token(expr)?;
        loop {
            let before = self.before(at)?;
            if self.is(before, keyword) {
                return Some(at);
            }
            match &self.tokens[before].token {
                Token::LParen | Token::LBracket | Token::Minus | Token::Plus | Token::Tilde => {}
                Token::Word(w) if w.quote_style.is_none() => {}
                _ => return None,
            }
            at = before;
        }
    }

    /// The first and last tokens of the table factor `factor`.
    pub(crate) fn factor(&self, factor: &TableFactor) -> Option<(usize, usize)> {
        let mut at = self.first_token(factor)?;
        loop {
            if let Some((parsed, last)) = self.parse_at(at, |p| p.parse_table_factor()) {
                if parsed == *factor {
                    return Some((at, last));
                }
            }
            let before = self.before(at)?;
            match &self.tokens[before].token {
                Token::LParen => {}
                Token::Word(w) if w.keyword == Keyword::LATERAL || w.keyword == Keyword::ONLY => {}
                _ => return None,
            }
            at = before;
        }
    }
}

/// A slice of a template as the parser reads it, its directives stood in again, parsed whole by
/// `parse`.
pub(crate) fn reparse<T>(
    slice: &str,
    parse: impl FnOnce(&mut Parser) -> Result<T, ParserError>,
) -> Option<T> {
    let stood = Stood::new(slice).ok()?;
    let tokens = tokens(&stood.parsed).ok()?;
    let dialect = Postgres::default();
    let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens);
    let parsed = parse(&mut parser).ok()?;
    (parser.peek_token().token == Token::EOF).then_some(parsed)
}

/// Whether `token` is whitespace or a comment.
fn blank(token: &Token) -> bool {
    matches!(token, Token::Whitespace(_))
}

/// Whether `token` is a run of spaces, tabs and newlines.
pub(crate) fn plain_space(token: &Token) -> bool {
    matches!(
        token,
        Token::Whitespace(Whitespace::Space | Whitespace::Newline | Whitespace::Tab)
    )
}

/// The tokens of `text`, as the parser reads them.
fn tokens(text: &str) -> Result<Vec<TokenWithSpan>, String> {
    Tokenizer::new(&Postgres::default(), text)
        .with_unescape(true)
        .tokenize_with_location()
        .map_err(|error| error.to_string())
}

/// `statement` with every parenthesis that groups an expression left out and every chain of `AND`s
/// and of `OR`s grouped from the left, so two statements that differ only there compare equal.
pub(crate) fn canonical(statement: &Statement) -> Statement {
    let mut statement = statement.clone();
    let _ = statement.visit(&mut Ungrouped);
    statement
}

/// `expr` with every parenthesis that groups an expression left out.
pub(crate) fn canonical_expr(expr: &Expr) -> Expr {
    let mut expr = expr.clone();
    let _ = expr.visit(&mut Ungrouped);
    expr
}

/// The operands of the chain of `op`s `expr` is, left to right, pushed onto `operands`.
fn chain(expr: Expr, op: &BinaryOperator, operands: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp { left, op: o, right } if o == *op => {
            chain(*left, op, operands);
            chain(*right, op, operands);
        }
        other => operands.push(other),
    }
}

/// Leaves out every parenthesis that groups an expression, and groups every chain of `AND`s and
/// every chain of `OR`s from the left.
struct Ungrouped;

impl VisitorMut for Ungrouped {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::Nested(inner) = expr {
            *expr = std::mem::replace(inner.as_mut(), Expr::value(sqlparser::ast::Value::Null));
        }
        if let Expr::BinaryOp {
            op: op @ (BinaryOperator::And | BinaryOperator::Or),
            ..
        } = expr
        {
            let op = op.clone();
            let mut operands = Vec::new();
            chain(
                std::mem::replace(expr, Expr::value(sqlparser::ast::Value::Null)),
                &op,
                &mut operands,
            );
            *expr = operands
                .into_iter()
                .reduce(|left, right| Expr::BinaryOp {
                    left: Box::new(left),
                    op: op.clone(),
                    right: Box::new(right),
                })
                .expect("a chain has operands");
        }
        ControlFlow::Continue(())
    }
}
