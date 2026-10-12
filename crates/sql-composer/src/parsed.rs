//! SQL for PostgreSQL as sqlparser reads it, shared by what reads a statement's own text: the
//! dialect, what the parser is spared, where each node stands in the text, and edits made where
//! it stands, every other byte left as written.

use std::any::TypeId;

use sqlparser::ast::Ident;
#[cfg(feature = "respell")]
use sqlparser::ast::{Spanned, TableFactor};
use sqlparser::dialect::{Dialect, PostgreSqlDialect, Precedence};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
#[cfg(feature = "respell")]
use sqlparser::tokenizer::{Location, Span};

use crate::scan::{Scanner, POSTGRES};

/// PostgreSQL as sqlparser reads it, with one thing more PostgreSQL reads: two string literals
/// with a newline between them are one.
#[derive(Debug, Default)]
pub(crate) struct Postgres(PostgreSqlDialect);

impl Dialect for Postgres {
    fn dialect(&self) -> TypeId {
        TypeId::of::<PostgreSqlDialect>()
    }

    fn supports_string_literal_concatenation_with_newline(&self) -> bool {
        true
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.0.identifier_quote_style(identifier)
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        self.0.is_delimited_identifier_start(ch)
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        self.0.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.0.is_identifier_part(ch)
    }

    fn supports_unicode_string_literal(&self) -> bool {
        self.0.supports_unicode_string_literal()
    }

    fn is_reserved_for_identifier(&self, kw: Keyword) -> bool {
        self.0.is_reserved_for_identifier(kw)
    }

    fn is_table_alias(&self, kw: &Keyword, parser: &mut Parser) -> bool {
        self.0.is_table_alias(kw, parser)
    }

    fn is_custom_operator_part(&self, ch: char) -> bool {
        self.0.is_custom_operator_part(ch)
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        self.0.get_next_precedence(parser)
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        self.0.supports_filter_during_aggregation()
    }

    fn supports_group_by_expr(&self) -> bool {
        self.0.supports_group_by_expr()
    }

    fn supports_alter_user_as_alter_role(&self) -> bool {
        self.0.supports_alter_user_as_alter_role()
    }

    fn prec_value(&self, prec: Precedence) -> u8 {
        self.0.prec_value(prec)
    }

    fn allow_extract_custom(&self) -> bool {
        self.0.allow_extract_custom()
    }

    fn allow_extract_single_quotes(&self) -> bool {
        self.0.allow_extract_single_quotes()
    }

    fn supports_create_index_with_clause(&self) -> bool {
        self.0.supports_create_index_with_clause()
    }

    fn supports_explain_with_utility_options(&self) -> bool {
        self.0.supports_explain_with_utility_options()
    }

    fn supports_listen_notify(&self) -> bool {
        self.0.supports_listen_notify()
    }

    fn supports_exclude_constraint(&self) -> bool {
        self.0.supports_exclude_constraint()
    }

    fn supports_factorial_operator(&self) -> bool {
        self.0.supports_factorial_operator()
    }

    fn supports_bitwise_shift_operators(&self) -> bool {
        self.0.supports_bitwise_shift_operators()
    }

    fn supports_comment_on(&self) -> bool {
        self.0.supports_comment_on()
    }

    fn supports_load_extension(&self) -> bool {
        self.0.supports_load_extension()
    }

    fn supports_named_fn_args_with_colon_operator(&self) -> bool {
        self.0.supports_named_fn_args_with_colon_operator()
    }

    fn supports_named_fn_args_with_expr_name(&self) -> bool {
        self.0.supports_named_fn_args_with_expr_name()
    }

    fn supports_empty_projections(&self) -> bool {
        self.0.supports_empty_projections()
    }

    fn supports_nested_comments(&self) -> bool {
        self.0.supports_nested_comments()
    }

    fn supports_string_escape_constant(&self) -> bool {
        self.0.supports_string_escape_constant()
    }

    fn supports_numeric_literal_underscores(&self) -> bool {
        self.0.supports_numeric_literal_underscores()
    }

    fn supports_array_typedef_with_brackets(&self) -> bool {
        self.0.supports_array_typedef_with_brackets()
    }

    fn supports_geometric_types(&self) -> bool {
        self.0.supports_geometric_types()
    }

    fn supports_order_by_using_operator(&self) -> bool {
        self.0.supports_order_by_using_operator()
    }

    fn supports_set_names(&self) -> bool {
        self.0.supports_set_names()
    }

    fn supports_alter_column_type_using(&self) -> bool {
        self.0.supports_alter_column_type_using()
    }

    fn supports_left_associative_joins_without_parens(&self) -> bool {
        self.0.supports_left_associative_joins_without_parens()
    }

    fn supports_notnull_operator(&self) -> bool {
        self.0.supports_notnull_operator()
    }

    fn supports_interval_options(&self) -> bool {
        self.0.supports_interval_options()
    }

    fn supports_insert_table_alias(&self) -> bool {
        self.0.supports_insert_table_alias()
    }

    fn supports_create_table_like_parenthesized(&self) -> bool {
        self.0.supports_create_table_like_parenthesized()
    }

    fn supports_select_wildcard_with_alias(&self) -> bool {
        self.0.supports_select_wildcard_with_alias()
    }

    fn supports_comma_separated_trim(&self) -> bool {
        self.0.supports_comma_separated_trim()
    }

    fn supports_xml_expressions(&self) -> bool {
        self.0.supports_xml_expressions()
    }

    fn supports_aliased_function_args(&self) -> bool {
        self.0.supports_aliased_function_args()
    }

    fn supports_comment_optimizer_hint(&self) -> bool {
        self.0.supports_comment_optimizer_hint()
    }
}

/// Where the first byte of SQL itself in `piece` stands: past whitespace, comments and psql
/// meta-commands.
pub(crate) fn first_code(piece: &str) -> Option<usize> {
    let mut first = None;
    Scanner::new(&POSTGRES).walk(piece, |at, byte| {
        if first.is_none() && !byte.is_ascii_whitespace() {
            first = Some(at);
        }
    });
    first
}

/// A name as PostgreSQL reads it: folded to lower case unless quoted.
pub(crate) fn normal(ident: &Ident) -> String {
    match ident.quote_style {
        Some(_) => ident.value.clone(),
        None => ident.value.to_lowercase(),
    }
}

/// Whether `b` is a byte of a word.
pub(crate) fn word(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

/// Each word and each mark of SQL itself in `text`, outside quotes and comments, by where it starts
/// and ends.
pub(crate) fn code_tokens(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut tokens: Vec<(usize, usize)> = Vec::new();
    Scanner::new(&POSTGRES).walk(text, |at, byte| {
        if byte.is_ascii_whitespace() {
            return;
        }
        match tokens.last_mut() {
            Some((start, end)) if *end == at && word(byte) && word(bytes[*start]) => *end = at + 1,
            _ => tokens.push((at, at + 1)),
        }
    });
    tokens
}

/// `text` with each `SEARCH` and `CYCLE` clause of a recursive WITH query blanked byte for byte,
/// newlines kept, for a parser that does not read them; every other byte as it is.
pub(crate) fn masked(text: &str) -> String {
    let bytes = text.as_bytes();
    let tokens = code_tokens(text);
    let is = |i: usize, w: &str| {
        tokens
            .get(i)
            .is_some_and(|(s, e)| text[*s..*e].eq_ignore_ascii_case(w))
    };
    let name = |i: usize| tokens.get(i).is_some_and(|(s, _)| word(bytes[*s]));
    // the last token of the clause that opens at token `i`
    let clause_end = |i: usize| -> Option<usize> {
        let mut j = i + 1;
        if is(i, "search") {
            if !(is(j, "breadth") || is(j, "depth")) || !is(j + 1, "first") || !is(j + 2, "by") {
                return None;
            }
            j += 3;
        }
        name(j).then_some(())?;
        while is(j + 1, ",") && name(j + 2) {
            j += 2;
        }
        (is(j + 1, "set") && name(j + 2)).then_some(())?;
        if is(i, "search") {
            return Some(j + 2);
        }
        (j + 3..j + 12)
            .find(|&k| is(k, "using") && name(k + 1))
            .map(|k| k + 1)
    };
    let mut out = bytes.to_vec();
    let mut i = 1;
    while i < tokens.len() {
        if (is(i, "cycle") || is(i, "search")) && is(i - 1, ")") {
            if let Some(end) = clause_end(i) {
                for b in &mut out[tokens[i].0..tokens[end].1] {
                    if *b != b'\n' {
                        *b = b' ';
                    }
                }
                i = end + 1;
                continue;
            }
        }
        i += 1;
    }
    String::from_utf8(out).expect("only whole tokens are blanked")
}

/// A statement's text, read by line and column.
#[cfg(feature = "respell")]
pub(crate) struct Source<'t> {
    /// The text as written.
    pub(crate) text: &'t str,
    /// The text as the parser reads it, each byte where it is in `text`.
    pub(crate) parsed: &'t str,
    lines: Vec<usize>,
    /// Each word and mark of SQL itself.
    pub(crate) tokens: Vec<(usize, usize)>,
}

#[cfg(feature = "respell")]
impl<'t> Source<'t> {
    pub(crate) fn new(text: &'t str, parsed: &'t str) -> Self {
        let mut lines = vec![0];
        lines.extend(parsed.match_indices('\n').map(|(at, _)| at + 1));
        Source {
            text,
            parsed,
            lines,
            tokens: code_tokens(parsed),
        }
    }

    /// The byte offset of `location`.
    pub(crate) fn offset(&self, location: Location) -> Option<usize> {
        let start = *self
            .lines
            .get(usize::try_from(location.line).ok()?.checked_sub(1)?)?;
        let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
        let line = &self.parsed[start..];
        if column == 0 {
            return Some(start);
        }
        line.char_indices()
            .nth(column)
            .map(|(at, _)| start + at)
            .or_else(|| (line.chars().count() == column).then_some(self.parsed.len()))
    }

    /// The byte range `span` covers.
    pub(crate) fn range(&self, span: Span) -> Option<(usize, usize)> {
        if span == Span::empty() {
            return None;
        }
        let (start, end) = (self.offset(span.start)?, self.offset(span.end)?);
        (start <= end).then_some((start, end))
    }

    /// The byte range of a derived table's query, between its own parentheses.
    pub(crate) fn inner(&self, factor: &TableFactor) -> Option<(usize, usize)> {
        let TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } = factor
        else {
            return None;
        };
        let start = self.range(subquery.span())?.0;
        let named = self.offset(alias.name.span.start)?;
        let open = self.tokens.iter().rev().find(|(_, e)| *e <= start)?;
        let close = self.tokens.iter().rev().find(|(_, e)| *e <= named)?;
        (&self.parsed[open.0..open.1] == "(" && &self.parsed[close.0..close.1] == ")")
            .then_some((start, close.0))
    }

    /// The text `span` covers.
    pub(crate) fn text(&self, span: Span) -> Option<String> {
        let (start, end) = self.range(span)?;
        Some(self.text[start..end].to_string())
    }
}

/// Replacements in a statement's text, none overlapping another.
#[cfg(feature = "respell")]
#[derive(Clone, Default)]
pub(crate) struct Edits(pub(crate) Vec<(usize, usize, String)>);

#[cfg(feature = "respell")]
impl Edits {
    /// The text from `start` to `end` with the replacements inside it made; none where one
    /// crosses either end.
    pub(crate) fn render(&self, text: &str, start: usize, end: usize) -> Option<String> {
        let mut out = String::new();
        let mut at = start;
        for (s, e, replacement) in &self.0 {
            if *e <= start || *s >= end {
                continue;
            }
            if *s < start || *e > end {
                return None;
            }
            out.push_str(&text[at..*s]);
            out.push_str(replacement);
            at = *e;
        }
        out.push_str(&text[at..end]);
        Some(out)
    }

    /// Replace `start` to `end` with `replacement`, which takes the place of every replacement
    /// inside it; false where one crosses either end.
    pub(crate) fn add(&mut self, start: usize, end: usize, replacement: String) -> bool {
        if self
            .0
            .iter()
            .any(|(s, e, _)| (*s < start && *e > start) || (*s < end && *e > end))
        {
            return false;
        }
        self.0.retain(|(s, e, _)| !(start <= *s && *e <= end));
        let at = self.0.partition_point(|(s, _, _)| *s < start);
        self.0.insert(at, (start, end, replacement));
        true
    }
}
