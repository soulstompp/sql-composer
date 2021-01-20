//! Span, GeneratedSpan and ParsedSpan

use std::convert::From;
use std::fmt;

use crate::types::SqlCompositionAlias;

pub use nom_locate::LocatedSpan;
pub type Span<'a> = LocatedSpan<&'a str>;

/// GeneratedSpan is for ...
#[derive(Debug, Hash, Eq, PartialEq, Default, Clone)]
pub struct GeneratedSpan {
    pub command: Option<String>,
}

impl From<Option<String>> for GeneratedSpan {
    fn from(os: Option<String>) -> Self {
        Self { command: os }
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct TextLocation {
    line:   u32,
    offset: usize,
}

impl Default for TextLocation {
    fn default() -> Self {
        Self {
            line:   1,
            offset: 0,
        }
    }
}

impl<'a> From<Span<'a>> for TextLocation {
    fn from(s: Span) -> Self {
        TextLocation {
            line:   s.line,
            offset: s.offset,
        }
    }
}

impl<'a> From<&Span<'a>> for TextLocation {
    fn from(s: &Span) -> Self {
        TextLocation {
            line:   s.line,
            offset: s.offset,
        }
    }
}

impl From<(u32, usize)> for TextLocation {
    fn from(v: (u32, usize)) -> Self {
        TextLocation {
            line:   v.0,
            offset: v.1,
        }
    }
}

/// ParsedSpan is for ...
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct ParsedSpan {
    pub alias: Option<SqlCompositionAlias>,
    pub start: TextLocation,
    pub end:   TextLocation,
}

/// line number should default to 1
impl Default for ParsedSpan {
    fn default() -> Self {
        Self {
            alias: None,
            start: Default::default(),
            end:   Default::default(),
        }
    }
}

impl ParsedSpan {
    pub fn new(start_span: Span, end_span: Span, alias: Option<SqlCompositionAlias>) -> Self {
        Self {
            alias,
            start: start_span.into(),
            end: end_span.into(),
        }
    }

    pub fn from_span(span: Span) -> Self {
        Self {
            start: span.into(),
            end: span.into(),
            ..Default::default()
        }
    }
}

// explicit lifetime for Span is required: Span<'a>
// because "implicit elided lifetime is not allowed here"
impl<'a> From<Span<'a>> for ParsedSpan {
    fn from(span: Span) -> Self {
        Self {
            start: span.into(),
            end: span.into(),
            ..Default::default()
        }
    }
}

impl fmt::Display for ParsedSpan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "from ({}, {}) to ({}, {})",
            self.start.line, self.start.offset, self.end.line, self.end.offset
        )?;

        if let Some(a) = &self.alias {
            write!(f, " of {}", a)?;
        }

        Ok(())
    }
}
