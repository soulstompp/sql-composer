//! Where SQL text stands, as one database reads it at its default settings: in SQL itself, or
//! inside a quoted span, a comment, a dollar quote or a psql meta-command. The template parser and
//! the composer read text through the same [`Scanner`], so they never disagree on where a quote
//! ends.

/// How one database, at its default settings, begins and ends its quoted spans and comments.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Regime {
    /// The spans a quote opens, each with the byte that closes it.
    quotes: &'static [Quote],
    /// `E'…'` opens a string in which a backslash takes the byte after it.
    escape_strings: bool,
    /// `#` begins a comment to the end of the line.
    hash_comments: bool,
    /// `--` begins a comment only when a space, a tab or another control byte follows it.
    spaced_dash_comments: bool,
    /// A `/*` inside a block comment opens another, which needs its own `*/`.
    nested_comments: bool,
    /// `$tag$` opens a span that the same `$tag$` closes.
    dollar_quotes: bool,
}

/// A quoted span: the byte that opens it, the byte that closes it, and whether a backslash inside
/// takes the byte after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Quote {
    open: u8,
    close: u8,
    backslash: bool,
}

const STRING: Quote = Quote {
    open: b'\'',
    close: b'\'',
    backslash: false,
};
const ESCAPE_STRING: Quote = Quote {
    backslash: true,
    ..STRING
};
const DOUBLE_QUOTED: Quote = Quote {
    open: b'"',
    close: b'"',
    backslash: false,
};
const BACKTICK: Quote = Quote {
    open: b'`',
    close: b'`',
    backslash: false,
};
const BRACKET: Quote = Quote {
    open: b'[',
    close: b']',
    backslash: false,
};

/// PostgreSQL 18, with `standard_conforming_strings` on, and DuckDB 1.5: `'…'` strings, `E'…'`
/// strings with backslash escapes, `"…"` names, `--` and nested `/* */` comments, dollar quotes.
pub(crate) const POSTGRES: Regime = Regime {
    quotes: &[STRING, DOUBLE_QUOTED],
    escape_strings: true,
    hash_comments: false,
    spaced_dash_comments: false,
    nested_comments: true,
    dollar_quotes: true,
};

/// MySQL 9.7, with neither `ANSI_QUOTES` nor `NO_BACKSLASH_ESCAPES`: `'…'` and `"…"` strings with
/// backslash escapes, `` `…` `` names, `#`, `-- ` and flat `/* */` comments, and the dollar quotes
/// a stored program's body takes.
pub(crate) const MYSQL: Regime = Regime {
    quotes: &[
        ESCAPE_STRING,
        Quote {
            backslash: true,
            ..DOUBLE_QUOTED
        },
        BACKTICK,
    ],
    escape_strings: false,
    hash_comments: true,
    spaced_dash_comments: true,
    nested_comments: false,
    dollar_quotes: true,
};

/// MariaDB 12.3: MySQL's, without dollar quotes, `$` being a letter of a name.
pub(crate) const MARIADB: Regime = Regime {
    dollar_quotes: false,
    ..MYSQL
};

/// SQLite 3.53: `'…'` strings, `"…"`, `` `…` `` and `[…]` names, `--` and flat `/* */` comments.
/// `$` begins a parameter's name.
pub(crate) const SQLITE: Regime = Regime {
    quotes: &[STRING, DOUBLE_QUOTED, BACKTICK, BRACKET],
    escape_strings: false,
    hash_comments: false,
    spaced_dash_comments: false,
    nested_comments: false,
    dollar_quotes: false,
};

/// Where the scanner stands after the bytes it has read.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Scan {
    Code,
    /// A `-` at this offset, which a second `-` makes a comment.
    Dash(usize),
    /// `--` from this offset, where a comment needs a space after it: the next byte decides.
    DashDash(usize),
    /// A `/` at this offset, which a `*` makes a comment.
    Slash(usize),
    /// A `$` at this offset and the tag after it, which a second `$` makes a dollar quote.
    Tag(usize, Vec<u8>),
    /// A quote just closed, which the same quote straight after it reopens: a doubled quote.
    Closed(Quote),
    Quoted(Quote),
    /// A backslash inside a span that reads them: the next byte is taken as it is.
    Escaped(Quote),
    LineComment,
    BlockComment {
        depth: u32,
        last: u8,
    },
    Dollar {
        tag: Vec<u8>,
        matched: usize,
    },
    MetaCommand,
}

/// Where the next byte stands, as far as a template's directives and `#` comments are concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stand {
    /// In SQL itself.
    Code,
    /// Inside a dollar quote: a function's body, a `DO` block.
    Dollar,
    /// Inside a quoted span, a comment or a psql meta-command.
    Inside,
}

/// Reads SQL text one byte at a time, as one [`Regime`] reads it.
#[derive(Debug, Clone)]
pub(crate) struct Scanner {
    regime: &'static Regime,
    scan: Scan,
    /// The last byte of SQL itself, and how many bytes of a name end there.
    prev: u8,
    word: usize,
}

impl Scanner {
    pub(crate) fn new(regime: &'static Regime) -> Self {
        Scanner {
            regime,
            scan: Scan::Code,
            prev: 0,
            word: 0,
        }
    }

    /// Whether this scanner reads as `regime` does.
    pub(crate) fn reads(&self, regime: &Regime) -> bool {
        self.regime == regime
    }

    /// The offsets just past each `;` in `text` that ends a statement.
    pub(crate) fn ends(&mut self, text: &str) -> Vec<usize> {
        let mut ends = Vec::new();
        self.walk(text, |i, byte| {
            if byte == b';' {
                ends.push(i + 1);
            }
        });
        ends
    }

    /// Walk `text`, calling `code` with the offset and value of each byte that is SQL itself:
    /// outside quotes, comments, dollar quotes and psql meta-commands, and opening none of them.
    /// The end of `text` is a break, as a directive is: what waited on the next byte is SQL.
    pub(crate) fn walk(&mut self, text: &str, mut code: impl FnMut(usize, u8)) {
        for (at, &byte) in text.as_bytes().iter().enumerate() {
            self.step(at, byte, &mut code);
        }
        self.settle(&mut code);
    }

    /// Read `plain`, bytes none of which opens anything (see [`plain`]), where the scanner stands
    /// in SQL itself, and say so; read nothing, and say so, where it does not.
    pub(crate) fn pass(&mut self, plain: &[u8]) -> bool {
        if !matches!(self.scan, Scan::Code) || plain.is_empty() {
            return false;
        }
        let name = plain.iter().rev().take_while(|b| is_name_byte(**b)).count();
        self.word = if name == plain.len() {
            self.word + name
        } else {
            name
        };
        self.prev = plain[plain.len() - 1];
        true
    }

    /// Read `bytes`, which start at offset `at`, as [`Scanner::step`] does one at a time, without
    /// a word of which are SQL: each run that leaves the scanner where it stands is passed over
    /// whole.
    pub(crate) fn skim(&mut self, at: usize, bytes: &[u8]) {
        let mut i = 0;
        while i < bytes.len() {
            let rest = &bytes[i..];
            let run = match self.scan {
                Scan::Code => rest.iter().position(|&b| OPENS[usize::from(b)]),
                Scan::Quoted(quote) => rest
                    .iter()
                    .position(|&b| b == quote.close || (quote.backslash && b == b'\\')),
                Scan::LineComment | Scan::MetaCommand => rest.iter().position(|&b| b == b'\n'),
                Scan::BlockComment { .. } => rest.iter().position(|&b| b == b'*' || b == b'/'),
                _ => Some(0),
            }
            .unwrap_or(rest.len());
            if run == 0 {
                self.step(at + i, bytes[i], &mut |_, _| {});
                i += 1;
                continue;
            }
            let passed = &rest[..run];
            match &mut self.scan {
                Scan::Code => {
                    let name = passed
                        .iter()
                        .rev()
                        .take_while(|b| is_name_byte(**b))
                        .count();
                    self.word = if name == run { self.word + name } else { name };
                    self.prev = passed[run - 1];
                }
                Scan::BlockComment { last, .. } => *last = passed[run - 1],
                _ => {}
            }
            i += run;
        }
    }

    /// Where the bytes read so far leave the next one.
    pub(crate) fn stands(&self) -> Stand {
        match self.scan {
            Scan::Code
            | Scan::Dash(_)
            | Scan::DashDash(_)
            | Scan::Slash(_)
            | Scan::Tag(..)
            | Scan::Closed(_) => Stand::Code,
            Scan::Dollar { .. } => Stand::Dollar,
            _ => Stand::Inside,
        }
    }

    /// Where the bytes read so far leave the next one, in words.
    pub(crate) fn reading(&self) -> &'static str {
        match self.scan {
            Scan::Quoted(quote) | Scan::Escaped(quote) => match quote.open {
                b'\'' => "inside a string",
                b'"' if quote.backslash => "inside a string",
                _ => "inside a quoted name",
            },
            Scan::LineComment | Scan::BlockComment { .. } => "inside a comment",
            Scan::Dollar { .. } => "inside a dollar quote",
            Scan::MetaCommand => "on a psql meta-command's line",
            _ => "in SQL",
        }
    }

    /// A break, at the end of a text or at a directive: the bytes that waited on the next one are
    /// SQL itself.
    pub(crate) fn settle(&mut self, code: &mut impl FnMut(usize, u8)) {
        self.flush(code);
        match &mut self.scan {
            Scan::BlockComment { last, .. } => *last = 0,
            Scan::Dollar { matched, .. } => *matched = 0,
            _ => {}
        }
        self.prev = 0;
        self.word = 0;
    }

    /// Read the byte at offset `at`, calling `code` for each byte that is now known to be SQL.
    pub(crate) fn step(&mut self, at: usize, byte: u8, code: &mut impl FnMut(usize, u8)) {
        // Most bytes leave the scanner where it stands.
        match self.scan {
            Scan::Code if !OPENS[usize::from(byte)] => {
                return self.code_byte(at, byte, code);
            }
            Scan::Quoted(quote) if byte != quote.close && byte != b'\\' => return,
            Scan::LineComment | Scan::MetaCommand if byte != b'\n' => return,
            Scan::BlockComment { ref mut last, .. } if !matches!(byte, b'*' | b'/') => {
                *last = byte;
                return;
            }
            _ => {}
        }
        let regime = self.regime;
        self.scan = match std::mem::replace(&mut self.scan, Scan::Code) {
            Scan::Code => return self.open(at, byte, code),
            Scan::Dash(start) if byte == b'-' => {
                if regime.spaced_dash_comments {
                    Scan::DashDash(start)
                } else {
                    Scan::LineComment
                }
            }
            Scan::DashDash(_) if byte == b'\n' => Scan::Code,
            Scan::DashDash(_) if byte == b' ' || byte.is_ascii_control() => Scan::LineComment,
            Scan::Slash(_) if byte == b'*' => Scan::BlockComment { depth: 1, last: 0 },
            Scan::Tag(start, mut tag) if is_tag_byte(byte, tag.len() == 1) => {
                tag.push(byte);
                Scan::Tag(start, tag)
            }
            Scan::Tag(_, mut tag) if byte == b'$' => {
                tag.push(byte);
                Scan::Dollar { tag, matched: 0 }
            }
            Scan::Closed(quote) if quote.open == quote.close && byte == quote.close => {
                Scan::Quoted(quote)
            }
            waiting @ (Scan::Dash(_)
            | Scan::DashDash(_)
            | Scan::Slash(_)
            | Scan::Tag(..)
            | Scan::Closed(_)) => {
                self.scan = waiting;
                self.flush(code);
                return self.open(at, byte, code);
            }
            Scan::Quoted(quote) if quote.backslash && byte == b'\\' => Scan::Escaped(quote),
            Scan::Quoted(quote) if byte == quote.close => Scan::Closed(quote),
            Scan::Escaped(quote) => Scan::Quoted(quote),
            Scan::LineComment | Scan::MetaCommand if byte == b'\n' => Scan::Code,
            Scan::BlockComment { depth, last: b'*' } if byte == b'/' => {
                if depth == 1 {
                    Scan::Code
                } else {
                    Scan::BlockComment {
                        depth: depth - 1,
                        last: 0,
                    }
                }
            }
            Scan::BlockComment { depth, last: b'/' } if byte == b'*' && regime.nested_comments => {
                Scan::BlockComment {
                    depth: depth + 1,
                    last: 0,
                }
            }
            Scan::BlockComment { depth, .. } => Scan::BlockComment { depth, last: byte },
            Scan::Dollar { tag, matched } if byte == tag[matched] => {
                if matched + 1 == tag.len() {
                    Scan::Code
                } else {
                    Scan::Dollar {
                        tag,
                        matched: matched + 1,
                    }
                }
            }
            Scan::Dollar { tag, .. } => Scan::Dollar {
                tag,
                matched: usize::from(byte == b'$'),
            },
            other => other,
        };
        if self.stands() != Stand::Code {
            self.prev = 0;
            self.word = 0;
        }
    }

    /// Read `byte` in SQL itself: the start of a span, a byte that waits on the next, or SQL.
    fn open(&mut self, at: usize, byte: u8, code: &mut impl FnMut(usize, u8)) {
        let regime = self.regime;
        self.scan = match byte {
            b'-' => Scan::Dash(at),
            b'/' => Scan::Slash(at),
            b'$' if regime.dollar_quotes && self.word == 0 => Scan::Tag(at, vec![b'$']),
            b'#' if regime.hash_comments => Scan::LineComment,
            b'\\' => Scan::MetaCommand,
            _ => match regime.quotes.iter().find(|quote| quote.open == byte) {
                Some(&quote) => {
                    let escape = quote == STRING
                        && regime.escape_strings
                        && self.word == 1
                        && matches!(self.prev, b'E' | b'e');
                    Scan::Quoted(if escape { ESCAPE_STRING } else { quote })
                }
                None => {
                    self.code_byte(at, byte, code);
                    Scan::Code
                }
            },
        };
        if self.stands() != Stand::Code {
            self.prev = 0;
            self.word = 0;
        }
    }

    /// The bytes that waited on the next one, given to `code` as SQL itself.
    fn flush(&mut self, code: &mut impl FnMut(usize, u8)) {
        let (start, bytes) = match std::mem::replace(&mut self.scan, Scan::Code) {
            Scan::Dash(start) => (start, vec![b'-']),
            Scan::DashDash(start) => (start, vec![b'-', b'-']),
            Scan::Slash(start) => (start, vec![b'/']),
            Scan::Tag(start, tag) => (start, tag),
            Scan::Closed(_) => return,
            other => {
                self.scan = other;
                return;
            }
        };
        for (i, byte) in bytes.into_iter().enumerate() {
            self.code_byte(start + i, byte, code);
        }
    }

    fn code_byte(&mut self, at: usize, byte: u8, code: &mut impl FnMut(usize, u8)) {
        self.word = if is_name_byte(byte) { self.word + 1 } else { 0 };
        self.prev = byte;
        code(at, byte);
    }
}

/// How many bytes at the start of `bytes` open nothing in SQL itself, in any regime.
pub(crate) fn plain(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .position(|&b| OPENS[usize::from(b)])
        .unwrap_or(bytes.len())
}

/// The bytes that can open something in SQL itself, in some regime: a quote, a comment, a dollar
/// quote or a psql meta-command.
const OPENS: [bool; 256] = {
    let mut opens = [false; 256];
    let bytes = b"-/$#\\'\"`[";
    let mut i = 0;
    while i < bytes.len() {
        opens[bytes[i] as usize] = true;
        i += 1;
    }
    opens
};

/// A byte that can stand inside an unquoted name, in every regime here.
fn is_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte >= 0x80
}

/// A byte of a dollar quote's tag; the first cannot be a digit, since `$1` is a parameter.
fn is_tag_byte(byte: u8, first: bool) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80 || (!first && byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ends(regime: &'static Regime, text: &str) -> usize {
        Scanner::new(regime).ends(text).len()
    }

    #[test]
    fn test_a_statement_ends_only_outside_quotes_comments_and_dollar_quotes() {
        let text =
            "SELECT ';', \";\" -- ;\n/* ; /* ; */ ; */ $$;$$, $q$;$q$, $1;\n\\echo ;\nSELECT 2;";
        let ends = Scanner::new(&POSTGRES).ends(text);
        assert_eq!(ends.len(), 2, "{ends:?}");
        assert!(text[..ends[0]].ends_with("$q$;$q$, $1;"));
        assert_eq!(ends[1], text.len());
    }

    #[test]
    fn test_an_escape_string_closes_past_its_escaped_quote() {
        let text = "SELECT E'it\\'s'; SELECT 2;";
        assert_eq!(ends(&POSTGRES, text), 2);
        assert_eq!(ends(&MYSQL, text), 2);
        assert_eq!(ends(&SQLITE, text), 0);
        assert_eq!(ends(&POSTGRES, "SELECT xE'a\\'; SELECT 2;"), 2);
        assert_eq!(ends(&POSTGRES, "SELECT E'a''\\'; SELECT 2;'; SELECT 3;"), 2);
    }

    #[test]
    fn test_each_regime_reads_its_own_quotes() {
        let double = "SELECT \"a\\\";b\"; SELECT 2;";
        assert_eq!(ends(&MYSQL, double), 2);
        assert_eq!(ends(&MARIADB, double), 2);
        assert_eq!(ends(&POSTGRES, double), 1);
        let backtick = "SELECT `a;b`; SELECT 2;";
        assert_eq!(ends(&MYSQL, backtick), 2);
        assert_eq!(ends(&SQLITE, backtick), 2);
        assert_eq!(ends(&POSTGRES, backtick), 3);
        let bracket = "SELECT [a;b] FROM t; SELECT 2;";
        assert_eq!(ends(&SQLITE, bracket), 2);
        assert_eq!(ends(&POSTGRES, bracket), 3);
        assert_eq!(ends(&SQLITE, "SELECT 'it''s;'; SELECT [a]]; SELECT 2;"), 3);
    }

    #[test]
    fn test_each_regime_reads_its_own_comments() {
        let hash = "SELECT 1 # x;\nSELECT 2;";
        assert_eq!(ends(&MYSQL, hash), 1);
        assert_eq!(ends(&POSTGRES, hash), 2);
        let unspaced = "SELECT 1 --x;\nSELECT 2;";
        assert_eq!(ends(&MYSQL, unspaced), 2);
        assert_eq!(ends(&POSTGRES, unspaced), 1);
        assert_eq!(ends(&SQLITE, unspaced), 1);
        assert_eq!(ends(&MYSQL, "SELECT 1 --\tx;\nSELECT 2;"), 1);
        assert_eq!(ends(&MYSQL, "SELECT 1 --\nSELECT 2;"), 1);
        let nested = "SELECT 1 /* a /* b */ ; */; SELECT 2;";
        assert_eq!(ends(&POSTGRES, nested), 2);
        assert_eq!(ends(&MYSQL, nested), 3);
        assert_eq!(ends(&SQLITE, nested), 3);
    }

    #[test]
    fn test_each_regime_reads_its_own_dollar_quotes() {
        let text = "SELECT $$a;b$$; SELECT $t$;$$;$t$;";
        assert_eq!(ends(&POSTGRES, text), 2);
        assert_eq!(ends(&MYSQL, text), 2);
        assert_eq!(ends(&MARIADB, text), 5);
        assert_eq!(ends(&SQLITE, text), 5);
        assert_eq!(ends(&POSTGRES, "SELECT a$b$; SELECT 2;"), 2);
    }

    #[test]
    fn test_the_end_of_a_text_is_a_break() {
        let mut scanner = Scanner::new(&POSTGRES);
        assert_eq!(scanner.ends("SELECT 1 -").len(), 0);
        assert_eq!(scanner.ends("- 2;").len(), 1);
        assert_eq!(ends(&POSTGRES, "SELECT 1 -- 2;"), 0);
    }

    #[test]
    fn test_where_the_next_byte_stands() {
        let stands = |regime: &'static Regime, text: &str| {
            let mut scanner = Scanner::new(regime);
            for (at, &byte) in text.as_bytes().iter().enumerate() {
                scanner.step(at, byte, &mut |_, _| {});
            }
            scanner.stands()
        };
        assert_eq!(stands(&POSTGRES, "SELECT -"), Stand::Code);
        assert_eq!(stands(&POSTGRES, "SELECT 'a'"), Stand::Code);
        assert_eq!(stands(&POSTGRES, "SELECT '"), Stand::Inside);
        assert_eq!(stands(&POSTGRES, "/* "), Stand::Inside);
        assert_eq!(stands(&POSTGRES, "\\set x "), Stand::Inside);
        assert_eq!(stands(&POSTGRES, "DO $$ "), Stand::Dollar);
        assert_eq!(stands(&MYSQL, "DO $$ "), Stand::Dollar);
        assert_eq!(stands(&MARIADB, "DO $$ "), Stand::Code);
        assert_eq!(stands(&SQLITE, "DO $$ "), Stand::Code);
    }

    #[test]
    fn test_where_the_next_byte_stands_in_words() {
        let reading = |regime: &'static Regime, text: &str| {
            let mut scanner = Scanner::new(regime);
            for (at, &byte) in text.as_bytes().iter().enumerate() {
                scanner.step(at, byte, &mut |_, _| {});
            }
            scanner.reading()
        };
        assert_eq!(reading(&POSTGRES, "SELECT -"), "in SQL");
        assert_eq!(reading(&POSTGRES, "SELECT '"), "inside a string");
        assert_eq!(reading(&POSTGRES, "SELECT \""), "inside a quoted name");
        assert_eq!(reading(&MYSQL, "SELECT \""), "inside a string");
        assert_eq!(reading(&SQLITE, "SELECT ["), "inside a quoted name");
        assert_eq!(reading(&POSTGRES, "DO $$ "), "inside a dollar quote");
        assert_eq!(reading(&MYSQL, "SELECT 1 # "), "inside a comment");
        assert_eq!(
            reading(&POSTGRES, "\\set x "),
            "on a psql meta-command's line"
        );
    }

    #[test]
    fn test_only_sql_itself_is_code() {
        let mut code = Vec::new();
        Scanner::new(&POSTGRES).walk("SELECT 'a', -1 -- b\n/c/* d */$x", |_, byte| {
            code.push(byte)
        });
        assert_eq!(String::from_utf8(code).unwrap(), "SELECT , -1 /c$x");
    }

    #[test]
    fn test_skimming_lands_where_stepping_does() {
        let text = "SELECT E'it\\'s', xE'a''b', \"a\\\"b\", `c#d`, [e;f], $1, a$b$, $$g;$$, \
                    $t$h$$i$t$ -- j\n--k\n/* l /* m */ n */ # o\n\\echo p\nSELECT 2 -/ 3;";
        for regime in [&POSTGRES, &MYSQL, &MARIADB, &SQLITE] {
            for cut in 0..=text.len() {
                let mut stepped = Scanner::new(regime);
                for (at, &byte) in text.as_bytes()[..cut].iter().enumerate() {
                    stepped.step(at, byte, &mut |_, _| {});
                }
                let mut skimmed = Scanner::new(regime);
                skimmed.skim(0, &text.as_bytes()[..cut]);
                assert_eq!(
                    (&skimmed.scan, skimmed.prev, skimmed.word),
                    (&stepped.scan, stepped.prev, stepped.word),
                    "{regime:?} at {cut}: {:?}",
                    &text[..cut]
                );
            }
        }
    }
}
