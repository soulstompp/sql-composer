use std::fmt;

use crate::types::{Position, SqlComposition, SqlCompositionAlias};

use nom_locate::LocatedSpan;

type Span<'a> = LocatedSpan<&'a str>;

error_chain! {
    errors {
        AliasConflict(t: String) {
            description("a new alias would conflicts with an already defined alias")
                display("alias used more than once: '{}'", t)
        }
        CompositionIncomplete(t: String) {
            description("no terminating character found")
                display("expected termination character, none found")
        }
    }

    foreign_links {
        Utf8(std::str::Utf8Error);
        Io(std::io::Error);
    }
}
