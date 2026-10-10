//! The places in a template where an axis applies, each with the spellings it can take there,
//! and each spelling read back before it is offered.

use std::ops::ControlFlow;

use sqlparser::ast::{BinaryOperator, Expr, Select, Statement, VisitMut, VisitorMut};

use super::read::{canonical, Read};

/// Edits of a template's text: each byte range replaced, and what is written there.
pub(crate) type Edits = Vec<(usize, usize, String)>;

/// One place in a template where an axis applies.
#[derive(Debug, Clone)]
pub(crate) struct Place {
    /// The statement it is in, by its place among the template's statements from 0.
    pub(crate) statement: usize,
    /// What stands there, in words and the template's own text.
    pub(crate) what: String,
    /// Each spelling it can take there, the first as written.
    pub(crate) choices: Vec<Choice>,
}

/// One spelling of a place.
#[derive(Debug, Clone)]
pub(crate) struct Choice {
    /// The spelling, in words.
    pub(crate) name: String,
    /// The picks this spelling is, by their value in `spellings.toml`.
    pub(crate) picks: Vec<&'static str>,
    /// The template's bytes it replaces, each with what it writes there.
    pub(crate) edits: Edits,
    /// The statement the template must read as once it is written.
    pub(crate) expected: Statement,
}

impl Choice {
    /// The place as written: the picks it already is.
    pub(crate) fn written(
        name: impl Into<String>,
        picks: Vec<&'static str>,
        ast: &Statement,
    ) -> Self {
        Choice {
            name: name.into(),
            picks,
            edits: vec![],
            expected: ast.clone(),
        }
    }
}

/// `text` with `edits` made, none of which may overlap another; none where two do.
pub(crate) fn apply(text: &str, edits: &[(usize, usize, String)]) -> Option<String> {
    let mut edits: Vec<&(usize, usize, String)> = edits.iter().collect();
    edits.sort_by_key(|(start, end, _)| (*start, *end));
    let mut out = String::new();
    let mut at = 0;
    for (start, end, replacement) in edits {
        if *start < at || end < start {
            return None;
        }
        out.push_str(&text[at..*start]);
        out.push_str(replacement);
        at = *end;
    }
    out.push_str(&text[at..]);
    Some(out)
}

/// The template `read` holds with `choice` made at its place in statement `statement`, where it
/// reads back as the statement the choice expects and every other statement as it was.
pub(crate) fn made(read: &Read, statement: usize, choice: &Choice) -> Option<String> {
    let text = apply(&read.stood.text, &choice.edits)?;
    let again = Read::new(&text).ok()?;
    if again.statements.len() != read.statements.len() {
        return None;
    }
    for (at, (before, after)) in read.statements.iter().zip(&again.statements).enumerate() {
        match (before, after) {
            (Ok(_), Ok(after)) if at == statement => {
                if canonical(&after.ast) != canonical(&choice.expected) {
                    return None;
                }
            }
            (Ok(before), Ok(after)) => {
                if canonical(&after.ast) != canonical(&before.ast) {
                    return None;
                }
            }
            (Err(_), Err(_)) => {}
            _ => return None,
        }
    }
    Some(text)
}

/// Each choice of `place` that reads back as it expects, the first, as written, kept always.
pub(crate) fn checked(read: &Read, mut place: Place) -> Place {
    let statement = place.statement;
    let mut choices = place.choices.into_iter();
    let mut kept: Vec<Choice> = choices.next().into_iter().collect();
    kept.extend(choices.filter(|choice| made(read, statement, choice).is_some()));
    place.choices = kept;
    place
}

/// The conjuncts of `expr`: the operands of the `AND`s it is, left to right, a parenthesised one
/// whole.
pub(crate) fn conjuncts(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut all = conjuncts(left);
            all.push(right);
            all
        }
        other => vec![other],
    }
}

/// `conjuncts` joined by `AND`, left to right; none where there are none.
pub(crate) fn conjoined(conjuncts: Vec<Expr>) -> Option<Expr> {
    conjuncts.into_iter().reduce(|left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    })
}

/// Change the select `at`, by its place among the statement's selects from 0, with `change`.
pub(crate) fn change_select(
    statement: &mut Statement,
    at: usize,
    change: impl FnOnce(&mut Select),
) {
    struct Visit<F> {
        at: usize,
        seen: usize,
        change: Option<F>,
    }
    impl<F: FnOnce(&mut Select)> VisitorMut for Visit<F> {
        type Break = ();
        fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<()> {
            if self.seen == self.at {
                if let Some(change) = self.change.take() {
                    change(select);
                }
                return ControlFlow::Break(());
            }
            self.seen += 1;
            ControlFlow::Continue(())
        }
    }
    let _ = statement.visit(&mut Visit {
        at,
        seen: 0,
        change: Some(change),
    });
}

/// `text` on one line, its runs of whitespace one space each.
pub(crate) fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}
