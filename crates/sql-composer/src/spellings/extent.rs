//! Where the parts of a select stand among a statement's tokens: a condition's conjuncts, a
//! `FROM` item's relations and joins. Each part is read back from where it is found and compared
//! with its node, and a part that does not read back is not found.

use sqlparser::ast::{Expr, Join, JoinConstraint, TableWithJoins};
use sqlparser::dialect::Precedence;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Token, Whitespace};

use super::join_order::constraint;
use super::place::{conjuncts, Edits};
use super::read::{plain_space, Read, Stmt};

/// A condition after its keyword: `WHERE`, `ON`, `HAVING`.
#[derive(Debug, Clone)]
pub(crate) struct Chain {
    /// The keyword's token.
    pub(crate) keyword: usize,
    /// Each conjunct's first and last tokens, left to right.
    pub(crate) conjuncts: Vec<(usize, usize)>,
}

impl Chain {
    /// The last token of the condition.
    pub(crate) fn last(&self) -> usize {
        self.conjuncts.last().expect("a condition has a conjunct").1
    }
}

/// Where `expr`, a condition after `keyword`, stands.
pub(crate) fn chain(statement: &Stmt, expr: &Expr, keyword: Keyword) -> Option<Chain> {
    let first = statement.clause(expr, keyword)?;
    let keyword = statement.before(first)?;
    let all = conjuncts(expr);
    let mut at = first;
    let mut found = Vec::new();
    for (k, conjunct) in all.iter().enumerate() {
        let stop = (all.len() > 1).then_some(Precedence::And);
        let last = statement.expr_from(at, conjunct, stop)?;
        found.push((at, last));
        if k + 1 < all.len() {
            let and = statement.after(last)?;
            if !statement.is(and, Keyword::AND) {
                return None;
            }
            at = statement.after(and)?;
        }
    }
    Some(Chain {
        keyword,
        conjuncts: found,
    })
}

/// A join of a `FROM` item.
#[derive(Debug, Clone)]
pub(crate) struct JoinAt {
    /// The tokens of its operator: the first, and its `JOIN`.
    pub(crate) operator: (usize, usize),
    /// The first and last tokens of the relation it joins.
    pub(crate) factor: (usize, usize),
    /// Its `ON` condition, where it has one.
    pub(crate) on: Option<Chain>,
    /// Its last token.
    pub(crate) last: usize,
}

/// A `FROM` item: its first relation and its joins.
#[derive(Debug, Clone)]
pub(crate) struct ItemAt {
    /// The first and last tokens of its first relation.
    pub(crate) relation: (usize, usize),
    /// Each join, in order.
    pub(crate) joins: Vec<JoinAt>,
}

impl ItemAt {
    /// The last token of the item.
    pub(crate) fn last(&self) -> usize {
        self.joins.last().map_or(self.relation.1, |join| join.last)
    }
}

/// Where the `FROM` item `item` stands.
pub(crate) fn item(statement: &Stmt, item: &TableWithJoins) -> Option<ItemAt> {
    let relation = statement.factor(&item.relation)?;
    let mut joins = Vec::new();
    for join in &item.joins {
        joins.push(join_at(statement, join)?);
    }
    Some(ItemAt { relation, joins })
}

/// Where the join `join` stands.
fn join_at(statement: &Stmt, join: &Join) -> Option<JoinAt> {
    let factor = statement.factor(&join.relation)?;
    let join_token = statement.before(factor.0)?;
    if !statement.is(join_token, Keyword::JOIN) {
        return None;
    }
    let mut first = join_token;
    while let Some(before) = statement.before(first) {
        let operator = [
            Keyword::INNER,
            Keyword::CROSS,
            Keyword::LEFT,
            Keyword::RIGHT,
            Keyword::FULL,
            Keyword::OUTER,
            Keyword::NATURAL,
        ]
        .iter()
        .any(|&keyword| statement.is(before, keyword));
        if !operator {
            break;
        }
        first = before;
    }
    let written = constraint(&join.join_operator)?;
    let (on, last) = match written {
        JoinConstraint::On(expr) => {
            let on = chain(statement, expr, Keyword::ON)?;
            let last = on.last();
            (Some(on), last)
        }
        JoinConstraint::Using(_) => {
            let using = statement.after(factor.1)?;
            let (read, last) = statement.parse_at(using, |p| p.parse_join_constraint(false))?;
            if read != *written {
                return None;
            }
            (None, last)
        }
        JoinConstraint::Natural | JoinConstraint::None => (None, factor.1),
    };
    Some(JoinAt {
        operator: (first, join_token),
        factor,
        on,
        last,
    })
}

/// The first of the plain spaces, tabs and newlines just before token `at`, or `at` where none
/// are. The newline that ends a comment is the comment's.
pub(crate) fn spaced_from(statement: &Stmt, at: usize) -> usize {
    let mut first = at;
    while first > 0 && plain_space(&statement.tokens[first - 1].token) {
        let ends_a_comment = first >= 2
            && matches!(
                statement.tokens[first - 1].token,
                Token::Whitespace(Whitespace::Newline)
            )
            && matches!(
                statement.tokens[first - 2].token,
                Token::Whitespace(Whitespace::SingleLineComment { .. })
            );
        if ends_a_comment {
            break;
        }
        first -= 1;
    }
    first
}

/// Whether token `at` is `token`.
pub(crate) fn is_token(statement: &Stmt, at: usize, token: &Token) -> bool {
    statement.tokens[at].token == *token
}

/// The edits that take conjunct `n` out of the condition `chain`, the clause's keyword with it
/// where it is the only one: the conjunct, the `AND` or keyword that joins it, and the plain
/// spaces between them and before the `AND`. A comment among them stays where it stands.
pub(crate) fn without(read: &Read, statement: &Stmt, chain: &Chain, n: usize) -> Option<Edits> {
    let (first, last) = chain.conjuncts[n];
    let mut runs = if chain.conjuncts.len() == 1 {
        vec![
            (spaced_from(statement, chain.keyword), chain.keyword),
            (first, last),
        ]
    } else if n == 0 {
        let and = statement.after(last)?;
        vec![
            (first, last),
            (spaced_from(statement, and), spaced_to(statement, and)),
        ]
    } else {
        let and = statement.before(first)?;
        vec![(spaced_from(statement, and), and), (first, last)]
    };
    runs = merged(statement, runs);
    Some(
        runs.into_iter()
            .map(|(first, last)| {
                let (start, end) = read.written(statement, first, last);
                (start, end, String::new())
            })
            .collect(),
    )
}

/// `runs` of tokens, in order, each joined to the next where only plain spaces lie between.
fn merged(statement: &Stmt, runs: Vec<(usize, usize)>) -> Vec<(usize, usize)> {
    let mut out: Vec<(usize, usize)> = Vec::new();
    for (first, last) in runs {
        if let Some(previous) = out.last_mut() {
            if (previous.1 + 1..first).all(|t| plain_space(&statement.tokens[t].token)) {
                previous.1 = last;
                continue;
            }
        }
        out.push((first, last));
    }
    out
}

/// The last of the plain spaces, tabs and newlines just after token `at`, or `at` where none are.
pub(crate) fn spaced_to(statement: &Stmt, at: usize) -> usize {
    let mut last = at;
    while last + 1 < statement.tokens.len() && plain_space(&statement.tokens[last + 1].token) {
        last += 1;
    }
    last
}

/// The edits that add `text` as the last conjunct of the condition `chain`, which is `whole`: an
/// `OR` no parenthesis holds is held in one first.
pub(crate) fn appended(
    read: &Read,
    statement: &Stmt,
    chain: &Chain,
    whole: &Expr,
    text: &str,
) -> Edits {
    let (_, end) = read.written(statement, chain.last(), chain.last());
    if chain.conjuncts.len() == 1 && or(whole) {
        let (start, _) = read.written(statement, chain.conjuncts[0].0, chain.conjuncts[0].0);
        vec![
            (start, start, "(".into()),
            (end, end, format!(") AND {text}")),
        ]
    } else {
        vec![(end, end, format!(" AND {text}"))]
    }
}

/// Whether `expr` is an `OR` no parenthesis holds.
pub(crate) fn or(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::BinaryOp {
            op: sqlparser::ast::BinaryOperator::Or,
            ..
        }
    )
}

/// What opens a clause added after token `last` of a select whose `SELECT` is token `select`: a
/// space where the select is on one line, else a new line indented as the `SELECT` is.
pub(crate) fn lead(read: &Read, statement: &Stmt, select: usize, last: usize) -> String {
    let text = &read.stood.text;
    let (at, _) = read.written(statement, select, select);
    let (_, end) = read.written(statement, last, last);
    if !text[at..end].contains('\n') {
        return " ".into();
    }
    let line = text[..at].rfind('\n').map_or(0, |n| n + 1);
    let indent: String = text[line..at]
        .chars()
        .map(|c| if c == '\t' { '\t' } else { ' ' })
        .collect();
    format!("\n{indent}")
}
