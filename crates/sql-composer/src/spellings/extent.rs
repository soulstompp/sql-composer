//! Where the parts of a select stand among a statement's tokens: a condition's conjuncts, a
//! `FROM` item's relations and joins. Each part is read back from where it is found and compared
//! with its node, and a part that does not read back is not found.

use sqlparser::ast::{Expr, Join, JoinConstraint, TableWithJoins};
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Token;

use super::join_order::constraint;
use super::place::conjuncts;
use super::read::{plain_space, Stmt};

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
        let last = statement.expr_from(at, conjunct, all.len() > 1)?;
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
/// are.
pub(crate) fn spaced_from(statement: &Stmt, at: usize) -> usize {
    let mut first = at;
    while first > 0 && plain_space(&statement.tokens[first - 1].token) {
        first -= 1;
    }
    first
}

/// Whether token `at` is `token`.
pub(crate) fn is_token(statement: &Stmt, at: usize, token: &Token) -> bool {
    statement.tokens[at].token == *token
}
