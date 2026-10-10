//! Negation: `NOT` over an `AND` or an `OR`, or pushed onto each of its operands, and `NOT NOT p`
//! written as `p`.
//!
//! A place is each condition built of `AND`, `OR` and `NOT` that some spelling writes otherwise:
//! a `WHERE`, an `ON`, a `HAVING`, a `CASE`'s condition, anywhere a condition stands. Pushed, every
//! `NOT` over a parenthesised `AND` or `OR` goes onto each operand, the `AND` becoming an `OR` and
//! the `OR` an `AND`. Over, every `AND` or `OR` whose operands are each a `NOT` becomes one `NOT`
//! over the other. A `NOT` over a `NOT` goes either way. What a `NOT` stands over is otherwise
//! kept as written: `NOT (a = b)` is never `a <> b`, nor `NOT (x IN …)` `x NOT IN …`, which rest on
//! operators the catalog holds.

use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, Statement, UnaryOperator, Visit, VisitMut, Visitor, VisitorMut,
};
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Token;

use super::extent::spaced_from;
use super::place::{apply, enough, one_line, passed, wants, Choice, Edits, Place, Want};
use super::read::{plain_space, Read, Stmt};
use super::MOVES;

/// The pick that writes each `NOT` onto the operands of the `AND` or `OR` it stands over.
pub(crate) const PUSHED: &str = "pushed";
/// The pick that writes one `NOT` over an `AND` or `OR` whose operands are each a `NOT`.
pub(crate) const OVER: &str = "over";

/// Each condition of `read` some spelling of negation writes otherwise, with each spelling.
pub(crate) fn places(read: &Read, want: Option<Want>) -> Vec<Place> {
    let mut places = Vec::new();
    for (at, statement) in read.parsed() {
        for (k, root) in roots(&statement.ast).iter().enumerate() {
            let as_written = super::read::canonical_expr(root);
            let mut picks = vec![];
            let mut choices = vec![];
            for (pick, name, form) in [
                (PUSHED, "NOT pushed onto each operand", pushed(root)),
                (OVER, "NOT over the AND or the OR", over(root)),
            ] {
                let form = super::read::canonical_expr(&form);
                if form == as_written {
                    picks.push(pick);
                    continue;
                }
                if !wants(want, pick) {
                    continue;
                }
                let Some(edits) = spelled(read, at, k, pick) else {
                    continue;
                };
                let mut expected = statement.ast.clone();
                replace_root(&mut expected, k, form);
                choices.push(Choice {
                    name: name.into(),
                    picks: vec![pick],
                    edits,
                    expected,
                });
            }
            if choices.is_empty() || passed(want, &picks) {
                continue;
            }
            let Some((first, last)) = extent(statement, root) else {
                continue;
            };
            choices.insert(0, Choice::written("as written", picks, &statement.ast));
            places.push(Place {
                statement: at,
                relations: None,
                what: format!(
                    "the condition {}",
                    one_line(read.text(statement, first, last))
                ),
                choices,
            });
            if enough(want, &places) {
                return places;
            }
        }
    }
    places
}

/// The edits that write root `k` of statement `at` in `pick`, one step at a time, each step read
/// back before the next is found.
fn spelled(read: &Read, at: usize, k: usize, pick: &str) -> Option<Edits> {
    let statement = read.statements[at].as_ref().ok()?;
    let root = roots(&statement.ast).get(k)?.clone();
    let (first, last) = extent(statement, &root)?;
    let (start, end) = read.written(statement, first, last);
    let count = roots(&statement.ast).len();
    let mut text = read.stood.text.clone();
    for _ in 0..MOVES {
        let again = Read::new(&text).ok()?;
        let statement = again.statements.get(at)?.as_ref().ok()?;
        let all = roots(&statement.ast);
        if all.len() != count {
            break;
        }
        let steps = Steps {
            read: &again,
            statement,
        };
        let step = match pick {
            PUSHED => steps.push(&all[k], Ctx::Root)?,
            _ => steps.pull(&all[k], false)?,
        };
        match step {
            Some(edits) => text = apply(&text, &edits)?,
            None => break,
        }
    }
    let grown = text.len() as isize - read.stood.text.len() as isize;
    let new_end = (end as isize + grown) as usize;
    Some(vec![(start, end, text.get(start..new_end)?.to_string())])
}

/// Whether `expr` is built of `AND`, `OR` and `NOT` at its top.
fn boolean(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp {
            op: BinaryOperator::And | BinaryOperator::Or,
            ..
        } => true,
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            ..
        } => true,
        Expr::Nested(inner) => boolean(inner),
        _ => false,
    }
}

/// Each condition of `statement` built of `AND`, `OR` and `NOT` that no other such condition
/// holds as an operand, in the order a visit reaches them.
fn roots(statement: &Statement) -> Vec<Expr> {
    struct Roots {
        path: Vec<bool>,
        found: Vec<Expr>,
    }
    impl Visitor for Roots {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            let is = boolean(expr);
            if is && self.path.last() != Some(&true) {
                self.found.push(expr.clone());
            }
            self.path.push(is);
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, _: &Expr) -> ControlFlow<()> {
            self.path.pop();
            ControlFlow::Continue(())
        }
    }
    let mut roots = Roots {
        path: vec![],
        found: vec![],
    };
    let _ = statement.visit(&mut roots);
    roots.found
}

/// `statement` with its root `k`, as [`roots`] finds them, replaced by `with`.
fn replace_root(statement: &mut Statement, k: usize, with: Expr) {
    struct Replace {
        path: Vec<bool>,
        seen: usize,
        k: usize,
        with: Option<Expr>,
    }
    impl VisitorMut for Replace {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            let is = boolean(expr);
            if is && self.path.last() != Some(&true) {
                if self.seen == self.k {
                    if let Some(with) = self.with.take() {
                        *expr = with;
                    }
                    return ControlFlow::Break(());
                }
                self.seen += 1;
            }
            self.path.push(is);
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, _: &mut Expr) -> ControlFlow<()> {
            self.path.pop();
            ControlFlow::Continue(())
        }
    }
    let _ = statement.visit(&mut Replace {
        path: vec![],
        seen: 0,
        k,
        with: Some(with),
    });
}

/// The `AND` or `OR` of `expr`, where it is one.
fn op(expr: &Expr) -> Option<BinaryOperator> {
    match expr {
        Expr::BinaryOp {
            op: op @ (BinaryOperator::And | BinaryOperator::Or),
            ..
        } => Some(op.clone()),
        _ => None,
    }
}

/// The other of `AND` and `OR`.
fn flip(op: &BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::And => BinaryOperator::Or,
        _ => BinaryOperator::And,
    }
}

/// The operands of the chain of `op`s `expr` is, left to right, a parenthesised one whole.
fn operands<'e>(expr: &'e Expr, op: &BinaryOperator) -> Vec<&'e Expr> {
    match expr {
        Expr::BinaryOp { left, op: o, right } if o == op => {
            let mut all = operands(left, op);
            all.extend(operands(right, op));
            all
        }
        other => vec![other],
    }
}

/// `operands` joined by `op`, left to right.
fn chained(operands: Vec<Expr>, op: &BinaryOperator) -> Expr {
    operands
        .into_iter()
        .reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: op.clone(),
            right: Box::new(right),
        })
        .expect("a chain has operands")
}

fn not(expr: Expr) -> Expr {
    Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(expr),
    }
}

/// What a `NOT` stands over, where `expr` is one.
fn negated(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Some(expr),
        _ => None,
    }
}

/// `expr` with every `NOT` over a parenthesised `AND` or `OR` pushed onto its operands, and every
/// `NOT` over a `NOT` left out.
pub(crate) fn pushed(expr: &Expr) -> Expr {
    if let Some(inner) = negated(expr) {
        if let Some(twice) = negated(inner) {
            return pushed(twice);
        }
        if let Expr::Nested(nested) = inner {
            if let Some(twice) = negated(nested) {
                return pushed(twice);
            }
            if let Some(op) = op(nested) {
                let flipped: Vec<Expr> = operands(nested, &op)
                    .into_iter()
                    .map(|o| pushed(&not(o.clone())))
                    .collect();
                return chained(flipped, &flip(&op));
            }
        }
        return expr.clone();
    }
    if let Some(op) = op(expr) {
        return chained(operands(expr, &op).into_iter().map(pushed).collect(), &op);
    }
    match expr {
        Expr::Nested(inner) => Expr::Nested(Box::new(pushed(inner))),
        other => other.clone(),
    }
}

/// `expr` with every `AND` or `OR` whose operands are each a `NOT` written as one `NOT` over the
/// other, from the innermost out, and every `NOT` over a `NOT` left out.
pub(crate) fn over(expr: &Expr) -> Expr {
    if let Some(op) = op(expr) {
        let all: Vec<Expr> = operands(expr, &op).into_iter().map(over).collect();
        if all.iter().all(|o| negated(o).is_some()) {
            let inner: Vec<Expr> = all
                .iter()
                .map(|o| negated(o).expect("a NOT").clone())
                .collect();
            return not(Expr::Nested(Box::new(chained(inner, &flip(&op)))));
        }
        return chained(all, &op);
    }
    if let Some(inner) = negated(expr) {
        let under = over(inner);
        if let Some(twice) = negated(&under) {
            return twice.clone();
        }
        if let Expr::Nested(nested) = &under {
            if let Some(twice) = negated(nested) {
                return twice.clone();
            }
        }
        return not(under);
    }
    match expr {
        Expr::Nested(inner) => match over(inner) {
            pulled @ Expr::UnaryOp {
                op: UnaryOperator::Not,
                ..
            } => pulled,
            other => Expr::Nested(Box::new(other)),
        },
        other => other.clone(),
    }
}

/// What a condition stands in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ctx {
    /// A clause, a parenthesis, an argument: nothing that binds it.
    Root,
    And,
    Or,
}

impl Ctx {
    fn of(op: &BinaryOperator) -> Ctx {
        match op {
            BinaryOperator::And => Ctx::And,
            _ => Ctx::Or,
        }
    }
}

/// The first and last tokens of `expr`, a condition or one of its operands.
fn extent(statement: &Stmt, expr: &Expr) -> Option<(usize, usize)> {
    match expr {
        Expr::Nested(inner) => {
            let (first, last) = extent(statement, inner)?;
            let open = statement.before(first)?;
            let close = statement.after(last)?;
            (statement.tokens[open].token == Token::LParen
                && statement.tokens[close].token == Token::RParen)
                .then_some((open, close))
        }
        Expr::BinaryOp { left, right, .. } if op(expr).is_some() => {
            Some((extent(statement, left)?.0, extent(statement, right)?.1))
        }
        _ => {
            let mut at = statement.first_token(expr)?;
            for _ in 0..8 {
                let read = statement.parse_at(at, |p| {
                    p.parse_subexpr(sqlparser::dialect::Dialect::prec_value(
                        &crate::parsed::Postgres::default(),
                        sqlparser::dialect::Precedence::UnaryNot,
                    ))
                });
                if let Some((parsed, last)) = read {
                    if parsed == *expr {
                        return Some((at, last));
                    }
                }
                at = statement.before(at)?;
            }
            None
        }
    }
}

/// Finds one step of a spelling of negation in a statement.
struct Steps<'r> {
    read: &'r Read,
    statement: &'r Stmt,
}

impl Steps<'_> {
    /// The template's bytes of tokens `first` to `last`.
    fn bytes(&self, first: usize, last: usize) -> (usize, usize) {
        self.read.written(self.statement, first, last)
    }

    /// The edit that leaves out token `at` and the plain spaces after it.
    fn drop_with_spaces_after(&self, at: usize) -> (usize, usize, String) {
        let mut last = at;
        while last + 1 < self.statement.tokens.len()
            && plain_space(&self.statement.tokens[last + 1].token)
        {
            last += 1;
        }
        let (start, end) = self.bytes(at, last);
        (start, end, String::new())
    }

    /// The edit that leaves out token `at` and the plain spaces before it.
    fn drop_with_spaces_before(&self, at: usize) -> (usize, usize, String) {
        let first = spaced_from(self.statement, at);
        let (start, end) = self.bytes(first, at);
        (start, end, String::new())
    }

    /// The edit that writes `text` before token `at`.
    fn before(&self, at: usize, text: &str) -> (usize, usize, String) {
        let (start, _) = self.bytes(at, at);
        (start, start, text.to_string())
    }

    /// The `NOT` token a negation starts with.
    fn not_token(&self, expr: &Expr) -> Option<usize> {
        let (first, _) = extent(self.statement, expr)?;
        self.statement.is(first, Keyword::NOT).then_some(first)
    }

    /// The edits that flip each `op` between the operands of a chain.
    fn flipped(&self, operands: &[&Expr], op: &BinaryOperator) -> Option<Edits> {
        let (word, to) = match op {
            BinaryOperator::And => (Keyword::AND, "OR"),
            _ => (Keyword::OR, "AND"),
        };
        let mut edits = Vec::new();
        for operand in &operands[..operands.len() - 1] {
            let (_, last) = extent(self.statement, operand)?;
            let between = self.statement.after(last)?;
            if !self.statement.is(between, word) {
                return None;
            }
            let (start, end) = self.bytes(between, between);
            edits.push((start, end, to.to_string()));
        }
        Some(edits)
    }

    /// The edits that leave out a `NOT` over a `NOT`, `NOT NOT p` or `NOT (NOT p)`.
    fn twice(&self, expr: &Expr) -> Option<Option<Edits>> {
        let inner = negated(expr)?;
        let outer = self.not_token(expr)?;
        if negated(inner).is_some() {
            let inner_not = self.not_token(inner)?;
            return Some(Some(vec![
                self.drop_with_spaces_after(outer),
                self.drop_with_spaces_after(inner_not),
            ]));
        }
        let Expr::Nested(nested) = inner else {
            return None;
        };
        negated(nested)?;
        let (open, close) = extent(self.statement, inner)?;
        let inner_not = self.not_token(nested)?;
        Some(Some(vec![
            self.drop_with_spaces_after(outer),
            self.drop_with_spaces_after(open),
            self.drop_with_spaces_after(inner_not),
            self.drop_with_spaces_before(close),
        ]))
    }

    /// One step pushing a `NOT` down in `expr`, which stands in `ctx`; none where none is left,
    /// and nothing where a part of it does not read back.
    fn push(&self, expr: &Expr, ctx: Ctx) -> Option<Option<Edits>> {
        if let Some(inner) = negated(expr) {
            if let Some(edits) = self.twice(expr) {
                return Some(edits);
            }
            let Expr::Nested(nested) = inner else {
                return Some(None);
            };
            let Some(op) = op(nested) else {
                return Some(None);
            };
            let all = operands(nested, &op);
            let mut edits = vec![self.drop_with_spaces_after(self.not_token(expr)?)];
            for operand in &all {
                match negated(operand) {
                    Some(_) => edits.push(self.drop_with_spaces_after(self.not_token(operand)?)),
                    None => edits.push(self.before(extent(self.statement, operand)?.0, "NOT ")),
                }
            }
            edits.extend(self.flipped(&all, &op)?);
            if ctx == Ctx::Root || ctx == Ctx::of(&flip(&op)) {
                let (open, close) = extent(self.statement, inner)?;
                edits.push(self.drop_with_spaces_after(open));
                edits.push(self.drop_with_spaces_before(close));
            }
            return Some(Some(edits));
        }
        if let Some(op) = op(expr) {
            for operand in operands(expr, &op) {
                if let Some(edits) = self.push(operand, Ctx::of(&op))? {
                    return Some(Some(edits));
                }
            }
            return Some(None);
        }
        match expr {
            Expr::Nested(inner) => self.push(inner, Ctx::Root),
            _ => Some(None),
        }
    }

    /// One step pulling `NOT`s up in `expr`, in parentheses of its own where `nested`; none
    /// where none is left, and nothing where a part of it does not read back.
    fn pull(&self, expr: &Expr, nested: bool) -> Option<Option<Edits>> {
        if let Some(op) = op(expr) {
            let all = operands(expr, &op);
            for operand in &all {
                if let Some(edits) = self.pull(operand, false)? {
                    return Some(Some(edits));
                }
            }
            if !all.iter().all(|o| negated(o).is_some()) {
                return Some(None);
            }
            let mut edits = Vec::new();
            for operand in &all {
                edits.push(self.drop_with_spaces_after(self.not_token(operand)?));
            }
            edits.extend(self.flipped(&all, &op)?);
            let (first, last) = extent(self.statement, expr)?;
            if nested {
                let open = self.statement.before(first)?;
                edits.push(self.before(open, "NOT "));
            } else {
                edits.push(self.before(first, "NOT ("));
                let (_, end) = self.bytes(last, last);
                edits.push((end, end, ")".into()));
            }
            return Some(Some(edits));
        }
        if let Some(inner) = negated(expr) {
            if let Some(edits) = self.twice(expr) {
                return Some(edits);
            }
            return self.pull(inner, false);
        }
        match expr {
            Expr::Nested(inner) => self.pull(inner, true),
            _ => Some(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::laws::{holds, Trap};
    use super::super::{written_in, Axis};
    use super::*;
    use crate::mock::MockTable;

    fn written(text: &str, pick: &str) -> String {
        written_in(text, Axis::Negation, pick, &|_| None)
    }

    #[test]
    fn a_not_over_an_and_is_pushed_onto_each_operand_and_pulled_back() {
        let text = "SELECT b.p FROM pairs b WHERE NOT (b.p AND b.q)";
        let pushed = written(text, PUSHED);
        assert_eq!(pushed, "SELECT b.p FROM pairs b WHERE NOT b.p OR NOT b.q");
        assert_eq!(written(&pushed, OVER), text);
        assert_eq!(written(text, OVER), text);
    }

    #[test]
    fn a_not_is_pushed_to_the_bottom_and_parentheses_stay_where_they_group() {
        assert_eq!(
            written("SELECT 1 FROM t WHERE t.x OR NOT (t.a OR t.b)", PUSHED),
            "SELECT 1 FROM t WHERE t.x OR (NOT t.a AND NOT t.b)"
        );
        assert_eq!(
            written("SELECT 1 FROM t WHERE t.x AND NOT (t.a OR t.b)", PUSHED),
            "SELECT 1 FROM t WHERE t.x AND NOT t.a AND NOT t.b"
        );
        let deep = "SELECT 1 FROM t WHERE NOT (t.a AND (t.b OR NOT t.c))";
        let pushed = written(deep, PUSHED);
        assert_eq!(pushed, "SELECT 1 FROM t WHERE NOT t.a OR (NOT t.b AND t.c)");
        assert_eq!(
            written(
                "SELECT 1 FROM t WHERE NOT t.a OR (NOT t.b AND NOT t.c)",
                OVER
            ),
            "SELECT 1 FROM t WHERE NOT (t.a AND (t.b OR t.c))"
        );
    }

    #[test]
    fn a_not_over_a_not_is_left_out_by_either_pick() {
        for pick in [PUSHED, OVER] {
            assert_eq!(
                written("SELECT 1 FROM t WHERE NOT NOT t.a AND t.b", pick),
                "SELECT 1 FROM t WHERE t.a AND t.b"
            );
            assert_eq!(
                written("SELECT 1 FROM t WHERE NOT (NOT t.a)", pick),
                "SELECT 1 FROM t WHERE t.a"
            );
        }
    }

    #[test]
    fn what_a_not_stands_over_is_kept_as_written() {
        for text in [
            "SELECT 1 FROM t WHERE NOT (t.a = t.b)",
            "SELECT 1 FROM t WHERE NOT (t.a IN (SELECT u.a FROM u))",
            "SELECT 1 FROM t WHERE t.a NOT IN (1, 2) AND NOT EXISTS (SELECT 1 FROM u)",
            "SELECT 1 FROM t WHERE NOT t.a AND t.b",
        ] {
            for pick in [PUSHED, OVER] {
                assert_eq!(written(text, pick), text, "{pick}");
            }
            assert!(places(&Read::new(text).unwrap(), None).is_empty(), "{text}");
        }
    }

    #[test]
    fn comments_inside_a_condition_and_its_directives_are_kept() {
        let text =
            "SELECT 1 FROM t\nWHERE NOT (\n    t.a = :bind(a) -- the first\n    AND t.b = 2\n)\n";
        assert_eq!(
            written(text, PUSHED),
            "SELECT 1 FROM t\nWHERE NOT t.a = :bind(a) -- the first\n    OR NOT t.b = 2\n"
        );
    }

    #[test]
    fn each_place_is_counted_with_each_spelling() {
        let read = Read::new(
            "SELECT CASE WHEN NOT (t.a OR t.b) THEN 1 END FROM t WHERE NOT t.a AND NOT t.b",
        )
        .unwrap();
        let found: Vec<(String, Vec<String>)> = places(&read, None)
            .iter()
            .map(|p| {
                (
                    p.what.clone(),
                    p.choices.iter().map(|c| c.name.clone()).collect(),
                )
            })
            .collect();
        assert_eq!(
            found,
            vec![
                (
                    "the condition NOT (t.a OR t.b)".to_string(),
                    vec![
                        "as written".to_string(),
                        "NOT pushed onto each operand".to_string()
                    ]
                ),
                (
                    "the condition NOT t.a AND NOT t.b".to_string(),
                    vec![
                        "as written".to_string(),
                        "NOT over the AND or the OR".to_string()
                    ]
                ),
            ]
        );
    }

    /// Every pair of true, false and NULL.
    fn pairs() -> Trap {
        let mut pairs = MockTable::new("pairs")
            .column("p", "boolean")
            .column("q", "boolean");
        for p in [Some("true"), Some("false"), None] {
            for q in [Some("true"), Some("false"), None] {
                pairs = pairs.row(&[p, q]);
            }
        }
        let sets = MockTable::new("sets")
            .column("set_num", "varchar")
            .row(&[Some("6000-1")])
            .row(&[Some("6001-1")]);
        let bought = MockTable::new("bought")
            .column("set_num", "varchar")
            .row(&[Some("6000-1")])
            .row(&[None]);
        Trap {
            tables: vec![pairs, sets, bought],
            binds: vec![],
            templates: vec![],
        }
    }

    #[test]
    #[ignore = "needs a PostgreSQL server: set SQLC_TEST_POSTGRES_URL"]
    fn law_de_morgan_and_a_not_over_a_not_return_the_same_rows_on_every_pair_of_truths() {
        let trap = pairs();
        let wrong_de_morgan = (
            "SELECT b.p, b.q FROM pairs b WHERE NOT (b.p AND b.q)",
            "SELECT b.p, b.q FROM pairs b WHERE NOT b.p AND NOT b.q",
        );
        let not_in_as_not_exists = (
            "SELECT s.set_num FROM sets s WHERE NOT (s.set_num IN (SELECT o.set_num FROM bought o))",
            "SELECT s.set_num FROM sets s WHERE NOT EXISTS (SELECT 1 FROM bought o WHERE o.set_num = s.set_num)",
        );
        for near in [wrong_de_morgan, not_in_as_not_exists] {
            for pick in [PUSHED, OVER] {
                assert_ne!(written(near.0, pick), near.1);
            }
            for (text, pick, rows) in [
                (
                    "SELECT b.p, b.q FROM pairs b WHERE NOT (b.p AND b.q)",
                    PUSHED,
                    5,
                ),
                (
                    "SELECT b.p, b.q FROM pairs b WHERE NOT (b.p OR b.q)",
                    PUSHED,
                    1,
                ),
                (
                    "SELECT b.p, b.q FROM pairs b WHERE NOT b.p OR NOT b.q",
                    OVER,
                    5,
                ),
                (
                    "SELECT b.p, b.q FROM pairs b WHERE NOT b.p AND NOT b.q",
                    OVER,
                    1,
                ),
                ("SELECT b.p, b.q FROM pairs b WHERE NOT NOT b.p", PUSHED, 3),
            ] {
                let spelled = written(text, pick);
                assert_eq!(
                    holds(text, &spelled, near, &trap).written,
                    rows,
                    "{spelled}"
                );
            }
        }
    }
}
