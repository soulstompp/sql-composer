//! A semi-join's form: `x IN (SELECT y …)`, or `EXISTS (SELECT … WHERE x = y)`.
//!
//! Either is a conjunct of a `WHERE`, under no `NOT`, over a plain select: no `WITH`, grouping,
//! `DISTINCT` on the way to `IN`, ordering or limit, and nothing in its select list a function
//! could make an aggregate of. `x` names a relation of the select the conjunct is in, every column
//! of it with its relation, none the subquery names again, and `y` reads the subquery's own
//! relations, each named with its relation. The equality is written `x = y`, as `IN` compares
//! them, and an `EXISTS` is read back only from that order.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Query, Select, SelectItem, SetExpr, Visit, Visitor,
};
use sqlparser::dialect::Precedence;
use sqlparser::keywords::Keyword;

use super::conditions::range_name;
use super::extent::{self, Chain};
use super::join_order::Selects;
use super::place::{
    change_select, conjoined, conjuncts, enough, one_line, passed, Choice, Edits, Place, Want,
};
use super::read::{Read, Stmt};
use crate::parsed::normal;

/// The pick that writes each semi-join as `x IN (SELECT y …)`.
pub(crate) const IN: &str = "in";
/// The pick that writes each semi-join as `EXISTS (SELECT … WHERE x = y)`.
pub(crate) const EXISTS: &str = "exists";

/// Each semi-join of `read` that reads back in its other form, with both.
pub(crate) fn places(read: &Read, want: Option<Want>) -> Vec<Place> {
    let mut places = Vec::new();
    for (at, statement) in read.parsed() {
        let mut selects = Selects::default();
        let _ = statement.ast.visit(&mut selects);
        for (k, select) in selects.0.iter().enumerate() {
            let Some(selection) = &select.selection else {
                continue;
            };
            let Some(chain) = extent::chain(statement, selection, Keyword::WHERE) else {
                continue;
            };
            let outer: BTreeSet<String> = select
                .from
                .iter()
                .flat_map(|from| {
                    std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation))
                })
                .filter_map(range_name)
                .collect();
            for (n, conjunct) in conjuncts(selection).into_iter().enumerate() {
                let written_as = match conjunct {
                    Expr::InSubquery { .. } => IN,
                    _ => EXISTS,
                };
                if passed(want, &[written_as]) {
                    continue;
                }
                let found = match conjunct {
                    Expr::InSubquery {
                        expr,
                        subquery,
                        negated: false,
                    } => as_exists(read, statement, &chain, n, expr, subquery, &outer).map(
                        |(edits, query)| {
                            (
                                IN,
                                EXISTS,
                                edits,
                                Expr::Exists {
                                    subquery: Box::new(query),
                                    negated: false,
                                },
                            )
                        },
                    ),
                    Expr::Exists {
                        subquery,
                        negated: false,
                    } => as_in(read, statement, &chain, n, subquery, &outer).map(
                        |(edits, x, query)| {
                            (
                                EXISTS,
                                IN,
                                edits,
                                Expr::InSubquery {
                                    expr: Box::new(x),
                                    subquery: Box::new(query),
                                    negated: false,
                                },
                            )
                        },
                    ),
                    _ => None,
                };
                let Some((written, other, edits, to)) = found else {
                    continue;
                };
                let mut expected = statement.ast.clone();
                change_select(&mut expected, k, |select| {
                    let mut all: Vec<Expr> = conjuncts(select.selection.as_ref().expect("a WHERE"))
                        .into_iter()
                        .cloned()
                        .collect();
                    all[n] = to;
                    select.selection = conjoined(all);
                });
                let (first, last) = chain.conjuncts[n];
                places.push(Place {
                    statement: at,
                    relations: None,
                    what: format!(
                        "the semi-join {}",
                        one_line(read.text(statement, first, last))
                    ),
                    choices: vec![
                        Choice::written(words(written), vec![written], &statement.ast),
                        Choice {
                            name: words(other).into(),
                            picks: vec![other],
                            edits,
                            expected,
                        },
                    ],
                });
                if enough(want, &places) {
                    return places;
                }
            }
        }
    }
    places
}

/// A form in words.
fn words(form: &str) -> &'static str {
    match form {
        IN => "x IN (SELECT y …)",
        _ => "EXISTS (SELECT … WHERE x = y)",
    }
}

/// The select a semi-join reads, where it is a plain one.
fn plain(query: &Query) -> Option<&Select> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
    {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let grouped =
        !matches!(&select.group_by, GroupByExpr::Expressions(e, m) if e.is_empty() && m.is_empty());
    if grouped
        || select.having.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.from.is_empty()
    {
        return None;
    }
    Some(select)
}

/// The names the relations of `select`'s `FROM` are read by.
fn names(select: &Select) -> BTreeSet<String> {
    select
        .from
        .iter()
        .flat_map(|from| {
            std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation))
        })
        .filter_map(range_name)
        .collect()
}

/// What an expression reads: the relations its columns are named with, whether a column is named
/// without its relation, and whether it calls a function or holds a query.
#[derive(Default)]
struct Reads {
    relations: BTreeSet<String>,
    plain: bool,
    calls: bool,
}

impl Reads {
    fn of(expr: &Expr) -> Reads {
        let mut reads = Reads::default();
        let _ = expr.visit(&mut reads);
        reads
    }
}

impl Visitor for Reads {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(_) => self.plain = true,
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                self.relations.insert(normal(&parts[parts.len() - 2]));
            }
            Expr::Function(_)
            | Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. } => self.calls = true,
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Whether `x` can stand outside or inside the subquery whose relations are `inner`, read by the
/// select whose relations are `outer`.
fn outside(x: &Expr, inner: &BTreeSet<String>, outer: &BTreeSet<String>) -> bool {
    let reads = Reads::of(x);
    !reads.plain
        && !reads.calls
        && !reads.relations.is_empty()
        && reads.relations.iter().any(|r| outer.contains(r))
        && reads.relations.is_disjoint(inner)
}

/// Whether `y` reads the subquery's own relations, `inner`, alone.
fn inside(y: &Expr, inner: &BTreeSet<String>) -> bool {
    let reads = Reads::of(y);
    !reads.plain && !reads.calls && !reads.relations.is_empty() && reads.relations.is_subset(inner)
}

/// The edits that write conjunct `n`, `x IN (subquery)`, as `EXISTS`, with the query it reads
/// then.
fn as_exists(
    read: &Read,
    statement: &Stmt,
    chain: &Chain,
    n: usize,
    x: &Expr,
    subquery: &Query,
    outer: &BTreeSet<String>,
) -> Option<(Edits, Query)> {
    let select = plain(subquery)?;
    let inner = names(select);
    let [item] = &select.projection[..] else {
        return None;
    };
    let y = match item {
        SelectItem::UnnamedExpr(y) | SelectItem::ExprWithAlias { expr: y, .. } => y,
        _ => return None,
    };
    if !outside(x, &inner, outer) || !inside(y, &inner) {
        return None;
    }
    let (first, _) = chain.conjuncts[n];
    let select_token = statement.first_token(&select.select_token.0)?;
    let open = statement.before(select_token)?;
    let in_token = statement.before(open)?;
    if !statement.is(in_token, Keyword::IN) {
        return None;
    }
    let x_last = statement.before(in_token)?;
    statement
        .expr_from(first, x, Some(Precedence::Between))
        .filter(|&last| last == x_last)?;
    let y_text = item_text(read, statement, select, select_token)?;
    let equality = format!("{} = {}", read.text(statement, first, x_last), y_text);
    let (start, _) = read.written(statement, first, first);
    let (_, end) = read.written(statement, in_token, in_token);
    let mut edits = vec![(start, end, "EXISTS".to_string())];
    match &select.selection {
        Some(selection) => {
            let within = extent::chain(statement, selection, Keyword::WHERE)?;
            edits.extend(extent::appended(
                read, statement, &within, selection, &equality,
            ));
        }
        None => {
            let last = extent::item(statement, select.from.last()?)?.last();
            let lead = extent::lead(read, statement, select_token, last);
            let (_, at) = read.written(statement, last, last);
            edits.push((at, at, format!("{lead}WHERE {equality}")));
        }
    }
    let mut query = subquery.clone();
    if let SetExpr::Select(select) = query.body.as_mut() {
        let mut all: Vec<Expr> = select
            .selection
            .iter()
            .flat_map(|s| conjuncts(s).into_iter().cloned())
            .collect();
        all.push(Expr::BinaryOp {
            left: Box::new(x.clone()),
            op: BinaryOperator::Eq,
            right: Box::new(y.clone()),
        });
        select.selection = conjoined(all);
    }
    Some((edits, query))
}

/// The template's text of the one item of `select`'s select list.
fn item_text(
    read: &Read,
    statement: &Stmt,
    select: &Select,
    select_token: usize,
) -> Option<String> {
    let first = statement.after(select_token)?;
    let from = statement.before(extent::item(statement, select.from.first()?)?.relation.0)?;
    if !statement.is(from, Keyword::FROM) {
        return None;
    }
    let last = statement.before(from)?;
    let [item] = &select.projection[..] else {
        return None;
    };
    let y = match item {
        SelectItem::UnnamedExpr(y) => y,
        SelectItem::ExprWithAlias { expr, .. } => {
            let end = statement.expr_from(first, expr, None)?;
            return Some(read.text(statement, first, end).to_string());
        }
        _ => return None,
    };
    statement
        .expr_from(first, y, None)
        .filter(|&end| end == last)?;
    Some(read.text(statement, first, last).to_string())
}

/// The edits that write conjunct `n`, `EXISTS (subquery)`, as `IN`, with what it then tests and
/// the query it reads.
fn as_in(
    read: &Read,
    statement: &Stmt,
    chain: &Chain,
    n: usize,
    subquery: &Query,
    outer: &BTreeSet<String>,
) -> Option<(Edits, Expr, Query)> {
    let select = plain(subquery)?;
    if select.distinct.is_some() {
        return None;
    }
    let inner = names(select);
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
            if Reads::of(e).calls {
                return None;
            }
        }
    }
    let selection = select.selection.as_ref()?;
    let all = conjuncts(selection);
    let mut found = None;
    for (m, conjunct) in all.iter().enumerate() {
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = conjunct
        {
            if outside(left, &inner, outer) && inside(right, &inner) {
                if found.is_some() {
                    return None;
                }
                found = Some((m, left.as_ref(), right.as_ref()));
            }
        }
    }
    let (m, x, y) = found?;
    let within = extent::chain(statement, selection, Keyword::WHERE)?;
    let (eq_first, eq_last) = within.conjuncts[m];
    let x_last = statement.expr_from(eq_first, x, Some(Precedence::Eq))?;
    let eq = statement.after(x_last)?;
    if statement.tokens[eq].token != sqlparser::tokenizer::Token::Eq {
        return None;
    }
    let y_first = statement.after(eq)?;
    let x_text = read.text(statement, eq_first, x_last).to_string();
    let y_text = read.text(statement, y_first, eq_last).to_string();
    let (first, _) = chain.conjuncts[n];
    if !statement.is(first, Keyword::EXISTS) {
        return None;
    }
    let select_token = statement.first_token(&select.select_token.0)?;
    let items_first = statement.after(select_token)?;
    let from = statement.before(extent::item(statement, select.from.first()?)?.relation.0)?;
    if !statement.is(from, Keyword::FROM) {
        return None;
    }
    let items_last = statement.before(from)?;
    let (a, b) = read.written(statement, first, first);
    let mut edits = vec![(a, b, format!("{x_text} IN"))];
    let (a, b) = read.written(statement, items_first, items_last);
    edits.push((a, b, y_text));
    edits.extend(extent::without(read, statement, &within, m)?);
    let mut query = subquery.clone();
    if let SetExpr::Select(select) = query.body.as_mut() {
        select.projection = vec![SelectItem::UnnamedExpr(y.clone())];
        let mut rest: Vec<Expr> = all.iter().map(|e| (*e).clone()).collect();
        rest.remove(m);
        select.selection = conjoined(rest);
    }
    Some((edits, x.clone(), query))
}

#[cfg(test)]
mod tests {
    use super::super::laws::{holds, Trap};
    use super::super::{written_in, Axis};
    use super::*;
    use crate::mock::MockTable;

    fn written(text: &str, pick: &str) -> String {
        written_in(text, Axis::SemiJoin, pick, &|_| None)
    }

    const IN_FORM: &str =
        "SELECT s.set_num FROM sets s WHERE s.set_num IN (SELECT o.set_num FROM bought o)";
    const EXISTS_FORM: &str = "SELECT s.set_num FROM sets s WHERE EXISTS (SELECT o.set_num FROM bought o WHERE s.set_num = o.set_num)";

    #[test]
    fn an_in_is_written_as_exists_and_back() {
        assert_eq!(written(IN_FORM, EXISTS), EXISTS_FORM);
        assert_eq!(written(EXISTS_FORM, IN), IN_FORM);
        assert_eq!(written(IN_FORM, IN), IN_FORM);
        assert_eq!(written(EXISTS_FORM, EXISTS), EXISTS_FORM);
    }

    #[test]
    fn the_subquery_keeps_its_own_conditions() {
        let text = "SELECT s.set_num\nFROM sets s\nWHERE s.year > 2000\n  AND s.set_num IN (\n    SELECT o.set_num\n    FROM bought o\n  )";
        assert_eq!(
            written(text, EXISTS),
            "SELECT s.set_num\nFROM sets s\nWHERE s.year > 2000\n  AND EXISTS (\n    SELECT o.set_num\n    FROM bought o\n    WHERE s.set_num = o.set_num\n  )"
        );
        let agent = "SELECT s.set_num FROM sets s WHERE EXISTS (SELECT 1 FROM bought o WHERE s.set_num = o.set_num AND o.quantity > :bind(least))";
        assert_eq!(
            written(agent, IN),
            "SELECT s.set_num FROM sets s WHERE s.set_num IN (SELECT o.set_num FROM bought o WHERE o.quantity > :bind(least))"
        );
    }

    #[test]
    fn a_semi_join_that_breaks_a_condition_stays_as_written() {
        for (condition, text) in [
            ("under a NOT", "SELECT s.set_num FROM sets s WHERE NOT (s.set_num IN (SELECT o.set_num FROM bought o))"),
            ("NOT IN", "SELECT s.set_num FROM sets s WHERE s.set_num NOT IN (SELECT o.set_num FROM bought o)"),
            ("NOT EXISTS", "SELECT s.set_num FROM sets s WHERE NOT EXISTS (SELECT 1 FROM bought o WHERE s.set_num = o.set_num)"),
            ("under an OR", "SELECT s.set_num FROM sets s WHERE s.year > 1 OR s.set_num IN (SELECT o.set_num FROM bought o)"),
            ("in an ON", "SELECT s.set_num FROM sets s JOIN themes t ON t.id IN (SELECT o.id FROM bought o)"),
            ("a limit", "SELECT s.set_num FROM sets s WHERE s.set_num IN (SELECT o.set_num FROM bought o LIMIT 1)"),
            ("a grouping", "SELECT s.set_num FROM sets s WHERE s.set_num IN (SELECT o.set_num FROM bought o GROUP BY o.set_num)"),
            ("an aggregate", "SELECT s.set_num FROM sets s WHERE EXISTS (SELECT count(*) FROM bought o WHERE s.set_num = o.set_num)"),
            ("a function over y", "SELECT s.set_num FROM sets s WHERE s.set_num IN (SELECT max(o.set_num) FROM bought o)"),
            ("a column without its relation", "SELECT s.set_num FROM sets s WHERE set_num IN (SELECT o.set_num FROM bought o)"),
            ("x's relation named again inside", "SELECT s.set_num FROM sets s WHERE s.set_num IN (SELECT s.set_num FROM bought s)"),
            ("the equality written inner first", "SELECT s.set_num FROM sets s WHERE EXISTS (SELECT 1 FROM bought o WHERE o.set_num = s.set_num)"),
            ("two correlations", "SELECT s.set_num FROM sets s WHERE EXISTS (SELECT 1 FROM bought o WHERE s.set_num = o.set_num AND s.year = o.year)"),
        ] {
            for pick in [IN, EXISTS] {
                assert_eq!(written(text, pick), text, "{condition}");
            }
            assert!(places(&Read::new(text).unwrap(), None).is_empty(), "{condition}");
        }
    }

    #[test]
    #[ignore = "needs a PostgreSQL server: set SQLC_TEST_POSTGRES_URL"]
    fn law_a_semi_join_in_either_form_returns_the_same_rows_under_no_not() {
        let sets = MockTable::new("sets")
            .column("set_num", "varchar")
            .column("year", "integer")
            .row(&[Some("6000-1"), Some("2001")])
            .row(&[Some("6001-1"), Some("2002")])
            .row(&[Some("6002-1"), None]);
        let bought = MockTable::new("bought")
            .column("set_num", "varchar")
            .column("quantity", "integer")
            .row(&[Some("6000-1"), Some("1")])
            .row(&[Some("6000-1"), Some("2")])
            .row(&[Some("6002-1"), Some("3")])
            .row(&[None, Some("4")]);
        let trap = Trap {
            tables: vec![sets, bought],
            binds: vec![("least", 2)],
            templates: vec![],
        };
        let under_not = (
            "SELECT s.set_num FROM sets s WHERE NOT (s.set_num IN (SELECT o.set_num FROM bought o))",
            "SELECT s.set_num FROM sets s WHERE NOT EXISTS (SELECT o.set_num FROM bought o WHERE s.set_num = o.set_num)",
        );
        assert_eq!(written(under_not.0, EXISTS), under_not.0);
        for (text, pick, rows) in [
            (IN_FORM, EXISTS, 2),
            (EXISTS_FORM, IN, 2),
            ("SELECT s.set_num FROM sets s WHERE EXISTS (SELECT 1 FROM bought o WHERE s.set_num = o.set_num AND o.quantity > :bind(least))", IN, 1),
        ] {
            let spelled = written(text, pick);
            assert_eq!(holds(text, &spelled, under_not, &trap).written, rows, "{spelled}");
        }
    }
}
