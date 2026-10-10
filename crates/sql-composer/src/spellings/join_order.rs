//! Join order: the relations of a join list of inner joins in another order, each condition at
//! the first `ON` where every relation it names is joined.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Join, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Visit, Visitor,
};
use sqlparser::tokenizer::{Token, Whitespace};

use super::columns::{Columns, Reader, Resolve, Withs};
use super::extent;
use super::place::{change_select, enough, passed, Choice, Place, Want};
use super::read::{Read, Stmt};
use crate::parsed::normal;

/// A join list of a statement: the relations a `FROM` item joins, two or more.
#[derive(Debug, Clone)]
pub(crate) struct JoinList {
    /// The select it is in, by its place among the statement's selects from 0.
    pub(crate) select: usize,
    /// The `FROM` item it is, by its place in the select's `FROM` from 0.
    pub(crate) item: usize,
    /// How many relations it joins.
    pub(crate) relations: usize,
    /// Whether a join of it joins by `USING`.
    pub(crate) using: bool,
    /// Each relation whose columns are not read from the text, with why, where a join of it
    /// joins by `USING`.
    pub(crate) unread: Vec<String>,
}

/// Every join list of `statement`.
pub(crate) fn join_lists(read: &Read, statement: &Statement, resolve: Resolve) -> Vec<JoinList> {
    let withs = Withs::of(statement);
    let reader = Reader {
        stood: &read.stood,
        withs: &withs,
        resolve,
        depth: 0,
    };
    let mut selects = Selects::default();
    let _ = statement.visit(&mut selects);
    let mut lists = Vec::new();
    for (at, select) in selects.0.iter().enumerate() {
        for (item, from) in select.from.iter().enumerate() {
            if from.joins.is_empty() {
                continue;
            }
            let using = from.joins.iter().any(|join| {
                matches!(
                    constraint(&join.join_operator),
                    Some(JoinConstraint::Using(_))
                )
            });
            let mut unread = Vec::new();
            if using {
                let relations =
                    std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation));
                for relation in relations {
                    if let Columns::Unread(why) = reader.factor(relation) {
                        unread.push(why);
                    }
                }
            }
            lists.push(JoinList {
                select: at,
                item,
                relations: from.joins.len() + 1,
                using,
                unread,
            });
        }
    }
    lists
}

/// The condition a join is joined by.
pub(crate) fn constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    match operator {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c) => Some(c),
        _ => None,
    }
}

/// Each select of a statement, in the order a visit reaches them.
#[derive(Default)]
pub(crate) struct Selects(pub(crate) Vec<Select>);

impl Visitor for Selects {
    type Break = ();

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
        self.0.push(select.clone());
        ControlFlow::Continue(())
    }
}

/// The pick that orders each join list outermost first.
pub(crate) const OUTERMOST: &str = "outermost first";

/// How many relations a join list may join for its orders to be laid out and read.
pub(crate) const MOST: usize = 8;

/// A relation of a join list, as an order moves it.
#[derive(Debug, Clone)]
struct Rel {
    /// The name it is read by.
    name: String,
    factor: TableFactor,
    /// Its text in the template.
    text: String,
    /// The columns it holds, where the list joins by `USING`.
    columns: Vec<String>,
    /// The other relations of the list it reads, where it is read laterally.
    reads: BTreeSet<usize>,
}

/// A condition of the list's `ON`s.
#[derive(Debug, Clone)]
struct Condition {
    expr: Expr,
    text: String,
    /// The relations of the list it names.
    names: BTreeSet<usize>,
}

/// A join list read for its orders.
#[derive(Debug, Clone)]
pub(crate) struct Model {
    rels: Vec<Rel>,
    /// Each column a `USING` joins by, with the relations it equates, as written.
    classes: Vec<(String, String, BTreeSet<usize>)>,
    conditions: Vec<Condition>,
    /// What stands between the first relation and the first join.
    separator: String,
    /// Whether the list joins by `USING`.
    using: bool,
}

impl Model {
    /// The relations' names, in `order`.
    pub(crate) fn names(&self, order: &[usize]) -> Vec<String> {
        order.iter().map(|&r| self.rels[r].name.clone()).collect()
    }

    /// The relation named `name`.
    pub(crate) fn position(&self, name: &str) -> Option<usize> {
        self.rels.iter().position(|r| r.name == name)
    }

    /// How many relations the list joins.
    pub(crate) fn len(&self) -> usize {
        self.rels.len()
    }
}

/// What a join of an order is joined by.
#[derive(Debug, Clone)]
enum By {
    Using(Vec<usize>),
    On(Vec<usize>),
    Cross,
}

/// The join list `item` of `select`, read for its orders; why not, where it is left as written.
pub(crate) fn model(
    read: &Read,
    statement: &Stmt,
    select: &Select,
    item: usize,
    resolve: Resolve,
) -> Result<Model, String> {
    let from = &select.from[item];
    let factors: Vec<&TableFactor> = std::iter::once(&from.relation)
        .chain(from.joins.iter().map(|j| &j.relation))
        .collect();
    if factors.len() > MOST {
        return Err(format!("it joins more than {MOST} relations"));
    }
    for join in &from.joins {
        if !super::conditions::inner(&join.join_operator) || join.global {
            return Err("a join of it is not inner".into());
        }
        if matches!(
            constraint(&join.join_operator),
            Some(JoinConstraint::Natural) | None
        ) {
            return Err("a join of it is natural".into());
        }
    }
    let at = extent::item(statement, from).ok_or("its text does not read back")?;
    if select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)))
    {
        return Err("its select list holds a bare *".into());
    }
    if !named_with_relations(select) {
        return Err("a column of its select is named without its relation".into());
    }
    let mut names = Vec::new();
    for factor in &factors {
        if matches!(factor, TableFactor::NestedJoin { .. }) {
            return Err("it holds a parenthesised join".into());
        }
        let name = super::conditions::range_name(factor).ok_or("a relation of it has no name")?;
        if names.contains(&name) {
            return Err(format!("two of its relations are named {name}"));
        }
        names.push(name);
    }
    let using = from
        .joins
        .iter()
        .any(|j| matches!(constraint(&j.join_operator), Some(JoinConstraint::Using(_))));
    let withs = Withs::of(&statement.ast);
    let reader = Reader {
        stood: &read.stood,
        withs: &withs,
        resolve,
        depth: 0,
    };
    let spans: Vec<(usize, usize)> = std::iter::once(at.relation)
        .chain(at.joins.iter().map(|j| j.factor))
        .collect();
    let mut rels = Vec::new();
    for (r, factor) in factors.iter().enumerate() {
        let columns = if using {
            match reader.factor(factor) {
                Columns::Named(columns) => columns,
                Columns::Unread(why) => return Err(format!("{}: {why}", names[r])),
            }
        } else {
            vec![]
        };
        let lateral = !matches!(
            factor,
            TableFactor::Table { .. } | TableFactor::Derived { lateral: false, .. }
        );
        let reads = if lateral {
            qualifiers(factor)
                .iter()
                .filter_map(|q| names.iter().position(|n| n == q))
                .filter(|&s| s != r)
                .collect()
        } else {
            BTreeSet::new()
        };
        rels.push(Rel {
            name: names[r].clone(),
            factor: (*factor).clone(),
            text: read.text(statement, spans[r].0, spans[r].1).to_string(),
            columns,
            reads,
        });
    }
    // Each USING column's relations, as the written order merges them.
    let mut held: Vec<(String, BTreeSet<usize>)> = rels[0]
        .columns
        .iter()
        .map(|c| (c.clone(), BTreeSet::from([0])))
        .collect();
    let mut classes: Vec<(String, String, BTreeSet<usize>)> = Vec::new();
    let mut conditions = Vec::new();
    for (j, join) in from.joins.iter().enumerate() {
        let r = j + 1;
        match constraint(&join.join_operator) {
            Some(JoinConstraint::Using(columns)) => {
                let using_at = statement
                    .after(at.joins[j].factor.1)
                    .ok_or("a USING does not read back")?;
                let mut words = Vec::new();
                let mut t = using_at;
                while t <= at.joins[j].last {
                    if let Token::Word(_) = &statement.tokens[t].token {
                        if t != using_at {
                            words.push(read.text(statement, t, t).to_string());
                        }
                    }
                    t += 1;
                }
                if words.len() != columns.len() {
                    return Err("a USING list does not read back".into());
                }
                for (column, word) in columns.iter().zip(words) {
                    let [ObjectNamePart::Identifier(ident)] = &column.0[..] else {
                        return Err("a USING column is not a plain name".into());
                    };
                    let k = normal(ident);
                    let holders: Vec<usize> = held
                        .iter()
                        .enumerate()
                        .filter(|(_, (c, _))| *c == k)
                        .map(|(h, _)| h)
                        .collect();
                    let [h] = holders[..] else {
                        return Err(format!("{k} is not held once before its USING"));
                    };
                    held[h].1.insert(r);
                    match classes.iter_mut().find(|(c, _, _)| *c == k) {
                        Some((_, _, members)) => members.extend(held[h].1.iter().copied()),
                        None => classes.push((k.clone(), word, held[h].1.clone())),
                    }
                }
                for column in &rels[r].columns {
                    if !columns.iter().any(|c| c.to_string() == *column || matches!(&c.0[..], [ObjectNamePart::Identifier(i)] if normal(i) == *column)) {
                        held.push((column.clone(), BTreeSet::from([r])));
                    }
                }
            }
            Some(JoinConstraint::On(on)) => {
                let chain = at.joins[j].on.as_ref().ok_or("an ON does not read back")?;
                for (n, conjunct) in super::place::conjuncts(on).into_iter().enumerate() {
                    let named: BTreeSet<usize> = qualifiers_of(conjunct)
                        .iter()
                        .filter_map(|q| names.iter().position(|name| name == q))
                        .collect();
                    if named.is_empty() {
                        return Err("a condition of its ON names none of its relations".into());
                    }
                    let (first, last) = chain.conjuncts[n];
                    let mut text = read.text(statement, first, last).to_string();
                    if extent::or(conjunct) {
                        text = format!("({text})");
                    }
                    conditions.push(Condition {
                        expr: conjunct.clone(),
                        text,
                        names: named,
                    });
                }
                for column in &rels[r].columns {
                    held.push((column.clone(), BTreeSet::from([r])));
                }
            }
            _ => {
                for column in &rels[r].columns {
                    held.push((column.clone(), BTreeSet::from([r])));
                }
            }
        }
    }
    let (_, gap_start) = read.written(statement, at.relation.1, at.relation.1);
    let (gap_end, _) = read.written(statement, at.joins[0].operator.0, at.joins[0].operator.0);
    let separator = read.stood.text[gap_start..gap_end].to_string();
    // A comment among the joins would be lost when they are written again.
    let between_relations = |t: usize| {
        spans.iter().any(|(a, b)| (*a..=*b).contains(&t))
            || at.joins.iter().any(|j| {
                j.on.as_ref()
                    .is_some_and(|on| on.conjuncts.iter().any(|(a, b)| (*a..=*b).contains(&t)))
            })
    };
    for t in at.relation.0..=at.last() {
        if matches!(
            statement.tokens[t].token,
            Token::Whitespace(
                Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_)
            )
        ) && !between_relations(t)
        {
            return Err("a comment stands among its joins".into());
        }
    }
    Ok(Model {
        rels,
        classes,
        conditions,
        separator,
        using,
    })
}

/// How each relation of `order` after the first is joined, where the order returns the rows the
/// written one does; why not, where it does not.
fn joined(model: &Model, order: &[usize]) -> Result<Vec<By>, String> {
    let mut done: BTreeSet<usize> = BTreeSet::new();
    let mut held: Vec<(String, BTreeSet<usize>)> = Vec::new();
    let mut by = Vec::new();
    for (p, &r) in order.iter().enumerate() {
        let rel = &model.rels[r];
        if let Some(missing) = rel.reads.iter().find(|s| !done.contains(s)) {
            return Err(format!(
                "{} reads {}, which is joined after it",
                rel.name, model.rels[*missing].name
            ));
        }
        let keys: Vec<usize> = model
            .classes
            .iter()
            .enumerate()
            .filter(|(_, (_, _, members))| {
                members.contains(&r) && members.iter().any(|m| done.contains(m))
            })
            .map(|(c, _)| c)
            .collect();
        for &c in &keys {
            let (column, _, members) = &model.classes[c];
            let holders: Vec<usize> = held
                .iter()
                .enumerate()
                .filter(|(_, (k, _))| k == column)
                .map(|(h, _)| h)
                .collect();
            let [h] = holders[..] else {
                return Err(format!(
                    "{column} is held {} times before {}'s USING",
                    holders.len(),
                    rel.name
                ));
            };
            if !held[h].1.is_subset(members)
                || rel.columns.iter().filter(|k| *k == column).count() != 1
            {
                return Err(format!(
                    "{column} is not held once where {} joins by it",
                    rel.name
                ));
            }
            held[h].1.insert(r);
        }
        for column in &rel.columns {
            if !keys.iter().any(|&c| model.classes[c].0 == *column) {
                held.push((column.clone(), BTreeSet::from([r])));
            }
        }
        done.insert(r);
        if p > 0 {
            by.push(if keys.is_empty() {
                By::Cross
            } else {
                By::Using(keys)
            });
        }
    }
    for (c, condition) in model.conditions.iter().enumerate() {
        let mut joined_so_far = BTreeSet::from([order[0]]);
        let mut placed = false;
        for (p, &r) in order.iter().enumerate().skip(1) {
            joined_so_far.insert(r);
            if !condition.names.is_subset(&joined_so_far) {
                continue;
            }
            match &mut by[p - 1] {
                By::Using(_) => continue,
                By::On(on) => on.push(c),
                cross @ By::Cross => *cross = By::On(vec![c]),
            }
            placed = true;
            break;
        }
        if !placed {
            return Err(format!("{} has no ON to sit in", condition.text));
        }
    }
    Ok(by)
}

/// The list's text in `order`, and the `FROM` item it reads as.
fn written(
    model: &Model,
    order: &[usize],
    by: &[By],
    item: &TableWithJoins,
) -> (String, TableWithJoins) {
    let mut text = model.rels[order[0]].text.clone();
    let mut joins = Vec::new();
    for (p, &r) in order.iter().enumerate().skip(1) {
        let rel = &model.rels[r];
        text.push_str(&model.separator);
        let operator = match &by[p - 1] {
            By::Cross => {
                text.push_str(&format!("CROSS JOIN {}", rel.text));
                JoinOperator::CrossJoin(JoinConstraint::None)
            }
            By::Using(keys) => {
                let words: Vec<&str> = keys.iter().map(|&c| model.classes[c].1.as_str()).collect();
                text.push_str(&format!("JOIN {} USING ({})", rel.text, words.join(", ")));
                JoinOperator::Join(JoinConstraint::Using(
                    keys.iter()
                        .map(|&c| {
                            ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                                model.classes[c].0.clone(),
                            ))])
                        })
                        .collect(),
                ))
            }
            By::On(conditions) => {
                let texts: Vec<&str> = conditions
                    .iter()
                    .map(|&c| model.conditions[c].text.as_str())
                    .collect();
                text.push_str(&format!("JOIN {} ON {}", rel.text, texts.join(" AND ")));
                JoinOperator::Join(JoinConstraint::On(
                    super::place::conjoined(
                        conditions
                            .iter()
                            .map(|&c| model.conditions[c].expr.clone())
                            .collect(),
                    )
                    .expect("a condition"),
                ))
            }
        };
        joins.push(Join {
            relation: rel.factor.clone(),
            global: false,
            join_operator: operator,
        });
    }
    let _ = item;
    (
        text,
        TableWithJoins {
            relation: model.rels[order[0]].factor.clone(),
            joins,
        },
    )
}

/// Every order of the list's relations that returns the rows the written one does, the written
/// order first.
pub(crate) fn orders(model: &Model) -> Vec<Vec<usize>> {
    let n = model.rels.len();
    let mut all = Vec::new();
    let mut order: Vec<usize> = (0..n).collect();
    permutations(&mut order, 0, &mut |order| {
        if joined(model, order).is_ok() {
            all.push(order.to_vec());
        }
    });
    all.sort_by_key(|order| *order != (0..n).collect::<Vec<_>>());
    all
}

/// Call `each` with every order of `order[at..]` after `order[..at]`.
fn permutations(order: &mut Vec<usize>, at: usize, each: &mut impl FnMut(&[usize])) {
    if at == order.len() {
        each(order);
        return;
    }
    for i in at..order.len() {
        let moved = order.remove(i);
        order.insert(at, moved);
        permutations(order, at + 1, each);
        let moved = order.remove(at);
        order.insert(i, moved);
    }
}

/// How many columns each relation of the list is joined by, read from its `USING` lists and its
/// `ON`s' equalities between columns.
pub(crate) fn widths(model: &Model) -> Vec<usize> {
    let mut keys: Vec<BTreeSet<String>> = vec![BTreeSet::new(); model.rels.len()];
    for (column, _, members) in &model.classes {
        for &m in members {
            keys[m].insert(column.clone());
        }
    }
    for condition in &model.conditions {
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = &condition.expr
        {
            for (this, other) in [(left, right), (right, left)] {
                if let (Expr::CompoundIdentifier(a), Expr::CompoundIdentifier(b)) =
                    (this.as_ref(), other.as_ref())
                {
                    if let (Some(r), Some(_)) = (
                        model.position(&normal(&a[a.len() - 2])),
                        model.position(&normal(&b[b.len() - 2])),
                    ) {
                        keys[r].insert(normal(&a[a.len() - 1]));
                    }
                }
            }
        }
    }
    keys.iter().map(BTreeSet::len).collect()
}

/// The order outermost first puts the list in: the relation joined by the fewest columns first,
/// then, a join at a time, the one joined by the fewest of those its `USING` or its `ON` joins to
/// the relations already placed; relations joined by as many stay as written.
pub(crate) fn outermost_first(model: &Model) -> Vec<usize> {
    let widths = widths(model);
    let n = model.rels.len();
    let mut placed: Vec<usize> = Vec::new();
    while placed.len() < n {
        let joined_to = |r: usize| {
            model.classes.iter().any(|(_, _, members)| {
                members.contains(&r) && placed.iter().any(|p| members.contains(p))
            }) || model
                .conditions
                .iter()
                .any(|c| c.names.contains(&r) && c.names.iter().any(|m| placed.contains(m)))
        };
        let left: Vec<usize> = (0..n).filter(|r| !placed.contains(r)).collect();
        let reachable: Vec<usize> = left.iter().copied().filter(|&r| joined_to(r)).collect();
        let from = if placed.is_empty() || reachable.is_empty() {
            left
        } else {
            reachable
        };
        let next = *from
            .iter()
            .min_by_key(|&&r| (widths[r], r))
            .expect("a relation is left");
        placed.push(next);
    }
    placed
}

/// The relations a factor's columns are named with, at any depth.
fn qualifiers(factor: &TableFactor) -> BTreeSet<String> {
    let mut found = Qualifiers::default();
    let _ = factor.visit(&mut found);
    found.0
}

/// The relations an expression's columns are named with.
fn qualifiers_of(expr: &Expr) -> BTreeSet<String> {
    let mut found = Qualifiers::default();
    let _ = expr.visit(&mut found);
    found.0
}

#[derive(Default)]
struct Qualifiers(BTreeSet<String>);

impl Visitor for Qualifiers {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if let Expr::CompoundIdentifier(parts) = expr {
            if parts.len() >= 2 {
                self.0.insert(normal(&parts[parts.len() - 2]));
            }
        }
        ControlFlow::Continue(())
    }
}

/// Whether every column `select` names, outside the subqueries of its `FROM` that read nothing
/// beside them, is named with its relation.
fn named_with_relations(select: &Select) -> bool {
    struct Plain(bool);
    impl Visitor for Plain {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if matches!(expr, Expr::Identifier(_)) {
                self.0 = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let mut plain = Plain(false);
    let mut bare = select.clone();
    for from in &mut bare.from {
        let factors = std::iter::once(&mut from.relation)
            .chain(from.joins.iter_mut().map(|j| &mut j.relation));
        for factor in factors {
            if let TableFactor::Derived {
                lateral: false,
                subquery,
                ..
            } = factor
            {
                *subquery.body = SetExpr::Values(sqlparser::ast::Values {
                    explicit_row: false,
                    value_keyword: false,
                    rows: vec![],
                });
            }
        }
    }
    let _ = bare.visit(&mut plain);
    !plain.0
}

/// Each join list of `read` with its orders.
pub(crate) fn places(read: &Read, resolve: Resolve, want: Option<Want>) -> Vec<Place> {
    let mut places = Vec::new();
    for (at, statement) in read.parsed() {
        let mut selects = Selects::default();
        let _ = statement.ast.visit(&mut selects);
        for (k, select) in selects.0.iter().enumerate() {
            for item in 0..select.from.len() {
                if select.from[item].joins.is_empty() {
                    continue;
                }
                let Ok(model) = model(read, statement, select, item, resolve) else {
                    continue;
                };
                let written_order: Vec<usize> = (0..model.len()).collect();
                let rule = outermost_first(&model);
                let picks_of = |order: &[usize]| {
                    if order == rule.as_slice() {
                        vec![OUTERMOST]
                    } else {
                        vec![]
                    }
                };
                if passed(want, &picks_of(&written_order)) {
                    continue;
                }
                let Some(place) = list_place(
                    read, at, statement, k, select, item, &model, want, &picks_of,
                ) else {
                    continue;
                };
                places.push(place);
                if enough(want, &places) {
                    return places;
                }
            }
        }
    }
    places
}

/// The place of one join list, each order a spelling of it.
#[allow(clippy::too_many_arguments)]
fn list_place(
    read: &Read,
    at: usize,
    statement: &Stmt,
    k: usize,
    select: &Select,
    item: usize,
    model: &Model,
    want: Option<Want>,
    picks_of: &dyn Fn(&[usize]) -> Vec<&'static str>,
) -> Option<Place> {
    let from = &select.from[item];
    let extent = extent::item(statement, from)?;
    let (start, end) = read.written(statement, extent.relation.0, extent.last());
    let n = model.len();
    let written_order: Vec<usize> = (0..n).collect();
    let mut choices = vec![Choice::written(
        model.names(&written_order).join(", "),
        picks_of(&written_order),
        &statement.ast,
    )];
    let all = if want.is_some() {
        let rule = outermost_first(model);
        if rule == written_order {
            vec![]
        } else {
            vec![rule]
        }
    } else {
        orders(model).into_iter().skip(1).collect()
    };
    for order in all {
        let Ok(by) = joined(model, &order) else {
            continue;
        };
        let (text, written_item) = written(model, &order, &by, from);
        let mut expected = statement.ast.clone();
        change_select(&mut expected, k, |select| select.from[item] = written_item);
        choices.push(Choice {
            name: model.names(&order).join(", "),
            picks: picks_of(&order),
            edits: vec![(start, end, text)],
            expected,
        });
    }
    if choices.len() < 2 {
        return None;
    }
    Some(Place {
        statement: at,
        relations: Some(model.len()),
        what: format!("the join of {}", model.names(&written_order).join(", ")),
        choices,
    })
}

#[cfg(test)]
mod tests {
    use super::super::laws::{holds, Trap};
    use super::super::{written_in, Axis};
    use super::*;
    use crate::mock::MockTable;

    /// The only join list of `text`, read.
    fn the_model(text: &str) -> Model {
        let read = Read::new(text).unwrap();
        let (_, statement) = read.parsed().next().unwrap();
        let mut selects = Selects::default();
        let _ = statement.ast.visit(&mut selects);
        model(&read, statement, &selects.0[0], 0, &|_| None).unwrap()
    }

    fn why_kept(text: &str) -> String {
        let read = Read::new(text).unwrap();
        let (_, statement) = read.parsed().next().unwrap();
        let mut selects = Selects::default();
        let _ = statement.ast.visit(&mut selects);
        model(&read, statement, &selects.0[0], 0, &|_| None).unwrap_err()
    }

    fn written(text: &str) -> String {
        written_in(text, Axis::JoinOrder, OUTERMOST, &|_| None)
    }

    const SCOPE_SETS: &str = "SELECT s.set_num, scope.name AS theme_group\nFROM lego_sets s\nJOIN (\n    :compose(@scope)\n) sc ON sc.theme_id = s.theme_id\nJOIN lego_themes scope ON scope.id = sc.scope_id\nWHERE s.year >= :bind(min_year)";

    const LAYERS: &str = "SELECT d.filing, d.layer, n.size, s.name\nFROM (SELECT x.filing, x.layer, x.size FROM demand x) d\nJOIN (SELECT y.filing, y.layer, y.size FROM nameplate y) n USING (filing, layer)\nJOIN (SELECT z.filing, z.name FROM scope z) s USING (filing)";

    #[test]
    fn every_order_of_three_relations_joined_on_is_counted_and_written() {
        let model = the_model(SCOPE_SETS);
        let orders = orders(&model);
        assert_eq!(orders.len(), 6);
        let read = Read::new(SCOPE_SETS).unwrap();
        let places = places(&read, &|_| None, None);
        let [place] = &places[..] else {
            panic!("{places:?}");
        };
        let checked = super::super::place::checked(&read, place.clone());
        assert_eq!(checked.choices.len(), 6);
        let spelled: Vec<String> = checked
            .choices
            .iter()
            .map(|c| super::super::place::apply(SCOPE_SETS, &c.edits).unwrap())
            .collect();
        assert!(spelled.contains(&"SELECT s.set_num, scope.name AS theme_group\nFROM lego_themes scope\nJOIN (\n    :compose(@scope)\n) sc ON scope.id = sc.scope_id\nJOIN lego_sets s ON sc.theme_id = s.theme_id\nWHERE s.year >= :bind(min_year)".to_string()), "{spelled:#?}");
        assert!(spelled.contains(&"SELECT s.set_num, scope.name AS theme_group\nFROM lego_sets s\nCROSS JOIN lego_themes scope\nJOIN (\n    :compose(@scope)\n) sc ON sc.theme_id = s.theme_id AND scope.id = sc.scope_id\nWHERE s.year >= :bind(min_year)".to_string()), "{spelled:#?}");
    }

    #[test]
    fn outermost_first_puts_the_narrowest_key_first_and_extends_it_a_join_at_a_time() {
        assert_eq!(
            the_model(SCOPE_SETS).names(&outermost_first(&the_model(SCOPE_SETS))),
            ["s", "sc", "scope"]
        );
        assert_eq!(written(SCOPE_SETS), SCOPE_SETS);
        assert_eq!(
            written(LAYERS),
            "SELECT d.filing, d.layer, n.size, s.name\nFROM (SELECT z.filing, z.name FROM scope z) s\nJOIN (SELECT x.filing, x.layer, x.size FROM demand x) d USING (filing)\nJOIN (SELECT y.filing, y.layer, y.size FROM nameplate y) n USING (filing, layer)"
        );
        assert_eq!(written(&written(LAYERS)), written(LAYERS));
    }

    #[test]
    fn an_order_a_using_cannot_take_is_not_offered() {
        let text = "SELECT a.k, b.v, d.w\nFROM (SELECT p.k, p.id FROM p) a\nJOIN (SELECT q.k, q.v FROM q) b USING (k)\nJOIN (SELECT r.k, r.id, r.w FROM r) d ON d.id = a.id";
        let model = the_model(text);
        let orders: Vec<Vec<String>> = orders(&model).iter().map(|o| model.names(o)).collect();
        assert_eq!(orders, [["a", "b", "d"], ["b", "a", "d"]]);
    }

    #[test]
    fn a_lateral_relation_follows_what_it_reads() {
        let text = "SELECT n.builder_id FROM lego_cities c CROSS JOIN LATERAL (SELECT b.builder_id FROM lego_builders b WHERE b.city = c.city_id) n";
        assert_eq!(orders(&the_model(text)).len(), 1);
        assert!(places(&Read::new(text).unwrap(), &|_| None, None).is_empty());
    }

    #[test]
    fn a_join_list_that_breaks_a_condition_stays_as_written_and_says_why() {
        for (text, why) in [
            (
                "SELECT * FROM a JOIN b ON b.id = a.id",
                "its select list holds a bare *",
            ),
            (
                "SELECT a.id, name FROM a JOIN b ON b.id = a.id",
                "a column of its select is named without its relation",
            ),
            ("SELECT a.k FROM a JOIN b USING (k)", "a: a, a table"),
            (
                "SELECT a.id FROM a LEFT JOIN b ON b.id = a.id",
                "a join of it is not inner",
            ),
            (
                "SELECT a.id FROM a JOIN b ON true",
                "a condition of its ON names none of its relations",
            ),
            (
                "SELECT a.id FROM a\n-- the b's\nJOIN b ON b.id = a.id",
                "a comment stands among its joins",
            ),
            (
                "SELECT k FROM (SELECT p.k FROM p) a JOIN (SELECT q.k FROM q) b USING (k)",
                "a column of its select is named without its relation",
            ),
        ] {
            assert_eq!(why_kept(text), why, "{text}");
            assert_eq!(
                written_in(text, Axis::JoinOrder, OUTERMOST, &|_| None),
                text
            );
        }
    }

    /// Two relations a `USING` joins by a numeric that holds 1.0 in one and 1.00 in the other,
    /// and a third joined `ON`, with a duplicate and a NULL.
    fn numerics() -> Trap {
        let d = MockTable::new("d")
            .column("k", "numeric")
            .column("v", "integer")
            .row(&[Some("1.0"), Some("10")])
            .row(&[Some("2.0"), Some("20")])
            .row(&[Some("2.0"), Some("20")])
            .row(&[None, Some("30")]);
        let n = MockTable::new("n")
            .column("k", "numeric")
            .column("w", "integer")
            .row(&[Some("1.00"), Some("100")])
            .row(&[Some("2.00"), Some("200")])
            .row(&[None, Some("300")]);
        let e = MockTable::new("e")
            .column("w", "integer")
            .column("label", "varchar")
            .row(&[Some("100"), Some("one")])
            .row(&[Some("200"), Some("two")])
            .row(&[Some("200"), Some("deux")]);
        Trap {
            tables: vec![d, n, e],
            binds: vec![],
            templates: vec![],
        }
    }

    #[test]
    #[ignore = "needs a PostgreSQL server: set SQLC_TEST_POSTGRES_URL"]
    fn law_every_order_of_inner_joins_returns_the_same_rows() {
        let trap = numerics();
        let unqualified = (
            "SELECT k, x.v FROM (SELECT d.k, d.v FROM d) x JOIN (SELECT n.k, n.w FROM n) y USING (k)",
            "SELECT k, x.v FROM (SELECT n.k, n.w FROM n) y JOIN (SELECT d.k, d.v FROM d) x USING (k)",
        );
        let star = (
            "SELECT * FROM n y JOIN e z ON z.w = y.w",
            "SELECT * FROM e z JOIN n y ON z.w = y.w",
        );
        for text in [unqualified.0, star.0] {
            assert_eq!(
                written_in(text, Axis::JoinOrder, OUTERMOST, &|_| None),
                text
            );
        }
        let text = "SELECT x.k, x.v, y.w, z.label\nFROM (SELECT d.k, d.v FROM d) x\nJOIN (SELECT n.k, n.w FROM n) y USING (k)\nJOIN (SELECT e.w, e.label FROM e) z ON z.w = y.w";
        let read = Read::new(text).unwrap();
        let places = places(&read, &|_| None, None);
        let [place] = &places[..] else {
            panic!("{places:?}");
        };
        let checked = super::super::place::checked(&read, place.clone());
        let names: Vec<&str> = checked.choices.iter().map(|c| c.name.as_str()).collect();
        // Where y comes last its USING takes no ON, and z.w = y.w has nowhere to sit.
        assert_eq!(names, ["x, y, z", "y, x, z", "y, z, x", "z, y, x"]);
        for choice in &checked.choices[1..] {
            let spelled = super::super::place::apply(text, &choice.edits).unwrap();
            for near in [unqualified, star] {
                assert_eq!(holds(text, &spelled, near, &trap).written, 5, "{spelled}");
            }
        }
    }
}
