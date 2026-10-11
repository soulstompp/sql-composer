//! Where a condition sits: in an inner join's `ON`, or in the `WHERE`.
//!
//! A conjunct moves between the `WHERE` and the `ON` of a join where the join, and every join
//! after it in its `FROM` item, is inner: `JOIN … ON`, `INNER JOIN … ON` or `CROSS JOIN`. It moves
//! only where its text says which relations it reads: every column is named with its relation,
//! it reads one relation of the item at least, and it holds no query. Its places are the `WHERE`
//! and the `ON` of each such join where every relation it names is joined.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{Expr, JoinConstraint, JoinOperator, Select, TableFactor, Visit, Visitor};
use sqlparser::keywords::Keyword;

use super::extent::{self, Chain, ItemAt};
use super::join_order::Selects;
use super::place::{
    change_select, conjoined, conjuncts, enough, one_line, passed, wants, Choice, Edits, Place,
    Want,
};
use super::read::{Read, Stmt};
use crate::parsed::normal;

/// The pick that puts each condition in the first `ON` where every relation it names is joined.
pub(crate) const ON: &str = "on";
/// The pick that puts each condition in the `WHERE`.
pub(crate) const WHERE: &str = "where";

/// Where a condition is, or can be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Spot {
    Where,
    /// The `ON` of join `join` (from 0) of the `FROM` item `item`.
    On {
        item: usize,
        join: usize,
    },
}

/// Each condition of `read` that can sit in more than one place, with each place.
pub(crate) fn places(read: &Read, want: Option<Want>) -> Vec<Place> {
    let mut places = Vec::new();
    for (at, statement) in read.parsed() {
        let mut selects = Selects::default();
        let _ = statement.ast.visit(&mut selects);
        for (k, select) in selects.0.iter().enumerate() {
            places.extend(select_places(read, at, statement, k, select, want));
            if enough(want, &places) {
                return places;
            }
        }
    }
    places.retain(|place| place.choices.len() > 1);
    places
}

/// The places of the conditions of one select.
fn select_places(
    read: &Read,
    at: usize,
    statement: &Stmt,
    k: usize,
    select: &Select,
    want: Option<Want>,
) -> Vec<Place> {
    let Some(form) = Form::new(statement, select) else {
        return vec![];
    };
    let mut written: Vec<(Spot, usize, &Expr)> = Vec::new();
    if let Some(selection) = &select.selection {
        for (n, conjunct) in conjuncts(selection).into_iter().enumerate() {
            written.push((Spot::Where, n, conjunct));
        }
    }
    for (i, from) in select.from.iter().enumerate() {
        for (j, join) in from.joins.iter().enumerate() {
            if let Some(JoinConstraint::On(on)) = constraint(&join.join_operator) {
                for (n, conjunct) in conjuncts(on).into_iter().enumerate() {
                    written.push((Spot::On { item: i, join: j }, n, conjunct));
                }
            }
        }
    }
    let mut places = Vec::new();
    for (spot, n, conjunct) in written {
        let Some(spots) = form.spots(conjunct) else {
            continue;
        };
        if spots.len() < 2 || !spots.contains(&spot) {
            continue;
        }
        let text = form.conjunct_text(read, statement, spot, n);
        let first_on = spots.iter().find(|s| matches!(s, Spot::On { .. })).copied();
        let picks_of = |s: Spot| -> Vec<&'static str> {
            match s {
                Spot::Where => vec![WHERE],
                s if Some(s) == first_on => vec![ON],
                _ => vec![],
            }
        };
        if passed(want, &picks_of(spot)) {
            continue;
        }
        let mut choices = vec![Choice::written(
            form.name(spot),
            picks_of(spot),
            &statement.ast,
        )];
        for &to in spots.iter().filter(|s| **s != spot) {
            if want.is_some() && !picks_of(to).iter().any(|p| wants(want, p)) {
                continue;
            }
            let Some(edits) = form.moved(read, statement, spot, n, to) else {
                continue;
            };
            let mut expected = statement.ast.clone();
            change_select(&mut expected, k, |select| moved(select, spot, n, to));
            choices.push(Choice {
                name: form.name(to),
                picks: picks_of(to),
                edits,
                expected,
            });
        }
        if choices.len() < 2 {
            continue;
        }
        places.push(Place {
            statement: at,
            relations: None,
            what: format!("the condition {}", one_line(&text)),
            choices,
        });
        if enough(want, &places) {
            break;
        }
    }
    places
}

/// `select` with conjunct `n` at `from` moved to `to`.
fn moved(select: &mut Select, from: Spot, n: usize, to: Spot) {
    let taken = match from {
        Spot::Where => {
            let mut all: Vec<Expr> = conjuncts(select.selection.as_ref().expect("a WHERE"))
                .into_iter()
                .cloned()
                .collect();
            let taken = all.remove(n);
            select.selection = conjoined(all);
            taken
        }
        Spot::On { item, join } => {
            let join = &mut select.from[item].joins[join];
            let Some(JoinConstraint::On(on)) = constraint(&join.join_operator) else {
                unreachable!("a condition taken from an ON");
            };
            let mut all: Vec<Expr> = conjuncts(on).into_iter().cloned().collect();
            let taken = all.remove(n);
            join.join_operator = match conjoined(all) {
                Some(on) => with_on(&join.join_operator, on),
                None => JoinOperator::CrossJoin(JoinConstraint::None),
            };
            taken
        }
    };
    match to {
        Spot::Where => {
            let mut all: Vec<Expr> = select
                .selection
                .iter()
                .flat_map(|s| conjuncts(s).into_iter().cloned())
                .collect();
            all.push(taken);
            select.selection = conjoined(all);
        }
        Spot::On { item, join } => {
            let join = &mut select.from[item].joins[join];
            join.join_operator = match constraint(&join.join_operator) {
                Some(JoinConstraint::On(on)) => {
                    let mut all: Vec<Expr> = conjuncts(on).into_iter().cloned().collect();
                    all.push(taken);
                    with_on(&join.join_operator, conjoined(all).expect("a conjunct"))
                }
                _ => JoinOperator::Join(JoinConstraint::On(taken)),
            };
        }
    }
}

/// `operator` joined `ON` `on` instead.
fn with_on(operator: &JoinOperator, on: Expr) -> JoinOperator {
    match operator {
        JoinOperator::Inner(_) => JoinOperator::Inner(JoinConstraint::On(on)),
        _ => JoinOperator::Join(JoinConstraint::On(on)),
    }
}

/// The condition a join is joined by.
fn constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    super::join_order::constraint(operator)
}

/// Whether `operator` is an inner join: `JOIN`, `INNER JOIN` or `CROSS JOIN`.
pub(crate) fn inner(operator: &JoinOperator) -> bool {
    matches!(
        operator,
        JoinOperator::Join(_) | JoinOperator::Inner(_) | JoinOperator::CrossJoin(_)
    )
}

/// A select's relations and where its conditions stand.
struct Form<'s> {
    /// The name each relation of each `FROM` item is read by.
    names: Vec<Vec<Option<String>>>,
    /// Where each `FROM` item stands.
    items: Vec<ItemAt>,
    /// Where the `WHERE` stands, where it has one.
    selection: Option<Chain>,
    /// Whether join `j` of item `i` can take a condition, and every join after it is inner.
    open: Vec<Vec<bool>>,
    select: &'s Select,
}

impl<'s> Form<'s> {
    /// The form of `select`; none where a part of it does not read back.
    fn new(statement: &Stmt, select: &'s Select) -> Option<Self> {
        let mut names = Vec::new();
        let mut items = Vec::new();
        let mut open = Vec::new();
        for from in &select.from {
            let mut item_names = vec![range_name(&from.relation)];
            item_names.extend(from.joins.iter().map(|j| range_name(&j.relation)));
            names.push(item_names);
            items.push(extent::item(statement, from)?);
            let mut item_open = Vec::new();
            for (j, join) in from.joins.iter().enumerate() {
                let takes = matches!(
                    join.join_operator,
                    JoinOperator::Join(JoinConstraint::On(_))
                        | JoinOperator::Inner(JoinConstraint::On(_))
                        | JoinOperator::CrossJoin(JoinConstraint::None)
                );
                let under = from.joins[j..].iter().all(|j| inner(&j.join_operator));
                item_open.push(takes && under);
            }
            open.push(item_open);
        }
        let selection = match &select.selection {
            Some(selection) => Some(extent::chain(statement, selection, Keyword::WHERE)?),
            None => None,
        };
        Some(Form {
            names,
            items,
            selection,
            open,
            select,
        })
    }

    /// Every place `conjunct` can sit; none where its text does not say what it reads.
    fn spots(&self, conjunct: &Expr) -> Option<Vec<Spot>> {
        let read = Reads::of(conjunct)?;
        let mut item = None;
        let mut reach = 0;
        for name in &read.0 {
            let found = self.names.iter().enumerate().find_map(|(i, names)| {
                names
                    .iter()
                    .position(|n| n.as_deref() == Some(name.as_str()))
                    .map(|j| (i, j))
            });
            if let Some((i, j)) = found {
                if item.is_some_and(|item| item != i) {
                    return None;
                }
                item = Some(i);
                reach = reach.max(j);
            }
        }
        let item = item?;
        let mut spots = vec![Spot::Where];
        for (j, open) in self.open[item].iter().enumerate() {
            if *open && j + 1 >= reach {
                spots.push(Spot::On { item, join: j });
            }
        }
        Some(spots)
    }

    /// The spot in words.
    fn name(&self, spot: Spot) -> String {
        match spot {
            Spot::Where => "in the WHERE".into(),
            Spot::On { item, join } => match &self.names[item][join + 1] {
                Some(name) => format!("in the ON joining {name}"),
                None => format!("in the ON of join {}", join + 1),
            },
        }
    }

    /// The chain conjunct `n` at `spot` is in.
    fn chain(&self, spot: Spot) -> Option<&Chain> {
        match spot {
            Spot::Where => self.selection.as_ref(),
            Spot::On { item, join } => self.items[item].joins[join].on.as_ref(),
        }
    }

    /// The template's text of conjunct `n` at `spot`.
    fn conjunct_text(&self, read: &Read, statement: &Stmt, spot: Spot, n: usize) -> String {
        let (first, last) = self.chain(spot).expect("a written condition").conjuncts[n];
        read.text(statement, first, last).to_string()
    }

    /// The edits that move conjunct `n` at `from` to `to`.
    fn moved(
        &self,
        read: &Read,
        statement: &Stmt,
        from: Spot,
        n: usize,
        to: Spot,
    ) -> Option<Edits> {
        let chain = self.chain(from)?;
        let (first, last) = chain.conjuncts[n];
        let conjunct = conjuncts(self.condition(from)?)[n];
        let mut text = read.text(statement, first, last).to_string();
        if extent::or(conjunct) {
            text = format!("({text})");
        }
        // Take it out where it is.
        let mut edits = extent::without(read, statement, chain, n)?;
        if chain.conjuncts.len() == 1 {
            if let Spot::On { item, join } = from {
                let (op_first, op_join) = self.items[item].joins[join].operator;
                let (a, b) = read.written(statement, op_first, op_join);
                edits.push((a, b, "CROSS JOIN".into()));
            }
        }
        // Put it where it goes.
        match to {
            Spot::Where => match &self.selection {
                Some(selection) => edits.extend(extent::appended(
                    read,
                    statement,
                    selection,
                    self.condition(Spot::Where)?,
                    &text,
                )),
                None => {
                    let last = self.items.last()?.last();
                    let select = statement.first_token(&self.select.select_token.0)?;
                    let lead = extent::lead(read, statement, select, last);
                    let (_, at) = read.written(statement, last, last);
                    edits.push((at, at, format!("{lead}WHERE {text}")));
                }
            },
            Spot::On { item, join } => {
                let at = &self.items[item].joins[join];
                match &at.on {
                    Some(on) => edits.extend(extent::appended(
                        read,
                        statement,
                        on,
                        self.condition(to)?,
                        &text,
                    )),
                    None => {
                        let (a, b) = read.written(statement, at.operator.0, at.operator.1);
                        edits.push((a, b, "JOIN".into()));
                        let (_, end) = read.written(statement, at.factor.1, at.factor.1);
                        edits.push((end, end, format!(" ON {text}")));
                    }
                }
            }
        }
        Some(edits)
    }

    /// The condition at `spot`.
    fn condition(&self, spot: Spot) -> Option<&'s Expr> {
        match spot {
            Spot::Where => self.select.selection.as_ref(),
            Spot::On { item, join } => {
                match constraint(&self.select.from[item].joins[join].join_operator) {
                    Some(JoinConstraint::On(on)) => Some(on),
                    _ => None,
                }
            }
        }
    }
}

/// The name a relation of a `FROM` is read by: its alias, or a table's own name.
pub(crate) fn range_name(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table { name, alias, .. } => match alias {
            Some(alias) => Some(normal(&alias.name)),
            None => match name.0.last()? {
                sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(normal(ident)),
                _ => None,
            },
        },
        TableFactor::Derived { alias, .. }
        | TableFactor::Function { alias, .. }
        | TableFactor::UNNEST { alias, .. } => alias.as_ref().map(|a| normal(&a.name)),
        _ => None,
    }
}

/// The relations a condition names, each by the name its columns are named with.
struct Reads(BTreeSet<String>);

impl Reads {
    /// The relations `expr` names; none where a column is named without its relation, or it
    /// holds a query.
    fn of(expr: &Expr) -> Option<Reads> {
        let mut reads = ReadsVisit {
            names: BTreeSet::new(),
            plain: false,
        };
        if expr.visit(&mut reads).is_break() || reads.plain {
            return None;
        }
        Some(Reads(reads.names))
    }
}

struct ReadsVisit {
    names: BTreeSet<String>,
    plain: bool,
}

impl Visitor for ReadsVisit {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(_) => self.plain = true,
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                self.names.insert(normal(&parts[parts.len() - 2]));
            }
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                return ControlFlow::Break(())
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, _: &sqlparser::ast::Query) -> ControlFlow<()> {
        ControlFlow::Break(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::{written_alone, Axis};
    use super::*;

    fn written(text: &str, pick: &str) -> String {
        written_alone(text, Axis::Conditions, pick, &|_| None)
    }

    const SETS: &str = "SELECT s.set_num, t.name\nFROM lego_sets s\nJOIN lego_themes t ON t.id = s.theme_id\nWHERE s.year >= :bind(min_year)\n  AND t.name = 'City'";

    #[test]
    fn each_condition_moves_to_the_first_on_where_its_relations_are_joined() {
        assert_eq!(
            written(SETS, ON),
            "SELECT s.set_num, t.name\nFROM lego_sets s\nJOIN lego_themes t ON t.id = s.theme_id AND s.year >= :bind(min_year) AND t.name = 'City'"
        );
    }

    #[test]
    fn each_condition_moves_to_the_where_and_a_join_left_with_none_is_crossed() {
        assert_eq!(
            written(SETS, WHERE),
            "SELECT s.set_num, t.name\nFROM lego_sets s\nCROSS JOIN lego_themes t\nWHERE s.year >= :bind(min_year)\n  AND t.name = 'City' AND t.id = s.theme_id"
        );
        let back = written(&written(SETS, WHERE), ON);
        assert_eq!(
            back,
            "SELECT s.set_num, t.name\nFROM lego_sets s\nJOIN lego_themes t ON s.year >= :bind(min_year) AND t.name = 'City' AND t.id = s.theme_id"
        );
    }

    #[test]
    fn the_places_are_counted_with_each_spelling() {
        let read = Read::new(SETS).unwrap();
        let places = places(&read, None);
        let found: Vec<(String, Vec<String>)> = places
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
                    "the condition s.year >= :bind(min_year)".to_string(),
                    vec![
                        "in the WHERE".to_string(),
                        "in the ON joining t".to_string()
                    ]
                ),
                (
                    "the condition t.name = 'City'".to_string(),
                    vec![
                        "in the WHERE".to_string(),
                        "in the ON joining t".to_string()
                    ]
                ),
                (
                    "the condition t.id = s.theme_id".to_string(),
                    vec![
                        "in the ON joining t".to_string(),
                        "in the WHERE".to_string()
                    ]
                ),
            ]
        );
    }

    #[test]
    fn a_condition_whose_join_or_a_join_over_it_is_outer_stays_where_it_is() {
        for (condition, text, conjunct, pick) in [
            (
                "a left join",
                "SELECT s.set_num FROM lego_sets s LEFT JOIN lego_themes t ON t.id = s.theme_id WHERE t.name = 'City'",
                "t.name = 'City'",
                ON,
            ),
            (
                "a left join's own condition",
                "SELECT s.set_num FROM lego_sets s LEFT JOIN lego_themes t ON t.id = s.theme_id WHERE t.name = 'City'",
                "t.id = s.theme_id",
                WHERE,
            ),
            (
                "a left join over the join",
                "SELECT s.set_num FROM lego_sets s CROSS JOIN lego_themes t LEFT JOIN lego_colors c ON c.id = s.id WHERE t.id = s.theme_id",
                "t.id = s.theme_id",
                ON,
            ),
            (
                "a column named without its relation",
                "SELECT s.set_num FROM lego_sets s CROSS JOIN lego_themes t WHERE name = s.name",
                "name = s.name",
                ON,
            ),
            (
                "a condition holding a query",
                "SELECT s.set_num FROM lego_sets s CROSS JOIN lego_themes t WHERE t.id IN (SELECT p.id FROM p WHERE p.s = s.id)",
                "t.id IN (SELECT p.id FROM p WHERE p.s = s.id)",
                ON,
            ),
            (
                "a condition naming no relation of the join",
                "SELECT s.set_num FROM lego_sets s CROSS JOIN lego_themes t WHERE o.id = 1",
                "o.id = 1",
                ON,
            ),
            (
                "relations of two FROM items",
                "SELECT s.set_num FROM lego_sets s, lego_themes t CROSS JOIN lego_colors c WHERE t.id = s.theme_id",
                "t.id = s.theme_id",
                ON,
            ),
        ] {
            let read = Read::new(text).unwrap();
            let found: Vec<String> = places(&read, None).into_iter().map(|p| p.what).collect();
            assert!(
                !found.contains(&format!("the condition {conjunct}")),
                "{condition}: {found:?}"
            );
            assert_eq!(written(text, pick), text, "{condition}");
        }
        let whole =
            "SELECT s.set_num FROM lego_sets s CROSS JOIN lego_themes t WHERE t.id = s.theme_id";
        assert_ne!(
            written(whole, ON),
            whole,
            "the cases above break one condition each"
        );
    }

    #[test]
    fn an_or_is_held_in_parentheses_where_it_joins_another_condition() {
        let text = "SELECT s.set_num FROM lego_sets s JOIN lego_themes t ON t.id = s.theme_id OR t.id = 0 WHERE s.year = 1 OR s.year = 2";
        assert_eq!(
            written(text, ON),
            "SELECT s.set_num FROM lego_sets s JOIN lego_themes t ON (t.id = s.theme_id OR t.id = 0) AND (s.year = 1 OR s.year = 2)"
        );
    }

    #[test]
    fn a_template_comment_between_conjuncts_stays_where_it_stands() {
        let text = "SELECT s.set_num\nFROM lego_sets s\nJOIN lego_themes t ON t.id = s.theme_id\nWHERE t.name = 'City'\n# the year\n  AND s.year > 2000";
        assert_eq!(
            written(text, ON),
            "SELECT s.set_num\nFROM lego_sets s\nJOIN lego_themes t ON t.id = s.theme_id AND t.name = 'City' AND s.year > 2000 \n# the year\n"
        );
    }

    #[test]
    fn comments_and_directives_stay_where_they_stand() {
        let text = "SELECT s.set_num\nFROM lego_sets s -- the sets\n# a template comment\nJOIN (\n    :compose(scopes/city.sqlc)\n) sc ON sc.theme_id = s.theme_id\nWHERE sc.scope_id = :bind(scope) -- the scope\n";
        assert_eq!(
            written(text, ON),
            "SELECT s.set_num\nFROM lego_sets s -- the sets\n# a template comment\nJOIN (\n    :compose(scopes/city.sqlc)\n) sc ON sc.theme_id = s.theme_id AND sc.scope_id = :bind(scope) -- the scope\n"
        );
    }

    /// The sets and themes a condition's law runs on: a set with no theme, and a theme that is
    /// not City, which a `LEFT JOIN` keeps when the condition is in its `ON`.
    fn sets_and_themes() -> crate::laws::Trap {
        use crate::mock::MockTable;
        let sets = MockTable::new("sets")
            .column("set_num", "varchar")
            .column("theme_id", "integer")
            .column("year", "integer")
            .row(&[Some("6000-1"), Some("1"), Some("2001")])
            .row(&[Some("6001-1"), Some("2"), Some("2002")])
            .row(&[Some("6002-1"), None, Some("2003")])
            .row(&[Some("6003-1"), Some("3"), Some("1999")])
            .row(&[Some("6004-1"), Some("1"), Some("2004")]);
        let themes = MockTable::new("themes")
            .column("id", "integer")
            .column("name", "varchar")
            .row(&[Some("1"), Some("City")])
            .row(&[Some("2"), Some("Technic")])
            .row(&[Some("3"), Some("City")]);
        crate::laws::Trap {
            tables: vec![sets, themes],
            binds: vec![("min_year", 2000)],
            templates: vec![],
        }
    }

    #[test]
    #[ignore = "needs a PostgreSQL server: set SQLC_TEST_POSTGRES_URL"]
    fn law_a_condition_in_an_inner_join_s_on_or_in_the_where_returns_the_same_rows() {
        let trap = sets_and_themes();
        let written = "SELECT s.set_num, t.name\nFROM sets s\nJOIN themes t ON t.id = s.theme_id\nWHERE t.name = 'City'\n  AND s.year >= :bind(min_year)";
        let near = (
            "SELECT s.set_num, t.name\nFROM sets s\nLEFT JOIN themes t ON t.id = s.theme_id\nWHERE t.name = 'City'\n  AND s.year >= :bind(min_year)",
            "SELECT s.set_num, t.name\nFROM sets s\nLEFT JOIN themes t ON t.id = s.theme_id AND t.name = 'City'\nWHERE s.year >= :bind(min_year)",
        );
        assert_eq!(
            written_alone(near.0, Axis::Conditions, ON, &|_| None),
            near.0
        );
        for pick in [ON, WHERE] {
            let spelled = written_alone(written, Axis::Conditions, pick, &|_| None);
            let found = crate::laws::holds(written, &spelled, near, &trap);
            assert_eq!(found.written, 2, "{spelled}");
        }
    }
}
