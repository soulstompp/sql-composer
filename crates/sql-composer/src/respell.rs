//! The respelling `cargo sqlc compose --respell` writes for PostgreSQL. A union whose arms each read
//! one roster, cut to their own rows of it and left joined `ON true` to what each reads, is written
//! as one join: the roster read once, each of its rows paired with the arms whose cut it passes,
//! and every arm's own rows read in one union, each tagged with its arm's number. Every other byte
//! of the file is written as composed, and a statement the parser cannot read is too.

use std::collections::HashMap;
use std::ops::ControlFlow;

use sqlparser::ast::comments::CommentWithSpan;
use sqlparser::ast::{
    Cte, Expr, GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectNamePart, Query, Select,
    SelectItem, SetExpr, SetOperator, SetQuantifier, Spanned, TableFactor, Value, Visit, Visitor,
};
use sqlparser::parser::Parser;

use crate::parsed::{first_code, masked, normal, Edits, Postgres, Source};
use crate::scan::{Scanner, POSTGRES};

/// A composed file as respelled.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Respelled {
    /// The file: each statement that changed respelled, every other byte as composed.
    pub sql: String,
    /// How many statements the file holds.
    pub statements: usize,
    /// The statements the parser cannot read, by their place in the file from 0. Each is written
    /// as composed.
    pub unparsed: Vec<usize>,
    /// Each union respelled, by how many arms it joins, in the order the file holds them.
    pub unions: Vec<usize>,
}

/// `sql`, a file composed for PostgreSQL, respelled.
pub fn respell(sql: &str) -> Respelled {
    let mut respelled = Respelled::default();
    let mut ends = Scanner::new(&POSTGRES).ends(sql);
    if ends.last() != Some(&sql.len()) {
        ends.push(sql.len());
    }
    let mut start = 0;
    for end in ends {
        let piece = &sql[start..end];
        start = end;
        let Some(code) = first_code(piece) else {
            respelled.sql.push_str(piece);
            continue;
        };
        respelled.sql.push_str(&piece[..code]);
        let statement = &piece[code..];
        match respell_statement(statement) {
            Some((text, unions)) => {
                respelled.sql.push_str(&text);
                respelled.unions.extend(unions);
            }
            None => {
                respelled.unparsed.push(respelled.statements);
                respelled.sql.push_str(statement);
            }
        }
        respelled.statements += 1;
    }
    respelled
}

/// One statement respelled, with the arms of each union it respells; none where it does not parse.
fn respell_statement(text: &str) -> Option<(String, Vec<usize>)> {
    let parsed = masked(text);
    let (statements, comments) =
        Parser::parse_sql_with_comments(&Postgres::default(), &parsed).ok()?;
    let [statement] = &statements[..] else {
        return None;
    };
    let comments: Vec<CommentWithSpan> = comments.into();
    let source = Source::new(text, &parsed);

    let mut found = Found::default();
    let _ = statement.visit(&mut found);
    let mut unions: Vec<&Query> = found.unions.iter().collect();
    unions.sort_by_key(|q| {
        let span = source.range(q.body.span());
        span.map_or(usize::MAX, |(start, end)| end - start)
    });

    let mut edits = Edits::default();
    let mut arms = Vec::new();
    for union in unions {
        let mut tried = edits.clone();
        if let Some(count) = respell_union(union, &found, &source, &comments, &mut tried) {
            edits = tried;
            arms.push((source.range(union.body.span())?.0, count));
        }
    }
    if arms.is_empty() {
        return Some((text.to_string(), vec![]));
    }
    let respelled = edits.render(text, 0, text.len())?;
    match Parser::parse_sql(&Postgres::default(), &masked(&respelled)) {
        Ok(parsed) if parsed.len() == 1 => {
            arms.sort();
            Some((
                respelled,
                arms.into_iter().map(|(_, count)| count).collect(),
            ))
        }
        _ => Some((text.to_string(), vec![])),
    }
}

/// What a statement holds that a respelling reads: each union of all its rows, the WITH queries
/// it defines, and how often each single name is read as a relation.
#[derive(Default)]
struct Found {
    unions: Vec<Query>,
    ctes: HashMap<String, Vec<(Cte, bool)>>,
    reads: HashMap<String, usize>,
}

impl Visitor for Found {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if arms(&query.body).len() > 1 {
            self.unions.push(query.clone());
        }
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.ctes
                    .entry(normal(&cte.alias.name))
                    .or_default()
                    .push((cte.clone(), with.recursive));
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table { name, .. } = table_factor {
            if let [ObjectNamePart::Identifier(ident)] = &name.0[..] {
                *self.reads.entry(normal(ident)).or_default() += 1;
            }
        }
        ControlFlow::Continue(())
    }
}

/// The arms of `body` where it is a `UNION ALL`, left to right; none where it is not one.
fn arms(body: &SetExpr) -> Vec<&SetExpr> {
    match body {
        SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
            left,
            right,
        } => {
            let mut arms = arms(left);
            if arms.is_empty() {
                arms.push(left);
            }
            arms.push(right);
            arms
        }
        _ => vec![],
    }
}

/// An arm of a union, as the respelling reads it.
struct Arm<'q> {
    select: &'q Select,
    roster: &'q TableFactor,
    roster_alias: &'q Ident,
    rows: &'q TableFactor,
    rows_alias: &'q Ident,
    /// Whether it is a WITH query something else reads too, so the union writes its rows in place
    /// of reading it, and the WITH query stays as written.
    shared: bool,
}

/// Respell `union` into `edits`, giving how many arms it joins; none where it is not respelled.
fn respell_union(
    union: &Query,
    found: &Found,
    source: &Source,
    comments: &[CommentWithSpan],
    edits: &mut Edits,
) -> Option<usize> {
    if union.with.is_some() || union.order_by.is_some() || union.limit_clause.is_some() {
        return None;
    }
    let written = arms(&union.body);
    let arms: Vec<Arm> = written
        .iter()
        .map(|arm| roster_arm(arm, found))
        .collect::<Option<_>>()?;
    let first = &arms[0];
    let (roster_alias, rows_alias) = (normal(first.roster_alias), normal(first.rows_alias));
    let roster = first.roster.to_string();
    for arm in &arms {
        if arm.roster.to_string() != roster
            || normal(arm.roster_alias) != roster_alias
            || normal(arm.rows_alias) != rows_alias
            || arm.select.projection.len() != first.select.projection.len()
        {
            return None;
        }
        if let Some(cut) = &arm.select.selection {
            if !reads_alone(cut, &roster_alias) {
                return None;
            }
        }
    }

    // Each column: the roster's, the same in every arm, or a column of each arm's own rows.
    let mut columns = Vec::new();
    for (k, item) in first.select.projection.iter().enumerate() {
        let (expr, _) = item_expr(item)?;
        if reads_alone(expr, &roster_alias) {
            if arms
                .iter()
                .any(|arm| arm.select.projection[k].to_string() != item.to_string())
            {
                return None;
            }
            columns.push(Column::Roster(source.text(item.span())?));
        } else {
            let mut picked = Vec::new();
            for arm in &arms {
                let (expr, _) = item_expr(&arm.select.projection[k])?;
                picked.push(column_of(expr, &rows_alias)?.to_string());
            }
            let (expr, alias) = item_expr(item)?;
            let name = match alias {
                Some(alias) => alias.clone(),
                None => column_of(expr, &rows_alias)?.clone(),
            };
            columns.push(Column::Rows { name, picked });
        }
    }
    let names: Vec<String> = columns
        .iter()
        .filter_map(|c| match c {
            Column::Rows { name, .. } => Some(normal(name)),
            Column::Roster(_) => None,
        })
        .collect();
    let mut unique = names.clone();
    unique.sort();
    unique.dedup();
    if unique.len() != names.len() || names.iter().any(|n| n == "arm") {
        return None;
    }
    let pairs = ["a", "arms", "_arms"]
        .into_iter()
        .find(|name| *name != roster_alias && *name != rows_alias)?;

    // What the arms cut and read of the roster, before each arm is written as its own rows.
    let cuts: Vec<String> = arms
        .iter()
        .enumerate()
        .map(|(j, arm)| {
            let cut = match &arm.select.selection {
                Some(cut) => checked_expr(source, edits, cut)?,
                None => "true".to_string(),
            };
            Some(format!("({pairs}.arm = {} AND ({cut}))", j + 1))
        })
        .collect::<Option<_>>()?;
    let roster_text = checked_roster(source, edits, first.roster)?;

    // Each arm becomes its own rows, tagged with its number.
    for (j, arm) in arms.iter().enumerate() {
        let mut span = source.range(arm.select.span())?;
        if let Some(cut) = &arm.select.selection {
            span.1 = span.1.max(source.range(cut.span())?.1);
        }
        if arm.shared {
            span = source.range(written[j].span())?;
        }
        let rows = format!(
            "({}) {}",
            checked_query(source, edits, arm.rows)?,
            alias_text(arm.rows)?
        );
        let mut kept = vec![source.inner(arm.rows)?];
        if let Some(cut) = &arm.select.selection {
            kept.push(source.range(cut.span())?);
        }
        if j == 0 {
            kept.push(
                source
                    .inner(arm.roster)
                    .or_else(|| source.range(arm.roster.span()))?,
            );
            for item in &arm.select.projection {
                kept.push(source.range(item.span())?);
            }
        }
        let mut text = String::new();
        for comment in comments {
            let at = source.range(comment.span)?;
            if span.0 <= at.0 && at.1 <= span.1 && !kept.iter().any(|k| k.0 <= at.0 && at.1 <= k.1)
            {
                text.push_str(&source.text[at.0..at.1]);
                text.push('\n');
            }
        }
        let picked: Vec<String> = columns
            .iter()
            .filter_map(|column| match column {
                Column::Rows { name, picked } => {
                    Some(format!("{}.{} AS {name}", arm.rows_alias, picked[j]))
                }
                Column::Roster(_) => None,
            })
            .collect();
        text.push_str(&format!("SELECT {} AS arm", j + 1));
        for column in picked {
            text.push_str(", ");
            text.push_str(&column);
        }
        text.push_str(&format!("\nFROM {rows}"));
        if !edits.add(span.0, span.1, text) {
            return None;
        }
    }

    // The union reads the roster once, each of its rows paired with the arms whose cut it passes.
    let chain = source.range(union.body.span())?;
    let numbers: Vec<String> = (1..=arms.len()).map(|n| format!("({n})")).collect();
    let outputs: Vec<String> = columns
        .iter()
        .map(|column| match column {
            Column::Roster(text) => text.clone(),
            Column::Rows { name, .. } => format!("{}.{name}", first.rows_alias),
        })
        .collect();
    let text = format!(
        "SELECT {}\nFROM {}\nJOIN (VALUES {}) AS {pairs}(arm)\n  ON {}\nLEFT JOIN (\n{}\n) {} ON {}.arm = {pairs}.arm",
        outputs.join(", "),
        roster_text,
        numbers.join(", "),
        cuts.join("\n  OR "),
        edits.render(source.text, chain.0, chain.1)?,
        first.rows_alias,
        first.rows_alias,
    );
    if !edits.add(chain.0, chain.1, text) {
        return None;
    }
    Some(arms.len())
}

/// A column of the respelled union.
enum Column {
    /// The roster's, as the first arm writes it.
    Roster(String),
    /// A column of each arm's own rows: the name the union gives it, and each arm's column.
    Rows { name: Ident, picked: Vec<String> },
}

/// The arm `arm` reads, past the derived tables and WITH queries that only pass its rows on,
/// where it is one roster left joined `ON true` to rows of its own; none where it is not.
fn roster_arm<'q>(mut arm: &'q SetExpr, found: &'q Found) -> Option<Arm<'q>> {
    let mut shared = false;
    loop {
        let select = match arm {
            SetExpr::Query(query) => {
                arm = plain_body(query)?;
                continue;
            }
            SetExpr::Select(select) => select,
            _ => return None,
        };
        if let Some(through) = passes_on(select) {
            match through {
                TableFactor::Derived {
                    lateral: false,
                    subquery,
                    ..
                } => arm = plain_body(subquery)?,
                TableFactor::Table { name, .. } => {
                    let [ObjectNamePart::Identifier(ident)] = &name.0[..] else {
                        return None;
                    };
                    let name = normal(ident);
                    let [(cte, _)] = &found.ctes.get(&name)?[..] else {
                        return None;
                    };
                    shared |= found.reads.get(&name) != Some(&1);
                    arm = plain_body(&cte.query)?;
                }
                _ => return None,
            }
            continue;
        }
        let mut arm = roster_select(select)?;
        arm.shared = shared;
        return Some(arm);
    }
}

/// The body of `query`, where it adds nothing to it.
fn plain_body(query: &Query) -> Option<&SetExpr> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
    {
        return None;
    }
    Some(&query.body)
}

/// Whether `select` holds nothing but its clauses' defaults past its select list and FROM.
fn bare(select: &Select) -> bool {
    select.distinct.is_none()
        && select.select_modifiers.is_none()
        && select.top.is_none()
        && select.exclude.is_none()
        && select.into.is_none()
        && select.lateral_views.is_empty()
        && select.prewhere.is_none()
        && select.connect_by.is_empty()
        && matches!(&select.group_by, GroupByExpr::Expressions(exprs, mods) if exprs.is_empty() && mods.is_empty())
        && select.cluster_by.is_empty()
        && select.distribute_by.is_empty()
        && select.sort_by.is_empty()
        && select.having.is_none()
        && select.named_window.is_empty()
        && select.qualify.is_none()
        && select.value_table_mode.is_none()
}

/// What `select` passes on whole, where it is `SELECT * FROM` one relation and nothing more.
fn passes_on(select: &Select) -> Option<&TableFactor> {
    if !bare(select)
        || select.selection.is_some()
        || !matches!(&select.projection[..], [SelectItem::Wildcard(w)] if w.to_string().is_empty())
    {
        return None;
    }
    let [from] = &select.from[..] else {
        return None;
    };
    from.joins.is_empty().then_some(&from.relation)
}

/// `select` as an arm: one roster with an alias, left joined `ON true` to a derived table of
/// rows with an alias.
fn roster_select(select: &Select) -> Option<Arm<'_>> {
    if !bare(select) {
        return None;
    }
    let [from] = &select.from[..] else {
        return None;
    };
    let [join] = &from.joins[..] else {
        return None;
    };
    let on = match &join.join_operator {
        JoinOperator::Left(JoinConstraint::On(on))
        | JoinOperator::LeftOuter(JoinConstraint::On(on)) => on,
        _ => return None,
    };
    if !matches!(on, Expr::Value(v) if v.value == Value::Boolean(true)) || join.global {
        return None;
    }
    let roster_alias = match &from.relation {
        TableFactor::Table {
            alias: Some(alias),
            args: None,
            ..
        } => &alias.name,
        TableFactor::Derived {
            lateral: false,
            alias: Some(alias),
            ..
        } => &alias.name,
        _ => return None,
    };
    let TableFactor::Derived {
        lateral: false,
        subquery: _,
        alias: Some(alias),
        sample: None,
    } = &join.relation
    else {
        return None;
    };
    Some(Arm {
        select,
        roster: &from.relation,
        roster_alias,
        rows: &join.relation,
        rows_alias: &alias.name,
        shared: false,
    })
}

/// The expression of a select list's item, with its alias.
fn item_expr(item: &SelectItem) -> Option<(&Expr, Option<&Ident>)> {
    match item {
        SelectItem::UnnamedExpr(expr) => Some((expr, None)),
        SelectItem::ExprWithAlias { expr, alias } => Some((expr, Some(alias))),
        _ => None,
    }
}

/// The column `expr` reads of `alias`, where it is that and nothing more.
fn column_of<'e>(expr: &'e Expr, alias: &str) -> Option<&'e Ident> {
    match expr {
        Expr::CompoundIdentifier(parts) => match &parts[..] {
            [table, column] if normal(table) == alias => Some(column),
            _ => None,
        },
        _ => None,
    }
}

/// Whether `expr` reads the relation `alias` alone: its columns, constants and the operators
/// between them, and no function or query.
fn reads_alone(expr: &Expr, alias: &str) -> bool {
    let alone = |e: &Expr| reads_alone(e, alias);
    match expr {
        Expr::CompoundIdentifier(_) => column_of(expr, alias).is_some(),
        Expr::Value(_) | Expr::TypedString(_) => true,
        Expr::Nested(e)
        | Expr::UnaryOp { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsNotTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotFalse(e)
        | Expr::IsUnknown(e)
        | Expr::IsNotUnknown(e) => alone(e),
        Expr::Cast { expr, .. } => alone(expr),
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => alone(left) && alone(right),
        Expr::InList { expr, list, .. } => alone(expr) && list.iter().all(alone),
        Expr::Between {
            expr, low, high, ..
        } => alone(expr) && alone(low) && alone(high),
        Expr::Like {
            expr,
            pattern,
            escape_char: None,
            ..
        }
        | Expr::ILike {
            expr,
            pattern,
            escape_char: None,
            ..
        } => alone(expr) && alone(pattern),
        _ => false,
    }
}

/// The text of the derived table `factor`'s query as the file holds it, the respellings inside it
/// applied, once it reads back as that query.
fn checked_query(source: &Source, edits: &Edits, factor: &TableFactor) -> Option<String> {
    let TableFactor::Derived {
        subquery: query, ..
    } = factor
    else {
        return None;
    };
    let (start, end) = source.inner(factor)?;
    let text = &source.parsed[start..end];
    let reread = Parser::new(&Postgres::default())
        .try_with_sql(text)
        .ok()?
        .parse_query()
        .ok()?;
    (reread.to_string() == query.to_string()).then_some(())?;
    edits.render(source.text, start, end)
}

/// The text of `expr` as the file holds it, once it reads back as `expr`.
fn checked_expr(source: &Source, edits: &Edits, expr: &Expr) -> Option<String> {
    let (start, end) = source.range(expr.span())?;
    let text = &source.parsed[start..end];
    let reread = Parser::new(&Postgres::default())
        .try_with_sql(text)
        .ok()?
        .parse_expr()
        .ok()?;
    (reread.to_string() == expr.to_string()).then_some(())?;
    edits.render(source.text, start, end)
}

/// The roster as the first arm reads it, with its alias.
fn checked_roster(source: &Source, edits: &Edits, roster: &TableFactor) -> Option<String> {
    match roster {
        TableFactor::Derived { .. } => Some(format!(
            "({}) {}",
            checked_query(source, edits, roster)?,
            alias_text(roster)?
        )),
        TableFactor::Table { .. } => {
            let (start, end) = source.range(roster.span())?;
            let text = &source.parsed[start..end];
            let reread = Parser::new(&Postgres::default())
                .try_with_sql(&format!("SELECT * FROM {text}"))
                .ok()?
                .parse_query()
                .ok()?;
            let SetExpr::Select(select) = reread.body.as_ref() else {
                return None;
            };
            (select.from.first()?.relation.to_string() == roster.to_string()).then_some(())?;
            Some(source.text[start..end].to_string())
        }
        _ => None,
    }
}

/// A derived table's alias, as written after it.
fn alias_text(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Derived {
            alias: Some(alias), ..
        } => Some(alias.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// How a test writes arm `n` of a union.
    type Spelled = Box<dyn Fn(usize) -> String>;

    /// A union of `arms` roster arms, each a derived table of its own, as `:union(ALL …)` composes
    /// them: arm `n` reads `law_n` and is cut to the slug `rule_n`.
    fn roster_union(arms: usize, arm: impl Fn(usize) -> String) -> String {
        let arms: Vec<String> = (1..=arms)
            .map(|n| format!("SELECT * FROM (\n{}\n) AS _union_{n}", arm(n)))
            .collect();
        format!(
            "SELECT * FROM (\n{}\n) AS _union",
            arms.join("\nUNION ALL\n")
        )
    }

    fn arm(n: usize) -> String {
        format!(
            "-- the law {n}\nSELECT r.rule, p.filing, p.violates\nFROM      (\n    SELECT * FROM roster\n) r\nLEFT JOIN (\n    SELECT l.filing, l.bad AS violates FROM law_{n} l\n) p ON true\nWHERE r.slug = 'rule_{n}'"
        )
    }

    #[test]
    fn a_union_of_roster_arms_reads_the_roster_once() {
        let sql = roster_union(3, arm);
        let respelled = respell(&sql);
        assert_eq!(respelled.unions, vec![3]);
        assert_eq!(respelled.statements, 1);
        assert!(respelled.unparsed.is_empty());
        assert_eq!(
            respelled.sql.matches("SELECT * FROM roster").count(),
            1,
            "{}",
            respelled.sql
        );
        assert!(
            respelled.sql.contains(
                "JOIN (VALUES (1), (2), (3)) AS a(arm)\n  ON (a.arm = 1 AND (r.slug = 'rule_1'))\n  OR (a.arm = 2 AND (r.slug = 'rule_2'))"
            ),
            "{}",
            respelled.sql
        );
        assert!(
            respelled.sql.contains(") p ON p.arm = a.arm"),
            "{}",
            respelled.sql
        );
        for n in 1..=3 {
            assert!(
                respelled.sql.contains(&format!("-- the law {n}\n")),
                "{}",
                respelled.sql
            );
            assert!(
                respelled.sql.contains(&format!(
                    "SELECT {n} AS arm, p.filing AS filing, p.violates AS violates"
                )),
                "{}",
                respelled.sql
            );
            assert!(
                respelled.sql.contains(&format!("FROM law_{n} l")),
                "{}",
                respelled.sql
            );
        }
        Parser::parse_sql(&Postgres::default(), &respelled.sql).unwrap();
    }

    #[test]
    fn arms_named_once_at_the_head_are_read_through_their_names() {
        let ctes: Vec<String> = (1..=2)
            .map(|n| format!("checks_{n} AS (\n{}\n)", arm(n)))
            .collect();
        let union = roster_union(2, |n| format!("SELECT * FROM checks_{n}"));
        let sql = format!("WITH {}\n{union};\n", ctes.join(",\n"));
        let respelled = respell(&sql);
        assert_eq!(respelled.unions, vec![2], "{}", respelled.sql);
        assert!(
            respelled
                .sql
                .contains("checks_1 AS (\n-- the law 1\nSELECT 1 AS arm, p.filing AS filing"),
            "{}",
            respelled.sql
        );
        assert!(
            respelled
                .sql
                .contains("SELECT * FROM checks_2\n) AS _union_2"),
            "{}",
            respelled.sql
        );
        assert!(respelled.sql.ends_with(";\n"), "{}", respelled.sql);
        Parser::parse_sql(&Postgres::default(), &respelled.sql).unwrap();
    }

    #[test]
    fn a_union_any_arm_of_which_breaks_a_condition_is_written_as_composed() {
        let broken: [(&str, Spelled); 9] = [
            (
                "another roster",
                Box::new(|n| {
                    if n == 2 {
                        arm(n).replace("FROM roster", "FROM other")
                    } else {
                        arm(n)
                    }
                }),
            ),
            (
                "joined on a condition",
                Box::new(|n| {
                    if n == 2 {
                        arm(n).replace("p ON true", "p ON p.filing = r.rule")
                    } else {
                        arm(n)
                    }
                }),
            ),
            (
                "an inner join",
                Box::new(|n| {
                    if n == 2 {
                        arm(n).replace("LEFT JOIN", "JOIN")
                    } else {
                        arm(n)
                    }
                }),
            ),
            (
                "a cut on the arm's rows",
                Box::new(|n| {
                    if n == 2 {
                        arm(n).replace("WHERE r.slug", "WHERE p.violates AND r.slug")
                    } else {
                        arm(n)
                    }
                }),
            ),
            (
                "a column over the arm's rows",
                Box::new(|n| {
                    if n == 2 {
                        arm(n).replace("p.violates\n", "coalesce(p.violates, false)\n")
                    } else {
                        arm(n)
                    }
                }),
            ),
            (
                "a roster column that differs",
                Box::new(|n| {
                    if n == 2 {
                        arm(n).replace("SELECT r.rule,", "SELECT r.slug,")
                    } else {
                        arm(n)
                    }
                }),
            ),
            (
                "another alias",
                Box::new(|n| {
                    if n == 2 {
                        arm(n).replace(") r\n", ") q\n").replace("r.", "q.")
                    } else {
                        arm(n)
                    }
                }),
            ),
            (
                "rows read laterally",
                Box::new(|n| {
                    if n == 2 {
                        arm(n).replace("LEFT JOIN (", "LEFT JOIN LATERAL (")
                    } else {
                        arm(n)
                    }
                }),
            ),
            (
                "a cut that calls a function",
                Box::new(|n| {
                    if n == 2 {
                        arm(n).replace("r.slug = ", "lower(r.slug) = ")
                    } else {
                        arm(n)
                    }
                }),
            ),
        ];
        for (condition, arm) in broken {
            let sql = roster_union(3, arm);
            let respelled = respell(&sql);
            assert_eq!(respelled.sql, sql, "{condition}");
            assert!(respelled.unions.is_empty(), "{condition}");
        }
    }

    #[test]
    fn an_arm_something_else_reads_too_is_written_in_the_union_and_kept_as_it_is() {
        let ctes: Vec<String> = (1..=2)
            .map(|n| format!("checks_{n} AS (\n{}\n)", arm(n)))
            .collect();
        let union = roster_union(2, |n| format!("SELECT * FROM checks_{n}"));
        let sql = format!(
            "WITH {}\nSELECT * FROM checks_1 UNION ALL {union};\n",
            ctes.join(",\n")
        );
        let respelled = respell(&sql);
        assert_eq!(respelled.unions, vec![2], "{}", respelled.sql);
        assert!(
            respelled.sql.starts_with(&format!("WITH {},\n", ctes[0])),
            "{}",
            respelled.sql
        );
        assert!(
            respelled.sql.contains("SELECT * FROM checks_1 UNION ALL"),
            "{}",
            respelled.sql
        );
        assert!(
            respelled
                .sql
                .contains("LEFT JOIN (\nSELECT 1 AS arm, p.filing AS filing"),
            "{}",
            respelled.sql
        );
        assert!(
            respelled
                .sql
                .contains("checks_2 AS (\n-- the law 2\nSELECT 2 AS arm"),
            "{}",
            respelled.sql
        );
        Parser::parse_sql(&Postgres::default(), &respelled.sql).unwrap();
    }

    #[test]
    fn psql_lines_and_statements_that_do_not_parse_are_written_as_composed() {
        let union = roster_union(2, arm);
        let sql = format!(
            "\\pset null '·'\n\\echo === the laws ===\n{union};\n\\echo\nSELEC 1;\n-- the end\n"
        );
        let respelled = respell(&sql);
        assert_eq!(respelled.statements, 2);
        assert_eq!(respelled.unparsed, vec![1]);
        assert_eq!(respelled.unions, vec![2]);
        assert!(
            respelled
                .sql
                .starts_with("\\pset null '·'\n\\echo === the laws ===\nSELECT * FROM (\n"),
            "{}",
            respelled.sql
        );
        assert!(
            respelled.sql.ends_with(";\n\\echo\nSELEC 1;\n-- the end\n"),
            "{}",
            respelled.sql
        );
    }

    #[test]
    fn a_cycle_clause_and_literals_joined_across_a_newline_are_kept_as_written() {
        let walk = "WITH RECURSIVE walk(n) AS (\n    SELECT 1 UNION ALL SELECT n + 1 FROM walk WHERE n < 3\n) CYCLE n SET is_cycle USING route\n";
        let union = roster_union(2, |n| {
            arm(n).replace("l.bad AS violates", "l.bad AS violates, 'a '\n'b' AS said")
        });
        let sql = format!("{walk}{}", union);
        let respelled = respell(&sql);
        assert_eq!(respelled.unions, vec![2], "{}", respelled.sql);
        assert!(respelled.sql.starts_with(walk), "{}", respelled.sql);
        assert_eq!(
            respelled.sql.matches("'a '\n'b' AS said").count(),
            2,
            "{}",
            respelled.sql
        );
    }

    #[test]
    fn a_file_with_nothing_to_respell_is_written_byte_for_byte() {
        let sql =
            "-- a note\nSELECT a FROM t UNION ALL SELECT b FROM u;\n\nSELECT 'é' AS x; -- after\n";
        let respelled = respell(sql);
        assert_eq!(respelled.sql, sql);
        assert_eq!(respelled.statements, 2);
        assert!(respelled.unions.is_empty() && respelled.unparsed.is_empty());
    }

    /// A roster of two rules, and each rule's filings: the second rule's are all good, so its
    /// arm finds nothing and keeps its roster row alone.
    fn roster_and_laws() -> crate::laws::Trap {
        use crate::mock::MockTable;
        let roster = MockTable::new("roster")
            .column("slug", "varchar")
            .column("rule", "varchar")
            .row(&[Some("rule_1"), Some("the first")])
            .row(&[Some("rule_2"), Some("the second")]);
        let laws = [
            ("law_1", ["true", "false", "true"]),
            ("law_2", ["false"; 3]),
        ];
        let tables = std::iter::once(roster)
            .chain(laws.into_iter().map(|(name, bad)| {
                let mut table = MockTable::new(name)
                    .column("filing", "integer")
                    .column("bad", "boolean");
                for (filing, bad) in bad.iter().enumerate() {
                    table = table.row(&[Some(&filing.to_string()), Some(bad)]);
                }
                table
            }))
            .collect();
        crate::laws::Trap {
            tables,
            binds: vec![],
            templates: vec![],
        }
    }

    #[test]
    #[ignore = "needs a PostgreSQL server: set SQLC_TEST_POSTGRES_URL"]
    fn law_a_union_of_roster_arms_respelled_returns_the_same_rows() {
        let trap = roster_and_laws();
        let arm = |n: usize| {
            format!(
                "SELECT r.rule, p.filing, p.violates\nFROM      (\n    SELECT * FROM roster\n) r\nLEFT JOIN (\n    SELECT l.filing, l.bad AS violates FROM law_{n} l WHERE l.bad\n) p ON true\nWHERE r.slug = 'rule_{n}'"
            )
        };
        let written = roster_union(2, arm);
        let respelled = respell(&written);
        assert_eq!(respelled.unions, vec![2], "{}", respelled.sql);
        let tagged = "LEFT JOIN (\nSELECT * FROM (\nSELECT 1 AS arm";
        assert!(respelled.sql.contains(tagged), "{}", respelled.sql);
        let inner = respelled.sql.replacen(tagged, &tagged["LEFT ".len()..], 1);
        let found = crate::laws::holds(&written, &respelled.sql, (&written, &inner), &trap);
        assert_eq!(found.written, 3, "{}", respelled.sql);
    }
}
