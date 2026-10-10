//! The columns a relation holds, read from the text alone: a select list that names each of its
//! columns, a subquery's, a WITH query's or a composed template's. A table's columns are the
//! catalog's, and a select list with a `*` names none, so neither is read.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::path::Path;

use sqlparser::ast::{
    Cte, Expr, ObjectNamePart, Query, SelectItem, SetExpr, Statement, TableFactor, Visit, Visitor,
};

use super::read::{Read, Stood};
use crate::parsed::normal;
use crate::types::{ComposeTarget, Element};

/// Reads the text of a template another one composes, by its path as `:compose` names it.
pub type Resolve<'r> = &'r dyn Fn(&Path) -> Option<String>;

/// The columns a relation holds, by name, as PostgreSQL names them; or why they are not read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Columns {
    /// Each column's name, in order.
    Named(Vec<String>),
    /// Why its columns are not read from the text.
    Unread(String),
}

/// What a statement defines that a relation's name can read: each WITH query, by its name.
#[derive(Default)]
pub(crate) struct Withs {
    queries: HashMap<String, Vec<Cte>>,
}

impl Withs {
    /// The WITH queries `statement` defines, at any depth.
    pub(crate) fn of(statement: &Statement) -> Withs {
        let mut withs = Withs::default();
        let _ = statement.visit(&mut withs);
        withs
    }
}

impl Visitor for Withs {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.queries
                    .entry(normal(&cte.alias.name))
                    .or_default()
                    .push(cte.clone());
            }
        }
        ControlFlow::Continue(())
    }
}

/// How deep composed templates are followed for their columns.
const DEPTH: usize = 16;

/// Reads relations' columns for one template.
pub(crate) struct Reader<'a> {
    pub(crate) stood: &'a Stood,
    pub(crate) withs: &'a Withs,
    pub(crate) resolve: Resolve<'a>,
    pub(crate) depth: usize,
}

impl Reader<'_> {
    /// The columns `factor` holds.
    pub(crate) fn factor(&self, factor: &TableFactor) -> Columns {
        match factor {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                if let Some(alias) = alias {
                    if !alias.columns.is_empty() {
                        return Columns::Unread("a column list after its name".into());
                    }
                }
                let [ObjectNamePart::Identifier(ident)] = &name.0[..] else {
                    return Columns::Unread(format!("{name}, a table"));
                };
                match self.withs.queries.get(&normal(ident)).map(Vec::as_slice) {
                    Some([cte]) => match cte.alias.columns.is_empty() {
                        true => self.query(&cte.query),
                        false => Columns::Named(
                            cte.alias.columns.iter().map(|c| normal(&c.name)).collect(),
                        ),
                    },
                    Some(_) => Columns::Unread(format!("{name}, a name defined twice")),
                    None => Columns::Unread(format!("{name}, a table")),
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => match alias.as_ref().filter(|a| !a.columns.is_empty()) {
                Some(alias) => {
                    Columns::Named(alias.columns.iter().map(|c| normal(&c.name)).collect())
                }
                None => self.query(subquery),
            },
            other => Columns::Unread(format!("{other}, not a table or a subquery")),
        }
    }

    /// The columns `query` returns.
    pub(crate) fn query(&self, query: &Query) -> Columns {
        self.body(&query.body)
    }

    fn body(&self, body: &SetExpr) -> Columns {
        match body {
            SetExpr::Select(select) => {
                if let ([SelectItem::Wildcard(_)], [from]) =
                    (&select.projection[..], &select.from[..])
                {
                    if from.joins.is_empty() {
                        if let Some(columns) = self.stand_in(&from.relation) {
                            return columns;
                        }
                    }
                }
                let mut names = Vec::new();
                for item in &select.projection {
                    match item {
                        SelectItem::ExprWithAlias { alias, .. } => names.push(normal(alias)),
                        SelectItem::UnnamedExpr(expr) => match named(expr) {
                            Some(name) => names.push(name),
                            None => {
                                return Columns::Unread(format!(
                                    "{expr}, a column it names by rule"
                                ))
                            }
                        },
                        _ => return Columns::Unread("a select list with a *".into()),
                    }
                }
                Columns::Named(names)
            }
            SetExpr::Query(query) => self.query(query),
            SetExpr::SetOperation { left, .. } => self.body(left),
            _ => Columns::Unread("a query that is not a select".into()),
        }
    }

    /// The columns of the template a relation's stand-in composes, where `factor` is one.
    fn stand_in(&self, factor: &TableFactor) -> Option<Columns> {
        let TableFactor::Table {
            name, alias: None, ..
        } = factor
        else {
            return None;
        };
        let [ObjectNamePart::Identifier(ident)] = &name.0[..] else {
            return None;
        };
        let element = self.stood.relation(&ident.value)?;
        Some(match element {
            Element::Compose(compose) => match &compose.target {
                ComposeTarget::Path(path) => self.template(path),
                ComposeTarget::Slot(slot) => Columns::Unread(format!("@{slot}, a slot")),
                ComposeTarget::Definition(path) => self.template(path),
            },
            Element::Command(command) => match command.sources.first() {
                Some(path) if command.kind == crate::types::CommandKind::Union => {
                    self.template(path)
                }
                _ => Columns::Unread(format!("{ident}, a command")),
            },
            _ => Columns::Unread(format!("{ident}")),
        })
    }

    /// The columns the template at `path` returns.
    fn template(&self, path: &Path) -> Columns {
        let unread = || Columns::Unread(format!("{}, a template not read", path.display()));
        if self.depth >= DEPTH {
            return unread();
        }
        let Some(text) = (self.resolve)(path) else {
            return unread();
        };
        let Ok(read) = Read::new(&text) else {
            return unread();
        };
        let [Ok(statement)] = &read.statements[..] else {
            return unread();
        };
        let Statement::Query(query) = &statement.ast else {
            return unread();
        };
        let withs = Withs::of(&statement.ast);
        Reader {
            stood: &read.stood,
            withs: &withs,
            resolve: self.resolve,
            depth: self.depth + 1,
        }
        .query(query)
    }
}

/// The name PostgreSQL gives the column `expr` returns, where its rule names one from the text:
/// a column's own name, a function's, `case`, what a cast reads, and `?column?` for a constant or
/// an operator's result.
fn named(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(_)
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsNotTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotFalse(_)
        | Expr::IsDistinctFrom(..)
        | Expr::IsNotDistinctFrom(..)
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::ILike { .. } => Some("?column?".into()),
        Expr::Exists { negated: false, .. } => Some("exists".into()),
        Expr::Identifier(ident) => Some(normal(ident)),
        Expr::CompoundIdentifier(parts) => parts.last().map(normal),
        Expr::Nested(inner) => named(inner),
        Expr::Cast { expr, .. } => named(expr),
        Expr::Function(function) => match function.name.0.last()? {
            ObjectNamePart::Identifier(ident) => Some(normal(ident)),
            _ => None,
        },
        Expr::Case { .. } => Some("case".into()),
        _ => None,
    }
}
