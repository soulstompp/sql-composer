//! Join order: the relations of a join list of inner joins in another order, each condition at
//! the first `ON` where every relation it names is joined.

use std::ops::ControlFlow;

use sqlparser::ast::{JoinConstraint, JoinOperator, Select, Statement, Visit, Visitor};

use super::columns::{Columns, Reader, Resolve, Withs};
use super::read::Read;

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
