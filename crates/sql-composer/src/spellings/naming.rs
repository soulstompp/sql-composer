//! How a relation read once is named: inline, as a WITH query at the statement's head, or as a
//! template of its own.
//!
//! A relation is read once where it is a subquery in a `FROM`, or a WITH query at the head of a
//! statement one `FROM` reads, not recursive, not marked `MATERIALIZED` and writing nothing. A
//! subquery moves to the head only from where nothing outside it could be read from inside it: no
//! expression and no `LATERAL` holds it. It takes its alias as its name, where no other relation
//! of the statement is read by that name, and goes to the head before the WITH query that holds it,
//! or last. A WITH query moves to the `FROM` that reads it, under the name that `FROM` reads it by.
//! A template composed in a `FROM` is written inline from its own text, each slot it leaves open
//! written as the template this `:compose` fills it with. A relation the statement writes inline
//! is not made a template of its own here: that needs a file named for it. A statement with a
//! WITH anywhere but at its head is left as written, and so is a statement that writes.

use std::collections::HashMap;
use std::ops::ControlFlow;

use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    Cte, Expr, ObjectNamePart, Query, SetExpr, Statement, TableAlias, TableFactor, Visit, VisitMut,
    Visitor, VisitorMut, With,
};
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Token;

use super::columns::Resolve;
use super::extent::{spaced_from, spaced_to};
use super::place::{enough, passed, wants, Choice, Edits, Place, Want};
use super::read::{plain_space, reparse, Read, Stmt, Stood};
use crate::parsed::normal;
use crate::parser::{marked, Mark};
use crate::types::{ComposeRef, ComposeTarget, Element};

/// The pick that writes each relation read once inline, where it is read.
pub(crate) const INLINE: &str = "inline";
/// The pick that writes each relation read once as a WITH query at the statement's head.
pub(crate) const WITH: &str = "with";
/// The pick that writes each relation read once as a template of its own, where it is one.
pub(crate) const TEMPLATE: &str = "template";

/// Where a relation of the statement is read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Scope {
    /// The body of the head's WITH query at this place.
    With(usize),
    /// The statement itself, past its head.
    #[default]
    Main,
}

/// A table factor of the statement, as a visit reaches them.
#[derive(Debug, Clone)]
struct Factor {
    /// Its place among the statement's table factors.
    index: usize,
    factor: TableFactor,
    scope: Scope,
    /// Whether nothing outside it can be read from inside it: no expression or `LATERAL` holds
    /// it.
    free: bool,
}

/// What a statement holds that naming reads.
struct Form {
    factors: Vec<Factor>,
    /// How often each name is read as a relation, bare.
    reads: HashMap<String, usize>,
    /// Whether a WITH stands anywhere but at the head.
    nested_with: bool,
}

impl Form {
    fn of(query: &Query) -> Form {
        let mut walk = Walk::default();
        if let Some(with) = &query.with {
            for (i, cte) in with.cte_tables.iter().enumerate() {
                walk.scope = Scope::With(i);
                walk.depth = 0;
                let _ = cte.query.visit(&mut walk);
            }
        }
        let mut main = query.clone();
        main.with = None;
        walk.scope = Scope::Main;
        walk.depth = 0;
        let _ = main.visit(&mut walk);
        Form {
            factors: walk.factors,
            reads: walk.reads,
            nested_with: walk.nested_with,
        }
    }
}

#[derive(Default)]
struct Walk {
    scope: Scope,
    depth: usize,
    /// Whether each frame open is an expression or a lateral relation.
    frames: Vec<bool>,
    factors: Vec<Factor>,
    reads: HashMap<String, usize>,
    nested_with: bool,
}

impl Visitor for Walk {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if self.depth > 0 && query.with.is_some() {
            self.nested_with = true;
        }
        if self.depth == 0 && query.with.is_some() && self.scope != Scope::Main {
            self.nested_with = true;
        }
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, _: &Expr) -> ControlFlow<()> {
        self.frames.push(true);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _: &Expr) -> ControlFlow<()> {
        self.frames.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        self.factors.push(Factor {
            index: self.factors.len(),
            factor: factor.clone(),
            scope: self.scope,
            free: !self.frames.contains(&true),
        });
        if let TableFactor::Table { name, .. } = factor {
            if let [ObjectNamePart::Identifier(ident)] = &name.0[..] {
                *self.reads.entry(normal(ident)).or_default() += 1;
            }
        }
        let lateral = !matches!(
            factor,
            TableFactor::Table { .. }
                | TableFactor::Derived { lateral: false, .. }
                | TableFactor::NestedJoin { .. }
        );
        self.frames.push(lateral);
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(&mut self, _: &TableFactor) -> ControlFlow<()> {
        self.frames.pop();
        ControlFlow::Continue(())
    }
}

/// Each relation read once in `read`, with each naming it reads back in.
pub(crate) fn places(read: &Read, resolve: Resolve, want: Option<Want>) -> Vec<Place> {
    let mut places = Vec::new();
    for (at, statement) in read.parsed() {
        let Statement::Query(query) = &statement.ast else {
            continue;
        };
        if matches!(
            query.body.as_ref(),
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_)
        ) {
            continue;
        }
        let form = Form::of(query);
        if form.nested_with {
            continue;
        }
        let head: Vec<&Cte> = query.with.iter().flat_map(|w| &w.cte_tables).collect();
        let names = Names { read, statement };
        for factor in &form.factors {
            let TableFactor::Derived {
                lateral: false,
                subquery,
                alias: Some(alias),
                sample: None,
            } = &factor.factor
            else {
                continue;
            };
            let compose = names.composes(subquery);
            let (written, written_name) = match compose {
                Some(_) => (TEMPLATE, "a template of its own"),
                None => (INLINE, "inline"),
            };
            if passed(want, &[written]) {
                continue;
            }
            let mut choices = vec![Choice::written(written_name, vec![written], &statement.ast)];
            let name = normal(&alias.name);
            let free_name = !head.iter().any(|c| normal(&c.alias.name) == name)
                && !form.reads.contains_key(&name);
            if factor.free && free_name && wants(want, WITH) {
                if let Some(choice) = names.hoisted(query, &head, factor, alias, subquery) {
                    choices.push(choice);
                }
            }
            if let Some(compose) = compose.filter(|_| wants(want, INLINE)) {
                if let Some(choice) = names.inlined(factor, compose, resolve) {
                    choices.push(choice);
                }
            }
            if choices.len() > 1 {
                let Some(first) = names.alias_first(alias) else {
                    continue;
                };
                let body = match compose {
                    Some(_) => read
                        .text(
                            statement,
                            subquery_first(statement, subquery),
                            subquery_last(statement, subquery),
                        )
                        .to_string(),
                    None => "a subquery".to_string(),
                };
                places.push(Place {
                    statement: at,
                    relations: None,
                    what: format!(
                        "the relation {} ({body})",
                        read.text(statement, first, first)
                    ),
                    choices,
                });
                if enough(want, &places) {
                    return places;
                }
            }
        }
        let Some(with) = &query.with else {
            continue;
        };
        if with.recursive {
            continue;
        }
        for (j, cte) in with.cte_tables.iter().enumerate() {
            let name = normal(&cte.alias.name);
            if cte.materialized.is_some()
                || cte.from.is_some()
                || form.reads.get(&name) != Some(&1)
                || !matches!(
                    cte.query.body.as_ref(),
                    SetExpr::Select(_)
                        | SetExpr::Query(_)
                        | SetExpr::SetOperation { .. }
                        | SetExpr::Values(_)
                )
            {
                continue;
            }
            let Some(reader) = form.factors.iter().find(|f| match &f.factor {
                TableFactor::Table { name: n, .. } => {
                    matches!(&n.0[..], [ObjectNamePart::Identifier(i)] if normal(i) == name)
                }
                _ => false,
            }) else {
                continue;
            };
            if reader.scope == Scope::With(j) || matches!(reader.scope, Scope::With(k) if k < j) {
                continue;
            }
            let compose = names.composes(&cte.query);
            if passed(want, &[WITH]) {
                continue;
            }
            let mut choices = vec![Choice::written(
                "a WITH query at the head",
                vec![WITH],
                &statement.ast,
            )];
            let at_reader = if compose.is_some() { TEMPLATE } else { INLINE };
            if wants(want, at_reader) {
                if let Some(choice) = names.at_reader(with, j, cte, reader, compose.is_some()) {
                    choices.push(choice);
                }
            }
            if let Some(compose) = compose.filter(|_| wants(want, INLINE)) {
                if let Some(choice) =
                    names.resolved_at_reader(with, j, cte, reader, compose, resolve)
                {
                    choices.push(choice);
                }
            }
            if choices.len() > 1 {
                let Some(first) = statement.ident_token(&cte.alias.name) else {
                    continue;
                };
                places.push(Place {
                    statement: at,
                    relations: None,
                    what: format!(
                        "the relation {} (a WITH query)",
                        read.text(statement, first, first)
                    ),
                    choices,
                });
                if enough(want, &places) {
                    return places;
                }
            }
        }
    }
    places
}

/// Writes a statement's relations under their other names.
struct Names<'r> {
    read: &'r Read,
    statement: &'r Stmt,
}

impl<'r> Names<'r> {
    /// The `:compose` a query is, where it composes a template by its path or a slot.
    fn composes(&self, query: &Query) -> Option<&'r ComposeRef> {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if query.with.is_some() || query.order_by.is_some() || query.limit_clause.is_some() {
            return None;
        }
        let [from] = &select.from[..] else {
            return None;
        };
        let TableFactor::Table {
            name, alias: None, ..
        } = &from.relation
        else {
            return None;
        };
        let [ObjectNamePart::Identifier(ident)] = &name.0[..] else {
            return None;
        };
        match self.read.stood.relation(&ident.value)? {
            Element::Compose(compose)
                if matches!(
                    compose.target,
                    ComposeTarget::Path(_) | ComposeTarget::Slot(_)
                ) =>
            {
                Some(compose)
            }
            _ => None,
        }
    }

    /// The first and last tokens of `factor`.
    fn factor(&self, factor: &Factor) -> Option<(usize, usize)> {
        self.statement.factor(&factor.factor)
    }

    /// The first token of `alias`.
    fn alias_first(&self, alias: &TableAlias) -> Option<usize> {
        self.statement.ident_token(&alias.name)
    }

    /// The template's bytes of tokens `first` to `last`.
    fn bytes(&self, first: usize, last: usize) -> (usize, usize) {
        self.read.written(self.statement, first, last)
    }

    /// The template's text of tokens `first` to `last`.
    fn text(&self, first: usize, last: usize) -> &str {
        self.read.text(self.statement, first, last)
    }

    /// The template's text between the parenthesis that opens at token `open` and the one that
    /// closes it, as written.
    fn between(&self, open: usize, close: usize) -> &str {
        let (_, start) = self.bytes(open, open);
        let (end, _) = self.bytes(close, close);
        &self.read.stood.text[start..end]
    }

    /// The parenthesis a derived table's query stands in, its first and last tokens.
    fn parens(&self, factor: &Factor, alias: &TableAlias) -> Option<(usize, usize)> {
        let (first, _) = self.factor(factor)?;
        if self.statement.tokens[first].token != Token::LParen {
            return None;
        }
        let mut close = self.statement.before(self.alias_first(alias)?)?;
        if self.statement.is(close, Keyword::AS) {
            close = self.statement.before(close)?;
        }
        (self.statement.tokens[close].token == Token::RParen).then_some((first, close))
    }

    /// The subquery `factor`, read once, written as a WITH query at the head.
    fn hoisted(
        &self,
        query: &Query,
        head: &[&Cte],
        factor: &Factor,
        alias: &TableAlias,
        subquery: &Query,
    ) -> Option<Choice> {
        let (open, close) = self.parens(factor, alias)?;
        let (_, last) = self.factor(factor)?;
        let alias_first = self.alias_first(alias)?;
        let name = self.text(alias_first, alias_first).to_string();
        let named = self.text(alias_first, last).to_string();
        let cte = format!("{named} AS ({})", self.between(open, close));
        let (start, end) = self.bytes(open, last);
        let mut edits: Edits = vec![(start, end, name.clone())];
        let position = match factor.scope {
            Scope::With(i) => {
                let first = self.statement.ident_token(&head[i].alias.name)?;
                let (at, _) = self.bytes(first, first);
                edits.push((at, at, format!("{cte},\n")));
                i
            }
            Scope::Main => match head.last() {
                Some(cte_last) => {
                    let close = self
                        .statement
                        .first_token(&cte_last.closing_paren_token.0)?;
                    let (_, at) = self.bytes(close, close);
                    edits.push((at, at, format!(",\n{cte}")));
                    head.len()
                }
                None => {
                    let (at, _) = self.bytes(0, 0);
                    edits.push((at, at, format!("WITH {cte}\n")));
                    0
                }
            },
        };
        let mut expected = self.statement.ast.clone();
        let table = table_named(&name)?;
        replace_factor(&mut expected, factor.index, table);
        let Statement::Query(expected_query) = &mut expected else {
            return None;
        };
        let new = Cte {
            alias: TableAlias {
                explicit: false,
                name: alias.name.clone(),
                columns: alias.columns.clone(),
                at: None,
            },
            query: Box::new(subquery.clone()),
            from: None,
            materialized: None,
            closing_paren_token: AttachedToken::empty(),
        };
        match &mut expected_query.with {
            Some(with) => with.cte_tables.insert(position, new),
            None => {
                expected_query.with = Some(With {
                    with_token: AttachedToken::empty(),
                    recursive: false,
                    cte_tables: vec![new],
                })
            }
        }
        let _ = query;
        Some(Choice {
            name: "a WITH query at the head".into(),
            picks: vec![WITH],
            edits,
            expected,
        })
    }

    /// The edits that take WITH query `j` out of the head `with`.
    fn without_cte(&self, with: &With, j: usize, cte: &Cte) -> Option<Edits> {
        let first = self.statement.ident_token(&cte.alias.name)?;
        let close = self.statement.first_token(&cte.closing_paren_token.0)?;
        let runs = if with.cte_tables.len() == 1 {
            let with_token = self.statement.first_token(&with.with_token.0)?;
            vec![
                (with_token, close),
                (close, spaced_to(self.statement, close)),
            ]
        } else if j == 0 {
            let comma = self.statement.after(close)?;
            (self.statement.tokens[comma].token == Token::Comma).then_some(())?;
            vec![(first, comma), (comma, spaced_to(self.statement, comma))]
        } else {
            let comma = self.statement.before(first)?;
            (self.statement.tokens[comma].token == Token::Comma).then_some(())?;
            vec![(spaced_from(self.statement, comma), comma), (first, close)]
        };
        let mut merged: Vec<(usize, usize)> = Vec::new();
        for (a, b) in runs {
            if let Some(previous) = merged.last_mut() {
                if a <= previous.1 + 1
                    || (previous.1 + 1..a).all(|t| plain_space(&self.statement.tokens[t].token))
                {
                    previous.1 = previous.1.max(b);
                    continue;
                }
            }
            merged.push((a, b));
        }
        Some(
            merged
                .into_iter()
                .map(|(a, b)| {
                    let (start, end) = self.bytes(a, b);
                    (start, end, String::new())
                })
                .collect(),
        )
    }

    /// WITH query `j`, read once by `reader`, written where it is read.
    fn at_reader(
        &self,
        with: &With,
        j: usize,
        cte: &Cte,
        reader: &Factor,
        template: bool,
    ) -> Option<Choice> {
        let TableFactor::Table { alias, .. } = &reader.factor else {
            return None;
        };
        if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) && !cte.alias.columns.is_empty() {
            return None;
        }
        let close = self.statement.first_token(&cte.closing_paren_token.0)?;
        let open = self.statement.after(self.cte_as(cte)?)?;
        (self.statement.tokens[open].token == Token::LParen).then_some(())?;
        let body = self.between(open, close).to_string();
        self.placed(
            with,
            j,
            cte,
            reader,
            &body,
            (*cte.query).clone(),
            if template { TEMPLATE } else { INLINE },
        )
    }

    /// The `AS` of WITH query `cte`.
    fn cte_as(&self, cte: &Cte) -> Option<usize> {
        let first = self.statement.ident_token(&cte.alias.name)?;
        let mut at = first;
        while !self.statement.is(at, Keyword::AS) {
            at = self.statement.after(at)?;
        }
        Some(at)
    }

    /// WITH query `j` written at `reader` as `body`, which reads as `query`.
    #[allow(clippy::too_many_arguments)]
    fn placed(
        &self,
        with: &With,
        j: usize,
        cte: &Cte,
        reader: &Factor,
        body: &str,
        query: Query,
        pick: &'static str,
    ) -> Option<Choice> {
        let TableFactor::Table { alias, .. } = &reader.factor else {
            return None;
        };
        let (first, last) = self.factor(reader)?;
        let (name_text, table_alias) = match alias {
            Some(alias) => {
                let alias_first = self.alias_first(alias)?;
                (
                    self.text(alias_first, last).to_string(),
                    TableAlias {
                        explicit: false,
                        name: alias.name.clone(),
                        columns: if alias.columns.is_empty() {
                            cte.alias.columns.clone()
                        } else {
                            alias.columns.clone()
                        },
                        at: None,
                    },
                )
            }
            None => {
                let name_first = self.statement.ident_token(&cte.alias.name)?;
                let name_last = self.statement.before(self.cte_as(cte)?)?;
                (
                    self.text(name_first, name_last).to_string(),
                    TableAlias {
                        explicit: false,
                        name: cte.alias.name.clone(),
                        columns: cte.alias.columns.clone(),
                        at: None,
                    },
                )
            }
        };
        let (start, end) = self.bytes(first, last);
        let mut edits = vec![(start, end, format!("({body}) {name_text}"))];
        edits.extend(self.without_cte(with, j, cte)?);
        let mut expected = self.statement.ast.clone();
        replace_factor(
            &mut expected,
            reader.index,
            TableFactor::Derived {
                lateral: false,
                subquery: Box::new(query),
                alias: Some(table_alias),
                sample: None,
            },
        );
        let Statement::Query(expected_query) = &mut expected else {
            return None;
        };
        if let Some(head) = &mut expected_query.with {
            head.cte_tables.remove(j);
            if head.cte_tables.is_empty() {
                expected_query.with = None;
            }
        }
        Some(Choice {
            name: match pick {
                TEMPLATE => "a template of its own".into(),
                _ => "inline".into(),
            },
            picks: vec![pick],
            edits,
            expected,
        })
    }

    /// WITH query `j`, a template of its own read once by `reader`, written inline there.
    fn resolved_at_reader(
        &self,
        with: &With,
        j: usize,
        cte: &Cte,
        reader: &Factor,
        compose: &ComposeRef,
        resolve: Resolve,
    ) -> Option<Choice> {
        let (text, query) = inline_text(compose, resolve)?;
        self.placed(with, j, cte, reader, &format!("\n{text}\n"), query, INLINE)
    }

    /// The template the derived table `factor` composes, written inline from its own text.
    fn inlined(&self, factor: &Factor, compose: &ComposeRef, resolve: Resolve) -> Option<Choice> {
        let TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } = &factor.factor
        else {
            return None;
        };
        let (text, query) = inline_text(compose, resolve)?;
        let (open, close) = self.parens(factor, alias)?;
        let first = self.statement.after(open)?;
        let last = self.statement.before(close)?;
        let (start, end) = self.bytes(first, last);
        let _ = subquery;
        let mut expected = self.statement.ast.clone();
        let mut inlined = factor.factor.clone();
        if let TableFactor::Derived { subquery, .. } = &mut inlined {
            **subquery = query;
        }
        replace_factor(&mut expected, factor.index, inlined);
        Some(Choice {
            name: "inline".into(),
            picks: vec![INLINE],
            edits: vec![(start, end, text)],
            expected,
        })
    }
}

/// The text of the template `compose` composes, each slot it leaves open written as the template
/// `compose` fills it with, with the query it reads as; none where it does not read as one query,
/// leaves a slot `compose` does not fill, or declares its dialects.
fn inline_text(compose: &ComposeRef, resolve: Resolve) -> Option<(String, Query)> {
    let ComposeTarget::Path(path) = &compose.target else {
        return None;
    };
    let text = resolve(path)?;
    let (dialects, marks) = marked(&text).ok()?;
    if !dialects.is_empty() {
        return None;
    }
    let mut out = String::new();
    let mut at = 0;
    for (start, end, mark) in marks {
        match mark {
            Mark::Directive(Element::Compose(ComposeRef {
                target: ComposeTarget::Slot(slot),
                slots,
            })) if slots.is_empty() => {
                let filled = compose.slots.iter().find(|s| s.name == slot)?;
                out.push_str(&text[at..start]);
                out.push_str(&format!(":compose({})", filled.path.display()));
                at = end;
            }
            Mark::Directive(Element::Compose(ComposeRef {
                target: ComposeTarget::Definition(_),
                ..
            }))
            | Mark::Declaration => return None,
            _ => {}
        }
    }
    out.push_str(&text[at..]);
    let out = out.trim_end().to_string();
    let read = Read::new(&out).ok()?;
    let [Ok(statement)] = &read.statements[..] else {
        return None;
    };
    if statement.text.trim_end().ends_with(';') {
        return None;
    }
    let Statement::Query(query) = &statement.ast else {
        return None;
    };
    let _: &Stood = &read.stood;
    Some((out, (**query).clone()))
}

/// The first token of a query that is a relation's stand-in.
fn subquery_first(statement: &Stmt, query: &Query) -> usize {
    statement.first_token(query).unwrap_or(0)
}

/// The last token of a query that is a relation's stand-in: its quoted name.
fn subquery_last(statement: &Stmt, query: &Query) -> usize {
    let first = subquery_first(statement, query);
    (first..statement.tokens.len())
        .find(
            |&t| matches!(&statement.tokens[t].token, Token::Word(w) if w.quote_style == Some('"')),
        )
        .unwrap_or(first)
}

/// A relation read by `name`, as a `FROM` holds it.
fn table_named(name: &str) -> Option<TableFactor> {
    let query = reparse(&format!("SELECT * FROM {name}"), |p| p.parse_query())?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    Some(select.from.first()?.relation.clone())
}

/// `statement` with its table factor `index`, in the order a visit reaches them, replaced by
/// `with`.
fn replace_factor(statement: &mut Statement, index: usize, with: TableFactor) {
    struct Replace {
        seen: usize,
        index: usize,
        with: Option<TableFactor>,
    }
    impl VisitorMut for Replace {
        type Break = ();
        fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
            if self.seen == self.index {
                if let Some(with) = self.with.take() {
                    *factor = with;
                }
                return ControlFlow::Break(());
            }
            self.seen += 1;
            ControlFlow::Continue(())
        }
    }
    let _ = statement.visit(&mut Replace {
        seen: 0,
        index,
        with: Some(with),
    });
}

#[cfg(test)]
mod tests {
    use super::super::laws::{holds, Trap};
    use super::super::{written_in, Axis};
    use super::*;
    use crate::mock::MockTable;
    use std::path::Path;

    /// The templates a test composes.
    const TEMPLATES: &[(&str, &str)] = &[
        ("scopes/city.sqlc", "# City's themes\nSELECT t.id AS theme_id FROM themes t WHERE t.name = 'City'\n"),
        ("shared/scope_sets.sqlc", "SELECT s.set_num FROM sets s JOIN (\n    :compose(@scope)\n) sc ON sc.theme_id = s.theme_id\n"),
    ];

    fn resolve(path: &Path) -> Option<String> {
        TEMPLATES
            .iter()
            .find(|(p, _)| Path::new(p) == path)
            .map(|(_, text)| text.to_string())
    }

    fn written(text: &str, pick: &str) -> String {
        written_in(text, Axis::Naming, pick, &resolve)
    }

    #[test]
    fn a_subquery_read_once_is_named_at_the_head_and_back() {
        let inline = "SELECT d.x FROM (SELECT t.x FROM t) d";
        let with = "WITH d AS (SELECT t.x FROM t)\nSELECT d.x FROM d";
        assert_eq!(written(inline, WITH), with);
        assert_eq!(written(with, INLINE), inline);
        assert_eq!(written(inline, INLINE), inline);
        assert_eq!(written(with, WITH), with);
    }

    #[test]
    fn a_subquery_goes_to_the_head_before_the_with_query_that_holds_it() {
        assert_eq!(
            written("WITH a AS (SELECT d.x FROM (SELECT t.x FROM t) d)\nSELECT a.x FROM a JOIN (SELECT u.x FROM u) e ON e.x = a.x", WITH),
            "WITH d AS (SELECT t.x FROM t),\na AS (SELECT d.x FROM d),\ne AS (SELECT u.x FROM u)\nSELECT a.x FROM a JOIN e ON e.x = a.x"
        );
        assert_eq!(
            written(
                "WITH d AS (SELECT t.x FROM t),\na AS (SELECT d.x FROM d)\nSELECT a.x FROM a",
                INLINE
            ),
            "SELECT a.x FROM (SELECT d.x FROM (SELECT t.x FROM t) d) a"
        );
    }

    #[test]
    fn a_template_of_its_own_is_written_at_the_head_or_inline_from_its_text() {
        let template = "SELECT sc.theme_id FROM (\n    :compose(scopes/city.sqlc)\n) sc";
        assert_eq!(
            written(template, WITH),
            "WITH sc AS (\n    :compose(scopes/city.sqlc)\n)\nSELECT sc.theme_id FROM sc"
        );
        assert_eq!(
            written(template, INLINE),
            "SELECT sc.theme_id FROM (\n    # City's themes\nSELECT t.id AS theme_id FROM themes t WHERE t.name = 'City'\n) sc"
        );
        assert_eq!(written(&written(template, WITH), TEMPLATE), template);
        let slot = "SELECT sc.x FROM (\n    :compose(@scope)\n) sc";
        assert_eq!(
            written(slot, WITH),
            "WITH sc AS (\n    :compose(@scope)\n)\nSELECT sc.x FROM sc"
        );
        assert_eq!(written(&written(slot, WITH), TEMPLATE), slot);
        let slotted = "SELECT x.set_num FROM (\n    :compose(shared/scope_sets.sqlc, @scope = scopes/city.sqlc)\n) x";
        assert_eq!(
            written(slotted, INLINE),
            "SELECT x.set_num FROM (\n    SELECT s.set_num FROM sets s JOIN (\n    # City's themes\nSELECT t.id AS theme_id FROM themes t WHERE t.name = 'City'\n) sc ON sc.theme_id = s.theme_id\n) x"
        );
    }

    #[test]
    fn a_relation_that_breaks_a_condition_keeps_its_name() {
        for (condition, text, pick) in [
            ("read laterally", "SELECT n.x FROM t CROSS JOIN LATERAL (SELECT u.x FROM u WHERE u.t = t.id) n", WITH),
            ("inside an expression", "SELECT t.x FROM t WHERE EXISTS (SELECT 1 FROM (SELECT u.x FROM u) d WHERE d.x = t.x)", WITH),
            ("its name read as a table", "SELECT d.x FROM (SELECT t.x FROM t) d JOIN d e ON e.x = d.x", WITH),
            ("its name a WITH query's", "WITH d AS (SELECT 1 AS x)\nSELECT e.x FROM (SELECT d.x FROM d) d", WITH),
            ("read twice", "WITH r AS (SELECT random() AS v)\nSELECT a.v = b.v FROM r a CROSS JOIN r b", INLINE),
            ("recursive", "WITH RECURSIVE r AS (SELECT 1 AS n)\nSELECT r.n FROM r", INLINE),
            ("materialized", "WITH r AS MATERIALIZED (SELECT 1 AS n)\nSELECT r.n FROM r", INLINE),
            ("a WITH below the head", "SELECT d.x FROM (WITH u AS (SELECT 1 AS x) SELECT u.x FROM u) d", WITH),
            ("a slot, inline", "SELECT sc.x FROM (\n    :compose(@scope)\n) sc", INLINE),
            ("a template that is not found", "SELECT sc.x FROM (\n    :compose(nowhere.sqlc)\n) sc", INLINE),
            ("a slot the compose does not fill", "SELECT x.set_num FROM (\n    :compose(shared/scope_sets.sqlc)\n) x", INLINE),
        ] {
            assert_eq!(written(text, pick), text, "{condition}");
        }
    }

    #[test]
    fn each_place_is_counted_with_each_naming() {
        let read = Read::new("WITH a AS (SELECT 1 AS x)\nSELECT a.x, sc.theme_id FROM a JOIN (\n    :compose(scopes/city.sqlc)\n) sc ON true").unwrap();
        let found: Vec<(String, Vec<String>)> = places(&read, &resolve, None)
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
                    "the relation sc (:compose(scopes/city.sqlc))".to_string(),
                    vec![
                        "a template of its own".into(),
                        "a WITH query at the head".into(),
                        "inline".into()
                    ]
                ),
                (
                    "the relation a (a WITH query)".to_string(),
                    vec!["a WITH query at the head".into(), "inline".into()]
                ),
            ]
        );
    }

    #[test]
    #[ignore = "needs a PostgreSQL server: set SQLC_TEST_POSTGRES_URL"]
    fn law_a_relation_read_once_returns_the_same_rows_under_each_name() {
        let sets = MockTable::new("sets")
            .column("set_num", "varchar")
            .column("theme_id", "integer")
            .row(&[Some("6000-1"), Some("1")])
            .row(&[Some("6000-1"), Some("1")])
            .row(&[Some("6001-1"), Some("2")])
            .row(&[Some("6002-1"), None]);
        let themes = MockTable::new("themes")
            .column("id", "integer")
            .column("name", "varchar")
            .row(&[Some("1"), Some("City")])
            .row(&[Some("2"), Some("Technic")]);
        let trap = Trap {
            tables: vec![sets, themes],
            binds: vec![],
            templates: TEMPLATES.iter().map(|(p, t)| (*p, t.to_string())).collect(),
        };
        let read_twice = (
            "SELECT a.v = b.v AS same FROM (SELECT random() AS v) a CROSS JOIN (SELECT random() AS v) b",
            "WITH r AS (SELECT random() AS v)\nSELECT a.v = b.v AS same FROM r a CROSS JOIN r b",
        );
        assert_ne!(written(read_twice.0, WITH), read_twice.1);
        for (text, pick, rows) in [
            ("SELECT x.set_num FROM (\n    :compose(shared/scope_sets.sqlc, @scope = scopes/city.sqlc)\n) x", INLINE, 2),
            ("SELECT x.set_num FROM (\n    :compose(shared/scope_sets.sqlc, @scope = scopes/city.sqlc)\n) x", WITH, 2),
            ("SELECT d.set_num FROM (SELECT s.set_num FROM sets s WHERE s.theme_id IS NOT NULL) d", WITH, 3),
            ("WITH d AS (SELECT s.set_num FROM sets s)\nSELECT d.set_num FROM d", INLINE, 4),
        ] {
            let spelled = written(text, pick);
            assert_eq!(holds(text, &spelled, read_twice, &trap).written, rows, "{spelled}");
        }
    }
}
