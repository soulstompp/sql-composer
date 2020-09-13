use crate::types::{ParsedItem, ParsedSpan, ParsedSql, ParsedSqlComposition, ParsedSqlStatement, Position, Span, Sql,
                   SqlBinding, SqlComposition, SqlCompositionAlias, SqlDbObject, SqlEnding,
                   SqlKeyword, SqlLiteral, SqlStatement};

use crate::error::Result;

use nom::{branch::alt,
          bytes::complete::{tag, tag_no_case, take, take_until, take_while1},
          character::complete::multispace0,
          combinator::{iterator, not, opt, peek},
          error::ErrorKind as NomErrorKind,
          multi::{many1, separated_list},
          sequence::{delimited, terminated},
          IResult, InputLength};

use nom_locate::position;

use std::path::PathBuf;

use std::fmt::Debug;

pub fn comma_padded(span: Span) -> IResult<Span, ()> {
    let (span, _) = multispace0(span)?;
    let (span, _) = tag(",")(span)?;
    let (span, _) = multispace0(span)?;

    Ok((span, ()))
}

pub fn ending(span: Span) -> IResult<Span, Span> {
    //! Checks for end of input
    //!
    //! succeeds if input_len is zero
    //! Returns an Eof Error if input_len is non-zero.
    //!   Returns Error rather than Failure to indicate
    //!   "I didn't match" vs "match can not succeed"
    match span.input_len() {
        0 => Ok((span, span)),
        _ => Err(nom::Err::Error((span, NomErrorKind::Eof))),
    }
}

pub fn statement(span: Span, alias: SqlCompositionAlias) -> Result<ParsedSqlStatement> {
    let start = span;
    let mut iter = iterator(span, sql_sets);
    let initial: Result<SqlStatement> = Ok(SqlStatement::default());

    let stmt = iter.fold(initial, |acc_res, items| match acc_res {
        Ok(mut acc) => {
            for pi in items {
                match pi.item {
                    Sql::Composition((sc, aliases)) => {
                        if acc.sql.len() == 0 {
                            let mut ss = SqlStatement::default();

                            ss.push_sql(ParsedItem {
                                item: Sql::Composition((sc, vec![])),
                                position: pi.position,
                            })?;

                            return Ok(ss);
                        }

                        acc.push_sql(ParsedItem {
                            item:     Sql::Composition((sc, aliases)),
                            position: pi.position,
                        })?;
                    }
                    _ => {
                        //acc.push_sql(item)?;
                        acc.push_sql(ParsedItem {
                            item:     pi.item,
                            position: pi.position,
                        })?;
                    }
                }
            }

            Ok(acc)
        }
        Err(e) => Err(e),
    })?;

    let (remaining, _) = iter.finish().expect("iterator should always finish");

    let pi = locate_parsed_item(stmt, Some(alias), &start, &remaining);
    
    Ok(pi)
}

pub fn sql_sets(span: Span) -> IResult<Span, Vec<ParsedSql>> {
    let (span, set) = alt((
        sql_ending_sql_set,
        bindvar_sql_set,
        composer_macro_sql_set,
        db_object_sql_set,
        keyword_sql_set,
        sql_literal_sql_set,
    ))(span)?;

    Ok((span, set))
}

pub fn parse_macro_name(span: Span) -> IResult<Span, ParsedItem<String>> {
    let (span, name) = delimited(tag(":"), take_until("("), tag("("))(span)?;

    Ok((
        span,
        ParsedItem::from_span(name.fragment.to_string(), name, None)
            .expect("invalid parsed item came from parser parse_macro_name"),
    ))
}

pub fn composer_macro_sql_set(span: Span) -> IResult<Span, Vec<ParsedSql>> {
    let start = span;

    let (span, sc) = composer_macro_item(span)?;

    let sc = Sql::Composition((sc.0.item, sc.1));
    
    Ok((
        span,
        vec![ParsedItem::from_span(sc, start, None)
            .expect("invalid parsed item from parser composer_macro_sql_set(span: Span)")],
    ))
}

pub fn composer_macro_item(
    span: Span,
) -> IResult<Span, (ParsedSqlComposition, Vec<SqlCompositionAlias>)> {
    let start = span;

    let (span, command) = parse_macro_name(span)?;
    let (span, distinct) = command_distinct_arg(span)?;
    let (span, _) = multispace0(span)?;
    let (span, all) = command_all_arg(span)?;
    let (span, _) = multispace0(span)?;
    let (span, columns) = opt(column_list)(span)?;
    let (span, _) = multispace0(span)?;
    let (span, of) = of_list(span)?;
    let (span, _) = tag(")")(span)?;

    let item = SqlComposition {
        command: Some(command),
        distinct,
        all,
        columns,
        of,
        ..Default::default()
    };

    let pi = locate_parsed_item(item, None, &start, &span);

    Ok((span, (pi, vec![])))
}

pub fn command_distinct_arg(span: Span) -> IResult<Span, Option<ParsedItem<bool>>> {
    let (span, distinct_tag) = opt(tag_no_case("distinct"))(span)?;

    let distinct = match distinct_tag {
        Some(d) => Some(
            ParsedItem::from_span(true, d, None)
                .expect("Unable to parse bool flag from command_distinct_arg"),
        ),
        None => None,
    };

    Ok((span, distinct))
}

pub fn command_all_arg(span: Span) -> IResult<Span, Option<ParsedItem<bool>>> {
    let (span, all_tag) = opt(tag_no_case("all"))(span)?;

    let all = match all_tag {
        Some(d) => Some(
            ParsedItem::from_span(true, d, None).expect("Unable to parse bool flag from command_all_arg"),
        ),
        None => None,
    };

    Ok((span, all))
}

pub fn of_padded(span: Span) -> IResult<Span, ()> {
    let (span, _) = multispace0(span)?;
    let (span, _) = tag("of")(span)?;
    let (span, _) = multispace0(span)?;

    Ok((span, ()))
}

pub fn column_list(span: Span) -> IResult<Span, Vec<ParsedItem<String>>> {
    let (span, columns) = separated_list(comma_padded, column_name)(span)?;
    let (span, _) = of_padded(span)?;

    Ok((span, columns))
}

pub fn column_item(span: Span) -> IResult<Span, ParsedItem<String>> {
    let (span, column) = terminated(column_name, opt(comma_padded))(span)?;

    Ok((span, column))
}

pub fn column_name(span: Span) -> IResult<Span, ParsedItem<String>> {
    let (span, column) = take_while_name_char(span)?;

    let p = ParsedItem::from_span(column.fragment.to_string(), column, None)
        .expect("unable to build ParsedItem of String from column_list parser");

    Ok((span, p))
}

pub fn take_while_name_char(span: Span) -> IResult<Span, Span> {
    let (span, name) = take_while1(|c| match c {
        'a'..='z' => true,
        'A'..='Z' => true,
        '0'..='9' => true,
        '_' => true,
        _ => false,
    })(span)?;

    Ok((span, name))
}

pub fn keyword_sql_set(span: Span) -> IResult<Span, Vec<ParsedSql>> {
    let (span, k) = keyword_item(span)?;

    Ok((
        span,
        vec![ParsedItem {
            item:     Sql::Keyword(k.item),
            position: k.position,
        }],
    ))
}

pub fn keyword_item(span: Span) -> IResult<Span, ParsedItem<SqlKeyword>> {
    let (span, keyword) = keyword_sql(span)?;
    let (span, _) = multispace0(span)?;

    let item = ParsedItem::from_span(
        SqlKeyword::new(keyword.fragment.to_string())
            .expect("SqlKeyword::new() failed unexpectedly from keyword parser"),
        keyword,
        None,
    )
    .expect("expected Ok from ParsedItem::from_span in keyword parser");

    Ok((span, item))
}

pub fn keyword_sql(span: Span) -> IResult<Span, Span> {
    let (span, keyword) = alt((command_sql, db_object_pre_sql, db_object_post_sql))(span)?;

    Ok((span, keyword))
}

pub fn command_sql(span: Span) -> IResult<Span, Span> {
    let (span, command) = alt((
        tag_no_case("SELECT"),
        tag_no_case("INSERT INTO"),
        tag_no_case("UPDATE"),
        tag_no_case("WHERE"),
    ))(span)?;

    Ok((span, command))
}

pub fn db_object_pre_sql(span: Span) -> IResult<Span, Span> {
    let (span, pre_sql) = alt((tag_no_case("FROM"), tag_no_case("JOIN")))(span)?;

    Ok((span, pre_sql))
}

pub fn db_object_post_sql(span: Span) -> IResult<Span, Span> {
    let (span, post_sql) = alt((tag_no_case("ON"), tag_no_case("USING")))(span)?;

    Ok((span, post_sql))
}

pub fn db_object_alias_sql(span: Span) -> IResult<Span, Span> {
    let (span, _) = opt(tag_no_case("AS"))(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = not(peek(keyword_sql))(span)?;
    let (span, _) = not(peek(tag("(")))(span)?;
    let (span, alias) = take_while_name_char(span)?;
    let (span, _) = multispace0(span)?;

    Ok((span, alias))
}

pub fn db_object_sql_set(span: Span) -> IResult<Span, Vec<ParsedSql>> {
    let (span, dbo) = db_object_item(span)?;

    Ok((
        span,
        vec![
            ParsedItem {
                item:     Sql::Keyword(dbo.0.item),
                position: dbo.0.position,
            },
            ParsedItem {
                item:     Sql::DbObject(dbo.1.item),
                position: dbo.1.position,
            },
        ],
    ))
}

pub fn db_object_item(
    span: Span,
) -> IResult<Span, (ParsedItem<SqlKeyword>, ParsedItem<SqlDbObject>)> {
    let (span, keyword) = db_object_pre_sql(span)?;
    let (span, _) = multispace0(span)?;
    let (span, table) = db_object_alias_sql(span)?;
    let (span, _) = multispace0(span)?;
    let (span, alias) = opt(db_object_alias_sql)(span)?;
    let (span, _) = multispace0(span)?;

    let k = SqlKeyword {
        value: keyword.fragment.to_string(),
    };

    let pk = ParsedItem::from_span(k, keyword, None)
        .expect("unable to build ParsedItem of SqlDbObject in db_object parser");

    let object_alias = alias.and_then(|a| Some(a.fragment.to_string()));

    let object = SqlDbObject {
        id: None,
        object_name: table.fragment.to_string(),
        object_alias,
    };

    let po = ParsedItem::from_span(object, table, None)
        .expect("unable to build ParsedItem of SqlDbObject in db_object parser");

    Ok((span, (pk, po)))
}

pub fn of_item(span: Span) -> IResult<Span, ParsedItem<SqlCompositionAlias>> {
    let start = span;

    let (span, of_name) = take_while1(|u| {
        let c = u as char;

        match c {
            'a'..='z' => true,
            'A'..='Z' => true,
            '0'..='9' => true,
            '-' | '_' => true,
            '.' | '/' | '\\' => true,
            _ => false,
        }
    })(span)?;

    //TODO: if we are going to disinguish between path and raw sql we should do it here in the
    //parser not in the real types
    // TODO: fix SqlCompositionAlias::from_span
    // let alias = SqlCompositionAlias::from_span(of_name).expect("expected alias from_span in of_list");
    let alias = SqlCompositionAlias::from(PathBuf::from(of_name.fragment));

    let pi = ParsedItem {
        item: alias, 
        position: ParsedSpan {
            alias: None,
            fragment: of_name.fragment.to_string(),
            line: start.line,
            offset: start.offset
        }.into()
    };

    Ok((span, pi))
}

pub fn of_list(span: Span) -> IResult<Span, Vec<ParsedItem<SqlCompositionAlias>>> {
    let (span, of_list) = many1(terminated(of_item, opt(comma_padded)))(span)?;

    Ok((span, of_list))
}

pub fn _parse_macro_include_alias(span: Span) -> IResult<Span, &str> {
    let (span, aliases) = take_while1(|u| {
        let c = u as char;

        match c {
            'a'..='z' => true,
            'A'..='Z' => true,
            '0'..='9' => true,
            '_' | '-' | '.' | '/' => true,
            _ => false,
        }
    })(span)?;

    Ok((span, aliases.fragment))
}

pub fn bindvar_expecting_exact(span: Span) -> IResult<Span, (Option<u32>, Option<u32>)> {
    let (span, exact_span) = take_while1(|c: char| c.is_digit(10))(span)?;

    let exact = exact_span
        .fragment
        .to_string()
        .parse::<u32>()
        .expect("exact could not be parsed as u32");

    Ok((span, (Some(exact), Some(exact))))
}

pub fn bindvar_expecting_min(span: Span) -> IResult<Span, u32> {
    let (span, _) = tag_no_case("min")(span)?;
    let (span, _) = multispace0(span)?;
    let (span, min_span) = take_while1(|c: char| c.is_digit(10))(span)?;

    let min = min_span
        .fragment
        .to_string()
        .parse::<u32>()
        .expect("min could not be parsed as u32");

    Ok((span, min))
}

pub fn bindvar_expecting_max(span: Span) -> IResult<Span, u32> {
    let (span, _) = tag_no_case("max")(span)?;
    let (span, _) = multispace0(span)?;
    let (span, max_span) = take_while1(|c: char| c.is_digit(10))(span)?;

    let max = max_span
        .fragment
        .to_string()
        .parse::<u32>()
        .expect("max could not be parsed as u32");

    Ok((span, max))
}

pub fn bindvar_expecting_min_max(span: Span) -> IResult<Span, (Option<u32>, Option<u32>)> {
    let (span, min) = opt(bindvar_expecting_min)(span)?;
    let (span, _) = multispace0(span)?;
    let (span, max) = opt(bindvar_expecting_max)(span)?;

    Ok((span, (min, max)))
}

pub fn bindvar_expecting(span: Span) -> IResult<Span, (Option<u32>, Option<u32>)> {
    let (span, _) = tag_no_case("expecting")(span)?;
    let (span, _) = multispace0(span)?;
    let (span, expecting) = alt((bindvar_expecting_exact, bindvar_expecting_min_max))(span)?;

    Ok((span, expecting))
}

pub fn bindvar_sql_set(span: Span) -> IResult<Span, Vec<ParsedSql>> {
    let (span, b) = bindvar_item(span)?;

    Ok((
        span,
        vec![ParsedItem {
            item:     Sql::Binding(b.item),
            position: b.position,
        }],
    ))
}

// name EXPECTING (i|MIN i|MAX i|MIN i MAX i)
pub fn bindvar_item(span: Span) -> IResult<Span, ParsedItem<SqlBinding>> {
    let (start_quote_span, start_quote) = opt(tag("'"))(span)?;
    let (span, _) = tag_no_case(":bind(")(start_quote_span)?;
    let (span, _) = multispace0(span)?;
    let (span, bindvar_name) = take_while_name_char(span)?;
    let (span, _) = multispace0(span)?;
    let (span, expecting) = opt(bindvar_expecting)(span)?;
    let (span, _) = multispace0(span)?;
    let (span, nullable) = opt(tag_no_case("null"))(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = tag(")")(span)?;
    let (span, _) = multispace0(span)?;
    let (span, end_quote) = opt(tag("'"))(span)?;

    let min = expecting.and_then(|m| m.0);
    let max = expecting.and_then(|m| m.1);

    if start_quote.is_some() && end_quote.is_none() {
        return Err(nom::Err::Failure((start_quote_span, NomErrorKind::Verify)));
    }
    else if end_quote.is_some() && start_quote.is_none() {
        return Err(nom::Err::Failure((span, NomErrorKind::Verify)));
    }

    let item = ParsedItem::from_span(
        SqlBinding::new(
            bindvar_name.fragment.to_string(),
            start_quote.is_some(),
            min,
            max,
            nullable.is_some(),
        )
        .expect("SqlBinding::new() failed unexpectedly from bindvar parser"),
        bindvar_name,
        None,
    )
    .expect("expected Ok from ParsedItem::from_span in bindvar parser");

    Ok((span, item))
}

pub fn sql_literal_sql_set(span: Span) -> IResult<Span, Vec<ParsedSql>> {
    let (span, l) = sql_literal_item(span)?;

    Ok((
        span,
        vec![ParsedItem {
            item:     Sql::Literal(l.item),
            position: l.position,
        }],
    ))
}

pub fn sql_literal(span: Span) -> IResult<Span, Span> {
    let (span, _) = not(peek(tag(":")))(span)?;
    let (span, _) = not(peek(tag(";")))(span)?;
    let (span, _) = not(peek(tag("'")))(span)?;
    let (span, _) = not(peek(db_object_pre_sql))(span)?;
    let (span, literal) = take(1u32)(span)?;

    Ok((span, literal))
}

named!(
    sql_literal_item(Span) -> ParsedItem<SqlLiteral>,
    complete!(
    do_parse!(
        pos: position!() >>
        parsed: fold_many1!(
            sql_literal,
            ParsedItem::from_span(SqlLiteral::default(), Span::new(""), None).expect("expected to make a Span in parse_sql parser"),
            |mut acc: ParsedItem<SqlLiteral>, item: Span| {
                acc.item.value.push_str(&item.fragment);
                acc
            }
        ) >>
        ({
            let mut p = parsed;

            p.position = Position::Parsed(ParsedSpan {
                line: pos.line,
                offset: pos.offset,
                fragment: p.item.value.to_string(),
                ..Default::default()
            });

            p.item.value = p.item.value.trim().to_string();
            p
        })
    ))
);

pub fn sql_ending_sql_set(span: Span) -> IResult<Span, Vec<ParsedSql>> {
    let (span, e) = sql_ending_item(span)?;

    Ok((
        span,
        vec![ParsedItem {
            item:     Sql::Ending(e.item),
            position: e.position,
        }],
    ))
}

pub fn sql_ending_item(span: Span) -> IResult<Span, ParsedItem<SqlEnding>> {
    let (span, ending) = tag(";")(span)?;
    let (span, _) = multispace0(span)?;

    let item = ParsedItem::from_span(
        SqlEnding::new(ending.fragment.to_string())
            .expect("SqlEnding::new() failed unexpectedly from parse_sql_end parser"),
        ending,
        None
    )
    .expect("expected Ok from ParsedItem::from_span in parse_sql_end");

    Ok((span, item))
}

pub fn locate_parsed_item<T: Debug + Default + PartialEq + Clone> (item: T, alias: Option<SqlCompositionAlias>, start: &Span, end: &Span) -> ParsedItem<T> {
    //NOTE: these parsers are likely using nom_locate wrong, because this feels like a heavy way to determine what span was parsed to make an object
    let fragment = start.fragment.strip_suffix(end.fragment).expect("missing characters between spans");

    ParsedItem {
        item, 
        position: ParsedSpan {
            alias: alias,
            fragment: fragment.to_string(),
            line: start.line,
            offset: start.offset
        }.into()
    }
}

#[cfg(test)]
mod tests {
    use super::{bindvar_expecting, bindvar_item, column_item, column_list, composer_macro_item,
                db_object_alias_sql, db_object_item, db_object_sql_set, ending, of_padded,
                sql_ending_item, sql_literal_item, statement};

    use crate::error::Result;
    use crate::types::{ParsedItem, ParsedSpan, ParsedSqlStatement, Position::Parsed, Span, Sql,
                       SqlComposition, SqlCompositionAlias, SqlDbObject, SqlEnding, SqlLiteral,
                       SqlStatement};

    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::path::PathBuf;

    type EmptyResult = Result<()>;

    use crate::tests::{build_parsed_binding_item, build_parsed_db_object,
                       build_parsed_ending_item, build_parsed_item, build_parsed_path_position,
                       build_parsed_quoted_binding_item, build_parsed_sql_binding,
                       build_parsed_sql_ending, build_parsed_sql_keyword,
                       build_parsed_sql_literal, build_parsed_sql_quoted_binding,
                       build_parsed_string, build_span};

    fn simple_aliases(
        shift_line: Option<u32>,
        shift_offset: Option<usize>,
    ) -> Vec<ParsedItem<SqlCompositionAlias>> {
        let shift_line = shift_line.unwrap_or(0);
        let shift_offset = shift_offset.unwrap_or(0);

        let item: SqlCompositionAlias = PathBuf::from("src/tests/simple-template.tql").into();

        vec![build_parsed_item(
            item,
            None,
            Some(1 + shift_line),
            Some(24 + shift_offset),
            "src/tests/simple-template.tql",
        )]
    }

    fn include_aliases() -> Vec<ParsedItem<SqlCompositionAlias>> {
        let item: SqlCompositionAlias = PathBuf::from("src/tests/include-template.tql").into();

        vec![build_parsed_item(
            item,
            None,
            None,
            Some(24),
            "src/tests/include-template.tql",
        )]
    }

    fn simple_alias_hash() -> HashMap<SqlCompositionAlias, ParsedSqlStatement> {
        let mut acc = HashMap::new();

        let p = PathBuf::from("src/tests/simple-template.tql");

        acc.entry(SqlCompositionAlias::from(&p)).or_insert(
            ParsedSqlStatement::try_from(p).expect("expected to parse into ParsedSqlStatement"),
        );

        acc
    }

    fn include_alias_hash() -> HashMap<SqlCompositionAlias, ParsedSqlStatement> {
        let mut acc = simple_alias_hash();

        let p = PathBuf::from("src/tests/include-template.tql");

        acc.entry(SqlCompositionAlias::from(&p)).or_insert(
            ParsedSqlStatement::try_from(p).expect("expected to parse into ParsedSqlStatement"),
        );

        acc
    }

    fn include_shallow_alias_hash() -> HashMap<SqlCompositionAlias, ParsedSqlStatement> {
        let mut acc = HashMap::new();

        let p = PathBuf::from("src/tests/include-template.tql");

        acc.entry(SqlCompositionAlias::from(&p)).or_insert(
            ParsedSqlStatement::try_from(p).expect("expected to parse into ParsedSqlStatement"),
        );

        acc
    }

    fn simple_statement_comp(
        shift_line: Option<u32>,
        shift_offset: Option<usize>,
    ) -> ParsedSqlStatement {
        let _shift_line = shift_line.unwrap_or(0);
        let _shift_offset = shift_offset.unwrap_or(0);

        let position = Some(build_parsed_path_position(
            PathBuf::from("src/tests/simple-template.tql").into(),
            1,
            0,
            "SELECT foo_id, bar FROM foo WHERE foo.bar = :bind(varname);\n",
        ));

        let item = SqlStatement {
            sql: vec![
                build_parsed_sql_keyword("SELECT", None, None, "SELECT"),
                build_parsed_sql_literal("foo_id, bar", None, Some(7), "foo_id, bar "),
                build_parsed_sql_keyword("FROM", None, Some(19), "FROM"),
                build_parsed_db_object("foo", None, None, Some(24), "foo"),
                build_parsed_sql_keyword("WHERE", None, Some(28), "WHERE"),
                build_parsed_sql_literal("foo.bar =", None, Some(34), "foo.bar = "),
                build_parsed_sql_binding("varname", None, None, false, None, Some(50), "varname"),
                build_parsed_sql_ending(";", None, Some(58), ";"),
            ],
            complete: true,
            ..Default::default()
        };

        ParsedItem {
            item,
            position: position.expect("position is some")
        }
    }

    fn include_statement_comp() -> ParsedSqlStatement {
        let position = Some(build_parsed_path_position(
            PathBuf::from("src/tests/include-template.tql").into(),
            1,
            0,
            "SELECT COUNT(foo_id)\nFROM (\n  :compose(src/tests/simple-template.tql)\n);\n",
        ));

        let item = SqlStatement {
            sql: vec![
                build_parsed_sql_keyword("SELECT", None, None, "SELECT"),
                build_parsed_sql_literal("COUNT(foo_id)", None, Some(7), "COUNT(foo_id)\n"),
                build_parsed_sql_keyword("FROM", Some(2), Some(21), "FROM"),
                build_parsed_sql_literal("(", Some(2), Some(26), "(\n  "),
                build_parsed_item(
                    Sql::Composition((
                        simple_statement_compose_comp(Some(2), Some(15)).item,
                        vec![],
                    )),
                    None,
                    Some(2),
                    Some(27),
                    ":",
                ),
                build_parsed_sql_literal(")", Some(3), Some(69), "\n)"),
                build_parsed_sql_ending(";", Some(4), Some(71), ";"),
            ],
            ..Default::default()
        };

        build_parsed_item(item, None, None, None, "")
    }

    fn simple_statement_compose_comp(
        shift_line: Option<u32>,
        shift_offset: Option<usize>,
    ) -> ParsedItem<SqlComposition> {
        let shift_line = shift_line.unwrap_or(0);
        let shift_offset = shift_offset.unwrap_or(0);

        let item = SqlComposition {
            command: Some(build_parsed_string(
                "compose",
                Some(1 + shift_line),
                Some(16 + shift_offset),
                "compose",
            )),
            of: simple_aliases(Some(0 + shift_line), Some(0 + shift_offset)),
            aliases: HashMap::new(),
            ..Default::default()
        };

        build_parsed_item(item, None, None, None, "")
    }

    fn include_statement_compose_comp() -> ParsedItem<SqlComposition> {
        let item = SqlComposition {
            command: Some(build_parsed_string("compose", None, Some(16), "compose")),
            of: include_aliases(),
            aliases: HashMap::new(),
            ..Default::default()
        };

        build_parsed_item(item, None, None, None, "")
    }

    #[test]
    fn it_parses_bindvar() {
        let input = ":bind(varname)blah blah blah";

        let out = bindvar_item(Span::new(input.into())).expect("expected Ok from bindvar");

        let expected_span = build_span(Some(1), Some(14), "blah blah blah");
        let expected_item =
            build_parsed_binding_item("varname", None, None, false, None, Some(6), "varname");

        let (span, item) = out;

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn it_parses_bindvar_expecting_only_min() {
        let input = "EXPECTING MIN 1blah blah blah";

        let out =
            bindvar_expecting(Span::new(input.into())).expect("expected Ok from bindvar_expecting");

        let expected_span = build_span(Some(1), Some(15), "blah blah blah");
        let expected_item = (Some(1), None);

        let (span, item) = out;

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn it_parses_bindvar_expecting_only_max() {
        let input = "EXPECTING MAX 1blah blah blah";

        let out = bindvar_expecting(Span::new(input.into())).expect("expected Ok from bindvar");

        let expected_span = build_span(Some(1), Some(15), "blah blah blah");
        let expected_item = (None, Some(1));

        let (span, item) = out;

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn it_parses_bindvar_expecting_min_and_max() {
        let input = "EXPECTING MIN 1 MAX 3blah blah blah";

        let out =
            bindvar_expecting(Span::new(input.into())).expect("expected Ok from bindvar_expecting");

        let expected_span = build_span(Some(1), Some(21), "blah blah blah");
        let expected_item = (Some(1), Some(3));

        let (span, item) = out;

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn it_parses_bindvar_expecting_exact() {
        let input = "EXPECTING 1blah blah blah";

        let out = bindvar_expecting(Span::new(input.into())).expect("expected Ok from bindvar");

        let expected_span = build_span(Some(1), Some(11), "blah blah blah");
        let expected_item = (Some(1), Some(1));

        let (span, item) = out;

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn it_parses_quoted_bindvar() {
        let input = "':bind(varname)'blah blah blah";

        let out = bindvar_item(Span::new(input.into())).expect("expected Ok from bindvar");

        let expected_span = build_span(Some(1), Some(16), "blah blah blah");
        let expected_item = build_parsed_quoted_binding_item(
            "varname",
            None,
            None,
            false,
            None,
            Some(7),
            "varname",
        );

        let (span, item) = out;

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_sql_ending_item() {
        let input = ";blah blah blah";

        let expected_span = build_span(Some(1), Some(1), "blah blah blah");

        let expected_item = build_parsed_ending_item(";", None, None, ";");

        let (span, item) =
            sql_ending_item(Span::new(input.into())).expect("expected Ok from parse_sql_end");

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_sql_until_path() {
        let input = "foo.bar = :bind(varname);";

        let out = sql_literal_item(Span::new(input.into())).expect("expected Ok from parse_sql");

        let (span, item) = out;

        let expected_span = build_span(Some(1), Some(10), ":bind(varname);");

        let expected = SqlLiteral {
            value: "foo.bar =".into(),
            ..Default::default()
        };

        let expected_item = build_parsed_item(expected, None, Some(1), Some(0), "foo.bar = ");

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_simple_statement() {
        let input =
            "SELECT * FROM (:compose(src/tests/simple-template.tql)) WHERE name = ':bind(bindvar)';";

        let item =
            statement(Span::new(input.into()), input.into()).expect("expected Ok from statement");

        let position = Some(
                Parsed(ParsedSpan {
                    alias: Some(
                               SqlCompositionAlias::SqlLiteral(SqlLiteral {
                                   id: None,
                                   value: "SELECT * FROM (:compose(src/tests/simple-template.tql)) WHERE name = \':bind(bindvar)\';".to_string(),
                                   generated: false,
                               }
                               )),
                               line: 1,
                               offset: 0,
                               fragment: "SELECT * FROM (:compose(src/tests/simple-template.tql)) WHERE name = \':bind(bindvar)\';".to_string(),
                })
            );

        let expected_item = SqlStatement {
            sql:      vec![
                build_parsed_sql_keyword("SELECT", None, None, "SELECT"),
                build_parsed_sql_literal("*", None, Some(7), "* "),
                build_parsed_sql_keyword("FROM", None, Some(9), "FROM"),
                build_parsed_sql_literal("(", None, Some(14), "("),
                build_parsed_item(
                    Sql::Composition((simple_statement_compose_comp(None, None).item, vec![])),
                    None,
                    None,
                    Some(15),
                    "(",
                ),
                build_parsed_sql_literal(") WHERE name =", None, Some(54), ") WHERE name = "),
                build_parsed_sql_quoted_binding(
                    "bindvar",
                    None,
                    None,
                    false,
                    None,
                    Some(76),
                    "bindvar",
                ),
                build_parsed_sql_ending(";", None, Some(85), ";"),
            ],
            complete: true,
        };

        let expected_item = build_parsed_item(expected_item, None, None, None, "");

        assert_eq!(item, expected_item, "items match");
    }

    #[test]
    fn test_parse_include_statement() -> EmptyResult {
        let input = "SELECT * FROM (:compose(src/tests/include-template.tql)) WHERE name = ':bind(bindvar)';";

        let out = statement(Span::new(input.into()), input.into())?;

        let position = Some(
            Parsed(
                ParsedSpan {
                    alias: Some(
                        SqlCompositionAlias::SqlLiteral(
                            SqlLiteral {
                                id: None,
                                value: "SELECT * FROM (:compose(src/tests/include-template.tql)) WHERE name = \':bind(bindvar)\';".into(), 
                                generated: false
                            }
                        )
                    ),
                    line: 1,
                    offset: 0,
                    fragment: "SELECT * FROM (:compose(src/tests/include-template.tql)) WHERE name = \':bind(bindvar)\';".into(),
                }
            )
        );

        let expected_comp = SqlStatement {
            sql: vec![
                build_parsed_sql_keyword("SELECT", None, None, "SELECT"),
                build_parsed_sql_literal("*", None, Some(7), "* "),
                build_parsed_sql_keyword("FROM", None, Some(9), "FROM"),
                build_parsed_sql_literal("(", None, Some(14), "("),
                build_parsed_item(
                    Sql::Composition((include_statement_compose_comp().item, vec![])),
                    None,
                    None,
                    Some(15),
                    ":",
                ),
                build_parsed_sql_literal(") WHERE name =", None, Some(55), ") WHERE name = "),
                build_parsed_sql_quoted_binding(
                    "bindvar",
                    None,
                    None,
                    false,
                    None,
                    Some(77),
                    "bindvar",
                ),
                build_parsed_sql_ending(";".into(), None, Some(86), ";"),
            ],
            ..Default::default()
        };

        let expected_comp = build_parsed_item(expected_comp, None, None, None, "");

        assert_eq!(out, expected_comp);
        Ok(())
    }

    #[test]
    fn test_parse_file_statement() {
        let stmt = ParsedSqlStatement::try_from(PathBuf::from("src/tests/simple-template.tql"))
            .expect("expected Ok from ParsedSqlComposition try_from");

        let expected = simple_statement_comp(None, None);

        assert_eq!(stmt, expected);
    }

    #[test]
    fn test_parse_file_inclusive_statement() {
        let stmt = ParsedSqlStatement::try_from(PathBuf::from("src/tests/include-template.tql"))
            .expect("expected Ok from ParsedSqlComposition try_from");
        let expected = include_statement_comp();

        assert_eq!(stmt, expected);
    }

    #[test]
    fn test_parse_composed_composer() {
        let sql_str = ":count(distinct col1, col2 of src/tests/simple-template.tql, src/tests/include-template.tql);";

        let comp = composer_macro_item(Span::new(sql_str.into()));

        let span = Span {
            offset:   92,
            line:     1,
            fragment: ";".into(),
            extra:    (),
        };

        let sc = SqlComposition {
                command: Some(build_parsed_string("count", None, Some(1), "count")),
                position: None,
                distinct: Some(build_parsed_item(true, None, None, Some(7), "distinct")),
                columns: Some(vec![
                    build_parsed_string("col1", None, Some(16), "col1"),
                    build_parsed_string("col2", None, Some(22), "col2"),
                ]),
                of: vec![
                    build_parsed_item(
                        SqlCompositionAlias::from(PathBuf::from(
                            "src/tests/simple-template.tql",
                        )),
                        None,
                        None,
                        Some(30),
                        "src/tests/simple-template.tql",
                    ),
                    build_parsed_item(
                        SqlCompositionAlias::Path("src/tests/include-template.tql".into()),
                        None,
                        None,
                        Some(61),
                        "src/tests/include-template.tql",
                    ),
                ],
                aliases: HashMap::new(),
                ..Default::default()
        };

        let psc = ParsedItem::from_span(sc, Span {
            offset:   0,
            line:     1,
            fragment: ":count(distinct col1, col2 of src/tests/simple-template.tql, src/tests/include-template.tql)".into(),
            extra:    (),
        }, None).expect("expected to convert from span");

        let expected = Ok((
            span,
            (psc, vec![]) 
        ));

        assert_eq!(comp, expected);
    }

    #[test]
    fn test_simple_composed_composer() -> EmptyResult {
        let sql_str = ":count(src/tests/simple-template.tql);";

        let stmt_item = ParsedSqlStatement::parse(sql_str)?;

        let comp = SqlComposition {
                command: Some(build_parsed_string("count", None, Some(1), "count")),
                position: None,
                of: vec![build_parsed_item(
                    SqlCompositionAlias::Path("src/tests/simple-template.tql".into()),
                    None,
                    None,
                    Some(7),
                    "src/tests/simple-template.tql",
                )],
                aliases: HashMap::new(),
                ..Default::default()
        };

        let comp = build_parsed_item(Sql::Composition((comp, vec![])), None,Some(1), Some(0), sql_str);

        let ending = build_parsed_item(SqlEnding::new(";".into())?.into(), None, None, Some(37), ";");

        let expected = build_parsed_item(
            SqlStatement {
                sql:      vec![comp, ending],
                complete: true,
            },
            None,
            Some(1),
            Some(0),
            sql_str,
        );

        assert_eq!(stmt_item, expected);
        Ok(())
    }

    #[test]
    fn test_parse_column_item() {
        let input = "col_1 , of ";
        let expected_fragment = "col_1";
        let expected_span_fragment = "of ";

        let result = column_item(Span::new(input.into()));
        match result {
            Ok((span, item)) => {
                println!("parse_column item={:?}", item);
                println!("parse_column span={:?}", span);
                assert_eq!(item.item, expected_fragment, "parse_column returns item");
                assert_eq!(
                    span.fragment, expected_span_fragment,
                    "parse_column returns span fragment"
                )
            }
            Err(e) => panic!("parse_column failed with e={:?}", e),
        }
    }

    #[test]
    fn test_of_padded() {
        let input = "of ";
        let expected_item = (); // of_padded returns an empty item

        match of_padded(Span::new(input.into())) {
            Ok((span, item)) => {
                println!("of_padded item={:?}", item);
                println!("of_padded span={:?}", span);
                assert_eq!(item, expected_item, "of_padded returned item");
                assert_eq!(span.fragment, "".to_string(), "returns empty span")
            }
            Err(e) => panic!("parse_column failed with e={:?}", e),
        }
    }

    #[test]
    fn test_parse_single_column_list() {
        let input = "col_1 of ";

        let expected_span = build_span(Some(1), Some(9), "");

        let expected_item = vec![build_parsed_item("col_1".to_string(), None, None, None, "col_1")];

        let (span, item) =
            column_list(Span::new(input.into())).expect("expected Ok from column_list");

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_multi_column_list() {
        let input = "col_1, col_2, col_3 of ";

        let expected_remaining_fragment = "";
        let expected_fragments = vec!["col_1", "col_2", "col_3"];

        match column_list(Span::new(input.into())) {
            Ok((span, items)) => {
                println!("items: {:?}, span {:?}", items, span);
                let items: Vec<String> = items.into_iter().map(|i| i.item).collect();
                assert_eq!(items, expected_fragments, "items match");
                assert_eq!(
                    span.fragment, expected_remaining_fragment,
                    "span fragments match"
                );
            }
            Err(e) => panic!("column_list failed with e={:?}", e),
        }
    }

    #[test]
    fn test_parse_db_object_alias_with_as() {
        let input = "AS tt WHERE 1";

        let expected_span = build_span(Some(1), Some(3), "tt");

        let (_leftover_span, span) = db_object_alias_sql(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_alias_with_as_preceding_space() {
        let input = " AS tt WHERE 1";

        let expected_span = build_span(Some(1), Some(1), "AS");

        let (_leftover_span, span) = db_object_alias_sql(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_alias_without_as() {
        let input = "tt WHERE 1";

        let expected_span = build_span(Some(1), Some(0), "tt");

        let (_leftover_span, span) = db_object_alias_sql(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_alias_without_as_preceeding_space() {
        let input = "tt WHERE 1";

        let expected_span = build_span(Some(1), Some(0), "tt");

        let (_leftover_span, span) = db_object_alias_sql(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_alias_with_as_preceded_space() {
        let input = " tt WHERE 1";

        let expected_span = build_span(Some(1), Some(1), "tt");

        let (_leftover_span, span) = db_object_alias_sql(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_with_no_alias() {
        let input = "FROM t1 WHERE 1";

        let expected_span = build_span(Some(1), Some(8), "WHERE 1");

        let expected_dbo = SqlDbObject {
            id:           None,
            object_name:  "t1".into(),
            object_alias: None,
        };

        let expected_dbo_item = build_parsed_item(expected_dbo, None, None, Some(5), "t1");

        let (span, (_keyword_item, dbo_item)) = db_object_item(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(dbo_item, expected_dbo_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_with_alias() {
        let input = "FROM t1 tt WHERE 1";

        let expected_span = build_span(Some(1), Some(11), "WHERE 1");

        let expected_dbo = SqlDbObject {
            id:           None,
            object_name:  "t1".into(),
            object_alias: Some("tt".into()),
        };

        let expected_dbo_item = build_parsed_item(expected_dbo, None, None, Some(5), "t1");

        let (span, (_keyword_item, dbo_item)) = db_object_item(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(dbo_item, expected_dbo_item, "DbObject items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_with_as_alias() {
        let input = "FROM t1 tt WHERE 1";

        let expected_span = build_span(Some(1), Some(11), "WHERE 1");

        let expected_dbo = SqlDbObject {
            id:           None,
            object_name:  "t1".into(),
            object_alias: Some("tt".into()),
        };

        let expected_dbo_item = build_parsed_item(expected_dbo, None, None, Some(5), "t1");

        let (span, (_keyword_item, dbo_item)) = db_object_item(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(dbo_item, expected_dbo_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_with_subquery() {
        let input = "FROM (SELECT * FROM t1) AS tt WHERE 1";

        db_object_sql_set(Span::new(input.into()))
            .expect_err(&format!("expected error from parsing {}", input));
    }

    #[test]
    fn test_ending() {
        let success_cases = [""];
        let failure_cases = ["foo", " ", "\n"];

        for &input in success_cases.iter() {
            match ending(Span::new(input)) {
                Ok((remaining, output)) => {
                    let expected_output = Span::new(input);
                    let expected_fragment = input;

                    assert_eq!(output, expected_output, "correct output for {:?}", input);
                    assert_eq!(
                        remaining.fragment, expected_fragment,
                        "input not consumed for {:?}",
                        input
                    );
                }
                Err(e) => {
                    println!("ending for input={:?} returned an error={:?}", input, e);
                    panic!(e)
                }
            };
        }

        for &input in failure_cases.iter() {
            match ending(Span::new(input)) {
                Ok((remaining, output)) => {
                    assert!(false, "ending() for input={:?} should not have succeeded. remaining:{:?} output:{:?}",
                            input, remaining, output);
                }
                Err(e) => {
                    assert!(
                        true,
                        "ending() for input='{:?}' returned an error={:?}",
                        input, e
                    );
                }
            };
        }
    }
}

pub fn bracket_start_fn<'a>(
    span: Span,
) -> IResult<Span, (Span, impl FnOnce(Span<'a>) -> IResult<Span, Span>)> {
    let (span, start) = alt((tag("["), tag("(")))(span)?;
    let (span, _) = multispace0(span)?;

    let bracket_end_func = tag::<&'static str, Span, _>(match start.fragment {
        "[" => "]",
        "(" => ")",
        _ => unreachable!(),
    });

    Ok((span, (start, Box::new(bracket_end_func))))
}
