use crate::types::{ParsedItem, ParsedSpan, Position, Span, Sql, SqlBinding, SqlComposition,
                   SqlCompositionAlias, SqlDbObject, SqlEnding, SqlKeyword, SqlLiteral};

use crate::error::Result;

use nom::{branch::alt,
          bytes::complete::{tag, tag_no_case, take, take_until, take_while1},
          character::complete::multispace0,
          combinator::{iterator, not, opt, peek},
          error::ErrorKind as NomErrorKind,
          multi::many1,
          sequence::{delimited, terminated},
          IResult};

#[cfg(feature = "composer-serde")]
use nom::{
          character::complete::{digit1, one_of},
          multi::separated_list,
          number::complete::double};

#[cfg(feature = "composer-serde")]
use serde_value::Value;

#[cfg(feature = "composer-serde")]
use crate::types::SerdeValue;

#[cfg(feature = "composer-serde")]
use std::collections::BTreeMap;

#[cfg(feature = "composer-serde")]
use std::str::FromStr;

pub fn comma_padded(span: Span) -> IResult<Span, ()> {
    let (span, _) = multispace0(span)?;
    let (span, _) = tag(",")(span)?;
    let (span, _) = multispace0(span)?;

    Ok((span, ()))
}

pub fn template(
    span: Span,
    alias: Option<SqlCompositionAlias>,
) -> Result<ParsedItem<SqlComposition>> {
    let comp = SqlComposition::default();

    let mut iter = iterator(span, sql_sets);

    let mut comp = iter.fold(ParsedItem::from_span(comp, Span::new(""), None), |acc_res, items| {
        match acc_res {
            Ok(mut acc) => {
                for item in items {
                    match item {
                        Sql::Composition((mut sc, aliases)) => {
                            for alias in &aliases {
                                let stmt_path = alias.path().expect("expected alias path");

                                sc.item.insert_alias(&stmt_path).expect("expected insert_alias");
                            }

                            if acc.item.sql.len() == 0 {
                                return Ok(sc);
                            }

                            acc.item.push_sql(Sql::Composition((sc, aliases)))?;
                        }
                        _ => {
                            acc.item.push_sql(item)?;
                        }
                    }
                }

                Ok(acc)
            }
            Err(e) => Err(e)
        }
    })?;

    let (_remaining, _) = iter.finish().expect("iterator should always finish");

    if let Some(a) = alias {
        comp.item
            .set_position(Position::Parsed(ParsedSpan::new(span, Some(a))))?;
    }


    Ok(comp)
}

pub fn sql_sets(span: Span) -> IResult<Span, Vec<Sql>> {
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

pub fn composer_macro_sql_set(span: Span) -> IResult<Span, Vec<Sql>> {
    let (span, sc) = composer_macro_item(span)?;

    let c = Sql::Composition((
        ParsedItem::from_span(sc.0, Span::new(""), None)
            .expect("invalid parsed item from parser composer_macro_sql_set(span: Span)"),
        sc.1,
    ));

    Ok((span, vec![c]))
}

pub fn composer_macro_item(span: Span) -> IResult<Span, (SqlComposition, Vec<SqlCompositionAlias>)> {
    let (span, command) = parse_macro_name(span)?;
    let (span, distinct) = command_distinct_arg(span)?;
    let (span, _) = multispace0(span)?;
    let (span, all) = command_all_arg(span)?;
    let (span, _) = multispace0(span)?;
    let (span, columns) = opt(column_list)(span)?;
    let (span, _) = multispace0(span)?;
    let (span, of) = of_list(span)?;
    let (span, _) = tag(")")(span)?;

    let mut sc = SqlComposition {
        command: Some(command),
        distinct,
        all,
        columns,
        of,
        ..Default::default()
    };

    sc.update_aliases().expect("expected to update aliases");

    Ok((span, (sc, vec![])))
}

pub fn command_distinct_arg(span: Span) -> IResult<Span, Option<ParsedItem<bool>>> {
    let (span, distinct_tag) = opt(tag_no_case("distinct"))(span)?;

    let distinct = match distinct_tag {
        Some(d) => {
            Some(ParsedItem::from_span(true, d, None).expect("Unable to parse bool flag from command_distinct_arg"))
        },
        None    => None
    };

    Ok((span, distinct))
}

pub fn command_all_arg(span: Span) -> IResult<Span, Option<ParsedItem<bool>>> {
    let (span, all_tag) = opt(tag_no_case("all"))(span)?;

    let all = match all_tag {
        Some(d) => {
            Some(ParsedItem::from_span(true, d, None).expect("Unable to parse bool flag from command_all_arg"))
        }
        None    => None
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
    let (span, columns) = terminated(many1(column_item), of_padded)(span)?;

    Ok((span, columns))
}

pub fn column_item(span: Span) -> IResult<Span, ParsedItem<String>> {
    let (span, column) = terminated(column_name, opt(comma_padded))(span)?;

    Ok((span, column))
}

pub fn column_name(span: Span) -> IResult<Span, ParsedItem<String>> {
    let (span, column) = take_while_name_char(span)?;

    let p = ParsedItem::from_span(
        column.fragment.to_string(),
        column,
        None
    ).expect("unable to build ParsedItem of String from column_list parser");

    Ok((span, p))
}

pub fn take_while_name_char(span: Span) -> IResult<Span, Span> {
    let (span, name) = take_while1(|c| {
        match c {
            'a'..='z' => true,
            'A'..='Z' => true,
            '0'..='9' => true,
            '_' => true,
            _ => false,
        }
    })(span)?;

    Ok((span, name))
}

pub fn keyword_sql_set(span: Span) -> IResult<Span, Vec<Sql>> {
    let (span, k) = keyword_item(span)?;

    Ok((span, vec![Sql::Keyword(k)]))
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
    let (span, keyword) = alt((
            command_sql,
            db_object_pre_sql,
            db_object_post_sql
    ))(span)?;

    Ok((span, keyword))
}

pub fn command_sql(span: Span) -> IResult<Span, Span> {
    let (span, command) = alt((
            tag_no_case("SELECT"),
            tag_no_case("INSERT INTO"),
            tag_no_case("UPDATE"),
            tag_no_case("WHERE")
    ))(span)?;

    Ok((span, command))
}

pub fn db_object_pre_sql(span: Span) -> IResult<Span, Span> {
    let(span, pre_sql) = alt((
            tag_no_case("FROM"),
            tag_no_case("JOIN")
    ))(span)?;

    Ok((span, pre_sql))
}

pub fn db_object_post_sql(span: Span) -> IResult<Span, Span> {
    let (span, post_sql) = alt((
            tag_no_case("ON"),
            tag_no_case("USING")
    ))(span)?;

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

pub fn db_object_sql_set(span: Span) -> IResult<Span, Vec<Sql>> {
    let (span, dbo) = db_object_item(span)?;

    Ok((span, vec![Sql::Keyword(dbo.0), Sql::DbObject(dbo.1)]))
}

pub fn db_object_item(span: Span) -> IResult<Span, (ParsedItem<SqlKeyword>, ParsedItem<SqlDbObject>)> {
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
        object_name: table.fragment.to_string(),
        object_alias,
    };

    let po = ParsedItem::from_span(object, table, None)
        .expect("unable to build ParsedItem of SqlDbObject in db_object parser");

    Ok((span, (pk, po)))
}

pub fn of_item(span: Span) -> IResult<Span, ParsedItem<SqlCompositionAlias>> {
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


    let alias = SqlCompositionAlias::from_span(of_name).expect("expected alias from_span in of_list");
    let pi = ParsedItem::from_span(alias, of_name, None).expect("Unable to create parsed item in of_list parser");

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
    let (span, exact_span) = take_while1(|c:char| c.is_digit(10))(span)?;

    let exact = exact_span.fragment.to_string().parse::<u32>().expect("exact could not be parsed as u32");

    Ok((span, (Some(exact), Some(exact))))
}

pub fn bindvar_expecting_min(span: Span) -> IResult<Span, u32> {
    let (span, _) = tag_no_case("min")(span)?;
    let (span, _) = multispace0(span)?;
    let (span, min_span) = take_while1(|c:char| c.is_digit(10))(span)?;

    let min = min_span.fragment.to_string().parse::<u32>().expect("min could not be parsed as u32");

    Ok((span, min))
}

pub fn bindvar_expecting_max(span: Span) -> IResult<Span, u32> {
    let (span, _) = tag_no_case("max")(span)?;
    let (span, _) = multispace0(span)?;
    let (span, max_span) = take_while1(|c:char| c.is_digit(10))(span)?;

    let max = max_span.fragment.to_string().parse::<u32>().expect("max could not be parsed as u32");

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
           let (span, expecting) = alt((
                   bindvar_expecting_exact,
                   bindvar_expecting_min_max
           ))(span)?;

           Ok((span, expecting))
}

pub fn bindvar_sql_set(span: Span) -> IResult<Span, Vec<Sql>> {
    let (span, b) = bindvar_item(span)?;

    Ok((span, vec![Sql::Binding(b)]))
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

pub fn sql_literal_sql_set(span: Span) -> IResult<Span, Vec<Sql>> {
    let (span, l) = sql_literal_item(span)?;

    Ok((span, vec![Sql::Literal(l)]))
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

pub fn sql_ending_sql_set(span: Span) -> IResult<Span, Vec<Sql>> {
    let (span, e) = sql_ending_item(span)?;

    Ok((span, vec![Sql::Ending(e)]))
}

pub fn sql_ending_item(span: Span) -> IResult<Span, ParsedItem<SqlEnding>> {
    let (span, ending) = tag(";")(span)?;
    let (span, _) = multispace0(span)?;

    let item = ParsedItem::from_span(
        SqlEnding::new(ending.fragment.to_string())
            .expect("SqlEnding::new() failed unexpectedly from parse_sql_end parser"),
        ending,
        None,
    )
    .expect("expected Ok from ParsedItem::from_span in parse_sql_end");

    Ok((span, item))
}

#[cfg(feature = "composer-serde")]
pub fn bind_value_text(span: Span) -> IResult<Span, SerdeValue> {
    let (span, _) = one_of("'")(span)?;
    let (span, found) = take_while1(|c: char| match c {
        '\'' => false,
        ']' => false,
        _ => true,
    })(span)?;
    let (span, _) = one_of("'")(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = check_bind_value_ending(span)?;

    Ok((span, SerdeValue(Value::String(found.fragment.to_string()))))
}

#[cfg(feature = "composer-serde")]
pub fn bind_value_integer(span: Span) -> IResult<Span, SerdeValue> {
    let (span, found) = digit1(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = check_bind_value_ending(span)?;

    Ok((
        span,
        SerdeValue(Value::I64(
            i64::from_str(&found.fragment)
                .expect("unable to parse integer found by bind_value_integer"),
        )),
    ))
}

#[cfg(feature = "composer-serde")]
pub fn bind_value_real(span: Span) -> IResult<Span, SerdeValue> {
    let (span, value) = double(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = check_bind_value_ending(span)?;
    let (span, _) = multispace0(span)?;

    Ok((span, SerdeValue(Value::F64(value))))
}

named!(ending(Span) -> Span,
    eof!()
);

#[cfg(feature = "composer-serde")]
pub fn check_bind_value_ending(span: Span) -> IResult<Span, Span> {
    let (span, _) = alt((ending, peek(tag(")")), peek(tag("]")), peek(tag(","))))(span)?;

    Ok((span, span))
}

#[cfg(feature = "composer-serde")]
pub fn bind_value(span: Span) -> IResult<Span, SerdeValue> {
    let (span, value) = alt((
            bind_value_text,
            bind_value_integer,
            bind_value_real
    ))(span)?;

    Ok((span, value))
}

#[cfg(feature = "composer-serde")]
pub fn bind_value_separated(span: Span) -> IResult<Span, SerdeValue> {
    let (span, value) = bind_value(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = opt(tag(","))(span)?;
    let (span, _) = multispace0(span)?;

    Ok((span, value))
}

#[cfg(feature = "composer-serde")]
pub fn bind_value_set(span: Span) -> IResult<Span, Vec<SerdeValue>> {
    let (start_span, start) = opt(alt((tag("["), tag("("))))(span)?;
    let mut list_iter = iterator(start_span, bind_value_separated);

    let list = list_iter.fold(vec![], |mut acc: Vec<SerdeValue>, item: SerdeValue| {
        acc.push(item);
        acc
    });

    let (span, _) = list_iter.finish().unwrap();

    let (span, end) = opt(alt((tag("]"), tag(")"))))(span)?;

    if let Some(s) = start {
        if let Some(e) = end {
            if s.fragment == "[" && e.fragment != "]" {
                return Err(nom::Err::Failure((s, NomErrorKind::Verify)));
            }
            else if s.fragment == "(" && e.fragment != ")" {
                return Err(nom::Err::Failure((e, NomErrorKind::Verify)));
            }
        }
        else {
            return Err(nom::Err::Failure((s, NomErrorKind::Verify)));
        }
    }
    else {
        if let Some(e) = end {
            return Err(nom::Err::Failure((e, NomErrorKind::Verify)));
        }
    }

    Ok((span, list))
}

#[cfg(feature = "composer-serde")]
//"a:[a_value, aa_value, aaa_value], b:b_value, c: (c_value, cc_value, ccc_value), d: d_value";
pub fn bind_value_kv_pair(span: Span) -> IResult<Span, (Span, Vec<SerdeValue>)> {
    let (span, key) = take_while_name_char(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = tag(":")(span)?;
    let (span, _) = multispace0(span)?;
    let (span, values) = bind_value_set(span)?;

    Ok((span, (key, values)))
}

#[cfg(feature = "composer-serde")]
//"a_value, aa_value, aaa_value
pub fn bind_value_named_item(span: Span) -> IResult<Span, (Span, Vec<(Span, Vec<SerdeValue>)>, Option<Span>)> {
    let (span, start) = alt((tag("["), tag("(")))(span)?;
    let (span, _) = multispace0(span)?;
    let (span, kv) = separated_list(comma_padded, bind_value_kv_pair)(span)?;
    let (span, _) = multispace0(span)?;
    let (span, end) = opt(alt((tag("]"), tag(")"))))(span)?;
    let (span, _) = multispace0(span)?;

    Ok((span, (start, kv, end)))
}

#[cfg(feature = "composer-serde")]
//"[a:[a_value, aa_value, aaa_value], b:b_value], [..=]";
pub fn bind_value_named_set(span: Span) -> IResult<Span, BTreeMap<String, Vec<SerdeValue>>> {
    let mut iter = iterator(span, bind_value_named_item);

    let (_, map) = iter.fold(Ok((span, BTreeMap::new())), |acc_res: IResult<Span, BTreeMap<String, Vec<SerdeValue>>>, items: (Span, Vec<(Span, Vec<SerdeValue>)>, Option<Span>)| {
        match acc_res {
            Ok((span, mut acc)) => {
                let s = items.0;
                let end = items.2;

                for (key, values) in items.1 {
                    let key = key.fragment.to_string();

                    let entry = acc.entry(key).or_insert(vec![]);

                    for v in values {
                        entry.push(v);
                    }
                }

                if let Some(e) = end {
                    if s.fragment == "[" && e.fragment != "]" {
                        return Err(nom::Err::Failure((s, NomErrorKind::Verify)));
                    }
                    else if s.fragment == "(" && e.fragment != ")" {
                        return Err(nom::Err::Failure((e, NomErrorKind::Verify)));
                    }
                }
                else {
                    return Err(nom::Err::Failure((s, NomErrorKind::Verify)));
                }

                Ok((span, acc))
            },
            Err(e) => Err(e)
        }
    })?;

    let (span, _) = iter.finish().unwrap();

    Ok((span, map))
}

#[cfg(feature = "composer-serde")]
pub fn bind_value_named_sets(span: Span) -> IResult<Span, Vec<BTreeMap<String, Vec<SerdeValue>>>> {
    let (span, values) = separated_list(comma_padded, bind_value_named_set)(span)?;

    Ok((span, values))
}

#[cfg(test)]
mod tests {
    use super::{bindvar_expecting, bindvar_item, column_list, composer_macro_item,
                db_object_alias_sql, db_object_item, db_object_sql_set, template,
                sql_ending_item, sql_literal_item};

    #[cfg(feature = "composer-serde")]
    use super::{bind_value, bind_value_integer, bind_value_named_set, bind_value_named_sets,
                bind_value_set, bind_value_text, check_bind_value_ending};

    use crate::types::{ParsedItem, Span, Sql, SqlComposition, SqlCompositionAlias, SqlDbObject,
                       SqlEnding, SqlLiteral};

    #[cfg(feature = "composer-serde")]
    use crate::types::SerdeValue;

    #[cfg(feature = "composer-serde")]
    use serde_value::Value;

    #[cfg(feature = "composer-serde")]
    use std::collections::BTreeMap;

    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

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

        let item = SqlCompositionAlias::Path("src/tests/simple-template.tql".into());

        vec![build_parsed_item(
            item,
            Some(1 + shift_line),
            Some(24 + shift_offset),
            "src/tests/simple-template.tql",
        )]
    }

    fn include_aliases() -> Vec<ParsedItem<SqlCompositionAlias>> {
        let item = SqlCompositionAlias::Path("src/tests/include-template.tql".into());

        vec![build_parsed_item(
            item,
            None,
            Some(24),
            "src/tests/include-template.tql",
        )]
    }

    fn simple_alias_hash() -> HashMap<SqlCompositionAlias, ParsedItem<SqlComposition>> {
        let mut acc = HashMap::new();

        let p = PathBuf::from("src/tests/simple-template.tql");

        acc.entry(SqlCompositionAlias::from_path(&p)).or_insert(
            SqlComposition::from_path(&p).expect("expected to insert in simple_alias_hash"),
        );

        acc
    }

    fn include_alias_hash() -> HashMap<SqlCompositionAlias, ParsedItem<SqlComposition>> {
        let mut acc = simple_alias_hash();

        let p = PathBuf::from("src/tests/include-template.tql");

        acc.entry(SqlCompositionAlias::from_path(&p)).or_insert(
            SqlComposition::from_path(&p).expect("expected to insert in include_alias_hash"),
        );

        acc
    }

    fn include_shallow_alias_hash() -> HashMap<SqlCompositionAlias, ParsedItem<SqlComposition>> {
        let mut acc = HashMap::new();

        let p = PathBuf::from("src/tests/include-template.tql");

        acc.entry(SqlCompositionAlias::from_path(&p)).or_insert(
            SqlComposition::from_path(&p).expect("expected to insert in include_shallow_alias"),
        );

        acc
    }

    fn simple_template_comp(
        shift_line: Option<u32>,
        shift_offset: Option<usize>,
    ) -> ParsedItem<SqlComposition> {
        let _shift_line = shift_line.unwrap_or(0);
        let _shift_offset = shift_offset.unwrap_or(0);

        let item = SqlComposition {
            position: Some(build_parsed_path_position(
                "src/tests/simple-template.tql".into(),
                1,
                0,
                "SELECT foo_id, bar FROM foo WHERE foo.bar = :bind(varname);\n",
            )),
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
            ..Default::default()
        };

        build_parsed_item(item, None, None, "")
    }

    fn include_template_comp() -> ParsedItem<SqlComposition> {
        let item = SqlComposition {
            position: Some(build_parsed_path_position(
                "src/tests/include-template.tql".into(),
                1,
                0,
                "SELECT COUNT(foo_id)\nFROM (\n  :compose(src/tests/simple-template.tql)\n);\n",
            )),
            sql: vec![
                build_parsed_sql_keyword("SELECT", None, None, "SELECT"),
                build_parsed_sql_literal("COUNT(foo_id)", None, Some(7), "COUNT(foo_id)\n"),
                build_parsed_sql_keyword("FROM", Some(2), Some(21), "FROM"),
                build_parsed_sql_literal("(", Some(2), Some(26), "(\n  "),
                Sql::Composition((simple_template_compose_comp(Some(2), Some(15)), vec![])),
                build_parsed_sql_literal(")", Some(3), Some(69), "\n)"),
                build_parsed_sql_ending(";", Some(4), Some(71), ";"),
            ],
            ..Default::default()
        };

        build_parsed_item(item, None, None, "")
    }

    fn simple_template_compose_comp(
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
            aliases: simple_alias_hash(),
            ..Default::default()
        };

        build_parsed_item(item, None, None, "")
    }

    fn include_template_compose_comp() -> ParsedItem<SqlComposition> {
        let item = SqlComposition {
            command: Some(build_parsed_string("compose", None, Some(16), "compose")),
            of: include_aliases(),
            aliases: include_shallow_alias_hash(),
            ..Default::default()
        };

        build_parsed_item(item, None, None, "")
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
    fn test_sql_ending_itme() {
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

        let expected_item = build_parsed_item(expected, Some(1), Some(0), "foo.bar = ");

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_simple_template() {
        let input =
            "SELECT * FROM (:compose(src/tests/simple-template.tql)) WHERE name = ':bind(bindvar)';";

        let item =
            template(Span::new(input.into()), None).expect("expected Ok from template");

        let expected_item = SqlComposition {
            sql: vec![
                build_parsed_sql_keyword("SELECT", None, None, "SELECT"),
                build_parsed_sql_literal("*", None, Some(7), "* "),
                build_parsed_sql_keyword("FROM", None, Some(9), "FROM"),
                build_parsed_sql_literal("(", None, Some(14), "("),
                Sql::Composition((simple_template_compose_comp(None, None), vec![])),
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
            ..Default::default()
        };

        let expected_item = build_parsed_item(expected_item, None, None, "");

        assert_eq!(item, expected_item, "items match");
    }

    #[test]
    fn test_parse_include_template() {
        let input = "SELECT * FROM (:compose(src/tests/include-template.tql)) WHERE name = ':bind(bindvar)';";

        let out = template(Span::new(input.into()), None).unwrap();

        let expected_comp = SqlComposition {
            sql: vec![
                build_parsed_sql_keyword("SELECT", None, None, "SELECT"),
                build_parsed_sql_literal("*", None, Some(7), "* "),
                build_parsed_sql_keyword("FROM", None, Some(9), "FROM"),
                build_parsed_sql_literal("(", None, Some(14), "("),
                Sql::Composition((include_template_compose_comp(), vec![])),
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

        let expected_comp = build_parsed_item(expected_comp, None, None, "");

        assert_eq!(out, expected_comp);
    }

    #[test]
    fn test_parse_file_template() {
        let stmt = SqlComposition::from_path(Path::new("src/tests/simple-template.tql"))
            .expect("expected Ok from from_path");

        let expected = simple_template_comp(None, None);

        assert_eq!(stmt, expected);
    }

    #[test]
    fn test_parse_file_inclusive_template() {
        let stmt = SqlComposition::from_path(Path::new("src/tests/include-template.tql"))
            .expect("expected Ok from from_path");
        let expected = include_template_comp();

        assert_eq!(stmt, expected);
    }

    #[test]
    fn test_parse_composed_composer() {
        let sql_str = ":count(distinct col1, col2 of src/tests/simple-template.tql, src/tests/include-template.tql);";

        let comp = composer_macro_item(Span::new(sql_str.into()));

        let expected = Ok((
            Span {
                offset:   92,
                line:     1,
                fragment: ";".into(),
                extra:    (),
            },
            (
                SqlComposition {
                    command: Some(build_parsed_string("count", None, Some(1), "count")),
                    position: None,
                    distinct: Some(build_parsed_item(true, None, Some(7), "distinct")),
                    columns: Some(vec![
                        build_parsed_string("col1", None, Some(16), "col1"),
                        build_parsed_string("col2", None, Some(22), "col2"),
                    ]),
                    of: vec![
                        build_parsed_item(
                            SqlCompositionAlias::Path("src/tests/simple-template.tql".into()),
                            None,
                            Some(30),
                            "src/tests/simple-template.tql",
                        ),
                        build_parsed_item(
                            SqlCompositionAlias::Path("src/tests/include-template.tql".into()),
                            None,
                            Some(61),
                            "src/tests/include-template.tql",
                        ),
                    ],
                    aliases: include_alias_hash(),
                    ..Default::default()
                },
                vec![],
            ),
        ));

        assert_eq!(comp, expected);
    }

    #[test]
    fn test_simple_composed_composer() {
        let sql_str = ":count(src/tests/simple-template.tql);";

        let comp = SqlComposition::parse(sql_str, None).unwrap();

        let expected = build_parsed_item(
            SqlComposition {
                command: Some(build_parsed_string("count", None, Some(1), "count")),
                position: None,
                of: vec![build_parsed_item(
                    SqlCompositionAlias::Path("src/tests/simple-template.tql".into()),
                    None,
                    Some(7),
                    "src/tests/simple-template.tql",
                )],
                aliases: simple_alias_hash(),
                sql: vec![Sql::Ending(build_parsed_item(
                    SqlEnding { value: ";".into() },
                    None,
                    Some(37),
                    ";",
                ))],
                ..Default::default()
            },
            None,
            None,
            "",
        );

        assert_eq!(comp, expected);
    }

    #[test]
    fn test_parse_single_column_list() {
        let input = "col_1 of ";

        let expected_span = build_span(Some(1), Some(9), "");

        let expected_item = vec![build_parsed_item("col_1".to_string(), None, None, "col_1")];

        let (span, item) =
            column_list(Span::new(input.into())).expect("expected Ok from column_list");

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_multi_column_list() {
        let input = "col_1, col_2, col_3 of ";

        let expected_span = build_span(Some(1), Some(23), "");

        let expected_item = vec![
            build_parsed_item("col_1".to_string(), None, Some(0), "col_1"),
            build_parsed_item("col_2".to_string(), None, Some(7), "col_2"),
            build_parsed_item("col_3".to_string(), None, Some(14), "col_3"),
        ];

        let (span, item) =
            column_list(Span::new(input.into())).expect("expected Ok from column_list");

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
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
            object_name:  "t1".into(),
            object_alias: None,
        };

        let expected_dbo_item = build_parsed_item(expected_dbo, None, Some(5), "t1");

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
            object_name:  "t1".into(),
            object_alias: Some("tt".into()),
        };

        let expected_dbo_item = build_parsed_item(expected_dbo, None, Some(5), "t1");

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
            object_name:  "t1".into(),
            object_alias: Some("tt".into()),
        };

        let expected_dbo_item = build_parsed_item(expected_dbo, None, Some(5), "t1");

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

    #[cfg(feature = "composer-serde")]
    fn build_expected_bind_values() -> BTreeMap<String, Vec<SerdeValue>> {
        let mut expected_values: BTreeMap<String, Vec<SerdeValue>> = BTreeMap::new();

        expected_values.insert(
            "a".into(),
            vec![
                SerdeValue(Value::String("a".into())),
                SerdeValue(Value::String("aa".into())),
                SerdeValue(Value::String("aaa".into())),
            ],
        );

        expected_values.insert("b".into(), vec![SerdeValue(Value::String("b".into()))]);

        expected_values.insert(
            "c".into(),
            vec![
                SerdeValue(Value::I64(2)),
                SerdeValue(Value::F64(2.25)),
                SerdeValue(Value::String("a".into())),
            ],
        );

        expected_values.insert("d".into(), vec![SerdeValue(Value::I64(2))]);

        expected_values.insert("e".into(), vec![SerdeValue(Value::F64(2.234566))]);

        expected_values
    }

    #[test]
    #[cfg(feature = "composer-serde")]
    fn test_bind_value() {
        let tests = vec![
            ("'a'", "", SerdeValue(Value::String("a".into()))),
            ("'a', ", ", ", SerdeValue(Value::String("a".into()))),
            ("'a' , ", ", ", SerdeValue(Value::String("a".into()))),
            ("34", "", SerdeValue(Value::I64(34))),
            ("34, ", ", ", SerdeValue(Value::I64(34))),
            ("34 ,", ",", SerdeValue(Value::I64(34))),
            ("54.3, ", ", ", SerdeValue(Value::F64(54.3))),
            ("54.3", "", SerdeValue(Value::F64(54.3))),
            ("54.3 ", "", SerdeValue(Value::F64(54.3))),
        ];
        for (i, (input, expected_remain, expected_values)) in tests.iter().enumerate() {
            println!("{}: input={:?}", i, input);
            let (remaining, output) = bind_value(Span::new(input)).unwrap();

            assert_eq!(
                &output, expected_values,
                "expected values for i:{} input:{:?}",
                i, input
            );
            assert_eq!(
                &remaining.fragment, expected_remain,
                "expected remaining for i:{} input:{:?}",
                i, input
            );
        }
    }

    #[test]
    #[cfg(feature = "composer-serde")]
    fn test_bind_value_text() {
        let input = "'a'";

        let (remaining, output) = bind_value_text(Span::new(input)).unwrap();

        let expected_output = SerdeValue(Value::String("a".into()));

        assert_eq!(output, expected_output, "correct output");
        assert_eq!(remaining.fragment, "", "nothing remaining");
    }

    #[test]
    #[cfg(feature = "composer-serde")]
    fn test_bind_value_integer() {
        let tests = vec![
            ("34, ", ", ", SerdeValue(Value::I64(34))),
            ("34 ", "", SerdeValue(Value::I64(34))),
            ("34 , ", ", ", SerdeValue(Value::I64(34))),
            ("34", "", SerdeValue(Value::I64(34))),
        ];
        for (i, (input, expected_remain, expected_values)) in tests.iter().enumerate() {
            println!("input={:?}", input);
            let (remaining, output) = bind_value_integer(Span::new(input)).unwrap();

            assert_eq!(
                &output, expected_values,
                "expected values for i:{} input:{:?}",
                i, input
            );
            assert_eq!(
                &remaining.fragment, expected_remain,
                "expected remaining for i:{} input:{:?}",
                i, input
            );
        }
    }

    #[test]
    #[cfg(feature = "composer-serde")]
    fn test_check_bind_value_ending() {
        for &input in [")", "]", ",", ""].iter() {
            // breaks for input="" if eof() is not listed first
            println!("test_check_bind_value_ending with input=\"{}\"", input);

            match check_bind_value_ending(Span::new(input)) {
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
                    println!(
                        "bind_value_ending for input={:?} returned an error={:?}",
                        input, e
                    );
                    panic!(e)
                }
            };
        }
    }

    #[test]
    #[cfg(feature = "composer-serde")]
    fn test_bind_value_set() {
        let input = "['a', 'aa', 'aaa']";

        let (remaining, output) = bind_value_set(Span::new(input)).unwrap();

        let expected_output = vec![
            SerdeValue(Value::String("a".into())),
            SerdeValue(Value::String("aa".into())),
            SerdeValue(Value::String("aaa".into())),
        ];

        assert_eq!(output, expected_output, "correct output");
        assert_eq!(remaining.fragment, "", "nothing remaining");
    }

    #[test]
    #[cfg(feature = "composer-serde")]
    fn test_bind_single_undelimited_value_set() {
        let input = "'a'";

        let (remaining, output) = bind_value_set(Span::new(input)).unwrap();

        let expected_output = vec![SerdeValue(Value::String("a".into()))];

        assert_eq!(output, expected_output, "correct output");
        assert_eq!(remaining.fragment, "", "nothing remaining");
    }

    #[test]
    #[cfg(feature = "composer-serde")]
    fn test_bind_value_named_set() {
        //TODO: this should work but "single" values chokes up the parser
        //TOOD: doesn't like spaces between keys still either
        //let input = "[a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566]";
        let input = "[a:['a', 'aa', 'aaa'], b:['b'], c:(2, 2.25, 'a'), d:[2], e:[2.234566]]";

        match bind_value_named_set(Span::new(input)) {
            Ok((remaining, output)) => {
                let expected_output = build_expected_bind_values();

                assert_eq!(output, expected_output, "correct output");
                assert_eq!(remaining.fragment, "", "nothing remaining");
            }
            Err(e) => {
                println!("bind_value_named_set returned an error={:?}", e);
                panic!(e)
            }
        }
    }

    #[test]
    #[cfg(feature = "composer-serde")]
    fn test_bind_value_named_sets() {
        //TODO: this should work but "single" values chokes up the parser
        //TOOD: doesn't like spaces between keys still either
        //let input = "[a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566], [a: ['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566]";
        let input = "[a:['a', 'aa', 'aaa'], b:['b'], c:(2, 2.25, 'a'), d:[2], e:[2.234566]], [a:['a', 'aa', 'aaa'], b:['b'], c:(2, 2.25, 'a'), d:[2], e:[2.234566]]";

        let (remaining, output) = bind_value_named_sets(Span::new(input)).unwrap();

        let expected_output = vec![build_expected_bind_values(), build_expected_bind_values()];

        assert_eq!(output, expected_output, "correct output");
        assert_eq!(remaining.fragment, "", "nothing remaining");
    }

    /*
    #[test]
    fn test_bind_path_alias_name_value_sets() {
        let input = "t1.tql: [[a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566], [a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566]], t2.tql: [[a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566], [a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566]]";

        let (remaining, output) = bind_path_alias_name_value_sets(input).unwrap();

        let expected_values = build_expected_bind_values();
        let mut expected_output: HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Vec<Value>>>> =
            HashMap::new();

        expected_output.insert(
            SqlCompositionAlias::Path("t1.tql".into()),
            vec![expected_values.clone(), expected_values.clone()],
            );

        expected_output.insert(
            SqlCompositionAlias::Path("t2.tql".into()),
            vec![expected_values.clone(), expected_values.clone()],
            );

        assert_eq!(output, expected_output, "correct output");
        assert_eq!(remaining, "", "nothing remaining");
    }
    */
}
