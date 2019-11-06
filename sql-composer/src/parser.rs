use crate::types::{ParsedItem, ParsedSpan, Position, Span, Sql, SqlBinding,
                   SqlComposition, SqlCompositionAlias, SqlDbObject, SqlEnding, SqlKeyword,
                   SqlLiteral};

use nom::{
    IResult,
    character::complete::multispace0
};

#[cfg(feature = "composer-serde")]
use nom::{
    bytes::complete::{
        take_while1,
    },
    character::complete::{
        digit1, one_of
    }
};

#[cfg(feature = "composer-serde")]
use serde_value::Value;

#[cfg(feature = "composer-serde")]
use crate::types::SerdeValue;

#[cfg(feature = "composer-serde")]
use std::collections::BTreeMap;

#[cfg(feature = "composer-serde")]
use std::str::FromStr;

named!(
    _parse_template(Span) -> ParsedItem<SqlComposition>,
    dbg_dmp!(
    fold_many1!(
            alt!(
                complete!(do_parse!(position!() >> e: parse_sql_end >> (vec![Sql::Ending(e)])))
                | complete!(do_parse!(position!() >> b: bindvar >> (vec![Sql::Binding(b)])))
                | complete!(do_parse!(position!() >> sc: parse_composer_macro >> (vec![Sql::Composition((ParsedItem::from_span(sc.0, Span::new(""), None).expect("expected to make a Span in sc _parse_template"), sc.1))])))
                | complete!(do_parse!(position!() >> dbo: db_object >> (vec![Sql::Keyword(dbo.0), Sql::DbObject(dbo.1)])))
                | complete!(do_parse!(position!() >> k: keyword >> (vec![Sql::Keyword(k)])))
                | complete!(do_parse!(position!() >> s: parse_sql >> (vec![Sql::Literal(s)]))
            )),
        ParsedItem::from_span(SqlComposition::default(), Span::new(""), None).expect("expected to make a Span in _parse_template parser"),
        |mut acc: ParsedItem<SqlComposition>, items: Vec<Sql>| {
            for item in items {
                match item {
                    Sql::Composition((mut sc, aliases)) => {
                        for alias in &aliases {
                            let stmt_path = alias.path().expect("expected alias path");

                            sc.item.insert_alias(&stmt_path).expect("expected insert_alias");
                        }

                        if acc.item.sql.len() == 0 {
                            return sc;
                        }

                        acc.item.push_sql(Sql::Composition((sc, aliases))).unwrap();
                    }
                    _ => {
                        acc.item.push_sql(item).unwrap();
                    }
                }
            }

            acc
        }
    ))
);

pub fn parse_template(
    span: Span,
    alias: Option<SqlCompositionAlias>,
) -> IResult<Span, ParsedItem<SqlComposition>> {
    let res = _parse_template(span);

    res.and_then(|(remaining, mut comp)| {
        if let Some(a) = alias {
            comp.item
                .set_position(Position::Parsed(ParsedSpan::new(span, Some(a))))
                .expect("unable to set position in parse_template");
        }

        Ok((remaining, comp))
    })
}

named!(
    parse_macro_name(Span) -> ParsedItem<String>,
       do_parse!(
           position!() >>
           name: delimited!(tag!(":"), take_until!("("), tag!("(")) >>
           (
               ParsedItem::from_span(name.fragment.to_string(), name, None).expect("invalid parsed item came from parser parse_macro_name")
           )
        )
);

named!(parse_composer_macro(Span) -> (SqlComposition, Vec<SqlCompositionAlias>),
       complete!(do_parse!(
               position!() >>
               command: parse_macro_name >>
               position!() >>
               distinct: command_distinct_arg >>
               multispace0 >>
               position!() >>
               all: command_all_arg >>
               multispace0 >>
               columns: opt!(column_list) >>
               multispace0 >>
               position!() >>
               of: of_list >>
               tag!(")") >>
               ({
                 let mut sc = SqlComposition {
                     command: Some(command),
                     distinct,
                     all,
                     columns,
                     of,
                     ..Default::default()
                 };

                 sc.update_aliases().expect("expected to update aliases");

                 (sc, vec![])
               })
       ))
);

named!(
    command_distinct_arg(Span) -> Option<ParsedItem<bool>>,
    do_parse!(
        position!() >>
        distinct_tag: opt!(tag_no_case!("distinct")) >>
        (
            match distinct_tag {
                Some(d) => {
                    Some(ParsedItem::from_span(true, d, None).expect("Unable to parse bool flag from command_distinct_arg"))
                }
                None    => None
            }
        )
    )
);

named!(
    command_all_arg(Span) -> Option<ParsedItem<bool>>,
    do_parse!(
        position!() >>
        all_tag: opt!(tag_no_case!("all")) >>
        (
            match all_tag {
                Some(d) => {
                    Some(ParsedItem::from_span(true, d, None).expect("Unable to parse bool flag from command_all_arg"))
                }
                None    => None
            }
        )
    )
);

named!(
    column_list(Span) -> Vec<ParsedItem<String>>,
    terminated!(
        many1!(complete!(column_name)),
        do_parse!(multispace0 >> tag_no_case!("of") >> multispace0 >> ())
    )
);

named!(
    column_name(Span) -> ParsedItem<String>,
    terminated!(
        do_parse!(
            position!() >>
            column: take_while_name_char >>
            ({
                let p = ParsedItem::from_span(
                    column.fragment.to_string(),
                    column,
                    None
                );

                p.expect("unable to build ParsedItem of String from column_list parser")
            })
          ),
          opt!(do_parse!(multispace0 >> tag!(",") >> multispace0 >> ()))
    )
);

named!(take_while_name_char(Span) -> Span,
    do_parse!(
        name: take_while1!(|c| {
            match c {
                'a'..='z' => true,
                'A'..='Z' => true,
                '0'..='9' => true,
                '_' => true,
                _ => false,
            }
        }) >>
        (
            name
        )
    )
);

named!(keyword(Span) -> ParsedItem<SqlKeyword>,
    do_parse!(
        position!() >>
        keyword: keyword_sql >>
        multispace0 >>
        (
            ParsedItem::from_span(
                SqlKeyword::new(keyword.fragment.to_string()).expect("SqlKeyword::new() failed unexpectedly from keyword parser"),
                keyword,
                None
            ).expect("expected Ok from ParsedItem::from_span in keyword parser")
        )
    )
);

named!(keyword_sql(Span) -> Span,
    do_parse!(
        keyword: complete!(
            alt!(
                command_sql |
                db_object_pre_sql |
                db_object_post_sql
            )
        ) >>
        (keyword)
    )
);

named!(command_sql(Span) -> Span,
    complete!(
        alt!(
            tag_no_case!("SELECT") |
            tag_no_case!("INSERT INTO") |
            tag_no_case!("UPDATE") |
            tag_no_case!("WHERE")
        )
    )
);

named!(db_object_pre_sql(Span) -> Span,
    complete!(
        alt!(
            tag_no_case!("FROM") |
            tag_no_case!("JOIN")
        )
    )
);

named!(db_object_post_sql(Span) -> Span,
    complete!(
        alt!(
            tag_no_case!("ON") |
            tag_no_case!("USING")
        )
    )
);

named!(db_object_alias_sql(Span) -> Span,
    do_parse!(
        opt!(tag_no_case!("AS")) >>
        multispace0 >>
        not!(peek!(keyword_sql)) >>
        not!(peek!(tag!("("))) >>
        alias: take_while_name_char >>
        multispace0 >>
        (
            alias
        )
    )
);

named!(
    db_object(Span) -> (ParsedItem<SqlKeyword>, ParsedItem<SqlDbObject>),
    do_parse!(
        keyword: db_object_pre_sql >>
        multispace0 >>
        position!() >>
        table: db_object_alias_sql >>
        multispace0 >>
        position!() >>
        alias: opt!(db_object_alias_sql) >>
        multispace0 >>
        ({
            let k = SqlKeyword {
                value: keyword.fragment.to_string()
            };

            let pk = ParsedItem::from_span(
                k,
                keyword,
                None
            ).expect("unable to build ParsedItem of SqlDbObject in db_object parser");

            let object_alias = alias.and_then(|a| Some(a.fragment.to_string()));

            let object = SqlDbObject {
                object_name: table.fragment.to_string(),
                object_alias
            };

            let po = ParsedItem::from_span(
                object,
                table,
                None
            ).expect("unable to build ParsedItem of SqlDbObject in db_object parser");

            (pk, po)
        })
     )
);

named!(
    of_list(Span) -> Vec<ParsedItem<SqlCompositionAlias>>,
    many1!(terminated!(
        do_parse!(
            position!() >>
            of_name: take_while1!(|u| {
                let c = u as char;

                match c {
                    'a'..='z' => true,
                    'A'..='Z' => true,
                    '0'..='9' => true,
                    '-' | '_' => true,
                    '.' | '/' | '\\' => true,
                    _ => false,
                }
            }) >> ({
                //TODO: clean this up properly
                let alias = SqlCompositionAlias::from_span(of_name).expect("expected alias from_span in of_list");

                ParsedItem::from_span(alias, of_name, None).expect("Unable to create parsed item in of_list parser")
            })
        ),
        opt!(do_parse!(
            multispace0 >> tag!(",") >> multispace0 >> ()
        ))
    ))
);

named!(
    _parse_macro_include_alias(Span) -> Span,
    dbg!(
    take_while1!(|u| {
        let c = u as char;

        match c {
            'a'..='z' => true,
            'A'..='Z' => true,
            '0'..='9' => true,
            '_' | '-' | '.' | '/' => true,
            _ => false,
        }
    })
    )
);

named!(bindvar_expecting(Span) -> (Option<u32>, Option<u32>),
       do_parse!(
           tag_no_case!("expecting") >>
           multispace0 >>
           expecting: complete!(
               alt!(
                   do_parse!(
                       position!() >>
                       exact_span: take_while1!(|c:char| c.is_digit(10)) >>
                       ({
                           let exact = exact_span.fragment.to_string().parse::<u32>().expect("exact could not be parsed as u32");

                           (Some(exact), Some(exact))
                       })
                   ) |
                   do_parse!(
                       position!() >>
                       min_span: opt!(
                           do_parse!(
                               tag_no_case!("min") >>
                               multispace0 >>
                               min: take_while1!(|c:char| c.is_digit(10)) >>
                               (min)
                           )
                       ) >>
                       multispace0 >>
                       max_span: opt!(
                           do_parse!(
                               tag_no_case!("max") >>
                               multispace0 >>
                               max: take_while1!(|c:char| c.is_digit(10)) >>
                               (max)
                           )
                       ) >>
                       ({
                           let min = min_span.and_then(|span| Some(span.fragment.to_string().parse::<u32>().expect("min could not be parsed as u32")));
                           let max = max_span.and_then(|span| Some(span.fragment.to_string().parse::<u32>().expect("max could not be parsed as u32")));

                           (min, max)
                       })
                )
                )
            ) >>
            ({
                expecting
            })
        )
);

// name EXPECTING (i|MIN i|MAX i|MIN i MAX i)
named!(bindvar(Span) -> ParsedItem<SqlBinding>,
       complete!(do_parse!(
               start_quote: opt!(tag!("'")) >>
               position!() >>
               tag_no_case!(":bind(") >>
               multispace0 >>
               position!() >>
               bindvar_name: take_while_name_char >>
               multispace0 >>
               position!() >>
               expecting: opt!(bindvar_expecting) >>
               multispace0 >>
               nullable: opt!(tag_no_case!("null")) >>
               multispace0 >>
               tag!(")") >>
               multispace0 >>
               end_quote: opt!(tag!("'")) >>
               ({
                   let min = expecting.and_then(|m| m.0);
                   let max = expecting.and_then(|m| m.1);

                   if start_quote.is_some() && end_quote.is_none() {
                       //TODO: proper error instead
                       panic!("start_quote but no end_quote");
                   }
                   else if end_quote.is_some() && start_quote.is_none() {
                       //TODO: proper error instead
                       panic!("end_quote but no start_quote");
                   }

                   ParsedItem::from_span(
                       SqlBinding::new(
                           bindvar_name.fragment.to_string(),
                           start_quote.is_some(),
                           min,
                           max,
                           nullable.is_some(),
                       ).expect("SqlBinding::new() failed unexpectedly from bindvar parser"),
                       bindvar_name,
                       None
                   ).expect("expected Ok from ParsedItem::from_span in bindvar parser")
               })
       ))
);

named!(
    parse_sql(Span) -> ParsedItem<SqlLiteral>,
    do_parse!(
        pos: position!() >>
        parsed: fold_many1!(
            do_parse!(
                not!(peek!(tag!(":"))) >>
                not!(peek!(tag!(";"))) >>
                not!(peek!(tag!("'"))) >>
                not!(peek!(db_object_pre_sql)) >>
                literal: take!(1) >>
                (literal)
            ),
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
    )
);

named!(
    parse_sql_end(Span) -> ParsedItem<SqlEnding>,
    do_parse!(
        position!() >>
        ending: tag!(";") >>
        multispace0 >>
        (
            ParsedItem::from_span(
                SqlEnding::new(ending.fragment.to_string()).expect("SqlEnding::new() failed unexpectedly from parse_sql_end parser"),
                ending,
                None
            ).expect("expected Ok from ParsedItem::from_span in parse_sql_end")
        )
    )
);

#[cfg(feature = "composer-serde")]
pub fn bind_value_text(
    span: Span,
    ) -> IResult<Span, SerdeValue> {

    let (span, _) = one_of("'")(span)?;
    let (span, found) = take_while1(
        |c:char| match c {
            '\'' => false,
            ']'  => false,
            _    => true,
        })(span)?;
    let (span, _) = one_of("'")(span)?;
    let (span, _) = check_bind_value_ending(span)?;
    return Ok(
        (
            span,
            SerdeValue(Value::String(found.fragment.to_string()))
        )
    )
}

#[cfg(feature = "composer-serde")]
pub fn bind_value_integer(
    span: Span,
    ) -> IResult<Span, SerdeValue> {

    let (remaining, found) = digit1(span)?;
    return Ok(
        (
            remaining,
            SerdeValue(Value::I64(i64::from_str(&found.fragment).expect("unable to parse integer found by bind_value_integer")))
        )
    )
}

#[cfg(feature = "composer-serde")]
named!(
    bind_value_real(Span) -> SerdeValue,
    do_parse!(
        wi: take_while!(|c| {
            match c {
                '0'..='9' => true,
                _ => false,
            }
        }) >>
        //TODO: support comma decimal format?
        char!('.') >>
        fi: take_while!(|c| {
            match c {
                '0'..='9' => true,
                _ => false,
            }
        }) >>
        multispace0 >>
        check_bind_value_ending >>
        multispace0 >>
        ({
            let r = format!("{}.{}", wi.fragment, fi.fragment);

            SerdeValue(Value::F64(f64::from_str(&r).expect("unable to parse real value")))
        })
    )
);

#[cfg(feature = "composer-serde")]
named!(
    check_bind_value_ending(Span) -> Span,
    alt!(
        eof!()           | // shortest first
        peek!(tag!(")")) |
        peek!(tag!("]")) |
        peek!(tag!(","))
    )
);

#[cfg(feature = "composer-serde")]
named!(
    bind_value(Span) -> (SerdeValue),
    do_parse!(
        value: complete!(
            alt!(
                do_parse!(t: bind_value_text >> (t)) |
                do_parse!(r: bind_value_real >> (r)) |
                do_parse!(i: bind_value_integer >> (i))
            )
        ) >>
        ({
            value
        })
    )
);

#[cfg(feature = "composer-serde")]
named!(
    bind_value_set(Span) -> Vec<SerdeValue>,
    do_parse!(
        start: opt!(complete!(alt!(tag!("[") | tag!("(")))) >>
        list: fold_many1!(
            do_parse!(
                value: bind_value >>
                multispace0 >>
                opt!(complete!(tag!(","))) >>
                multispace0 >>
                (value)
            ),
            vec![], |mut acc: Vec<SerdeValue>, item: SerdeValue| {
                acc.push(item);
                acc
            }) >>
        end: opt!(complete!(alt!(tag!("]") | tag!(")")))) >>
        ({
            if let Some(s) = start {
                if let Some(e) = end {
                    if s.fragment == "[" && e.fragment != "]" {
                        panic!("bind_value_set: no corresponding '[' for ']'");
                    }
                    else if s.fragment == "(" && e.fragment != ")" {
                        panic!("bind_value_set: no corresponding ')' for '('");
                    }
                }
                else {
                    panic!("bind_value_set: no matching end found for start: {:?}", s);
                }
            }
            else {
                if let Some(e) = end {
                    panic!("bind_value_set: found ending {} with no starter", e.fragment);
                }
            }

            list
        })
)
);

#[cfg(feature = "composer-serde")]
//"a:[a_value, aa_value, aaa_value], b:b_value, c: (c_value, cc_value, ccc_value), d: d_value";
named!(
    bind_value_kv_pair(Span) -> (Span, Vec<SerdeValue>),
    do_parse!(
        key: take_while_name_char >>
        multispace0 >>
        tag!(":") >>
        multispace0 >>
        values: bind_value_set >>
        ({
            (key, values)
        })
    )
);

#[cfg(feature = "composer-serde")]
//"[a:[a_value, aa_value, aaa_value], b:b_value], [..=]";
named!(
    pub bind_value_named_set(Span) -> BTreeMap<String, Vec<SerdeValue>>,
    fold_many1!(
        do_parse!(
            //XXX: failed with Incomplete(Size(1) when start was opt!
            start: complete!(alt!(tag!("[") | tag!("("))) >>
            multispace0 >>
            kv: separated_list!(
                do_parse!(multispace0 >> tag!(",") >> multispace0 >> ()),
                bind_value_kv_pair
            ) >>
            multispace0 >>
            end: opt!(complete!(alt!(tag!("]") | tag!(")")))) >>
            multispace0 >>
            ((start, kv, end))
        ),
        BTreeMap::new(), |mut acc: BTreeMap<String, Vec<SerdeValue>>, items: (Span, Vec<(Span, Vec<SerdeValue>)>, Option<Span>)| {
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
                    panic!("bind_value_named_set: no corresponding ']'for '['");
                }
                else if s.fragment == "(" && e.fragment != ")" {
                    panic!("bind_value_named_set: no corresponding ')'for '('");
                }
            }
            else {
                panic!("bind_value_named_set: no matching end found for start: {:?}, end:{:?}", s, end);
            }

            acc
        }
    )
);

#[cfg(feature = "composer-serde")]
named!(
    bind_value_named_sets(Span) -> Vec<BTreeMap<String, Vec<SerdeValue>>>,
    do_parse!(
        values: separated_list!(
            do_parse!(multispace0 >> complete!(tag!(",")) >> multispace0 >> ()),
            bind_value_named_set
        ) >>
        (
            values
        )
    )
);

#[cfg(test)]
mod tests {
    use super::{bindvar, bindvar_expecting, column_list, db_object, db_object_alias_sql,
                parse_composer_macro, parse_sql, parse_sql_end, parse_template};

    #[cfg(feature = "composer-serde")]
    use super::{
        bind_value,
        bind_value_named_set,
        bind_value_named_sets,
        bind_value_set,
        bind_value_text,
        check_bind_value_ending
    };

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

        let out = bindvar(Span::new(input.into())).expect("expected Ok from bindvar");

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

        let out = bindvar(Span::new(input.into())).expect("expected Ok from bindvar");

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
    fn test_parse_sql_end() {
        let input = ";blah blah blah";

        let expected_span = build_span(Some(1), Some(1), "blah blah blah");

        let expected_item = build_parsed_ending_item(";", None, None, ";");

        let (span, item) =
            parse_sql_end(Span::new(input.into())).expect("expected Ok from parse_sql_end");

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_sql_until_path() {
        let input = "foo.bar = :bind(varname);";

        let out = parse_sql(Span::new(input.into())).expect("expected Ok from parse_sql");

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

        let (span, item) =
            parse_template(Span::new(input.into()), None).expect("expected Ok from parse_template");

        let expected_span = build_span(Some(1), Some(86), "");

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
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_include_template() {
        let input = "SELECT * FROM (:compose(src/tests/include-template.tql)) WHERE name = ':bind(bindvar)';";

        let out = parse_template(Span::new(input.into()), None);

        let expected_span = build_span(Some(1), Some(87), "");

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

        assert_eq!(out, Ok((expected_span, expected_comp)));
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

        let comp = parse_composer_macro(Span::new(sql_str.into()));

        let expected = Ok((
            Span {
                offset:   92,
                line:     1,
                fragment: ";".into(),
                extra: (),
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

        let comp = SqlComposition::from_str(sql_str);

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

        let (span, (_keyword_item, dbo_item)) = db_object(Span::new(input.into()))
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

        let (span, (_keyword_item, dbo_item)) = db_object(Span::new(input.into()))
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

        let (span, (_keyword_item, dbo_item)) = db_object(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(dbo_item, expected_dbo_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_with_subquery() {
        let input = "FROM (SELECT * FROM t1) AS tt WHERE 1";

        db_object(Span::new(input.into()))
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
        let input = "'a'";

        let (remaining, output) = bind_value(Span::new(input)).unwrap();

        let expected_output = SerdeValue(Value::String("a".into()));

        assert_eq!(output, expected_output, "correct output");
        assert_eq!(remaining.fragment, "", "nothing remaining");
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
    fn test_check_bind_value_ending() {
        for &input in [")", "]", ",", ""].iter() {
            // breaks for input="" if eof() is not listed first
            println!("test_check_bind_value_ending with input=\"{}\"", input);

            match check_bind_value_ending(Span::new(input)) {
                Ok((remaining, output)) => {
                    let expected_output = Span::new(input);
                    let expected_fragment = input;

                    assert_eq!(output, expected_output, "correct output for {:?}", input);
                    assert_eq!(remaining.fragment, expected_fragment, "input not consumed for {:?}", input);
                },
                Err(e) => {
                    println!("bind_value_ending for input={:?} returned an error={:?}", input, e);
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
            Ok((remaining, output)) =>{
                let expected_output = build_expected_bind_values();

                assert_eq!(output, expected_output, "correct output");
                assert_eq!(remaining.fragment, "", "nothing remaining");
            },
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
