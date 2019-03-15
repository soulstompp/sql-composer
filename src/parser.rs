use crate::types::{CompleteStr, ParsedItem, ParsedSpan, Position, Span, Sql, SqlBinding,
                   SqlComposition, SqlCompositionAlias, SqlEnding, SqlLiteral};

use nom::{multispace, IResult};
use std::path::PathBuf;

named!(opt_multispace(Span) -> Option<Span>,
    opt!(complete!(multispace))
);

named!(
    _parse_template(Span) -> ParsedItem<SqlComposition>,
    fold_many1!(
        alt_complete!(
            do_parse!(position!() >> e: parse_sql_end >> (Sql::Ending(e)))
            //TODO: collect aliases properly
            | do_parse!(position!() >> q: parse_quoted_bindvar >> (Sql::Binding(q)))
            | do_parse!(position!() >> b: parse_bindvar >> (Sql::Binding(b)))
            | do_parse!(position!() >> sc: parse_composer_macro >> (Sql::Composition((ParsedItem::from_span(sc.0, Span::new(CompleteStr("")), None).unwrap(), sc.1))))
            | do_parse!(position!() >> s: parse_sql >> (Sql::Literal(s)))
        ),
        ParsedItem::from_span(SqlComposition::default(), Span::new(CompleteStr("")), None).unwrap(),
        |mut acc: ParsedItem<SqlComposition>, item: Sql| {
            match item {
                Sql::Composition((mut sc, aliases)) => {
                    for alias in &aliases {
                        let stmt_path = alias.path().unwrap();

                        sc.item.insert_alias(&stmt_path).unwrap();
                    }

                    if acc.item.sql.len() == 0 {
                        return sc;
                    }

                    acc.item.push_sql(Sql::Composition((sc, aliases)));
                }
                _ => {
                    acc.item.push_sql(item);
                }
            }

            acc
        }
    )
);

pub fn parse_template(
    span: Span,
    alias: Option<SqlCompositionAlias>,
) -> IResult<Span, ParsedItem<SqlComposition>> {
    let res = _parse_template(span);

    res.and_then(|(remaining, mut comp)| {
        if let Some(a) = alias {
            comp.item
                .set_position(Position::Parsed(ParsedSpan::new(span, Some(a))));
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
               opt_multispace >>
               position!() >>
               all: command_all_arg >>
               opt_multispace >>
               columns: opt!(column_list) >>
               opt_multispace >>
               position!() >>
               of: of_list >>
               tag!(")") >>
               ({
                 println!("we made it!");

                 let mut sc = SqlComposition {
                     command: Some(command),
                     distinct,
                     all,
                     columns,
                     of,
                     ..Default::default()
                 };

                 sc.update_aliases().unwrap();

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
        many1!(column_listing),
        do_parse!(opt_multispace >> tag_no_case!("of") >> opt_multispace >> ())
    )
);

named!(
    column_name(Span) -> ParsedItem<String>,
    do_parse!(
        position!() >>
        column: take_while!(|u| {
            let c = u as char;

            match c {
                'a'...'z' => true,
                '0'...'9' => true,
                '_' => true,
                _ => false,
            }
        }) >>
        ({
            let p = ParsedItem::from_span(
                column.fragment.to_string(),
                column,
                None
            );

            p.expect("unable to build ParsedItem of String from column_list parser")
        })
    )
);

named!(
    column_listing(Span) -> ParsedItem<String>,
    do_parse!(
        i: column_name >>
        opt_multispace >>
        opt!(tag!(",")) >>
        opt_multispace >>
        (i)
    )
);

pub fn _column_list(span: Span) -> IResult<Span, Vec<ParsedItem<String>>> {
    let res = _column_list(span);

    res.and_then(|(remaining, mut vec)| Ok((remaining, vec)))
}

/*
named!(
    column_list(Span) -> Vec<ParsedItem<String>>,
    complete!(many1!(terminated!(
        do_parse!(
            position!() >>
            column_name: take_while!(|u| {
                let c = u as char;

                match c {
                    'a'...'z' => true,
                    '0'...'9' => true,
                    '_' => true,
                    _ => false,
                }
            }) >> ({
                //TODO: clean this up properly
                let name = String::from(*column_name.fragment);

                ParsedItem::from_span(name, column_name, None).expect("Unable to create parsed item in column_list parser")
            })
        ),
        opt!(do_parse!(
            opt_multispace >> tag!(",") >> opt_multispace >> ()
        ))
    )))
);
*/
named!(
    of_list(Span) -> Vec<ParsedItem<SqlCompositionAlias>>,
    complete!(many1!(terminated!(
        do_parse!(
            position!() >>
            of_name: take_while!(|u| {
                let c = u as char;

                match c {
                    'a'...'z' => true,
                    '0'...'9' => true,
                    '-' | '_' => true,
                    '.' | '/' | '\\' => true,
                    _ => false,
                }
            }) >> ({
                //TODO: clean this up properly
                let alias = SqlCompositionAlias::from_span(of_name).unwrap();

                println!("built alias: {:?}!", alias);

                ParsedItem::from_span(alias, of_name, None).expect("Unable to create parsed item in of_list parser")
            })
        ),
        opt!(do_parse!(
            opt_multispace >> tag!(",") >> opt_multispace >> ()
        ))
    )))
);

named!(
    _parse_macro_include_alias(Span) -> Span,
    take_while!(|u| {
        let c = u as char;

        match c {
            'a'...'z' => true,
            '0'...'9' => true,
            '_' | '-' | '.' | '/' => true,
            _ => false,
        }
    })
);

named!(
    parse_quoted_bindvar(Span) -> ParsedItem<SqlBinding>,
    do_parse!(
        position!() >>
        bindvar: delimited!(tag!("':bind("), take_until!(")"), tag!(")'")) >>
        (
            ParsedItem::from_span(
                SqlBinding::new_quoted(bindvar).expect("SqlBinding::new_quoted() failed unexpectedly from parse_quoted_bindvar parser"),
                    bindvar,
                    None
            ).unwrap()
        )
    )
);

named!(
    parse_bindvar(Span) -> ParsedItem<SqlBinding>,
    do_parse!(
        position!() >>
        bindvar: delimited!(tag!(":bind("), take_until!(")"), tag!(")")) >>
        (
            ParsedItem::from_span(
                SqlBinding::new(bindvar).expect("SqlBinding::new() failed unexpectedly from parse_bindvar parser"),
                bindvar,
                None
            ).unwrap()
        )
    )
);

named!(
    parse_sql(Span) -> ParsedItem<SqlLiteral>,
    do_parse!(
        position!() >>
        literal: take_until_either!(":;'") >>
        (
            ParsedItem::from_span(
                SqlLiteral::new(literal.fragment.to_string()).expect("SqlLiteral::new() failed unexpectedly from parse_sql"),
                literal,
                None
            ).unwrap()
        )
    )
);

named!(
    parse_sql_end(Span) -> ParsedItem<SqlEnding>,
    do_parse!(
        position!() >>
        ending: tag!(";") >>
        (
            ParsedItem::from_span(
                SqlEnding::new(ending.fragment.to_string()).expect("SqlEnding::new() failed unexpectedly from parse_sql_end parser"),
                ending,
                None
            ).unwrap()
        )
    )
);

#[cfg(test)]
mod tests {
    use super::{parse_bindvar, parse_composer_macro, parse_sql, parse_sql_end, parse_template};
    use crate::types::{Span, Sql, SqlBinding, SqlComposition, SqlCompositionAlias, SqlEnding,
                       SqlLiteral};
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    use nom::types::CompleteStr;

    fn simple_aliases() -> Vec<SqlCompositionAlias> {
        vec![SqlCompositionAlias {
            name: None,
            path: Some("src/tests/simple-template.tql".into()),
        }]
    }

    fn include_aliases() -> Vec<SqlCompositionAlias> {
        vec![SqlCompositionAlias {
            name: None,
            path: Some("src/tests/include-template.tql".into()),
        }]
    }

    fn simple_alias_hash() -> HashMap<SqlCompositionAlias, SqlComposition> {
        let mut acc = HashMap::new();

        let p = PathBuf::from("src/tests/simple-template.tql");

        acc.entry(SqlCompositionAlias::from_path(&p))
            .or_insert(SqlComposition::from_path(&p).unwrap());

        acc
    }

    fn include_alias_hash() -> HashMap<SqlCompositionAlias, SqlComposition> {
        let mut acc = simple_alias_hash();

        let p = PathBuf::from("src/tests/include-template.tql");

        acc.entry(SqlCompositionAlias::from_path(&p))
            .or_insert(SqlComposition::from_path(&p).unwrap());

        acc
    }

    fn include_shallow_alias_hash() -> HashMap<SqlCompositionAlias, SqlComposition> {
        let mut acc = HashMap::new();

        let p = PathBuf::from("src/tests/include-template.tql");

        acc.entry(SqlCompositionAlias::from_path(&p))
            .or_insert(SqlComposition::from_path(&p).unwrap());

        acc
    }

    fn simple_template_comp() -> SqlComposition {
        SqlComposition {
            path: Some(PathBuf::from("src/tests/simple-template.tql")),
            sql: vec![
                Sql::Literal(
                    SqlLiteral::from_span(Span::new(
                        "SELECT foo_id, bar FROM foo WHERE foo.bar = ".into(),
                    ))
                    .unwrap(),
                ),
                Sql::Binding(SqlBinding::from_span(Span::new("varname".into())).unwrap()),
                Sql::Ending(SqlEnding::from_span(Span::new(";".into())).unwrap()),
            ],
            ..Default::default()
        }
    }

    fn include_template_comp() -> SqlComposition {
        SqlComposition {
            path: Some(PathBuf::from("src/tests/include-template.tql")),
            sql: vec![
                Sql::Literal(
                    SqlLiteral::from_span(Span::new("SELECT COUNT(foo_id)\nFROM (\n  ".into()))
                        .unwrap(),
                ),
                Sql::Composition((simple_template_compose_comp(), vec![])),
                Sql::Literal(SqlLiteral::from_span(Span::new("\n)".into())).unwrap()),
                Sql::Ending(SqlEnding::from_span(Span::new(";".into())).unwrap()),
            ],
            ..Default::default()
        }
    }

    fn simple_template_compose_comp() -> SqlComposition {
        SqlComposition {
            command: Some("compose".into()),
            of: simple_aliases(),
            aliases: simple_alias_hash(),
            ..Default::default()
        }
    }

    fn include_template_compose_comp() -> SqlComposition {
        SqlComposition {
            command: Some("compose".into()),
            of: include_aliases(),
            aliases: include_shallow_alias_hash(),
            ..Default::default()
        }
    }

    #[test]
    fn test_parse_bindvar() {
        let input = ":bind(varname)blah blah blah";

        let out = parse_bindvar(Span::new(input.into()));

        let expected = Ok((
            Span {
                offset:   14,
                line:     1,
                fragment: "blah blah blah".into(),
            },
            SqlBinding {
                name:   "varname".into(),
                quoted: false,
            },
        ));
        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_sql_end() {
        let input = ";blah blah blah";

        let expected = Ok((
            Span {
                offset:   1,
                line:     1,
                fragment: "blah blah blah".into(),
            },
            SqlEnding { value: ";".into() },
        ));

        let out = parse_sql_end(Span::new(input.into()));

        assert_eq!(out, expected);
    }

    #[test]
    fn parse_sql_until_path() {
        let input = "select * from foo where foo.bar = :bind(varname);";

        let out = parse_sql(Span::new(input.into()));

        let expected = Ok((
            Span {
                offset:   34,
                line:     1,
                fragment: ":bind(varname);".into(),
            },
            SqlLiteral {
                value: "select * from foo where foo.bar = ".into(),
                ..Default::default()
            },
        ));
        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_simple_template() {
        let input =
            "SELECT * FROM (:compose(src/tests/simple-template.tql)) WHERE name = ':bind(bindvar)';";

        let out = parse_template(Span::new(input.into()), None);

        let expected = Ok((
            Span {
                offset:   86,
                line:     1,
                fragment: "".into(),
            },
            SqlComposition {
                sql: vec![
                    Sql::Literal(
                        SqlLiteral::from_span(Span::new("SELECT * FROM (".into())).unwrap(),
                    ),
                    Sql::Composition((simple_template_compose_comp(), vec![])),
                    Sql::Literal(
                        SqlLiteral::from_span(Span::new(") WHERE name = ".into())).unwrap(),
                    ),
                    Sql::Binding(
                        SqlBinding::from_quoted_span(Span::new("bindvar".into())).unwrap(),
                    ),
                    Sql::Ending(SqlEnding::from_span(Span::new(";".into())).unwrap()),
                ],
                ..Default::default()
            },
        ));

        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_include_template() {
        let input = "SELECT * FROM (:compose(src/tests/include-template.tql)) WHERE name = ':bind(bindvar)';";

        let out = parse_template(Span::new(input.into()), None);

        let expected = Ok((
            Span {
                offset:   87,
                line:     1,
                fragment: "".into(),
            },
            SqlComposition {
                sql: vec![
                    Sql::Literal(
                        SqlLiteral::from_span(Span::new("SELECT * FROM (".into())).unwrap(),
                    ),
                    Sql::Composition((include_template_compose_comp(), vec![])),
                    Sql::Literal(
                        SqlLiteral::from_span(Span::new(") WHERE name = ".into())).unwrap(),
                    ),
                    Sql::Binding(
                        SqlBinding::from_quoted_span(Span::new("bindvar".into())).unwrap(),
                    ),
                    Sql::Ending(SqlEnding::from_span(Span::new(";".into())).unwrap()),
                ],
                ..Default::default()
            },
        ));

        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_file_template() {
        let stmt = SqlComposition::from_path(Path::new("src/tests/simple-template.tql")).unwrap();
        let expected = simple_template_comp();

        assert_eq!(stmt, expected);
    }

    #[test]
    fn test_parse_file_inclusive_template() {
        let stmt = SqlComposition::from_path(Path::new("src/tests/include-template.tql")).unwrap();
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
            },
            (
                SqlComposition {
                    command: Some("count".into()),
                    path: None,
                    distinct: true,
                    columns: Some(vec!["col1".into(), "col2".into()]),
                    of: vec![
                        SqlCompositionAlias {
                            name: None,
                            path: Some("src/tests/simple-template.tql".into()),
                        },
                        SqlCompositionAlias {
                            name: None,
                            path: Some("src/tests/include-template.tql".into()),
                        },
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

        println!("final comp: {}", comp);

        let expected = SqlComposition {
            command: Some("count".into()),
            path: None,
            of: vec![SqlCompositionAlias {
                name: None,
                path: Some("src/tests/simple-template.tql".into()),
            }],
            aliases: simple_alias_hash(),
            sql: vec![Sql::Ending(SqlEnding { value: ";".into() })],
            ..Default::default()
        };

        assert_eq!(comp, expected);
    }

}
