use crate::types::{CompleteStr, LocatedSpan, ParsedItem, ParsedSpan, Position, Span, Sql,
                   SqlBinding, SqlComposition, SqlCompositionAlias, SqlDbObject, SqlEnding,
                   SqlKeyword, SqlLiteral};

use nom::{multispace, IResult};
use std::path::PathBuf;

named!(opt_multispace(Span) -> Option<Span>,
    opt!(complete!(multispace))
);

named!(
    _parse_template(Span) -> ParsedItem<SqlComposition>,
    fold_many1!(
        alt_complete!(
            do_parse!(position!() >> e: parse_sql_end >> (vec![Sql::Ending(e)]))
            | do_parse!(position!() >> q: parse_quoted_bindvar >> (vec![Sql::Binding(q)]))
            | do_parse!(position!() >> b: parse_bindvar >> (vec![Sql::Binding(b)]))
            | do_parse!(position!() >> sc: parse_composer_macro >> (vec![Sql::Composition((ParsedItem::from_span(sc.0, Span::new(CompleteStr("")), None).expect("expected to make a Span in sc _parse_template"), sc.1))]))
            | do_parse!(position!() >> dbo: db_object >> (vec![Sql::Keyword(dbo.0), Sql::DbObject(dbo.1)]))
            | do_parse!(position!() >> k: keyword >> (vec![Sql::Keyword(k)]))
            | do_parse!(position!() >> s: parse_sql >> (vec![Sql::Literal(s)]))
        ),
        ParsedItem::from_span(SqlComposition::default(), Span::new(CompleteStr("")), None).expect("expected to make a Span in _parse_template parser"),
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

                        acc.item.push_sql(Sql::Composition((sc, aliases)));
                    }
                    _ => {
                        acc.item.push_sql(item);
                    }
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
        many1!(column_name),
        do_parse!(opt_multispace >> tag_no_case!("of") >> opt_multispace >> ())
    )
);

named!(
    column_name(Span) -> ParsedItem<String>,
    dbg_dmp!(
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
          opt!(do_parse!(opt_multispace >> tag!(",") >> opt_multispace >> ()))
    )
    )
);

named!(take_while_name_char(Span) -> Span,
    do_parse!(
        name: take_while!(|c| {
            match c {
                'a'...'z' => true,
                'A'...'Z' => true,
                '0'...'9' => true,
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
        opt_multispace >>
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
        keyword: alt_complete!(
            command_sql |
            db_object_pre_sql |
            db_object_post_sql
        ) >>
        (keyword)
    )
);

named!(command_sql(Span) -> Span,
    alt_complete!(
        tag_no_case!("SELECT") |
        tag_no_case!("INSERT INTO") |
        tag_no_case!("UPDATE") |
        tag_no_case!("WHERE")
    )
);

named!(db_object_pre_sql(Span) -> Span,
    alt_complete!(
        tag_no_case!("FROM") |
        tag_no_case!("JOIN")
    )
);

named!(db_object_post_sql(Span) -> Span,
    alt_complete!(
        tag_no_case!("ON") |
        tag_no_case!("USING")
    )
);

named!(db_object_alias_sql(Span) -> Span,
    do_parse!(
        opt!(tag_no_case!("AS")) >>
        opt_multispace >>
        not!(peek!(keyword_sql)) >>
        not!(peek!(tag!("("))) >>
        alias: take_while_name_char >>
        opt_multispace >>
        (
            alias
        )
    )
);

named!(
    db_object(Span) -> (ParsedItem<SqlKeyword>, ParsedItem<SqlDbObject>),
    do_parse!(
        keyword: db_object_pre_sql >>
        opt_multispace >>
        position!() >>
        table: db_object_alias_sql >>
        opt_multispace >>
        position!() >>
        alias: opt!(db_object_alias_sql) >>
        opt_multispace >>
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
    complete!(many1!(terminated!(
        do_parse!(
            position!() >>
            of_name: take_while!(|u| {
                let c = u as char;

                match c {
                    'a'...'z' => true,
                    'A'...'Z' => true,
                    '0'...'9' => true,
                    '-' | '_' => true,
                    '.' | '/' | '\\' => true,
                    _ => false,
                }
            }) >> ({
                //TODO: clean this up properly
                let alias = SqlCompositionAlias::from_span(of_name).expect("expected alias from_span in of_list");

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
            'A'...'Z' => true,
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
        tag!("':bind(") >>
        bindvar: take_while_name_char >>
        tag!(")'") >>
        opt_multispace >>
        (
            ParsedItem::from_span(
                SqlBinding::new_quoted(bindvar.fragment.to_string()).expect("SqlBinding::new_quoted() failed unexpectedly from parse_quoted_bindvar parser"),
                    bindvar,
                    None
            ).expect("expected a parsed item from_span in parse_quoted_bindvar")
        )
    )
);

named!(
    parse_bindvar(Span) -> ParsedItem<SqlBinding>,
    do_parse!(
        position!() >>
        bindvar: delimited!(tag!(":bind("), take_until!(")"), tag!(")")) >>
        opt_multispace >>
        (
            ParsedItem::from_span(
                SqlBinding::new(bindvar.fragment.to_string()).expect("SqlBinding::new() failed unexpectedly from parse_bindvar parser"),
                bindvar,
                None
            ).expect("expected Ok from ParsedItem::from_span in parse_bindvar")
        )
    )
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
            ParsedItem::from_span(SqlLiteral::default(), Span::new(CompleteStr("")), None).expect("expected to make a Span in parse_sql parser"),
            |mut acc: ParsedItem<SqlLiteral>, item: Span| {
                acc.item.value.push_str(&item.fragment);
                acc
            }
        ) >>
        ({
            let mut p = parsed;

            println!("pos: {:?}, value: {}", pos, &p.item.value);
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
        opt_multispace >>
        (
            ParsedItem::from_span(
                SqlEnding::new(ending.fragment.to_string()).expect("SqlEnding::new() failed unexpectedly from parse_sql_end parser"),
                ending,
                None
            ).expect("expected Ok from ParsedItem::from_span in parse_sql_end")
        )
    )
);

#[cfg(test)]
mod tests {
    use super::{column_list, db_object, db_object_alias_sql, parse_bindvar, parse_composer_macro,
                parse_quoted_bindvar, parse_sql, parse_sql_end, parse_template};
    use crate::types::{ParsedItem, Span, Sql, SqlBinding, SqlComposition, SqlCompositionAlias,
                       SqlDbObject, SqlEnding, SqlLiteral};
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    use crate::tests::{build_parsed_binding_item, build_parsed_db_object,
                       build_parsed_ending_item, build_parsed_item, build_parsed_keyword_item,
                       build_parsed_literal_item, build_parsed_path_position,
                       build_parsed_quoted_binding_item, build_parsed_sql_binding,
                       build_parsed_sql_ending, build_parsed_sql_keyword,
                       build_parsed_sql_literal, build_parsed_sql_quoted_binding,
                       build_parsed_string, build_span};

    use nom::types::CompleteStr;
    use nom::{multispace, IResult};

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
        let shift_line = shift_line.unwrap_or(0);
        let shift_offset = shift_offset.unwrap_or(0);

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
                build_parsed_sql_binding("varname", None, Some(50), "varname"),
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
    fn test_parse_bindvar() {
        let input = ":bind(varname)blah blah blah";

        let out = parse_bindvar(Span::new(input.into())).expect("expected Ok from parse_bindvar");

        let expected_span = build_span(Some(1), Some(14), "blah blah blah");
        let expected_item = build_parsed_binding_item("varname", None, Some(6), "varname");

        let (span, item) = out;

        assert_eq!(item, expected_item, "items match");
        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_quoted_bindvar() {
        let input = "':bind(varname)'blah blah blah";

        let out = parse_quoted_bindvar(Span::new(input.into()))
            .expect("expected Ok from parse_quoted_bindvar");

        let expected_span = build_span(Some(1), Some(16), "blah blah blah");
        let expected_item = build_parsed_quoted_binding_item("varname", None, Some(7), "varname");

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
                build_parsed_sql_quoted_binding("bindvar", None, Some(76), "bindvar"),
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
                build_parsed_sql_quoted_binding("bindvar", None, Some(77), "bindvar"),
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

        let (leftover_span, span) = db_object_alias_sql(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_alias_with_as_preceding_space() {
        let input = " AS tt WHERE 1";

        let expected_span = build_span(Some(1), Some(1), "AS");

        let (leftover_span, span) = db_object_alias_sql(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_alias_without_as() {
        let input = "tt WHERE 1";

        let expected_span = build_span(Some(1), Some(0), "tt");

        let (leftover_span, span) = db_object_alias_sql(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_alias_without_as_preceeding_space() {
        let input = "tt WHERE 1";

        let expected_span = build_span(Some(1), Some(0), "tt");

        let (leftover_span, span) = db_object_alias_sql(Span::new(input.into()))
            .expect(&format!("expected Ok from parsing {}", input));

        assert_eq!(span, expected_span, "spans match");
    }

    #[test]
    fn test_parse_db_object_alias_with_as_preceded_space() {
        let input = " tt WHERE 1";

        let expected_span = build_span(Some(1), Some(1), "tt");

        let (leftover_span, span) = db_object_alias_sql(Span::new(input.into()))
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

        let (span, (keyword_item, dbo_item)) = db_object(Span::new(input.into()))
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

        let (span, (keyword_item, dbo_item)) = db_object(Span::new(input.into()))
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

        let (span, (keyword_item, dbo_item)) = db_object(Span::new(input.into()))
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
}
