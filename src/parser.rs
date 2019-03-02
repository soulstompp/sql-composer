use crate::types::{Sql, SqlBinding, SqlComposition, SqlCompositionAlias, SqlEnding, SqlLiteral};
use nom::{multispace, IResult};
use std::path::PathBuf;

use nom::types::CompleteStr;
use nom_locate::LocatedSpan;

type Span<'a> = LocatedSpan<CompleteStr<'a>>;

struct Token<'a> {
    pub position: Span<'a>,
    pub sql: Option<Sql>,
    pub notes: Vec<String>,
}

named!(opt_multispace(CompleteStr) -> Option<CompleteStr>,
    opt!(complete!(multispace))
);

named!(
    _parse_template(CompleteStr) -> SqlComposition,
    fold_many1!(
        alt_complete!(
            do_parse!(position!() >> e: parse_sql_end >> (Sql::Ending(e)))
            //TODO: collect aliases properly
            | do_parse!(position!() >> q: parse_quoted_bindvar >> (Sql::Binding(q)))
            | do_parse!(position!() >> b: parse_bindvar >> (Sql::Binding(b)))
            | do_parse!(position!() >> sc: parse_composer_macro >> (Sql::Composition((sc.0, sc.1))))
            | do_parse!(position!() >> s: parse_sql >> (Sql::Literal(s)))
        ),
        SqlComposition::default(),
        |mut acc: SqlComposition, item: Sql| {
            match item {
                Sql::Composition((mut sc, aliases)) => {
                    for alias in &aliases {
                        let stmt_path = alias.path().unwrap();

                        sc.insert_alias(&stmt_path).unwrap();
                    }

                    if acc.sql.len() == 0 {
                        return sc;
                    }

                    acc.push_sql(Sql::Composition((sc, aliases)));
                }
                _ => {
                    acc.push_sql(item);
                }
            }

            acc
        }
    )
);

pub fn parse_template(input: CompleteStr, path: Option<PathBuf>) -> IResult<CompleteStr, SqlComposition> {
    let res = _parse_template(input);

    res.and_then(|(remaining, mut comp)| {
        if let Some(p) = path {
            comp.set_path(&p).unwrap();
        }

        Ok((remaining, comp))
    })
}

named!(
    parse_path_arg(CompleteStr) -> CompleteStr,
    delimited!(tag_s!("<"), take_until_s!(">"), tag_s!(">"))
);

named!(
    parse_macro_name(CompleteStr) -> CompleteStr,
    delimited!(tag_s!(":"), take_until_s!("("), tag_s!("("))
);

named!(parse_composer_macro(CompleteStr) -> (SqlComposition, Vec<SqlCompositionAlias>),
       complete!(do_parse!(
               command: parse_macro_name >>
               distinct: opt!(tag_no_case!("distinct")) >>
               opt_multispace >>
               all: opt!(tag_no_case!("all")) >>
               opt_multispace >>
               columns: opt!(do_parse!(
                       columns: column_list >>
                       (columns)
                       )
               ) >>
               opt_multispace >>
               of: do_parse!(
                       of: of_list >>
                       ({
                           println!("of: {:?}", of);
                           of
                       })
               ) >>
               tag!(")") >>
               ({
                 println!("we made it!");

                 let mut sc = SqlComposition {
                     command: Some(command.to_string()),
                     distinct: distinct.is_some(),
                     all: all.is_some(),
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
    column_list(CompleteStr) -> Vec<String>,
    complete!(terminated!(
        many1!(terminated!(
            do_parse!(
                column: take_while!(|u| {
                    let c = u as char;

                    match c {
                        'a'...'z' => true,
                        '0'...'9' => true,
                        '_' => true,
                        _ => false,
                    }
                }) >> ({ column.to_string() })
            ),
            opt!(do_parse!(
                opt_multispace >> tag_s!(",") >> opt_multispace >> ()
            ))
        )),
        do_parse!(opt_multispace >> tag_no_case!("of") >> opt_multispace >> ())
    ))
);

named!(
    of_list(CompleteStr) -> Vec<SqlCompositionAlias>,
    complete!(many1!(terminated!(
        do_parse!(
            column: take_while!(|u| {
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
                let alias = SqlCompositionAlias::from_completestr(column).unwrap();

                println!("built alias: {:?}!", alias);

                alias
            })
        ),
        opt!(do_parse!(
            opt_multispace >> tag_s!(",") >> opt_multispace >> ()
        ))
    )))
);

named!(
    _parse_macro_include_alias(CompleteStr) -> CompleteStr,
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
    parse_quoted_bindvar(CompleteStr) -> SqlBinding,
    map_res!(
        delimited!(tag_s!("':bind("), take_until_s!(")"), tag_s!(")'")),
        SqlBinding::from_quoted_completestr
    )
);

named!(
    parse_bindvar(CompleteStr) -> SqlBinding,
    map_res!(
        delimited!(tag_s!(":bind("), take_until_s!(")"), tag_s!(")")),
        SqlBinding::from_completestr
    )
);

named!(
    parse_sql(CompleteStr) -> SqlLiteral,
    map_res!(take_until_either!(":;'"), SqlLiteral::from_completestr)
);

named!(
    parse_sql_end(CompleteStr) -> SqlEnding,
    map_res!(tag_s!(";"), SqlEnding::from_completestr)
);

#[cfg(test)]
mod tests {
    use super::{parse_bindvar, parse_composer_macro, parse_sql, parse_sql_end, parse_template};
    use crate::types::{Sql, SqlBinding, SqlComposition, SqlCompositionAlias, SqlEnding, SqlLiteral};
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
                    SqlLiteral::from_completestr("SELECT foo_id, bar FROM foo WHERE foo.bar = ".into()).unwrap(),
                ),
                Sql::Binding(SqlBinding::from_completestr("varname".into()).unwrap()),
                Sql::Ending(SqlEnding::from_completestr(";".into()).unwrap()),
            ],
            ..Default::default()
        }
    }

    fn include_template_comp() -> SqlComposition {
        SqlComposition {
            path: Some(PathBuf::from("src/tests/include-template.tql")),
            sql: vec![
                Sql::Literal(SqlLiteral::from_completestr("SELECT COUNT(foo_id)\nFROM (\n  ".into()).unwrap()),
                Sql::Composition((simple_template_compose_comp(), vec![])),
                Sql::Literal(SqlLiteral::from_completestr("\n)".into()).unwrap()),
                Sql::Ending(SqlEnding::from_completestr(";".into()).unwrap()),
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

        let out = parse_bindvar(input.into());

        let expected = Ok((
            "blah blah blah".into(),
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

        let expected = Ok(("blah blah blah".into(), SqlEnding { value: ";".into() }));

        let out = parse_sql_end(input.into());

        assert_eq!(out, expected);
    }

    #[test]
    fn parse_sql_until_path() {
        let input = "select * from foo where foo.bar = :bind(varname);";

        let out = parse_sql(input.into());

        let expected = Ok((
            ":bind(varname);".into(),
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

        let out = parse_template(input.into(), None);

        let expected = Ok((
            "".into(),
            SqlComposition {
                sql: vec![
                    Sql::Literal(SqlLiteral::from_completestr("SELECT * FROM (".into()).unwrap()),
                    Sql::Composition((simple_template_compose_comp(), vec![])),
                    Sql::Literal(SqlLiteral::from_completestr(") WHERE name = ".into()).unwrap()),
                    Sql::Binding(SqlBinding::from_quoted_completestr("bindvar".into()).unwrap()),
                    Sql::Ending(SqlEnding::from_completestr(";".into()).unwrap()),
                ],
                ..Default::default()
            },
        ));

        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_include_template() {
        let input = "SELECT * FROM (:compose(src/tests/include-template.tql)) WHERE name = ':bind(bindvar)';";

        let out = parse_template(input.into(), None);

        let expected: Result<(CompleteStr, SqlComposition), nom::Err<CompleteStr>> = Ok((
            "".into(),
            SqlComposition {
                sql: vec![
                    Sql::Literal(SqlLiteral::from_completestr("SELECT * FROM (".into()).unwrap()),
                    Sql::Composition((include_template_compose_comp(), vec![])),
                    Sql::Literal(SqlLiteral::from_completestr(") WHERE name = ".into()).unwrap()),
                    Sql::Binding(SqlBinding::from_quoted_completestr("bindvar".into()).unwrap()),
                    Sql::Ending(SqlEnding::from_completestr(";".into()).unwrap()),
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

        let comp = parse_composer_macro(sql_str.into());

        let expected = Ok((
            ";".into(),
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
