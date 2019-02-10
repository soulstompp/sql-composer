use std::str;

use nom::{IResult, multispace};
use std::io::prelude::*;
use std::fs::File;
use std::fmt;
use std::path::{Path, PathBuf};
use crate::types::{Sql, SqlBinding, SqlCompositionAlias, SqlComposition, SqlEnding, SqlText};

named!(opt_multispace<&[u8], Option<&[u8]>>,
    opt!(complete!(multispace))
);

named!(_parse_template<SqlComposition>,
    fold_many1!(
        alt_complete!(
            do_parse!(e: parse_sql_end >> (Sql::Ending(e)))
            //TODO: collect aliases properly
            | do_parse!(q: parse_quoted_bindvar >> (Sql::Binding(q)))
            | do_parse!(b: parse_bindvar >> (Sql::Binding(b)))
            | do_parse!(sc: parse_expander_macro >> (Sql::Composition((sc.0, sc.1))))
            | do_parse!(s: parse_sql >> (Sql::Text(s)))
        ),
        SqlComposition::default(), |mut acc: SqlComposition, item: Sql| {
            let item_sql = item;

            match item_sql {
                Sql::Composition((mut sc, aliases)) => {
                    for alias in &aliases {
                        let stmt_path = alias.path().unwrap();

                        sc.insert_alias(&stmt_path).unwrap();
                    }

                    if acc.sql.len() == 0 {
                        return sc;
                    }


                    acc.push_sql(Sql::Composition((sc, aliases)));
                },
                _ => {
                    acc.push_sql(item_sql);
                }
            }

            acc
        }
     )
);

pub fn parse_template(input: &[u8], path: Option<PathBuf>) -> IResult<&[u8], SqlComposition> {
    let res = _parse_template(input);

    res.and_then(|(remaining, mut comp)| {
        if let Some(p) = path {
            comp.set_path(&p);
        }

        Ok((remaining, comp))
    })
}

pub fn parse_expand(input: &[u8]) -> IResult<&[u8], SqlComposition> {
    let expand_res = _parse_expand(input);

    expand_res.and_then(|(remaining, s)| {
        parse_path_arg(s).and_then(|(_r, p)| {
            let statement = SqlComposition::from_utf8_path_name(p).unwrap();

            Ok((remaining, statement))
        })
    })
}

named!(_parse_expand<&[u8]>,
   delimited!(
       tag_s!(":expand("),
       take_until_s!(")"),
       tag_s!(")")
   )
);

named!(parse_path_arg<&[u8]>,
   delimited!(
       tag_s!("<"),
       take_until_s!(">"),
       tag_s!(">")
   )
);

named!(parse_macro_name<&[u8]>,
   delimited!(
       tag_s!(":"),
       take_until_s!("("),
       tag_s!("(")
   )
);

named!(parse_expander_macro<&[u8], (SqlComposition, Vec<SqlCompositionAlias>)>,
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
                     command: Some(String::from_utf8(command.to_vec()).unwrap()),
                     distinct: distinct.is_some(),
                     all: all.is_some(),
                     columns,
                     of,
                     ..Default::default()
                 };

                 sc.update_aliases();

                 (sc, vec![])
               })
       ))
);

named!(column_list<Vec<String>>,
    complete!(
        terminated!(
            many1!(
                terminated!(
                    do_parse!(
                        column: take_while!(|u| {
                            let c = u as char;

                            match c {
                                'a'...'z' => true,
                                '0'...'9' => true,
                                '_' => true,
                                _ => false
                            }
                        })
                        >>
                        ({
                            String::from_utf8(column.to_vec()).unwrap()
                        })),
                        opt!(
                            do_parse!(
                                opt_multispace >>
                                tag_s!(",") >>
                                opt_multispace >> ()
                            )
                        )
                )
            ),
            do_parse!(
                opt_multispace >>
                tag_no_case!("of") >>
                opt_multispace >> ()
                )
        )
    )
);

named!(of_list<Vec<SqlCompositionAlias>>,
    complete!(
        many1!(
            terminated!(
                do_parse!(
                    column: take_while!(|u| {
                        let c = u as char;

                        match c {
                            'a'...'z' => true,
                            '0'...'9' => true,
                            '-'|'_' => true,
                            '.'|'/'|'\\' => true,
                            _ => false
                        }
                    })
                    >>
                    ({
                        //TODO: clean this up properly
                        let alias = SqlCompositionAlias::from_utf8(column).unwrap();

                        println!("built alias: {:?}!", alias);

                        alias
                    })
                ),
                opt!(
                    do_parse!(
                        opt_multispace >>
                        tag_s!(",") >>
                        opt_multispace >> ()
                        )
                    )
            )
        )
    )
);

named!(_parse_macro_include_alias<&[u8]>,
    take_while!(|u| {
        let c = u as char;

        match c {
          'a'...'z' => true,
          '0'...'9' => true,
          '_'|'-'|'.'|'/' => true,
          _ => false
        }
    })
);

named!(parse_quoted_bindvar<SqlBinding>,
   map_res!(
       delimited!(
           tag_s!("':bind("),
           take_until_s!(")"),
           tag_s!(")'")
       ),
       SqlBinding::from_quoted_utf8
   )
);

named!(parse_bindvar<SqlBinding>,
   map_res!(
       delimited!(
           tag_s!(":bind("),
           take_until_s!(")"),
           tag_s!(")")
       ),
       SqlBinding::from_utf8
   )
);

named!(parse_sql<SqlText>,
   map_res!(
       take_until_either!(":;'"),
       SqlText::from_utf8
   )
);


named!(parse_sql_end<SqlEnding>,
   map_res!(
       tag_s!(";"),
       SqlEnding::from_utf8
   )
);

#[cfg(test)]
mod tests {
    use super::{ parse_bindvar, parse_sql, parse_sql_end, parse_template, parse_expander_macro };
    use crate::types::{SqlCompositionAlias, SqlComposition, SqlBinding, SqlEnding, SqlText, Sql};
    use std::path::{Path, PathBuf};
    use std::collections::HashMap;

    fn simple_aliases() -> Vec<SqlCompositionAlias> {
        vec![
            SqlCompositionAlias {
                name: None,
                path: Some("src/tests/simple-template.tql".into())
            }]
    }

    fn include_aliases() -> Vec<SqlCompositionAlias> {
        vec![
            SqlCompositionAlias {
                name: None,
                path: Some("src/tests/include-template.tql".into())
            }]

    }

    fn simple_alias_hash() -> HashMap<SqlCompositionAlias, SqlComposition> {
        let mut acc = HashMap::new();

        let p = PathBuf::from("src/tests/simple-template.tql");

        acc.entry(SqlCompositionAlias::from_path(&p)).or_insert(SqlComposition::from_path(&p).unwrap());

        acc
    }

    fn include_alias_hash() -> HashMap<SqlCompositionAlias, SqlComposition> {
        let mut acc = simple_alias_hash();

        let p = PathBuf::from("src/tests/include-template.tql");

        acc.entry(SqlCompositionAlias::from_path(&p)).or_insert(SqlComposition::from_path(&p).unwrap());

        acc
    }

    fn include_shallow_alias_hash() -> HashMap<SqlCompositionAlias, SqlComposition> {
        let mut acc = HashMap::new();

        let p = PathBuf::from("src/tests/include-template.tql");

        acc.entry(SqlCompositionAlias::from_path(&p)).or_insert(SqlComposition::from_path(&p).unwrap());

        acc
    }

    fn simple_template_comp() -> SqlComposition {
        SqlComposition{
            path: Some(PathBuf::from("src/tests/simple-template.tql")),
            sql: vec![
                Sql::Text(SqlText::from_utf8(b"SELECT foo_id, bar FROM foo WHERE foo.bar = ").unwrap()),
                Sql::Binding(SqlBinding::from_utf8(b"varname").unwrap()),
                Sql::Ending(SqlEnding::from_utf8(b";").unwrap()),
            ],
            ..Default::default()
        }
    }

    fn include_template_comp() -> SqlComposition {
        SqlComposition{
            path: Some(PathBuf::from("src/tests/include-template.tql")),
            sql: vec![
                Sql::Text(SqlText::from_utf8(b"SELECT COUNT(foo_id)\nFROM (\n  ").unwrap()),
                Sql::Composition((simple_template_expand_comp(), vec![])),
                Sql::Text(SqlText::from_utf8(b"\n)").unwrap()),
                Sql::Ending(SqlEnding::from_utf8(b";").unwrap()),
            ],
            ..Default::default()
        }
    }

    fn simple_template_expand_comp() -> SqlComposition {
        SqlComposition{
            command: Some("expand".into()),
            of: simple_aliases(),
            aliases: simple_alias_hash(),
            ..Default::default()
        }
    }

    fn include_template_expand_comp() -> SqlComposition {
        SqlComposition{
            command: Some("expand".into()),
            of: include_aliases(),
            aliases: include_shallow_alias_hash(),
            ..Default::default()
        }
    }

    #[test]
    fn test_parse_bindvar() {
        let input = b":bind(varname)blah blah blah";

        let out = parse_bindvar(input);

        let expected = Ok((&b"blah blah blah"[..], SqlBinding{ name: "varname".into(), quoted: false  }));
        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_sql_end() {
        let input = b";blah blah blah";

        let expected = Ok((&b"blah blah blah"[..], SqlEnding{ value: ";".into() }));

        let out = parse_sql_end(input);

        assert_eq!(out, expected);

    }

    #[test]
    fn parse_sql_until_path() {
        let input = b"select * from foo where foo.bar = :bind(varname);";

        let out = parse_sql(input);

        let expected = Ok((&b":bind(varname);"[..], SqlText{ value: "select * from foo where foo.bar = ".into(), ..Default::default() } ));
        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_simple_template() {
        let input = "SELECT * FROM (:expand(src/tests/simple-template.tql)) WHERE name = ':bind(bindvar)';";

        let out = parse_template(input.as_bytes(), None);

        let expected = Ok((&b""[..],
                           SqlComposition {
                               sql: vec![
                                   Sql::Text(SqlText::from_utf8(b"SELECT * FROM (").unwrap()),
                                   Sql::Composition((simple_template_expand_comp(), vec![])),
                                   Sql::Text(SqlText::from_utf8(b") WHERE name = ").unwrap()),
                                   Sql::Binding(SqlBinding::from_quoted_utf8(b"bindvar").unwrap()),
                                   Sql::Ending(SqlEnding::from_utf8(b";").unwrap())
                               ],
                               ..Default::default()
                           }
                          ));

        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_include_template() {
        let input = "SELECT * FROM (:expand(src/tests/include-template.tql)) WHERE name = ':bind(bindvar)';";

        let out = parse_template(input.as_bytes(), None);

        let expected:Result<(&[u8], SqlComposition), nom::Err<&[u8]>> = Ok((&b""[..],
                           SqlComposition {
                               sql: vec![
                                   Sql::Text(SqlText::from_utf8(b"SELECT * FROM (").unwrap()),
                                   Sql::Composition((include_template_expand_comp(), vec![])),
                                   Sql::Text(SqlText::from_utf8(b") WHERE name = ").unwrap()),
                                   Sql::Binding(SqlBinding::from_quoted_utf8(b"bindvar").unwrap()),
                                   Sql::Ending(SqlEnding::from_utf8(b";").unwrap())
                               ],
                               ..Default::default()
                           }
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
    fn test_parse_composed_expander() {
        let sql_str = b":count(distinct col1, col2 of src/tests/simple-template.tql, src/tests/include-template.tql);";

        let comp = parse_expander_macro(sql_str);

        let expected = Ok((&b";"[..], (SqlComposition{
            command: Some("count".into()),
            path: None,
            distinct: true,
            columns: Some(vec!["col1".into(), "col2".into()]),
            of: vec![
                SqlCompositionAlias {
                    name: None,
                    path: Some("src/tests/simple-template.tql".into())
                },
                SqlCompositionAlias {
                    name: None,
                    path: Some("src/tests/include-template.tql".into())
                }],
            aliases: include_alias_hash(),
            ..Default::default()
        }, vec![])));

        assert_eq!(comp, expected);
    }

    #[test]
    fn test_simple_composed_expander() {
        let sql_str = ":count(src/tests/simple-template.tql);";

        let comp = SqlComposition::from_str(sql_str);

        println!("final comp: {}", comp);

        let mut expected = SqlComposition{
            command: Some("count".into()),
            path: None,
            of: vec![
                SqlCompositionAlias {
                    name: None,
                    path: Some("src/tests/simple-template.tql".into())
                }],
            aliases: simple_alias_hash(),
            sql: vec![
                Sql::Ending(SqlEnding{ value: ";".into() })
            ],
            ..Default::default()
        };

        assert_eq!(comp, expected);
    }

}
