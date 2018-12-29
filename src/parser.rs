use std::str;

use nom::IResult;
use std::collections::HashMap;
use std::io::prelude::*;
use std::fs::File;
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct SqlStatementAlias {
    name: String,
    path: Option<PathBuf>,
}

impl SqlStatementAlias {
    pub fn path(&self) -> Option<PathBuf> {
        if let Some(p) = &self.path {
            return Some(p.to_path_buf());
        }
        else {
            return None;
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct SqlComposition {
    pub name: Option<String>,
    columns: Vec<String>,
    pub stmt: Option<SqlStatement>,
    wraps: Vec<SqlStatementAlias>,
    pub aliases: HashMap<SqlStatementAlias, SqlStatement>,
    pub path: Option<PathBuf>,
}

impl SqlComposition {
    pub fn new(stmt: SqlStatement) -> Self {
        Self {
            stmt: Some(stmt),
            ..Default::default()
        }
    }

    pub fn from_str(q: &str) -> Self {
        let (remaining, stmt) = parse_template(&q.as_bytes(), None).unwrap();

        if remaining.len() > 0 {
            panic!("found extra information: {}", String::from_utf8(remaining.to_vec()).unwrap());
        }

        stmt
    }

    pub fn from_path(path: &Path) -> ::std::io::Result<SqlComposition> {
        let mut f = File::open(path).unwrap();
        let mut s = String::new();

        let _res = f.read_to_string(&mut s);

        let (_remaining, stmt) = parse_template(&s.as_bytes(), Some(path.into())).unwrap();

        Ok(stmt)
    }

    pub fn from_utf8_path_name(vec: &[u8]) -> ::std::io::Result<SqlComposition> {
        //TODO: don't unwrap here
        let s = &std::str::from_utf8(vec).unwrap();
        let p = Path::new(s);

        Self::from_path(p)
    }

    pub fn column_list(&self) -> Result<String, ()> {
        let s = self.columns.iter().enumerate().fold(String::new(), |mut acc, (i, name)| {
            if i > 0 {
                acc.push(',');
            }

            acc.push_str(name);

            acc
        });

        Ok(s)
    }
}

impl fmt::Display for SqlComposition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

      match &self.name {
         Some(n) => write!(f, ":{}(", n)?,
         None => write!(f, ":expand(")?
      }

      let mut c = 0;

      for col in &self.columns {
          if c > 0 {
              write!(f, ",")?;
          }

          write!(f, "{}", col)?;

          c += 1;
      }

      write!(f, ")")
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct SqlStatement {
    template: String,
    pub chunks: Vec<Sql>,
}

impl SqlStatement {
    pub fn new(template: String) -> Self {
        SqlStatement {
            template: template,
            ..Default::default()
        }
    }

    fn push_chunk(&mut self, c: Sql) {
        self.chunks.push(c);
    }

    pub fn push_text(&mut self, value: &str) {
        self.push_chunk(Sql::Text(SqlText{
            value: value.into(),
            quoted: false
        }))
    }

    pub fn push_quoted_text(&mut self, value: &str) {
        self.push_chunk(Sql::Text(SqlText{
            value: value.into(),
            quoted: true
        }))
    }

    pub fn end(&mut self, value: &str) {
        //TODO: check if this has already ended
        self.push_chunk(Sql::Ending(SqlEnding{
            value: value.into()
        }));
    }
}

impl fmt::Display for SqlStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

      for c in self.chunks.iter() {
          write!(f, "{}", c)?;
      }

      write!(f, "")
    }
}

#[derive(Debug, PartialEq)]
pub enum Sql {
  Text(SqlText),
  Binding(SqlBinding),
  SubStatement(SqlComposition),
  Composition(SqlComposition),
  Ending(SqlEnding)
}

impl fmt::Display for Sql {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
      match self {
          Sql::Text(t) => write!(f, "{}", t)?,
          Sql::Binding(b) => write!(f, "{}", b)?,
          Sql::SubStatement(s) => write!(f, "{}", s)?,
          Sql::Composition(w) => write!(f, "{}", w)?,
          Sql::Ending(e) => write!(f, "{}", e)?
      }

      write!(f, "")
    }
}

#[derive(Debug, PartialEq)]
pub struct SqlEnding {
    value: String
}

impl SqlEnding {
    fn from_utf8(vec: &[u8]) -> Result<Self, ::std::string::FromUtf8Error> {
        let s = String::from_utf8(vec.to_vec())?;

        Ok(Self{value: s})
    }
}

impl fmt::Display for SqlEnding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, PartialEq, Default)]
pub struct SqlText {
    value: String,
    quoted: bool
}

impl SqlText {
    fn from_utf8(vec: &[u8]) -> Result<Self, ::std::string::FromUtf8Error> {
        let s = String::from_utf8(vec.to_vec())?;

        Ok(Self{ value: s, ..Default::default() })
    }
}

impl fmt::Display for SqlText {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, PartialEq)]
pub struct SqlBinding {
    pub name: String,
    pub quoted: bool
}

impl SqlBinding {
    fn from_utf8(vec: &[u8]) -> Result<Self, ::std::string::FromUtf8Error> {
        let s = String::from_utf8(vec.to_vec())?;

        Ok(Self{ name: s, quoted: false })
    }

    fn from_quoted_utf8(vec: &[u8]) -> Result<Self, ::std::string::FromUtf8Error> {
        let s = String::from_utf8(vec.to_vec())?;

        Ok(Self{ name: s, quoted: true })
    }
}

impl fmt::Display for SqlBinding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

named!(_parse_template<(Vec<Sql>, HashMap<SqlStatementAlias, SqlComposition>)>,
    fold_many1!(
        alt_complete!(
            do_parse!(e: parse_sql_end >> (Sql::Ending(e), vec![]))
            //TODO: collect aliases properly
            | do_parse!(i: parse_expand >> (Sql::SubStatement(i), vec![]))
            | do_parse!(q: parse_quoted_bindvar >> (Sql::Binding(q), vec![]))
            | do_parse!(b: parse_bindvar >> (Sql::Binding(b), vec![]))
            //TODO: collect aliases properly
            | do_parse!(w: parse_expander_macro >> (Sql::Composition(w), vec![]))
            | do_parse!(s: parse_sql >> (Sql::Text(s), vec![]))
        ),
        (vec![], HashMap::new()), |mut acc: (Vec<Sql>, HashMap<SqlStatementAlias, SqlComposition>), item: (Sql, Vec<SqlStatementAlias>)| {
            let (item_sql, item_aliases) = item;

            acc.0.push(item_sql);

            for alias in item_aliases {
                let stmt_path = alias.path().unwrap();

                let alias_entry = acc.1.entry(alias).or_insert(SqlComposition::from_path(&stmt_path).unwrap());
            }

            acc
        }
    )
);

pub fn parse_template(input: &[u8], path: Option<PathBuf>) -> IResult<&[u8], SqlComposition> {
    let res = _parse_template(input);

    res.and_then(|(remaining, (chunks, aliases))| {
        Ok((remaining, (SqlComposition {
            path: path,
            stmt: Some(SqlStatement {
                template: String::from_utf8(input.to_vec()).unwrap(),
                chunks: chunks,
                ..Default::default()
            }),
            ..Default::default()
        })))
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

named!(_parse_macro_name<&[u8]>,
   delimited!(
       tag_s!(":"),
       take_until_s!("("),
       tag_s!("(")
   )
);

pub fn parse_expander_macro(input: &[u8]) -> IResult<&[u8], SqlComposition> {
    let name_res = _parse_macro_name(input);

    name_res.and_then(|(remaining, name)| {
            println!("name: {:?}, remaining: {:?}", String::from_utf8(name.to_vec()), String::from_utf8(remaining.to_vec()));
        _parse_macro_body(name).and_then(|(_r, (body, end_tag))| {
                //register the b as something processed in the parser
                Ok((body, SqlComposition{
                    name: Some(String::from_utf8(name.to_vec()).unwrap()),
                    ..Default::default()
                }))
        })
    })
}


named!(_parse_macro_body<(&[u8], &[u8])>,
   pair!(
       take_until_s!(")"),
       tag_s!(")")
   )
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
    use super::{ parse_bindvar, parse_sql, parse_sql_end, parse_expand, parse_template, SqlStatement, SqlComposition, SqlBinding, SqlEnding, SqlText, Sql };
    use std::path::{Path, PathBuf};

    fn simple_template_stmt() -> SqlComposition {
        SqlComposition{
            path: Some(PathBuf::from("src/tests/simple-template.tql")),
            stmt: Some(SqlStatement{
                template: "SELECT foo_id, bar FROM foo WHERE foo.bar = :bind(varname);\n".into(),
                chunks: vec![
                    Sql::Text(SqlText::from_utf8(b"SELECT foo_id, bar FROM foo WHERE foo.bar = ").unwrap()),
                    Sql::Binding(SqlBinding::from_utf8(b"varname").unwrap()),
                    Sql::Ending(SqlEnding::from_utf8(b";").unwrap()),
                ],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn include_template_stmt() -> SqlComposition {
        SqlComposition{
            path: Some(PathBuf::from("src/tests/include-template.tql")),
            stmt: Some(SqlStatement{
                template: "SELECT COUNT(foo_id)\nFROM (\n  :expand(<src/tests/simple-template.tql>)\n);\n".into(),
                chunks: vec![
                    Sql::Text(SqlText::from_utf8(b"SELECT COUNT(foo_id)\nFROM (\n  ").unwrap()),
                    Sql::SubStatement(SqlComposition{
                        path: Some(PathBuf::from("src/tests/simple-template.tql")),
                        stmt: Some(SqlStatement{
                            template: "SELECT foo_id, bar FROM foo WHERE foo.bar = :bind(varname);\n".into(),
                            chunks: vec![
                                Sql::Text(SqlText::from_utf8(b"SELECT foo_id, bar FROM foo WHERE foo.bar = ").unwrap()),
                                Sql::Binding(SqlBinding::from_utf8(b"varname").unwrap()),
                                Sql::Ending(SqlEnding::from_utf8(b";").unwrap()),
                            ],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    Sql::Text(SqlText::from_utf8(b"\n)").unwrap()),
                    Sql::Ending(SqlEnding::from_utf8(b";").unwrap()),
                ],
                ..Default::default()
            }),
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
    fn test_parse_include() {
        let input = b":expand(<src/tests/simple-template.tql>)blah blah blah";

        let out = parse_expand(input);

        let expected = Ok((&b"blah blah blah"[..],
                           simple_template_stmt() ));
        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_simple_template() {
        let input = "SELECT * FROM (:expand(<src/tests/simple-template.tql>)) WHERE name = ':bind(bindvar)';";

        let out = parse_template(input.as_bytes(), None);

        let expected = Ok((&b""[..],
                           SqlComposition {
                               stmt: Some(SqlStatement{
                                   template: input.to_string(),
                                   chunks: vec![
                                       Sql::Text(SqlText::from_utf8(b"SELECT * FROM (").unwrap()),
                                       Sql::SubStatement(simple_template_stmt()),
                                       Sql::Text(SqlText::from_utf8(b") WHERE name = ").unwrap()),
                                       Sql::Binding(SqlBinding::from_quoted_utf8(b"bindvar").unwrap()),
                                       Sql::Ending(SqlEnding::from_utf8(b";").unwrap())
                                   ],
                                   ..Default::default()
                               }),
                               ..Default::default()
                           }
                          ));

        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_include_template() {
        let input = "SELECT * FROM (:expand(<src/tests/include-template.tql>)) WHERE name = ':bind(bindvar)';";

        let out = parse_template(input.as_bytes(), None);

        let expected:Result<(&[u8], SqlComposition), nom::Err<&[u8]>> = Ok((&b""[..],
                           SqlComposition {
                               stmt: Some(SqlStatement{
                                   template: input.to_string(),
                                   chunks: vec![
                                       Sql::Text(SqlText::from_utf8(b"SELECT * FROM (").unwrap()),
                                       Sql::SubStatement(include_template_stmt()),
                                       Sql::Text(SqlText::from_utf8(b") WHERE name = ").unwrap()),
                                       Sql::Binding(SqlBinding::from_quoted_utf8(b"bindvar").unwrap()),
                                       Sql::Ending(SqlEnding::from_utf8(b";").unwrap())
                                   ],
                                   ..Default::default()
                               }),
                               ..Default::default()
                           }
                          ));

        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_file_template() {
        let stmt = SqlComposition::from_path(Path::new("src/tests/simple-template.tql")).unwrap();                                                                                                                  //TODO: this shouldn't have the extra \n at the end?
        let expected = simple_template_stmt();

        assert_eq!(stmt, expected);
    }

    #[test]
    fn test_parse_file_inclusive_template() {
        let stmt = SqlComposition::from_path(Path::new("src/tests/include-template.tql")).unwrap();                                                                                                                  //TODO: this shouldn't have the extra \n at the end?
        let expected = include_template_stmt();

        assert_eq!(stmt, expected);
    }
}
