use std::str;

use nom::IResult;
use std::io::prelude::*;
use std::fs::File;
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, PartialEq)]
pub struct SqlStatement {
    template: String,
    pub path: Option<PathBuf>,
    pub chunks: Vec<Sql>
}

impl SqlStatement {
    pub fn from_str(q: &str) -> Self {
        let (remaining, stmt) = parse_template(&q.as_bytes(), None).unwrap();

        if remaining.len() > 0 {
            panic!("found extra information: {}", String::from_utf8(remaining.to_vec()).unwrap());
        }

        stmt
    }

    pub fn from_path(path: &Path) -> ::std::io::Result<SqlStatement> {
        let mut f = File::open(path).unwrap();
        let mut s = String::new();

        let _res = f.read_to_string(&mut s);

        let (_remaining, stmt) = parse_template(&s.as_bytes(), Some(path.into())).unwrap();

        Ok(stmt)
    }

    pub fn from_utf8_path_name(vec: &[u8]) -> ::std::io::Result<SqlStatement> {
        //TODO: don't unwrap here
        let s = &std::str::from_utf8(vec).unwrap();
        let p = Path::new(s);

        Self::from_path(p)
    }

    fn push_chunk(&mut self, c: Sql) {
        self.chunks.push(c);
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
  SubStatement(SqlStatement),
  Ending(SqlEnding)
}

impl fmt::Display for Sql {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
      match self {
          Sql::Text(t) => write!(f, "{}", t)?,
          Sql::Binding(b) => write!(f, "{}", b)?,
          Sql::SubStatement(s) => write!(f, "{}", s)?,
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

named!(_parse_template<Vec<Sql>>,
       many1!(
        alt_complete!(
            do_parse!(e: parse_sql_end >> (Sql::Ending(e)))
            | do_parse!(i: parse_include >> (Sql::SubStatement(i)))
            | do_parse!(q: parse_quoted_bindvar >> (Sql::Binding(q)))
            | do_parse!(b: parse_bindvar >> (Sql::Binding(b)))
            | do_parse!(s: parse_sql >> (Sql::Text(s)))
        )
    )
);

pub fn parse_template(input: &[u8], path: Option<PathBuf>) -> IResult<&[u8], SqlStatement> {
    let res = _parse_template(input);

    res.and_then(|(remaining, chunks)| {
        Ok((remaining, SqlStatement {
            template: String::from_utf8(input.to_vec()).unwrap(),
            chunks: chunks,
            path: path,
            ..Default::default()
        }))
    })
}

named!(parse_include<SqlStatement>,
   map_res!(
       delimited!(
           tag_s!("::"),
           take_until_s!("::"),
           tag_s!("::")
       ),
       SqlStatement::from_utf8_path_name
   )
);

named!(parse_quoted_bindvar<SqlBinding>,
   map_res!(
       delimited!(
           tag_s!("':"),
           take_until_s!(":"),
           tag_s!(":'")
       ),
       SqlBinding::from_quoted_utf8
   )
);

named!(parse_bindvar<SqlBinding>,
   map_res!(
       delimited!(
           tag_s!(":"),
           take_until_s!(":"),
           tag_s!(":")
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
    use super::{ parse_bindvar, parse_sql, parse_sql_end, parse_include, parse_template, SqlStatement, SqlBinding, SqlEnding, SqlText, Sql };
    use std::path::{Path, PathBuf};

    fn simple_template_stmt() -> SqlStatement {
        SqlStatement{
            template: "SELECT foo_id, bar FROM foo WHERE foo.bar = :varname:;\n".into(),
            path: Some(PathBuf::from("src/tests/simple-template.tql")),
            chunks: vec![
                Sql::Text(SqlText::from_utf8(b"SELECT foo_id, bar FROM foo WHERE foo.bar = ").unwrap()),
                Sql::Binding(SqlBinding::from_utf8(b"varname").unwrap()),
                Sql::Ending(SqlEnding::from_utf8(b";").unwrap()),
            ],
            ..Default::default()
        }
    }

    fn include_template_stmt() -> SqlStatement {
        SqlStatement{
            template: "SELECT COUNT(foo_id)\nFROM (\n  ::src/tests/simple-template.tql::\n);\n".into(),
            path: Some(PathBuf::from("src/tests/include-template.tql")),
            chunks: vec![
                Sql::Text(SqlText::from_utf8(b"SELECT COUNT(foo_id)\nFROM (\n  ").unwrap()),
                Sql::SubStatement(SqlStatement{
                    template: "SELECT foo_id, bar FROM foo WHERE foo.bar = :varname:;\n".into(),
                    path: Some(PathBuf::from("src/tests/simple-template.tql")),
                    chunks: vec![
                        Sql::Text(SqlText::from_utf8(b"SELECT foo_id, bar FROM foo WHERE foo.bar = ").unwrap()),
                        Sql::Binding(SqlBinding::from_utf8(b"varname").unwrap()),
                        Sql::Ending(SqlEnding::from_utf8(b";").unwrap()),
                    ],
                    ..Default::default()
                }),
                Sql::Text(SqlText::from_utf8(b"\n)").unwrap()),
                Sql::Ending(SqlEnding::from_utf8(b";").unwrap()),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn test_parse_bindvar() {
        let input = b":varname:blah blah blah";

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
        let input = b"select * from foo where foo.bar = :varname:;";

        let out = parse_sql(input);

        let expected = Ok((&b":varname:;"[..], SqlText{ value: "select * from foo where foo.bar = ".into(), ..Default::default() } ));
        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_include() {
        let input = b"::src/tests/simple-template.tql::blah blah blah";

        let out = parse_include(input);

        let expected = Ok((&b"blah blah blah"[..],
                           simple_template_stmt() ));
        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_simple_template() {
        let input = "SELECT * FROM (::src/tests/simple-template.tql::) WHERE name = ':bindvar:';";

        let out = parse_template(input.as_bytes(), None);

        let expected = Ok((&b""[..],
                           SqlStatement{
                               template: input.to_string(),
                               chunks: vec![
                                 Sql::Text(SqlText::from_utf8(b"SELECT * FROM (").unwrap()),
                                 Sql::SubStatement(simple_template_stmt()),
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
        let input = "SELECT * FROM (::src/tests/include-template.tql::) WHERE name = ':bindvar:';";

        let out = parse_template(input.as_bytes(), None);

        let expected:Result<(&[u8], SqlStatement), nom::Err<&[u8]>> = Ok((&b""[..],
                           SqlStatement{
                               template: input.to_string(),
                               chunks: vec![
                                 Sql::Text(SqlText::from_utf8(b"SELECT * FROM (").unwrap()),
                                 Sql::SubStatement(include_template_stmt()),
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
        let stmt = SqlStatement::from_path(Path::new("src/tests/simple-template.tql")).unwrap();                                                                                                                  //TODO: this shouldn't have the extra \n at the end?
        let expected = simple_template_stmt();

        assert_eq!(stmt, expected);
    }

    #[test]
    fn test_parse_file_inclusive_template() {
        let stmt = SqlStatement::from_path(Path::new("src/tests/include-template.tql")).unwrap();                                                                                                                  //TODO: this shouldn't have the extra \n at the end?
        let expected = include_template_stmt();

        assert_eq!(stmt, expected);
    }
}
