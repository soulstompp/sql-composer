use std::str;

use nom::IResult;
use std::io;
use std::io::prelude::*;
use std::fs::File;
use std::fmt;
use std::path::Path;

#[derive(Debug, Default, PartialEq)]
pub struct SQLStatement {
    template: String,
    pub chunks: Vec<SQL>
}

impl SQLStatement {
    fn from_str(q: &str) -> Self {
        let (remaining, stmt) = parse_template(&q.as_bytes()).unwrap();
        stmt
    }

    fn from_path(f: &Path) -> ::std::io::Result<SQLStatement> {
        let mut f = File::open(f).unwrap();
        let mut s = String::new();

        f.read_to_string(&mut s);

        let (remaining, stmt) = parse_template(&s.as_bytes()).unwrap();

        Ok(stmt)
    }

    fn from_utf8_path_name(vec: &[u8]) -> ::std::io::Result<SQLStatement> {
        //TODO: don't unwrap here
        let s = &std::str::from_utf8(vec).unwrap();
        let p = Path::new(s);

        Self::from_path(p)
    }

    fn push_chunk(&mut self, c: SQL) {
        self.chunks.push(c);
    }
}

impl fmt::Display for SQLStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

      for c in self.chunks.iter() {
          write!(f, "{}", c)?;
      }

      write!(f, "")
    }
}

#[derive(Debug, PartialEq)]
pub enum SQL {
  Text(SQLText),
  Binding(SQLBinding),
  SubStatement(SQLStatement),
  Ending(SQLEnding)
}

impl fmt::Display for SQL {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
      match self {
          SQL::Text(t) => write!(f, "{}", t)?,
          SQL::Binding(b) => write!(f, "{}", b)?,
          SQL::SubStatement(s) => write!(f, "{}", s)?,
          SQL::Ending(e) => write!(f, "{}", e)?
      }

      write!(f, "")
    }
}

#[derive(Debug, PartialEq)]
pub struct SQLEnding {
    value: String
}

impl SQLEnding {
    fn from_utf8(vec: &[u8]) -> Result<Self, ::std::string::FromUtf8Error> {
        let s = String::from_utf8(vec.to_vec())?;

        Ok(Self{value: s})
    }
}

impl fmt::Display for SQLEnding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, PartialEq)]
pub struct SQLText {
    value: String,
}

impl SQLText {
    fn from_utf8(vec: &[u8]) -> Result<Self, ::std::string::FromUtf8Error> {
        let s = String::from_utf8(vec.to_vec())?;

        Ok(Self{value: s})
    }
}

impl fmt::Display for SQLText {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, PartialEq)]
pub struct SQLBinding {
    pub name: String,
    pub quoted: bool
}

impl SQLBinding {
    fn from_utf8(vec: &[u8]) -> Result<Self, ::std::string::FromUtf8Error> {
        let s = String::from_utf8(vec.to_vec())?;

        println!("found unquoted bounded!");
        Ok(Self{ name: s, quoted: false })
    }

    fn from_quoted_utf8(vec: &[u8]) -> Result<Self, ::std::string::FromUtf8Error> {
        let s = String::from_utf8(vec.to_vec())?;

        println!("found quoted bounded!");
        Ok(Self{ name: s, quoted: true })
    }
}

impl fmt::Display for SQLBinding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

named!(_parse_template<Vec<SQL>>,
       many1!(
        alt_complete!(
            do_parse!(e: parse_sql_end >> (SQL::Ending(e)))
            | do_parse!(i: parse_include >> (SQL::SubStatement(i)))
            | do_parse!(q: parse_quoted_bindvar >> (SQL::Binding(q)))
            | do_parse!(b: parse_bindvar >> (SQL::Binding(b)))
            | do_parse!(s: parse_sql >> (SQL::Text(s)))
        )
    )
);

pub fn parse_template(input: &[u8]) -> IResult<&[u8], SQLStatement> {
    let res = _parse_template(input);

    res.and_then(|(remaining, chunks)| {
        Ok((remaining, SQLStatement {
            template: String::from_utf8(input.to_vec()).unwrap(),
            chunks: chunks
        }))
    })
}

named!(parse_include<SQLStatement>,
   map_res!(
       delimited!(
           tag_s!("::"),
           take_until_s!("::"),
           tag_s!("::")
       ),
       SQLStatement::from_utf8_path_name
   )
);

named!(parse_quoted_bindvar<SQLBinding>,
   map_res!(
       delimited!(
           tag_s!("':"),
           take_until_s!(":"),
           tag_s!(":'")
       ),
       SQLBinding::from_quoted_utf8
   )
);

named!(parse_bindvar<SQLBinding>,
   map_res!(
       delimited!(
           tag_s!(":"),
           take_until_s!(":"),
           tag_s!(":")
       ),
       SQLBinding::from_utf8
   )
);

named!(parse_sql<SQLText>,
   map_res!(
       alt_complete!(
             take_until_s!("::")
           | take_until_s!("':")
           | take_until_s!(":")
           | take_until_s!(";")
           | take!(1)
       ),
       SQLText::from_utf8
   )
);

named!(parse_sql_end<SQLEnding>,
   map_res!(
       tag_s!(";"),
       SQLEnding::from_utf8
   )
);

#[cfg(test)]
mod tests {
    use nom::IResult;
    use super::{ parse_bindvar, parse_sql, parse_sql_end, parse_include, parse_template, SQLStatement, SQLBinding, SQLEnding, SQLText, SQL };
    use std::str;
    use std::path::Path;

    fn simple_template_stmt() -> SQLStatement {
        SQLStatement{
            template: "SELECT foo_id, bar FROM foo WHERE foo.bar = :varname:;\n".into(),
            chunks: vec![
                SQL::Text(SQLText::from_utf8(b"SELECT foo_id, bar FROM foo WHERE foo.bar = ").unwrap()),
                SQL::Binding(SQLBinding::from_utf8(b"varname").unwrap()),
                SQL::Ending(SQLEnding::from_utf8(b";").unwrap()),
                SQL::Text(SQLText::from_utf8(b"\n").unwrap())

            ]
        }
    }

    fn include_template_stmt() -> SQLStatement {
        SQLStatement{
            template: "SELECT COUNT(foo_id)\nFROM (\n  ::src/tests/simple-template.tql::\n);\n".into(),
            chunks: vec![
                SQL::Text(SQLText::from_utf8(b"SELECT COUNT(foo_id)\nFROM (\n  ").unwrap()),
                SQL::SubStatement(SQLStatement{
                    template: "SELECT foo_id, bar FROM foo WHERE foo.bar = :varname:;\n".into(),
                    chunks: vec![
                        SQL::Text(SQLText::from_utf8(b"SELECT foo_id, bar FROM foo WHERE foo.bar = ").unwrap()),
                        SQL::Binding(SQLBinding::from_utf8(b"varname").unwrap()),
                        SQL::Ending(SQLEnding::from_utf8(b";").unwrap()),
                        SQL::Text(SQLText::from_utf8(b"\n").unwrap()),
                    ]
                }),
                SQL::Text(SQLText::from_utf8(b"\n)").unwrap()),
                SQL::Ending(SQLEnding::from_utf8(b";").unwrap()),
                SQL::Text(SQLText::from_utf8(b"\n").unwrap()),
            ]
        }
    }

    #[test]
    fn test_parse_bindvar() {
        let input = b":varname:blah blah blah";

        let out = parse_bindvar(input);

        let expected = Ok((&b"blah blah blah"[..], SQLBinding{ name: "varname".into(), quoted: false  }));
        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_sql_end() {
        let input = b";blah blah blah";

        let expected = Ok((&b"blah blah blah"[..], SQLEnding{ value: ";".into() }));

        let out = parse_sql_end(input);

        assert_eq!(out, expected);

    }

    #[test]
    fn parse_sql_until_path() {
        let input = b"select * from foo where foo.bar = :varname:;";

        let out = parse_sql(input);

        let expected = Ok((&b":varname:;"[..], SQLText{ value: "select * from foo where foo.bar = ".into() } ));
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

        let out = parse_template(input.as_bytes());

        let expected = Ok((&b""[..],
                           SQLStatement{
                               template: input.to_string(),
                               chunks: vec![
                                 SQL::Text(SQLText::from_utf8(b"SELECT * FROM (").unwrap()),
                                 SQL::SubStatement(simple_template_stmt()),
                                 SQL::Text(SQLText::from_utf8(b") WHERE name = ").unwrap()),
                                 SQL::Binding(SQLBinding::from_quoted_utf8(b"bindvar").unwrap()),
                                 SQL::Ending(SQLEnding::from_utf8(b";").unwrap())
                               ]
                           }
                          ));

        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_include_template() {
        let input = "SELECT * FROM (::src/tests/include-template.tql::) WHERE name = ':bindvar:';";

        let out = parse_template(input.as_bytes());

        let expected:Result<(&[u8], SQLStatement), nom::Err<&[u8]>> = Ok((&b""[..],
                           SQLStatement{
                               template: input.to_string(),
                               chunks: vec![
                                 SQL::Text(SQLText::from_utf8(b"SELECT * FROM (").unwrap()),
                                 SQL::SubStatement(include_template_stmt()),
                                 SQL::Text(SQLText::from_utf8(b") WHERE name = ").unwrap()),
                                 SQL::Binding(SQLBinding::from_quoted_utf8(b"bindvar").unwrap()),
                                 SQL::Ending(SQLEnding::from_utf8(b";").unwrap())
                               ]
                           }
                          ));

        assert_eq!(out, expected);
    }

    #[test]
    fn test_parse_file_template() {
        let stmt = SQLStatement::from_path(Path::new("src/tests/simple-template.tql")).unwrap();                                                                                                                  //TODO: this shouldn't have the extra \n at the end?
        let expected = simple_template_stmt();

        //println!("found stmt: {:?}", stmt);

        assert_eq!(stmt, expected);
    }
}
