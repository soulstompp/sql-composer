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
    chunks: Vec<SQL>
}

impl SQLStatement {
    fn from_path(f: &Path) -> ::std::io::Result<SQLStatement> {
        let mut f = File::open(f).unwrap();
        let mut s = String::new();

        f.read_to_string(&mut s);

        let (remaining, stmt) = template(&s.as_bytes()).unwrap();

        Ok(stmt)
    }

    fn from_utf8(vec: &[u8]) -> ::std::io::Result<SQLStatement> {
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
    name: String
}

impl SQLBinding {
    fn from_utf8(vec: &[u8]) -> Result<Self, ::std::string::FromUtf8Error> {
        let s = String::from_utf8(vec.to_vec())?;

        Ok(Self{name: s})
    }
}

impl fmt::Display for SQLBinding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

named!(_template<Vec<SQL>>,
       many1!(
        alt_complete!(
            do_parse!(e: sql_end >> (SQL::Ending(e)))
            | do_parse!(i: include >> (SQL::SubStatement(i)))
            | do_parse!(b: bindvar >> (SQL::Binding(b)))
            | do_parse!(s: sql >> (SQL::Text(s)))
        )
    )
);

fn template(input: &[u8]) -> IResult<&[u8], SQLStatement> {
    let res = _template(input);

    res.and_then(|(remaining, chunks)| {
        Ok((remaining, SQLStatement {
            template: String::from_utf8(input.to_vec()).unwrap(),
            chunks: chunks
        }))
    })
}

named!(include<SQLStatement>,
   map_res!(
       delimited!(
           tag_s!("::"),
           take_until_s!("::"),
           tag_s!("::")
       ),
       SQLStatement::from_utf8
   )
);

named!(bindvar<SQLBinding>,
   map_res!(
       delimited!(
           tag_s!(":"),
           take_until_s!(":"),
           tag_s!(":")
       ),
       SQLBinding::from_utf8
   )
);

named!(sql<SQLText>,
   map_res!(
       alt_complete!(
             take_until_s!(":")
           | take_until_s!(";")
       ),
       SQLText::from_utf8
   )
);

named!(sql_end<SQLEnding>,
   map_res!(
       tag_s!(";"),
       SQLEnding::from_utf8
   )
);

#[cfg(test)]
mod tests {
    use nom::IResult;
    use super::{ bindvar, sql, sql_end, include, template, SQLStatement, SQLBinding, SQLEnding, SQLText, SQL };
    use std::str;
    use std::path::Path;

    fn simple_template_stmt() -> SQLStatement {
        SQLStatement{
            template: "SELECT foo_id, bar FROM foo WHERE foo.bar = :varname:;\n".into(),
            chunks: vec![
                SQL::Text(SQLText::from_utf8(b"SELECT foo_id, bar FROM foo WHERE foo.bar = ").unwrap()),
                SQL::Binding(SQLBinding::from_utf8(b"varname").unwrap()),
                SQL::Ending(SQLEnding::from_utf8(b";").unwrap())
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
                    ]
                }),
                SQL::Text(SQLText::from_utf8(b"\n)").unwrap()),
                SQL::Ending(SQLEnding::from_utf8(b";").unwrap()),
            ]
        }
    }

    #[test]
    fn parse_bindvar() {
        let input = b":varname:blah blah blah";

        let out = bindvar(input);

        let expected = Ok((&b"blah blah blah"[..], SQLBinding{ name: "varname".into() }));
        assert_eq!(out, expected);
    }

    #[test]
    fn parse_sql_end() {
        let input = b";blah blah blah";

        let expected = Ok((&b"blah blah blah"[..], SQLEnding{ value: ";".into() }));

        let out = sql_end(input);

        assert_eq!(out, expected);

    }

    #[test]
    fn parse_sql_until_path() {
        let input = b"select * from foo where foo.bar = :varname:;";

        let out = sql(input);

        let expected = Ok((&b":varname:;"[..], SQLText{ value: "select * from foo where foo.bar = ".into() } ));
        assert_eq!(out, expected);
    }

    #[test]
    fn parse_include() {
        let input = b"::src/tests/simple-template.tql::blah blah blah";

        let out = include(input);

        let expected = Ok((&b"blah blah blah"[..],
                           simple_template_stmt() ));
        assert_eq!(out, expected);
    }

    #[test]
    fn parse_simple_template() {
        let input = "SELECT * FROM (::src/tests/simple-template.tql::) WHERE name = ':bindvar:';";

        let out = template(input.as_bytes());

        let expected = Ok((&b""[..],
                           SQLStatement{
                               template: input.to_string(),
                               chunks: vec![
                                 SQL::Text(SQLText::from_utf8(b"SELECT * FROM (").unwrap()),
                                 SQL::SubStatement(simple_template_stmt()),
                                 SQL::Text(SQLText::from_utf8(b") WHERE name = '").unwrap()),
                                 SQL::Binding(SQLBinding::from_utf8(b"bindvar").unwrap()),
                                 SQL::Text(SQLText::from_utf8(b"'").unwrap()),
                                 SQL::Ending(SQLEnding::from_utf8(b";").unwrap())
                               ]
                           }
                          ));

        assert_eq!(out, expected);
    }

    #[test]
    fn parse_include_template() {
        let input = "SELECT * FROM (::src/tests/include-template.tql::) WHERE name = ':bindvar:';";

        let out = template(input.as_bytes());

        let expected:Result<(&[u8], SQLStatement), nom::Err<&[u8]>> = Ok((&b""[..],
                           SQLStatement{
                               template: input.to_string(),
                               chunks: vec![
                                 SQL::Text(SQLText::from_utf8(b"SELECT * FROM (").unwrap()),
                                 SQL::SubStatement(include_template_stmt()),
                                 SQL::Text(SQLText::from_utf8(b") WHERE name = '").unwrap()),
                                 SQL::Binding(SQLBinding::from_utf8(b"bindvar").unwrap()),
                                 SQL::Text(SQLText::from_utf8(b"'").unwrap()),
                                 SQL::Ending(SQLEnding::from_utf8(b";").unwrap())
                               ]
                           }
                          ));

        assert_eq!(out, expected);
    }

    #[test]
    fn parse_file_template() {
        let stmt = SQLStatement::from_path(Path::new("src/tests/simple-template.tql")).unwrap();                                                                                                                  //TODO: this shouldn't have the extra \n at the end?
        let expected = simple_template_stmt();

        println!("found stmt: {:?}", stmt);

        assert_eq!(stmt, expected);
    }
}
