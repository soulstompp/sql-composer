use std::str;

use std::io;
use std::io::prelude::*;
use std::fs::File;
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

        let (remainder, chunks) = template(&s.as_bytes()).unwrap();

        let mut stmt = SQLStatement{ template: s.to_string(), chunks: chunks };

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

#[derive(Debug, PartialEq)]
pub enum SQL {
  Text(SQLText),
  Binding(SQLBinding),
  SubStatement(SQLStatement),
  Ending(SQLEnding)
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

named!(template<Vec<SQL>>,
    many1!(
        alt_complete!(
            do_parse!(e: sql_end >> (SQL::Ending(e)))
            | do_parse!(i: include >> (SQL::SubStatement(i)))
            | do_parse!(b: bindvar >> (SQL::Binding(b)))
            | do_parse!(s: sql >> (SQL::Text(s)))
        )
    )
);

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

    #[test]
    fn parse_include() {
        let input = b"::src/tests/simple-template.tql::blah blah blah";

        let out = include(input);

        let expected = Ok((&b"blah blah blah"[..],
                           simple_template_stmt() ));
        assert_eq!(out, expected);
    }

    #[test]
    fn parse_template() {
        let input = b"select * from (::src/tests/simple-template.tql::) where name = ':bindvar:';";

        let out = template(input);

        let expected = Ok((&b""[..],
                           vec![
                             SQL::Text(SQLText::from_utf8(b"select * from (").unwrap()),
                             SQL::SubStatement(simple_template_stmt()),
                             SQL::Text(SQLText::from_utf8(b") where name = '").unwrap()),
                             SQL::Binding(SQLBinding::from_utf8(b"bindvar").unwrap()),
                             SQL::Text(SQLText::from_utf8(b"'").unwrap()),
                             SQL::Ending(SQLEnding::from_utf8(b";").unwrap())
                           ]));

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
