use std::str;

named!(template<Vec<&str>>,
    many1!(
        alt_complete!(
              sql_end
            | include
            | bindvar
            | sql
        )
    )
);

named!(include<&str>,
   map_res!(
       delimited!(
           tag_s!("::"),
           take_until_s!("::"),
           tag_s!("::")
       ),
       &::std::str::from_utf8
   )
);

named!(bindvar<&str>,
   map_res!(
       delimited!(
           tag_s!(":"),
           take_until_s!(":"),
           tag_s!(":")
       ),
       &::std::str::from_utf8
   )
);

named!(sql<&str>,
   map_res!(
       alt_complete!(
             take_until_s!(":")
           | take_until_s!(";")
       ),
       &::std::str::from_utf8
   )
);

named!(sql_end<&str>,
   map_res!(
       tag_s!(";"),
       &::std::str::from_utf8
   )
);

#[cfg(test)]
mod tests {
    use nom::IResult;
    use super::{bindvar, sql, sql_end, include, template};
    use std::str;

    #[test]
    fn parse_bindvar() {
        let input = b":varname:blah blah blah";

        let out = bindvar(input);

        let expected = Ok((&b"blah blah blah"[..], "varname"));
        assert_eq!(out, expected);
    }

    #[test]
    fn parse_sql_end() {
        let input = b";blah blah blah";

        let expected = Ok((&b"blah blah blah"[..], ";"));

        let out = sql_end(input);

        assert_eq!(out, expected);

    }

    #[test]
    fn parse_sql_until_path() {
        let input = b"select * from foo where foo.bar = :varname:;";

        let out = sql(input);

        let expected = Ok((&b":varname:;"[..], "select * from foo where foo.bar = "));
        assert_eq!(out, expected);
    }

    #[test]
    fn parse_include() {
        let input = b"::path::blah blah blah";

        let out = include(input);

        let expected = Ok((&b"blah blah blah"[..], "path"));
        assert_eq!(out, expected);
    }

    #[test]
    fn parse_template() {
        let input = b"select * from (::path::) where name = ':bindvar:';";

        let out = template(input);

        let expected = Ok((&b""[..], vec!["select * from (", "path", ") where name = '", "bindvar", "'", ";"]));
        assert_eq!(out, expected);
    }
}
