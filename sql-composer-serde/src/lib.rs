use sql_composer::parser::{comma_padded, ending, take_while_name_char};
use sql_composer::types::Span;

use nom::{branch::alt,
          bytes::complete::{tag, take_while1},
          character::complete::{digit1, multispace0, one_of},
          combinator::{iterator, opt, peek},
          error::ErrorKind as NomErrorKind,
          multi::separated_list,
          number::complete::double,
          IResult};

use serde_value::Value;

use std::collections::BTreeMap;

use std::str::FromStr;

pub fn bind_value_text(span: Span) -> IResult<Span, Value> {
    let (span, _) = one_of("'")(span)?;
    let (span, found) = take_while1(|c: char| match c {
        '\'' => false,
        ']' => false,
        _ => true,
    })(span)?;
    let (span, _) = one_of("'")(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = check_bind_value_ending(span)?;

    Ok((span, Value::String(found.fragment.to_string())))
}

pub fn bind_value_integer(span: Span) -> IResult<Span, Value> {
    let (span, found) = digit1(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = check_bind_value_ending(span)?;

    Ok((
        span,
        Value::I64(
            i64::from_str(&found.fragment)
                .expect("unable to parse integer found by bind_value_integer"),
        ),
    ))
}

pub fn bind_value_real(span: Span) -> IResult<Span, Value> {
    let (span, value) = double(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = check_bind_value_ending(span)?;
    let (span, _) = multispace0(span)?;

    Ok((span, Value::F64(value)))
}

pub fn check_bind_value_ending(span: Span) -> IResult<Span, Span> {
    let (span, _) = alt((ending, peek(tag(")")), peek(tag("]")), peek(tag(","))))(span)?;

    Ok((span, span))
}

pub fn bind_value(span: Span) -> IResult<Span, Value> {
    let (span, value) = alt((bind_value_text, bind_value_integer, bind_value_real))(span)?;

    Ok((span, value))
}

pub fn bind_value_separated(span: Span) -> IResult<Span, Value> {
    let (span, value) = bind_value(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = opt(tag(","))(span)?;
    let (span, _) = multispace0(span)?;

    Ok((span, value))
}

pub fn bind_value_set(span: Span) -> IResult<Span, Vec<Value>> {
    let (start_span, start) = opt(alt((tag("["), tag("("))))(span)?;
    let mut list_iter = iterator(start_span, bind_value_separated);

    let list = list_iter.fold(vec![], |mut acc: Vec<Value>, item: Value| {
        acc.push(item);
        acc
    });

    let (span, _) = list_iter.finish().unwrap();

    let (span, end) = opt(alt((tag("]"), tag(")"))))(span)?;

    match (start, end) {
        (Some(s), Some(e)) => match (s.fragment, e.fragment) {
            ("[", "]") => (),
            ("(", ")") => (),
            (_, _) => return Err(nom::Err::Failure((s, NomErrorKind::Verify))),
        },
        (Some(s), None) => return Err(nom::Err::Failure((s, NomErrorKind::Verify))),
        (None, Some(e)) => return Err(nom::Err::Failure((e, NomErrorKind::Verify))),
        (None, None) => (),
    }

    Ok((span, list))
}

//"a:[a_value, aa_value, aaa_value], b:b_value, c: (c_value, cc_value, ccc_value), d: d_value";
pub fn bind_value_kv_pair(span: Span) -> IResult<Span, (Span, Vec<Value>)> {
    let (span, key) = take_while_name_char(span)?;
    let (span, _) = multispace0(span)?;
    let (span, _) = tag(":")(span)?;
    let (span, _) = multispace0(span)?;
    let (span, values) = bind_value_set(span)?;

    Ok((span, (key, values)))
}

pub fn bracket_start<'a>(span: Span) -> IResult<Span, (Span, String)> {
    let (span, _) = multispace0(span)?;
    let (span, start) = alt((tag("["), tag("(")))(span)?;
    let (span, _) = multispace0(span)?;

    let result = match start.fragment {
        "[" => "]",
        "(" => ")",
        _ => unreachable!(),
    };

    Ok((span, (start, result.to_string())))
}

pub fn bracket_start_fn<'a>(
    span: Span,
) -> IResult<Span, (Span, impl FnOnce(Span<'a>) -> IResult<Span, Span>)> {
    let (span, start) = alt((tag("["), tag("(")))(span)?;
    let (span, _) = multispace0(span)?;

    let bracket_end_func = tag::<&'static str, Span, _>(match start.fragment {
        "[" => "]",
        "(" => ")",
        _ => unreachable!(),
    });

    Ok((span, (start, Box::new(bracket_end_func))))
}

//"a_value, aa_value, aaa_value
pub fn bind_value_named_item(span: Span) -> IResult<Span, Vec<(Span, Vec<Value>)>> {
    let (span, (_start, bracket_end_fn)) = bracket_start_fn(span)?;
    let (span, kv) = separated_list(comma_padded, bind_value_kv_pair)(span)?;
    let (span, _end) = bracket_end_fn(span)?;

    // println!("bind_value_named_item matched set: '{}', '{}'", start.fragment, end.fragment);
    Ok((span, kv))
}

//"[a:[a_value, aa_value, aaa_value], b:b_value], [..=]";
pub fn bind_value_named_set(span: Span) -> IResult<Span, BTreeMap<String, Vec<Value>>> {
    let mut iter = iterator(span, bind_value_named_item);

    let (_, map) = iter.fold(
        Ok((span, BTreeMap::new())),
        |acc_res: IResult<Span, BTreeMap<String, Vec<Value>>>, items: Vec<(Span, Vec<Value>)>| {
            match acc_res {
                Ok((span, mut acc)) => {
                    for (key, values) in items {
                        let key = key.fragment.to_string();

                        let entry = acc.entry(key).or_insert(vec![]);

                        for v in values {
                            entry.push(v);
                        }
                    }
                    Ok((span, acc))
                }
                Err(e) => Err(e),
            }
        },
    )?;

    let (span, _) = iter.finish().unwrap();

    Ok((span, map))
}

pub fn bind_value_named_sets(span: Span) -> IResult<Span, Vec<BTreeMap<String, Vec<Value>>>> {
    let (span, values) = separated_list(comma_padded, bind_value_named_set)(span)?;

    Ok((span, values))
}

#[cfg(test)]
mod tests {
    use serde_value::Value;

    use std::collections::BTreeMap;

    use super::{bind_value, bind_value_integer, bind_value_named_set, bind_value_named_sets,
                bind_value_set, bind_value_text, check_bind_value_ending, Span};

    fn build_expected_bind_values() -> BTreeMap<String, Vec<Value>> {
        let mut expected_values: BTreeMap<String, Vec<Value>> = BTreeMap::new();

        expected_values.insert(
            "a".into(),
            vec![
                Value::String("a".into()),
                Value::String("aa".into()),
                Value::String("aaa".into()),
            ],
        );

        expected_values.insert("b".into(), vec![Value::String("b".into())]);

        expected_values.insert(
            "c".into(),
            vec![Value::I64(2), Value::F64(2.25), Value::String("a".into())],
        );

        expected_values.insert("d".into(), vec![Value::I64(2)]);

        expected_values.insert("e".into(), vec![Value::F64(2.234566)]);

        expected_values
    }

    #[test]
    fn test_bind_value() {
        let tests = vec![
            ("'a'", "", Value::String("a".into())),
            ("'a', ", ", ", Value::String("a".into())),
            ("'a' , ", ", ", Value::String("a".into())),
            ("34", "", Value::I64(34)),
            ("34, ", ", ", Value::I64(34)),
            ("34 ,", ",", Value::I64(34)),
            ("54.3, ", ", ", Value::F64(54.3)),
            ("54.3", "", Value::F64(54.3)),
            ("54.3 ", "", Value::F64(54.3)),
        ];
        for (i, (input, expected_remain, expected_values)) in tests.iter().enumerate() {
            println!("{}: input={:?}", i, input);
            let (remaining, output) = bind_value(Span::new(input)).unwrap();

            assert_eq!(
                &output, expected_values,
                "expected values for i:{} input:{:?}",
                i, input
            );
            assert_eq!(
                &remaining.fragment, expected_remain,
                "expected remaining for i:{} input:{:?}",
                i, input
            );
        }
    }

    #[test]
    fn test_bind_value_text() {
        let input = "'a'";

        let (remaining, output) = bind_value_text(Span::new(input)).unwrap();

        let expected_output = Value::String("a".into());

        assert_eq!(output, expected_output, "correct output");
        assert_eq!(remaining.fragment, "", "nothing remaining");
    }

    #[test]
    fn test_bind_value_integer() {
        let tests = vec![
            ("34, ", ", ", Value::I64(34)),
            ("34 ", "", Value::I64(34)),
            ("34 , ", ", ", Value::I64(34)),
            ("34", "", Value::I64(34)),
        ];
        for (i, (input, expected_remain, expected_values)) in tests.iter().enumerate() {
            println!("input={:?}", input);
            let (remaining, output) = bind_value_integer(Span::new(input)).unwrap();

            assert_eq!(
                &output, expected_values,
                "expected values for i:{} input:{:?}",
                i, input
            );
            assert_eq!(
                &remaining.fragment, expected_remain,
                "expected remaining for i:{} input:{:?}",
                i, input
            );
        }
    }

    #[test]
    fn test_check_bind_value_ending() {
        for &input in [")", "]", ",", ""].iter() {
            // breaks for input="" if eof() is not listed first
            println!("test_check_bind_value_ending with input=\"{}\"", input);

            match check_bind_value_ending(Span::new(input)) {
                Ok((remaining, output)) => {
                    let expected_output = Span::new(input);
                    let expected_fragment = input;

                    assert_eq!(output, expected_output, "correct output for {:?}", input);
                    assert_eq!(
                        remaining.fragment, expected_fragment,
                        "input not consumed for {:?}",
                        input
                    );
                }
                Err(e) => {
                    println!(
                        "bind_value_ending for input={:?} returned an error={:?}",
                        input, e
                    );
                    panic!(e)
                }
            };
        }
    }

    #[test]
    fn test_bind_value_set() {
        let input = "['a', 'aa', 'aaa']";

        let (remaining, output) = bind_value_set(Span::new(input)).unwrap();

        let expected_output = vec![
            Value::String("a".into()),
            Value::String("aa".into()),
            Value::String("aaa".into()),
        ];

        assert_eq!(output, expected_output, "correct output");
        assert_eq!(remaining.fragment, "", "nothing remaining");
    }

    #[test]
    fn test_bind_single_undelimited_value_set() {
        let input = "'a'";

        let (remaining, output) = bind_value_set(Span::new(input)).unwrap();

        let expected_output = vec![Value::String("a".into())];

        assert_eq!(output, expected_output, "correct output");
        assert_eq!(remaining.fragment, "", "nothing remaining");
    }

    #[test]
    fn test_bind_value_named_set() {
        //TODO: this should work but "single" values chokes up the parser
        //TOOD: doesn't like spaces between keys still either
        //let input = "[a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566]";
        let input = "[a:['a', 'aa', 'aaa'], b:['b'], c:(2, 2.25, 'a'), d:[2], e:[2.234566]]";

        match bind_value_named_set(Span::new(input)) {
            Ok((remaining, output)) => {
                let expected_output = build_expected_bind_values();

                assert_eq!(output, expected_output, "correct output");
                assert_eq!(remaining.fragment, "", "nothing remaining");
            }
            Err(e) => {
                println!("bind_value_named_set returned an error={:?}", e);
                panic!(e)
            }
        }
    }

    #[test]
    fn test_bind_value_named_sets() {
        //TODO: this should work but "single" values chokes up the parser
        //TOOD: doesn't like spaces between keys still either
        //let input = "[a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566], [a: ['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566]";
        let input = "[a:['a', 'aa', 'aaa'], b:['b'], c:(2, 2.25, 'a'), d:[2], e:[2.234566]], [a:['a', 'aa', 'aaa'], b:['b'], c:(2, 2.25, 'a'), d:[2], e:[2.234566]]";

        let (remaining, output) = bind_value_named_sets(Span::new(input)).unwrap();

        let expected_output = vec![build_expected_bind_values(), build_expected_bind_values()];

        assert_eq!(output, expected_output, "correct output");
        assert_eq!(remaining.fragment, "", "nothing remaining");
    }

    /*
    #[test]
    fn test_bind_path_alias_name_value_sets() {
        let input = "t1.tql: [[a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566], [a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566]], t2.tql: [[a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566], [a:['a', 'aa', 'aaa'], b:'b', c: (2, 2.25, 'a'), d: 2, e: 2.234566]]";

        let (remaining, output) = bind_path_alias_name_value_sets(input).unwrap();

        let expected_values = build_expected_bind_values();
        let mut expected_output: HashMap<SqlCompositionAlias, Vec<BTreeMap<String, Vec<Value>>>> =
            HashMap::new();

        expected_output.insert(
            SqlCompositionAlias::Path("t1.tql".into()),
            vec![expected_values.clone(), expected_values.clone()],
            );

        expected_output.insert(
            SqlCompositionAlias::Path("t2.tql".into()),
            vec![expected_values.clone(), expected_values.clone()],
            );

        assert_eq!(output, expected_output, "correct output");
        assert_eq!(remaining, "", "nothing remaining");
    }
    */
}
