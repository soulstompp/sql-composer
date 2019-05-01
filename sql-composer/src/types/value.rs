use super::Null;

use chrono::prelude::*;

//borrowed from rusqlite's Value type
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// The value is a `NULL` value.
    Null,
    /// The value is a signed integer.
    Integer(i64),
    /// The value is a floating point number.
    Real(f64),
    /// The value is a text string.
    Text(String),
    /// The value is a blob of data
    Blob(Vec<u8>),
}

pub trait ToValue {
    fn to_value(&self) -> Result<Value, ()>;

    fn to_sql_text(&self) -> Result<String, ()> {
        let value = self.to_value()?;

        Ok(match value {
            Value::Integer(i) => i.to_string(),
            Value::Real(f) => f.to_string(),
            Value::Text(s) => format!("'{}'", s.to_string()),
            Value::Blob(b) => format!("'{}'", String::from_utf8(b.to_vec()).unwrap()),
            Value::Null => format!("NULL"),
        })
    }
}

impl ToValue for Null {
    fn to_value(&self) -> Result<Value, ()> {
        Ok(Value::Null)
    }
}

impl ToValue for bool {
    fn to_value(&self) -> Result<Value, ()> {
        Ok(Value::Integer(*self as i64))
    }
}

impl ToValue for isize {
    fn to_value(&self) -> Result<Value, ()> {
        Ok(Value::Integer(*self as i64))
    }
}

macro_rules! from_optional_value(
    ($t: ty, $b: block) => (
        impl ToValue for $t {
            fn to_value(&self) -> Result<Value, ()> {
                $b
            }
        }

        impl ToValue for Option<$t> {
            fn to_value(&self) -> Result<Value, ()> {
                match *self {
                    Some(v) => v.to_value()?,
                    None => Value::Null
                }
            }
        }
    )
);

macro_rules! from_i64(
    ($t:ty) => (
        impl ToValue for $t {
            fn to_value(&self) -> Result<Value, ()> {
                Ok(Value::Integer(i64::from(*self)))
            }
        }
    )
);

from_i64!(i8);
from_i64!(i16);
from_i64!(i32);
from_i64!(u8);
from_i64!(u16);
from_i64!(u32);

impl ToValue for i64 {
    fn to_value(&self) -> Result<Value, ()> {
        Ok(Value::Integer(*self))
    }
}

impl ToValue for f64 {
    fn to_value(&self) -> Result<Value, ()> {
        Ok(Value::Real(*self))
    }
}

impl ToValue for String {
    fn to_value(&self) -> Result<Value, ()> {
        Ok(Value::Text(self.to_string()))
    }
}

impl ToValue for &str {
    fn to_value(&self) -> Result<Value, ()> {
        Ok(Value::Text(self.to_string()))
    }
}

impl ToValue for Vec<u8> {
    fn to_value(&self) -> Result<Value, ()> {
        Ok(Value::Blob(self.to_vec()))
    }
}

impl<Tz: TimeZone> ToValue for DateTime<Tz> {
    fn to_value(&self) -> Result<Value, ()> {
        let utc = self.with_timezone(&Utc).format("%Y-%m-%dT%H:%M:%S%.f");

        Ok(Value::Text(utc.to_string()))
    }
}

macro_rules! from_nullable(
    ($t: ty) => (
        impl ToValue for Option<$t> {
            fn to_value(&self) -> Result<Value, ()> {
                match self {
                    Some(v) => v.to_value(),
                    None => Ok(Value::Null)
                }
            }
        }
    )
);

from_nullable!(i8);
from_nullable!(i16);
from_nullable!(i32);
from_nullable!(u8);
from_nullable!(u16);
from_nullable!(u32);
from_nullable!(bool);
from_nullable!(isize);
from_nullable!(i64);
from_nullable!(f64);
from_nullable!(String);
from_nullable!(Vec<u8>);

#[derive(Clone, Debug)]
pub struct Rows {
    rows:         Vec<Row>,
    column_names: Vec<String>,
}

impl Rows {
    pub fn new(cn: Vec<String>) -> Self {
        Rows {
            rows:         vec![],
            column_names: cn,
        }
    }

    pub fn push_row(&mut self, r: Row) -> Result<(), ()> {
        self.rows.push(r);

        Ok(())
    }

    pub fn rows(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }

    pub fn grid(&self) -> Vec<Vec<&Value>> {
        self.rows().fold(Vec::new(), |mut acc, r| {
            let row = r.columns().fold(Vec::new(), |mut racc, c| {
                racc.push(&c.value);

                racc
            });

            acc.push(row);

            acc
        })
    }
}

#[derive(Clone, Debug)]
pub struct Row {
    columns: Vec<Column>,
}

impl Row {
    pub fn new(cn: Vec<String>) -> Self {
        Row { columns: vec![] }
    }

    pub fn push_column(&mut self, c: Column) -> Result<(), ()> {
        self.columns.push(c);

        Ok(())
    }

    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter()
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    value: Value,
}

impl Column {
    pub fn new(v: Value) -> Self {
        Column { value: v }
    }

    pub fn value(&self) -> Value {
        self.value.clone()
    }
}
