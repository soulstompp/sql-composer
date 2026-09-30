//! Rows written straight into a COPY buffer, in Postgres's text or binary COPY format.

use crate::calendar;
use crate::world::{
    BuilderOut, CollectionOut, InvOut, LineOut, ManifestOut, NestOut, PurchaseOut, SetOut, Stamp,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Text,
    Binary,
}

impl Format {
    pub fn parse(s: &str) -> Result<Format, String> {
        match s {
            "text" => Ok(Format::Text),
            "binary" => Ok(Format::Binary),
            other => Err(format!("unknown COPY format `{other}`")),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Format::Text => "text",
            Format::Binary => "binary",
        }
    }
}

const BINARY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";

/// A COPY payload under construction. The buffer is reused between batches.
pub struct Enc {
    pub fmt: Format,
    pub buf: Vec<u8>,
    pub rows: u64,
    col: u16,
}

impl Enc {
    pub fn new(fmt: Format) -> Enc {
        let mut e = Enc {
            fmt,
            buf: Vec::with_capacity(1 << 20),
            rows: 0,
            col: 0,
        };
        e.reset();
        e
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.rows = 0;
        if self.fmt == Format::Binary {
            self.buf.extend_from_slice(BINARY_HEADER);
        }
    }

    /// Closes the payload: the binary trailer, nothing for text.
    pub fn finish(&mut self) {
        if self.fmt == Format::Binary {
            self.buf.extend_from_slice(&(-1i16).to_be_bytes());
        }
    }

    fn begin(&mut self, ncols: i16) {
        self.col = 0;
        if self.fmt == Format::Binary {
            self.buf.extend_from_slice(&ncols.to_be_bytes());
        }
    }

    fn sep(&mut self) {
        if self.fmt == Format::Text && self.col > 0 {
            self.buf.push(b'\t');
        }
        self.col += 1;
    }

    fn end(&mut self) {
        if self.fmt == Format::Text {
            self.buf.push(b'\n');
        }
        self.rows += 1;
    }

    fn null(&mut self) {
        self.sep();
        match self.fmt {
            Format::Text => self.buf.extend_from_slice(b"\\N"),
            Format::Binary => self.buf.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }

    fn i32(&mut self, v: i32) {
        self.sep();
        match self.fmt {
            Format::Text => {
                let mut b = [0u8; 12];
                self.buf.extend_from_slice(itoa(i64::from(v), &mut b));
            }
            Format::Binary => {
                self.buf.extend_from_slice(&4i32.to_be_bytes());
                self.buf.extend_from_slice(&v.to_be_bytes());
            }
        }
    }

    fn opt_i32(&mut self, v: Option<i32>) {
        match v {
            Some(v) => self.i32(v),
            None => self.null(),
        }
    }

    fn i64(&mut self, v: i64) {
        self.sep();
        match self.fmt {
            Format::Text => {
                let mut b = [0u8; 24];
                self.buf.extend_from_slice(itoa(v, &mut b));
            }
            Format::Binary => {
                self.buf.extend_from_slice(&8i32.to_be_bytes());
                self.buf.extend_from_slice(&v.to_be_bytes());
            }
        }
    }

    fn bool(&mut self, v: bool) {
        self.sep();
        match self.fmt {
            Format::Text => self.buf.push(if v { b't' } else { b'f' }),
            Format::Binary => {
                self.buf.extend_from_slice(&1i32.to_be_bytes());
                self.buf.push(u8::from(v));
            }
        }
    }

    fn str(&mut self, s: &str) {
        self.sep();
        match self.fmt {
            Format::Text => {
                for &b in s.as_bytes() {
                    match b {
                        b'\\' => self.buf.extend_from_slice(b"\\\\"),
                        b'\t' => self.buf.extend_from_slice(b"\\t"),
                        b'\n' => self.buf.extend_from_slice(b"\\n"),
                        b'\r' => self.buf.extend_from_slice(b"\\r"),
                        _ => self.buf.push(b),
                    }
                }
            }
            Format::Binary => {
                self.buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                self.buf.extend_from_slice(s.as_bytes());
            }
        }
    }

    fn opt_str(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.str(s),
            None => self.null(),
        }
    }

    fn stamp(&mut self, s: &Stamp) {
        match (self.fmt, s) {
            (Format::Text, Stamp::At { t, offset_min }) => {
                self.str(&calendar::render_with_offset(*t, *offset_min))
            }
            (Format::Text, Stamp::Infinity) => self.str("infinity"),
            (Format::Binary, Stamp::At { t, .. }) => self.i64(calendar::pg_micros(*t)),
            (Format::Binary, Stamp::Infinity) => self.i64(i64::MAX),
        }
    }

    pub fn set(&mut self, r: &SetOut) {
        self.begin(5);
        self.str(&r.set_num);
        self.str(&r.name);
        self.opt_i32(r.year);
        self.opt_i32(r.theme_id);
        self.opt_i32(r.num_parts);
        self.end();
    }

    pub fn inventory(&mut self, r: &InvOut) {
        self.begin(3);
        self.i32(r.id);
        self.i32(r.version);
        self.str(&r.set_num);
        self.end();
    }

    pub fn line(&mut self, r: &LineOut, pool: &[String]) {
        self.begin(5);
        self.i32(r.inventory_id);
        self.str(&pool[r.part as usize]);
        self.i32(r.color_id);
        self.i32(r.quantity);
        self.bool(r.is_spare);
        self.end();
    }

    pub fn nest(&mut self, r: &NestOut) {
        self.begin(3);
        self.i32(r.inventory_id);
        self.str(&r.set_num);
        self.i32(r.quantity);
        self.end();
    }

    pub fn builder(&mut self, r: &BuilderOut) {
        self.begin(3);
        self.i32(r.builder_id);
        self.str(&r.name);
        self.str(r.home_zone);
        self.end();
    }

    pub fn collection(&mut self, r: &CollectionOut) {
        self.begin(6);
        self.i32(r.builder_id);
        self.i32(r.row_no);
        self.str(&r.set_num);
        self.opt_str(r.typed_set_num.as_deref());
        self.opt_str(r.typed_name.as_deref());
        self.i32(r.quantity);
        self.end();
    }

    pub fn purchase(&mut self, r: &PurchaseOut) {
        self.begin(8);
        self.i64(r.purchase_id);
        self.i32(r.builder_id);
        self.i32(r.row_no);
        self.str(&r.set_num);
        self.str(r.store);
        self.stamp(&r.ordered_at);
        self.str(&r.ordered_local);
        self.stamp(&r.delivered_at);
        self.end();
    }

    pub fn manifest(&mut self, r: &ManifestOut) {
        self.begin(8);
        self.str(&r.trap.to_string());
        self.str(r.origin.name());
        self.str(r.table);
        self.str(&r.row_key);
        self.str(&r.phase);
        self.i64(r.wave);
        self.opt_str(r.socket.as_deref());
        self.str(&r.detail);
        self.end();
    }

    /// A reference table row: every column text or int4, as the reference tables hold.
    pub fn reference_row(&mut self, cols: &[Cell<'_>]) {
        self.begin(cols.len() as i16);
        for c in cols {
            match c {
                Cell::Int(v) => self.i32(*v),
                Cell::OptInt(v) => self.opt_i32(*v),
                Cell::Text(s) => self.str(s),
            }
        }
        self.end();
    }
}

pub enum Cell<'a> {
    Int(i32),
    OptInt(Option<i32>),
    Text(&'a str),
}

fn itoa(v: i64, buf: &mut [u8]) -> &[u8] {
    let mut n = v.unsigned_abs();
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    if v < 0 {
        i -= 1;
        buf[i] = b'-';
    }
    &buf[i..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_escapes_and_nulls() {
        let mut e = Enc::new(Format::Text);
        e.set(&SetOut {
            set_num: "10182-1".into(),
            name: "Cafe\tCorner\\".into(),
            year: None,
            theme_id: Some(155),
            num_parts: Some(-1),
        });
        assert_eq!(
            std::str::from_utf8(&e.buf).unwrap(),
            "10182-1\tCafe\\tCorner\\\\\t\\N\t155\t-1\n"
        );
    }

    #[test]
    fn binary_layout_of_one_inventory() {
        let mut e = Enc::new(Format::Binary);
        e.inventory(&InvOut {
            id: 7,
            version: 1,
            set_num: "ab".into(),
        });
        e.finish();
        let mut want = BINARY_HEADER.to_vec();
        want.extend_from_slice(&3i16.to_be_bytes());
        want.extend_from_slice(&4i32.to_be_bytes());
        want.extend_from_slice(&7i32.to_be_bytes());
        want.extend_from_slice(&4i32.to_be_bytes());
        want.extend_from_slice(&1i32.to_be_bytes());
        want.extend_from_slice(&2i32.to_be_bytes());
        want.extend_from_slice(b"ab");
        want.extend_from_slice(&(-1i16).to_be_bytes());
        assert_eq!(e.buf, want);
    }

    #[test]
    fn itoa_edges() {
        let mut b = [0u8; 24];
        assert_eq!(itoa(0, &mut b), b"0");
        assert_eq!(itoa(-1, &mut b), b"-1");
        assert_eq!(itoa(i64::MIN, &mut b), b"-9223372036854775808");
    }
}
