//! Spellings: canonical decomposition of accented Latin letters, the Latin-1 round trip, and the
//! keyboard variants of a set number.

/// Canonical decomposition (NFD) of the precomposed letters of the Latin-1 Supplement block.
/// Letters with no canonical decomposition (Æ, Ð, Ø, Þ, ß and their lower cases) stay as they are.
fn decompose(c: char) -> Option<(char, char)> {
    const GRAVE: char = '\u{0300}';
    const ACUTE: char = '\u{0301}';
    const CIRCUMFLEX: char = '\u{0302}';
    const TILDE: char = '\u{0303}';
    const DIAERESIS: char = '\u{0308}';
    const RING: char = '\u{030A}';
    const CEDILLA: char = '\u{0327}';
    let pair = match c {
        'À' => ('A', GRAVE),
        'Á' => ('A', ACUTE),
        'Â' => ('A', CIRCUMFLEX),
        'Ã' => ('A', TILDE),
        'Ä' => ('A', DIAERESIS),
        'Å' => ('A', RING),
        'Ç' => ('C', CEDILLA),
        'È' => ('E', GRAVE),
        'É' => ('E', ACUTE),
        'Ê' => ('E', CIRCUMFLEX),
        'Ë' => ('E', DIAERESIS),
        'Ì' => ('I', GRAVE),
        'Í' => ('I', ACUTE),
        'Î' => ('I', CIRCUMFLEX),
        'Ï' => ('I', DIAERESIS),
        'Ñ' => ('N', TILDE),
        'Ò' => ('O', GRAVE),
        'Ó' => ('O', ACUTE),
        'Ô' => ('O', CIRCUMFLEX),
        'Õ' => ('O', TILDE),
        'Ö' => ('O', DIAERESIS),
        'Ù' => ('U', GRAVE),
        'Ú' => ('U', ACUTE),
        'Û' => ('U', CIRCUMFLEX),
        'Ü' => ('U', DIAERESIS),
        'Ý' => ('Y', ACUTE),
        'à' => ('a', GRAVE),
        'á' => ('a', ACUTE),
        'â' => ('a', CIRCUMFLEX),
        'ã' => ('a', TILDE),
        'ä' => ('a', DIAERESIS),
        'å' => ('a', RING),
        'ç' => ('c', CEDILLA),
        'è' => ('e', GRAVE),
        'é' => ('e', ACUTE),
        'ê' => ('e', CIRCUMFLEX),
        'ë' => ('e', DIAERESIS),
        'ì' => ('i', GRAVE),
        'í' => ('i', ACUTE),
        'î' => ('i', CIRCUMFLEX),
        'ï' => ('i', DIAERESIS),
        'ñ' => ('n', TILDE),
        'ò' => ('o', GRAVE),
        'ó' => ('o', ACUTE),
        'ô' => ('o', CIRCUMFLEX),
        'õ' => ('o', TILDE),
        'ö' => ('o', DIAERESIS),
        'ù' => ('u', GRAVE),
        'ú' => ('u', ACUTE),
        'û' => ('u', CIRCUMFLEX),
        'ü' => ('u', DIAERESIS),
        'ý' => ('y', ACUTE),
        'ÿ' => ('y', DIAERESIS),
        _ => return None,
    };
    Some(pair)
}

/// The NFD spelling of a string whose accented letters are all in the Latin-1 Supplement block,
/// as a file system that stores decomposed names hands it back.
pub fn nfd_latin1(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match decompose(c) {
            Some((base, mark)) => {
                out.push(base);
                out.push(mark);
            }
            None => out.push(c),
        }
    }
    out
}

/// True when the string holds a letter that `nfd_latin1` decomposes.
#[cfg(test)]
pub fn has_decomposable(s: &str) -> bool {
    s.chars().any(|c| decompose(c).is_some())
}

/// UTF-8 bytes read back as Latin-1 and written out again as UTF-8: every byte of a multi-byte
/// character becomes a character of its own.
pub fn latin1_round_trip(s: &str) -> String {
    s.bytes().map(char::from).collect()
}

/// True when the string has a character outside ASCII.
pub fn has_non_ascii(s: &str) -> bool {
    !s.is_ascii()
}

/// True when a set number carries an ASCII letter.
pub fn is_lettered(set_num: &str) -> bool {
    set_num.bytes().any(|b| b.is_ascii_alphabetic())
}

/// A different-case spelling of a lettered set number, as a spreadsheet's auto-capitalisation or a
/// shift key leaves it.
pub fn case_variant(set_num: &str, which: u64) -> String {
    let upper = set_num.to_ascii_uppercase();
    let lower = set_num.to_ascii_lowercase();
    let mut first_flipped: String = String::with_capacity(set_num.len());
    let mut flipped = false;
    for c in set_num.chars() {
        if !flipped && c.is_ascii_alphabetic() {
            flipped = true;
            if c.is_ascii_uppercase() {
                first_flipped.push(c.to_ascii_lowercase());
            } else {
                first_flipped.push(c.to_ascii_uppercase());
            }
        } else {
            first_flipped.push(c);
        }
    }
    let candidates = [upper, lower, first_flipped];
    let start = (which % 3) as usize;
    for k in 0..3 {
        let c = &candidates[(start + k) % 3];
        if c != set_num {
            return c.clone();
        }
    }
    unreachable!("a lettered set number has a spelling in another case")
}

/// A set number padded with ASCII spaces, as a spreadsheet cell exports it.
pub fn whitespace_variant(set_num: &str, which: u64) -> String {
    match which % 3 {
        0 => format!("{set_num} "),
        1 => format!(" {set_num}"),
        _ => format!(" {set_num} "),
    }
}

/// A set number whose hyphen or trailing space came through a word processor or a web page: an
/// en dash, a non-breaking hyphen, or a trailing no-break space.
/// A number with no hyphen gets the no-break space.
pub fn dash_variant(set_num: &str, which: u64) -> String {
    match (set_num.contains('-'), which % 3) {
        (true, 0) => set_num.replacen('-', "\u{2013}", 1),
        (true, 1) => set_num.replacen('-', "\u{2011}", 1),
        _ => format!("{set_num}\u{00A0}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cafe_decomposes() {
        let nfc = "Café Corner";
        let nfd = nfd_latin1(nfc);
        assert_eq!(nfd.as_bytes(), b"Cafe\xcc\x81 Corner");
        assert!(has_decomposable(nfc));
        assert!(!has_decomposable(&nfd));
        assert_eq!(nfd_latin1("København"), "København");
    }

    #[test]
    fn latin1_round_trip_splits_multibyte_characters() {
        assert_eq!(latin1_round_trip("København"), "KÃ¸benhavn");
        assert_eq!(
            latin1_round_trip("Joker™").as_bytes(),
            b"Joker\xc3\xa2\xc2\x84\xc2\xa2"
        );
        assert_eq!(latin1_round_trip("U-wing"), "U-wing");
    }

    #[test]
    fn variants_differ_from_the_key() {
        for which in 0..6 {
            for key in ["Vancouver-1", "vwkit-1", "WHITEHOUSE-1", "K8672-1"] {
                let v = case_variant(key, which);
                assert_ne!(v, key);
                assert_eq!(v.to_ascii_lowercase(), key.to_ascii_lowercase());
            }
            let w = whitespace_variant("10182-1", which);
            assert_eq!(w.trim(), "10182-1");
            let d = dash_variant("10182-1", which);
            assert_ne!(d, "10182-1");
            let bare = dash_variant("4180878", which);
            assert_ne!(
                bare, "4180878",
                "a number with no hyphen still gets a variant"
            );
        }
    }
}
