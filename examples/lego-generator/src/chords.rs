//! The release years of a pack's child sets, written as a pattern of offsets from the earliest.

/// Pairs of patterns whose years, read by their last digit round the decade, lie the same distances
/// apart two at a time, but not three at a time.
pub const Z_PAIRS: [([i32; 5], [i32; 5]); 3] = [
    ([0, 1, 2, 5, 7], [0, 1, 3, 5, 6]),
    ([0, 1, 2, 4, 7], [0, 1, 3, 4, 6]),
    ([0, 1, 2, 3, 6], [0, 1, 2, 4, 5]),
];

/// How many two of the years lie 1, 2, 3, 4 and 5 years apart, each read by its last digit round the
/// decade, the shorter way: 2019 and 2021 are 2 apart, and so are 2011 and 2029.
pub fn gap_counts(years: &[i32]) -> [u32; 5] {
    let mut v = [0u32; 5];
    for (k, &a) in years.iter().enumerate() {
        for &b in &years[k + 1..] {
            let d = (b - a).rem_euclid(10);
            let g = d.min(10 - d);
            if g > 0 {
                v[(g - 1) as usize] += 1;
            }
        }
    }
    v
}

/// How many three of the years are three consecutive years.
pub fn consecutive_triples(years: &[i32]) -> u32 {
    let mut ys: Vec<i32> = years.to_vec();
    ys.sort_unstable();
    ys.dedup();
    ys.windows(3)
        .filter(|w| w[1] == w[0] + 1 && w[2] == w[1] + 1)
        .count() as u32
}

/// The pattern's offsets, or their mirror image within the pattern's own span.
pub fn placed(form: &[i32], mirrored: bool) -> Vec<i32> {
    let top = form.iter().copied().max().unwrap_or(0);
    let mut out: Vec<i32> = if mirrored {
        form.iter().map(|x| top - x).collect()
    } else {
        form.to_vec()
    };
    out.sort_unstable();
    out
}

pub fn class_label(form: &[i32]) -> String {
    form.iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paired_patterns_match_on_gaps_and_differ_on_consecutive_years() {
        let expected_triples = [(1, 0), (1, 0), (2, 1)];
        for (k, (a, b)) in Z_PAIRS.iter().enumerate() {
            assert_eq!(gap_counts(a), gap_counts(b));
            for mirrored in [false, true] {
                let pa = placed(a, mirrored);
                let pb = placed(b, mirrored);
                assert_eq!(gap_counts(&pa), gap_counts(a));
                assert_eq!(
                    (consecutive_triples(&pa), consecutive_triples(&pb)),
                    expected_triples[k]
                );
            }
        }
    }

    #[test]
    fn a_wrong_partner_breaks_the_gap_counts() {
        let (a, _) = Z_PAIRS[0];
        let wrong = [0, 1, 2, 3, 4];
        assert_ne!(gap_counts(&a), gap_counts(&wrong));
    }
}
