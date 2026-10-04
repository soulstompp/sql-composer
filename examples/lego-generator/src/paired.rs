//! The packs of the `paired` phase: written in pairs, each pack's child sets released in the years
//! of a pattern, written as offsets from the earliest.

/// Three pairs of release-year patterns. The two patterns of a pair hold as many pairs of
/// consecutive years, and a different number of runs of three consecutive years, mirrored or not.
pub const YEAR_PAIRS: [([i32; 5], [i32; 5]); 3] = [
    ([0, 1, 2, 5, 7], [0, 1, 3, 5, 6]),
    ([0, 1, 2, 4, 7], [0, 1, 3, 4, 6]),
    ([0, 1, 2, 3, 6], [0, 1, 2, 4, 5]),
];

/// The pattern's offsets, or their mirror image within the pattern's own span.
pub fn placed(pattern: &[i32], mirrored: bool) -> Vec<i32> {
    let top = pattern.iter().copied().max().unwrap_or(0);
    let mut out: Vec<i32> = if mirrored {
        pattern.iter().map(|x| top - x).collect()
    } else {
        pattern.to_vec()
    };
    out.sort_unstable();
    out
}

/// The offsets as the manifest writes them: `0,1,2,5,7`.
pub fn label(offsets: &[i32]) -> String {
    offsets
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",")
}
