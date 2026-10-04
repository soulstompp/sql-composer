//! The traps: legitimate rows a naive partitioning or indexing scheme gets wrong, each one a real
//! LEGO fact. A trap is planted at a declared rate over its population, with a floor so that the
//! smallest run still holds some, or arises naturally from the real catalogue; either way every
//! trapped row is written to the manifest.

use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Trap {
    K1,
    K2,
    K3,
    K4,
    K5,
    K6,
    K7,
    K8,
    B1,
    B2,
    B3,
    B4,
    B5,
    B6,
    B7,
    B8,
    B9,
    O1,
    O2,
    O3,
    O4,
    O5,
    D1,
    D2,
    D3,
    D4,
    D5,
    D6,
    D7,
    D8,
    /// Listed after the others, so that each earlier trap keeps the draws its number seeds.
    B10,
}

impl fmt::Display for Trap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// What a trap is planted over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Population {
    /// One synthesized set each.
    Sets,
    /// One builder's collection row each (and that row's purchases).
    CollectionRows,
    /// Not planted: it arises from the real catalogue or from other traps' rows.
    Natural,
    /// Written by a phase of the patch, not at a rate.
    Phase,
}

#[derive(Clone, Copy, Debug)]
pub struct Decl {
    pub trap: Trap,
    pub title: &'static str,
    pub population: Population,
    /// Planted per million of the population.
    pub per_million: u32,
    /// Planted at least this many.
    pub floor: u32,
}

impl Decl {
    pub fn planted(&self, population: u64) -> u64 {
        if matches!(self.population, Population::Natural | Population::Phase) || population == 0 {
            return 0;
        }
        (u64::from(self.per_million) * population / 1_000_000).max(u64::from(self.floor))
    }
}

pub const DECLS: &[Decl] = &[
    Decl { trap: Trap::K1, title: "a lettered set number typed in another case", population: Population::CollectionRows, per_million: 5_000, floor: 3 },
    Decl { trap: Trap::K2, title: "a set number padded with spaces by a spreadsheet cell", population: Population::CollectionRows, per_million: 5_000, floor: 3 },
    Decl { trap: Trap::K3, title: "Café Corner typed composed (NFC) and decomposed (NFD)", population: Population::CollectionRows, per_million: 2_500, floor: 4 },
    Decl { trap: Trap::K4, title: "a set name after a Latin-1 round trip", population: Population::Sets, per_million: 3_000, floor: 3 },
    Decl { trap: Trap::K5, title: "a set number with an en dash, a non-breaking hyphen or a no-break space", population: Population::CollectionRows, per_million: 5_000, floor: 3 },
    Decl { trap: Trap::K6, title: "part numbers read as text against as numbers (the basic bricks 3001 to 3010)", population: Population::Natural, per_million: 0, floor: 0 },
    Decl { trap: Trap::K7, title: "a bare set number beside its -1 record", population: Population::Sets, per_million: 250, floor: 3 },
    Decl { trap: Trap::K8, title: "an inventory filed under the number on the box, without its -1", population: Population::Sets, per_million: 100, floor: 3 },
    Decl { trap: Trap::B1, title: "a set released in a decade's first year", population: Population::Natural, per_million: 0, floor: 0 },
    Decl { trap: Trap::B2, title: "an unreleased set with no year", population: Population::Sets, per_million: 50, floor: 3 },
    Decl { trap: Trap::B3, title: "a set with no theme, or with a root theme the real catalogue's theme list does not hold", population: Population::Sets, per_million: 50, floor: 4 },
    Decl { trap: Trap::B4, title: "an Automatic Binding Bricks set of 1949", population: Population::Sets, per_million: 10, floor: 2 },
    Decl { trap: Trap::B5, title: "a set announced for 2031", population: Population::Sets, per_million: 10, floor: 2 },
    Decl { trap: Trap::B6, title: "a part count filed as -1", population: Population::Sets, per_million: 250, floor: 3 },
    Decl { trap: Trap::B7, title: "lines in colour -1 (Unknown) and 9999 ([No Color], Black's rgb)", population: Population::Natural, per_million: 0, floor: 0 },
    Decl { trap: Trap::B8, title: "NOT IN over a list that holds a NULL (sets with no theme)", population: Population::Natural, per_million: 0, floor: 0 },
    Decl { trap: Trap::B9, title: "the 2000s counted two ways: 2001 to 2010 holds no set, 2000 to 2009 only 2000", population: Population::Natural, per_million: 0, floor: 0 },
    Decl { trap: Trap::B10, title: "a real line naming a part number the parts list does not hold", population: Population::Natural, per_million: 0, floor: 0 },
    Decl { trap: Trap::O1, title: "physical row order shuffled within every wave", population: Population::Natural, per_million: 0, floor: 0 },
    Decl { trap: Trap::O2, title: "lettered set numbers in byte order against linguistic order", population: Population::Natural, per_million: 0, floor: 0 },
    Decl { trap: Trap::O3, title: "a range on set numbers whose split point moves with the collation", population: Population::Natural, per_million: 0, floor: 0 },
    Decl { trap: Trap::O4, title: "sets tied on their brick count at a top-N cut", population: Population::Natural, per_million: 0, floor: 0 },
    Decl { trap: Trap::O5, title: "pairs of packs whose sets' release years match two at a time and differ in a run of three", population: Population::Phase, per_million: 0, floor: 0 },
    Decl { trap: Trap::D1, title: "two purchases in the hour a fall-back clock reads twice", population: Population::CollectionRows, per_million: 2_000, floor: 2 },
    Decl { trap: Trap::D2, title: "a receipt printed in standard time inside a spring-forward gap", population: Population::CollectionRows, per_million: 2_000, floor: 2 },
    Decl { trap: Trap::D3, title: "a purchase whose local month is not its UTC month", population: Population::CollectionRows, per_million: 5_000, floor: 4 },
    Decl { trap: Trap::D4, title: "a midnight launch written as 24:00", population: Population::CollectionRows, per_million: 2_000, floor: 2 },
    Decl { trap: Trap::D5, title: "a pre-order delivered at infinity", population: Population::CollectionRows, per_million: 2_000, floor: 2 },
    Decl { trap: Trap::D6, title: "a purchase on 29 February", population: Population::CollectionRows, per_million: 2_000, floor: 2 },
    Decl { trap: Trap::D7, title: "a purchase whose ISO week-year is not its calendar year", population: Population::CollectionRows, per_million: 2_000, floor: 2 },
    Decl { trap: Trap::D8, title: "a local day that begins twice", population: Population::CollectionRows, per_million: 2_000, floor: 2 },
];

pub fn decl(trap: Trap) -> &'static Decl {
    DECLS
        .iter()
        .find(|d| d.trap == trap)
        .expect("every trap is declared")
}
