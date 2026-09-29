//! A seeded SplitMix64 generator, and independent streams keyed by (seed, purpose, index).
//!
//! Every generated row draws from a stream keyed by the row's own index, so the rows are a
//! function of the seed and the wiring alone: neither the chunk a row is written in nor the
//! number of concurrent writers changes what is generated.

/// SplitMix64: a 64-bit state advanced by the golden-ratio increment and finalised.
#[derive(Clone, Debug)]
pub struct Rng(u64);

/// The SplitMix64 finaliser.
pub fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// What a stream is for. Two purposes never share a stream, whatever their indices.
#[derive(Clone, Copy, Debug)]
#[repr(u64)]
pub enum Purpose {
    SetKind = 1,
    SetSocket,
    SetTemplate,
    SetName,
    SetLines,
    SetCord,
    SetNumber,
    TrapPick,
    Builder,
    Collection,
    Purchase,
    Shuffle,
    Popularity,
    Timeline,
    ThemeSpike,
}

impl Rng {
    #[cfg(test)]
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    /// The stream for one purpose and one index.
    pub fn stream(seed: u64, purpose: Purpose, index: u64) -> Self {
        let p = (purpose as u64).wrapping_mul(0xD1B5_4A32_D192_ED03);
        let i = index.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        Rng(mix(seed ^ mix(p ^ mix(i))))
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.0)
    }

    /// Uniform in `0..n`, by rejection so every value is equally likely. `n` must be positive.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0)");
        let zone = u64::MAX - (u64::MAX % n);
        loop {
            let x = self.next_u64();
            if x < zone {
                return x % n;
            }
        }
    }

    /// Uniform in `lo..=hi`.
    pub fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        assert!(lo <= hi);
        lo + self.below((hi - lo + 1) as u64) as i64
    }

    /// True with probability `ppm` per million.
    pub fn chance_ppm(&mut self, ppm: u32) -> bool {
        self.below(1_000_000) < u64::from(ppm)
    }

    /// Uniform in `[0, 1)`, with 53 bits.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }

    /// Fisher-Yates.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            items.swap(i, j);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draws(mut r: Rng) -> Vec<u64> {
        (0..4).map(|_| r.next_u64()).collect()
    }

    #[test]
    fn streams_are_reproducible_and_distinct() {
        let a = draws(Rng::stream(7, Purpose::SetKind, 3));
        assert_eq!(a, draws(Rng::stream(7, Purpose::SetKind, 3)));
        assert_ne!(a, draws(Rng::stream(7, Purpose::SetKind, 4)));
        assert_ne!(a, draws(Rng::stream(7, Purpose::SetName, 3)));
        assert_ne!(a, draws(Rng::stream(8, Purpose::SetKind, 3)));
    }

    #[test]
    fn below_stays_in_range() {
        let mut r = Rng::new(1);
        for n in [1u64, 2, 3, 7, 1000] {
            for _ in 0..1000 {
                assert!(r.below(n) < n);
            }
        }
    }
}
