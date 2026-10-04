//! What builders buy and when: each released set's popularity and its timeline of demand, and the
//! alias tables collection rows draw their sets from.
//!
//! A set's POPULARITY is uneven (`SIGMA`): the most popular tenth of sets holds about half of it.
//! Its TIMELINE is a sum of bursts over the days from its release to `TODAY`: the launch, which
//! fades; comebacks and re-releases at random later dates; the spikes of its root theme, shared by
//! every set of the theme; and a thin second-hand trickle. A set's DEMAND is its
//! popularity times how much of its timeline has passed by today, so a set released last month has
//! had little time to sell.

use crate::calendar;
use crate::rng::{Purpose, Rng};

/// How uneven popularity is: with this spread the most popular tenth of sets holds about half of it.
pub const SIGMA: f64 = 1.2816;
/// How long the launch takes to fade, drawn from this range, in days.
const LAUNCH_TAU_DAYS: (f64, f64) = (90.0, 270.0);
/// Comebacks and re-releases per set, on average.
const COMEBACKS_MEAN: f64 = 0.9;
/// How long after its release a set comes back, in days.
const COMEBACK_AFTER_DAYS: (i64, i64) = (730, 9_125);
/// A comeback's size, against the launch's 1.
const COMEBACK_AMOUNT: (f64, f64) = (0.15, 0.6);
const COMEBACK_TAU_DAYS: f64 = 60.0;
/// The chance, per root theme and year, that the year holds a spike.
const SPIKE_YEAR_PPM: u32 = 70_000;
const SPIKE_DAYS: (i64, i64) = (14, 60);
/// A spike's size for each set of the theme on the shelf by then, against the launch's 1.
const SPIKE_AMOUNT: (f64, f64) = (0.05, 0.2);
/// A set joins its theme's spikes this many days after its release.
const SPIKE_SHELF_DAYS: i64 = 60;
/// The second-hand trickle, per year on the shelf, against the launch's 1.
const TRICKLE_PER_YEAR: f64 = 0.015;

/// A part of a timeline.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Burst {
    /// Demand from day `start`, fading over about `tau` days; `amount` in all.
    Fade { start: i64, tau: f64, amount: f64 },
    /// Demand spread evenly over the `days` days from day `start`.
    Even { start: i64, days: i64, amount: f64 },
}

impl Burst {
    /// The burst's amount over the days `lo..hi`.
    pub fn amount_in(&self, lo: i64, hi: i64) -> f64 {
        match *self {
            Burst::Fade { start, tau, amount } => {
                let (a, b) = ((lo - start).max(0) as f64, (hi - start).max(0) as f64);
                amount * ((-a / tau).exp() - (-b / tau).exp())
            }
            Burst::Even {
                start,
                days,
                amount,
            } => {
                let (a, b) = (lo.max(start), hi.min(start + days));
                if b > a {
                    amount * (b - a) as f64 / days as f64
                } else {
                    0.0
                }
            }
        }
    }

    /// A day in `lo..hi`, drawn where the burst's demand falls. The burst must have some demand there.
    fn draw(&self, lo: i64, hi: i64, r: &mut Rng) -> i64 {
        match *self {
            Burst::Fade { start, tau, .. } => {
                let (a, b) = ((lo - start).max(0) as f64, (hi - start).max(0) as f64);
                let (ea, eb) = ((-a / tau).exp(), (-b / tau).exp());
                let x = -tau * (ea - r.unit() * (ea - eb)).ln();
                (start + x as i64).clamp(lo.max(start), hi - 1)
            }
            Burst::Even { start, days, .. } => {
                let (a, b) = (lo.max(start), hi.min(start + days));
                a + r.below((b - a) as u64) as i64
            }
        }
    }
}

/// A set's demand over the days `lo..hi` (its first day on sale to the day after `TODAY`).
#[derive(Clone, Debug, PartialEq)]
pub struct Timeline {
    pub release: i64,
    pub lo: i64,
    pub hi: i64,
    pub bursts: Vec<Burst>,
}

impl Timeline {
    /// The timeline's amount over `lo..hi`.
    pub fn amount(&self) -> f64 {
        self.bursts
            .iter()
            .map(|b| b.amount_in(self.lo, self.hi))
            .sum()
    }

    /// A day drawn where the timeline's demand falls.
    pub fn day(&self, r: &mut Rng) -> i64 {
        let total = self.amount();
        let mut pick = r.unit() * total;
        for b in &self.bursts {
            let m = b.amount_in(self.lo, self.hi);
            if pick < m {
                return b.draw(self.lo, self.hi, r);
            }
            pick -= m;
        }
        let last = self
            .bursts
            .iter()
            .rev()
            .find(|b| b.amount_in(self.lo, self.hi) > 0.0)
            .expect("a timeline with amount");
        last.draw(self.lo, self.hi, r)
    }
}

/// A window of days in which every set of one root theme on the shelf sells more.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Spike {
    pub start: i64,
    pub days: i64,
    pub amount: f64,
}

/// The spikes of a root theme over the years `first..=last`.
pub fn theme_spikes(seed: u64, root: i32, first: i64, last: i64) -> Vec<Spike> {
    let mut r = Rng::stream(seed, Purpose::ThemeSpike, root as u64);
    let mut out = Vec::new();
    for y in first..=last {
        let spiked = r.chance_ppm(SPIKE_YEAR_PPM);
        let day = r.below(365) as i64;
        let days = r.range_i64(SPIKE_DAYS.0, SPIKE_DAYS.1);
        let amount = SPIKE_AMOUNT.0 + r.unit() * (SPIKE_AMOUNT.1 - SPIKE_AMOUNT.0);
        if spiked {
            out.push(Spike {
                start: calendar::days_from_civil(y, 1, 1) + day,
                days,
                amount,
            });
        }
    }
    out
}

/// A standard normal draw (Box-Muller).
fn normal(r: &mut Rng) -> f64 {
    let u1 = 1.0 - r.unit();
    let u2 = r.unit();
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

/// A set's popularity, spread by `SIGMA`, averaging 1.
pub fn popularity(seed: u64, set: u64) -> f64 {
    let mut r = Rng::stream(seed, Purpose::Popularity, set);
    (SIGMA * normal(&mut r) - SIGMA * SIGMA / 2.0).exp()
}

/// The timeline of set `set`, released in `year`, among its root theme's `spikes`: on sale from
/// `first_day` or its release, whichever is later, to `today`. `None` when it is not on sale by then.
pub fn timeline(
    seed: u64,
    set: u64,
    year: i32,
    spikes: &[Spike],
    first_day: i64,
    today: i64,
) -> Option<Timeline> {
    let year = i64::from(year);
    let opens = calendar::days_from_civil(year, 1, 1);
    let closes = calendar::days_from_civil(year, 12, 31).min(today);
    if closes < opens {
        return None;
    }
    let mut r = Rng::stream(seed, Purpose::Timeline, set);
    let release = r.range_i64(opens, closes);
    let tau = LAUNCH_TAU_DAYS.0 + r.unit() * (LAUNCH_TAU_DAYS.1 - LAUNCH_TAU_DAYS.0);
    let mut bursts = vec![Burst::Fade {
        start: release,
        tau,
        amount: 1.0,
    }];
    // Comebacks: how many, around `COMEBACKS_MEAN` on average.
    let (mut k, mut p, u) = (0, (-COMEBACKS_MEAN).exp(), r.unit());
    let mut cum = p;
    while u > cum && k < 16 {
        k += 1;
        p *= COMEBACKS_MEAN / f64::from(k);
        cum += p;
    }
    for _ in 0..k {
        let after = r.range_i64(COMEBACK_AFTER_DAYS.0, COMEBACK_AFTER_DAYS.1);
        let amount = COMEBACK_AMOUNT.0 + r.unit() * (COMEBACK_AMOUNT.1 - COMEBACK_AMOUNT.0);
        bursts.push(Burst::Fade {
            start: release + after,
            tau: COMEBACK_TAU_DAYS,
            amount,
        });
    }
    for s in spikes {
        if s.start >= release + SPIKE_SHELF_DAYS {
            bursts.push(Burst::Even {
                start: s.start,
                days: s.days,
                amount: s.amount,
            });
        }
    }
    let lo = release.max(first_day);
    let hi = today + 1;
    bursts.push(Burst::Even {
        start: release,
        days: hi - release,
        amount: TRICKLE_PER_YEAR * (hi - release) as f64 / 365.25,
    });
    let t = Timeline {
        release,
        lo,
        hi,
        bursts,
    };
    (lo < hi && t.amount() > 0.0).then_some(t)
}

/// Vose's alias table: draws an index in proportion to its weight in two draws.
#[derive(Clone, Debug, Default)]
pub struct Alias {
    prob: Vec<f64>,
    alias: Vec<u32>,
}

impl Alias {
    /// A table over `weights`, which must hold a positive total.
    pub fn new(weights: &[f64]) -> Alias {
        let n = weights.len();
        let total: f64 = weights.iter().sum();
        assert!(n > 0 && total > 0.0, "an alias table over no weight");
        let mut prob: Vec<f64> = weights.iter().map(|w| w * n as f64 / total).collect();
        let mut alias: Vec<u32> = (0..n as u32).collect();
        let (mut small, mut large): (Vec<u32>, Vec<u32>) =
            (0..n as u32).partition(|&i| prob[i as usize] < 1.0);
        while let (Some(s), Some(&l)) = (small.pop(), large.last()) {
            alias[s as usize] = l;
            prob[l as usize] -= 1.0 - prob[s as usize];
            if prob[l as usize] < 1.0 {
                large.pop();
                small.push(l);
            }
        }
        for i in large.into_iter().chain(small) {
            prob[i as usize] = 1.0;
        }
        Alias { prob, alias }
    }

    pub fn draw(&self, r: &mut Rng) -> usize {
        let i = r.below(self.prob.len() as u64) as usize;
        if r.unit() < self.prob[i] {
            i
        } else {
            self.alias[i] as usize
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_alias_table_draws_in_proportion_to_its_weights() {
        let weights = [1.0, 0.0, 3.0, 6.0];
        let a = Alias::new(&weights);
        let mut r = Rng::new(3);
        let mut seen = [0u32; 4];
        for _ in 0..100_000 {
            seen[a.draw(&mut r)] += 1;
        }
        assert_eq!(seen[1], 0);
        for (i, w) in weights.iter().enumerate() {
            let share = f64::from(seen[i]) / 100_000.0;
            assert!((share - w / 10.0).abs() < 0.01, "{i}: {share}");
        }
    }

    #[test]
    fn the_top_tenth_of_popularity_holds_about_half() {
        let mut p: Vec<f64> = (0..200_000).map(|s| popularity(7, s)).collect();
        p.sort_by(|a, b| b.total_cmp(a));
        let total: f64 = p.iter().sum();
        let top: f64 = p[..20_000].iter().sum();
        assert!((top / total - 0.5).abs() < 0.03, "{}", top / total);
    }

    #[test]
    fn a_timeline_draws_only_days_on_sale_and_mostly_near_its_launch() {
        let today = calendar::days_from_civil(2026, 9, 25);
        let first = calendar::days_from_civil(1950, 1, 1);
        assert_eq!(timeline(7, 1, 2027, &[], first, today), None);
        let t = timeline(7, 1, 2015, &[], first, today).expect("on sale");
        let mut r = Rng::new(5);
        let days: Vec<i64> = (0..20_000).map(|_| t.day(&mut r)).collect();
        assert!(days.iter().all(|d| (t.lo..t.hi).contains(d)));
        let first_year = days.iter().filter(|&&d| d < t.release + 365).count();
        assert!(first_year > 20_000 / 3, "{first_year} in the first year");
        let late = timeline(7, 2, 2026, &[], first, today).expect("on sale");
        assert!(late.amount() < t.amount());
    }
}
