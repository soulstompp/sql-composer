//! The switchboard: which socket each synthesized set is plugged into.
//!
//! The sockets are the cells root theme × decade (see `catalogue`). A synthesized set takes its
//! root theme, year and contents from a real set in its socket, or, in a modelled year, from a real
//! set of its root theme; a socket with neither stays empty at every size.
//!
//! The synthesized sets are split into PHASES, each wired to a PATTERN:
//! - `natural`: the real catalogue's own weight on each socket;
//! - `sorted`: one sweep through the sockets in key order (root theme, then decade);
//! - `interleaved`: a walk that steps across the whole socket order within every wave;
//! - `wavy`: a wave travelling over the ordered sockets, forward then back, over and over;
//! - `hotspot`: one socket takes a fixed share of the sets;
//! - `swing`: waves alternate between two groups of sockets;
//! - `paired`: packs written in pairs, whose child sets' release years match two at a time and
//!   differ in a run of three (see `paired`).
//!
//! Every pattern except `natural` keeps the real weights along the socket order, and changes
//! only which wave a set arrives in. A WAVE is one chunk of the loader.

use std::fmt;

use crate::catalogue::{Real, DECADE_LO};
use crate::rng::Rng;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pattern {
    Natural,
    Sorted,
    Interleaved,
    Wavy,
    Hotspot,
    Swing,
    /// Packs in pairs (see `paired`), placed by their child sets' release years, not by a socket.
    Paired,
}

impl Pattern {
    pub fn parse(s: &str) -> Result<Pattern, String> {
        Ok(match s {
            "natural" => Pattern::Natural,
            "sorted" => Pattern::Sorted,
            "interleaved" => Pattern::Interleaved,
            "wavy" => Pattern::Wavy,
            "hotspot" => Pattern::Hotspot,
            "swing" => Pattern::Swing,
            "paired" => Pattern::Paired,
            other => return Err(format!("unknown pattern `{other}`")),
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Pattern::Natural => "natural",
            Pattern::Sorted => "sorted",
            Pattern::Interleaved => "interleaved",
            Pattern::Wavy => "wavy",
            Pattern::Hotspot => "hotspot",
            Pattern::Swing => "swing",
            Pattern::Paired => "paired",
        }
    }
}

/// A contiguous run of synthesized sets wired to one pattern.
#[derive(Clone, Debug)]
pub struct Phase {
    /// From 1; phase 0 is the real catalogue.
    pub number: usize,
    pub pattern: Pattern,
    pub percent: u32,
    pub start: u64,
    pub end: u64,
}

impl Phase {
    pub fn label(&self) -> String {
        format!("{}:{}", self.number, self.pattern.name())
    }
}

/// The patterns' dials.
#[derive(Clone, Debug)]
pub struct Dials {
    /// Waves in one sweep of the travelling wave, one way.
    pub wavy_period: u64,
    /// Width of the travelling wave, as a share of the socket order.
    pub wavy_amplitude: f64,
    pub hotspot_share: f64,
    /// The hot socket as `r<root>/<decade>s`; the heaviest socket when absent.
    pub hotspot_socket: Option<String>,
    pub swing_period: u64,
    /// Each group as `<root>,<root>,…@<from>-<to>`: root theme ids and a half-open year range.
    pub swing_a: String,
    pub swing_b: String,
}

/// A group of drawable sockets with the running total of their weights.
#[derive(Clone, Debug)]
struct Group {
    sockets: Vec<u16>,
    cum: Vec<u64>,
}

impl Group {
    fn new(real: &Real, sockets: impl Iterator<Item = u16>) -> Group {
        let mut g = Group {
            sockets: Vec::new(),
            cum: Vec::new(),
        };
        let mut total = 0u64;
        for s in sockets {
            let w = real.sockets[usize::from(s)].weight();
            if w > 0 {
                total += w;
                g.sockets.push(s);
                g.cum.push(total);
            }
        }
        g
    }

    fn total(&self) -> u64 {
        self.cum.last().copied().unwrap_or(0)
    }

    /// The socket a draw `q` in `[0, 1)` lands on, by the group's weights, in socket order.
    fn at(&self, q: f64) -> u16 {
        let target = ((q.clamp(0.0, 1.0) * self.total() as f64) as u64).min(self.total() - 1);
        let k = self.cum.partition_point(|&c| c <= target);
        self.sockets[k]
    }
}

#[derive(Clone, Debug)]
pub struct Switchboard {
    pub phases: Vec<Phase>,
    pub dials: Dials,
    pub chunk: u64,
    all: Group,
    hot: u16,
    swing: [Group; 2],
}

impl fmt::Display for Switchboard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self
            .phases
            .iter()
            .map(|p| {
                format!(
                    "{}:{} (sets {}..{})",
                    p.pattern.name(),
                    p.percent,
                    p.start,
                    p.end
                )
            })
            .collect();
        write!(f, "{}", parts.join(", "))
    }
}

fn parse_group(real: &Real, spec: &str) -> Result<Vec<u16>, String> {
    let (roots, years) = spec
        .split_once('@')
        .ok_or_else(|| format!("swing group `{spec}`: want <roots>@<from>-<to>"))?;
    let (from, to) = years
        .split_once('-')
        .ok_or_else(|| format!("swing group `{spec}`: want a year range"))?;
    let from: i32 = from
        .parse()
        .map_err(|_| format!("swing group `{spec}`: bad year"))?;
    let to: i32 = to
        .parse()
        .map_err(|_| format!("swing group `{spec}`: bad year"))?;
    let roots: Vec<i32> = roots
        .split(',')
        .map(|r| {
            r.trim()
                .parse::<i32>()
                .map_err(|_| format!("swing group `{spec}`: bad root `{r}`"))
        })
        .collect::<Result<_, _>>()?;
    Ok(real
        .sockets
        .iter()
        .enumerate()
        .filter(|(_, s)| {
            let lo = DECADE_LO + 10 * i32::from(s.decade);
            roots.contains(&s.root) && lo >= from && lo + 10 <= to
        })
        .map(|(i, _)| i as u16)
        .collect())
}

impl Switchboard {
    /// `patch` is `<pattern>:<percent>,…`, the percents summing to 100.
    pub fn new(
        real: &Real,
        patch: &str,
        dials: Dials,
        sets: u64,
        chunk: u64,
    ) -> Result<Switchboard, String> {
        let mut specs = Vec::new();
        for item in patch.split(',') {
            let (name, pct) = item
                .split_once(':')
                .ok_or_else(|| format!("patch item `{item}`: want <pattern>:<percent>"))?;
            let pct: u32 = pct
                .trim()
                .parse()
                .map_err(|_| format!("patch item `{item}`: bad percent"))?;
            specs.push((Pattern::parse(name.trim())?, pct));
        }
        let sum: u32 = specs.iter().map(|s| s.1).sum();
        if sum != 100 {
            return Err(format!("patch `{patch}`: percents sum to {sum}, not 100"));
        }
        let mut phases = Vec::new();
        let mut cum = 0u64;
        for (k, (pattern, percent)) in specs.into_iter().enumerate() {
            let start = sets * cum / 100;
            cum += u64::from(percent);
            let end = sets * cum / 100;
            phases.push(Phase {
                number: k + 1,
                pattern,
                percent,
                start,
                end,
            });
        }
        let all = Group::new(real, 0..real.sockets.len() as u16);
        if all.total() == 0 {
            return Err("the source catalogue has no set with an inventory in any socket".into());
        }
        let hot = match &dials.hotspot_socket {
            Some(label) => real
                .sockets
                .iter()
                .position(|s| &s.label() == label)
                .map(|i| i as u16)
                .ok_or_else(|| format!("hotspot socket `{label}` is not a socket"))?,
            None => {
                let mut best = 0u16;
                for (i, s) in real.sockets.iter().enumerate() {
                    if s.weight() > real.sockets[usize::from(best)].weight() {
                        best = i as u16;
                    }
                }
                best
            }
        };
        if real.sockets[usize::from(hot)].weight() == 0 {
            return Err("the hotspot socket holds no real set".into());
        }
        let swing = [
            Group::new(real, parse_group(real, &dials.swing_a)?.into_iter()),
            Group::new(real, parse_group(real, &dials.swing_b)?.into_iter()),
        ];
        let uses_swing = phases.iter().any(|p| p.pattern == Pattern::Swing);
        if uses_swing && (swing[0].total() == 0 || swing[1].total() == 0) {
            return Err("a swing group holds no real set".into());
        }
        Ok(Switchboard {
            phases,
            dials,
            chunk: chunk.max(1),
            all,
            hot,
            swing,
        })
    }

    pub fn phase_of(&self, i: u64) -> &Phase {
        let k = self.phases.partition_point(|p| p.end <= i);
        &self.phases[k.min(self.phases.len() - 1)]
    }

    pub fn hot_socket(&self) -> u16 {
        self.hot
    }

    /// The socket synthesized set `i` is plugged into.
    pub fn socket_for(&self, i: u64, rng: &mut Rng) -> u16 {
        let ph = self.phase_of(i);
        let k = i - ph.start;
        let n = (ph.end - ph.start).max(1);
        let wave = k / self.chunk;
        match ph.pattern {
            Pattern::Natural | Pattern::Paired => self.all.at(rng.unit()),
            Pattern::Sorted => self.all.at((k as f64 + 0.5) / n as f64),
            Pattern::Interleaved => {
                const GOLDEN: f64 = 0.618_033_988_749_894_9;
                self.all.at(((k + 1) as f64 * GOLDEN).fract())
            }
            Pattern::Wavy => {
                let period = self.dials.wavy_period.max(1);
                let t = (wave % (2 * period)) as f64 / period as f64;
                let centre = if t < 1.0 { t } else { 2.0 - t };
                let mut q = centre + (rng.unit() - 0.5) * self.dials.wavy_amplitude;
                if q < 0.0 {
                    q = -q;
                }
                if q >= 1.0 {
                    q = 2.0 - q - f64::EPSILON;
                }
                self.all.at(q)
            }
            Pattern::Hotspot => {
                if rng.unit() < self.dials.hotspot_share {
                    self.hot
                } else {
                    self.all.at(rng.unit())
                }
            }
            Pattern::Swing => {
                let g = &self.swing[((wave / self.dials.swing_period.max(1)) % 2) as usize];
                g.at(rng.unit())
            }
        }
    }
}
