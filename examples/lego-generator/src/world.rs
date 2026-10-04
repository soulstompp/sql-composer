//! What is generated: the real catalogue passed through, the synthesized sets with their
//! inventories, lines and nesting, and the builders with their collections and purchase logs.
//!
//! Every row is a function of the wiring (seed, sizes, patch, dials) and of the row's own index,
//! except that a wave's rows are written in a shuffled physical order.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::calendar::{self, LocalTime, Shift, Zone};
use crate::catalogue::{decade_of, is_release_year, split_version, Real, Theme, DECADE_LO};
use crate::demand::{self, Alias, Spike, Timeline};
use crate::paired::{self, YEAR_PAIRS};
use crate::places::Places;
use crate::rng::{Purpose, Rng};
use crate::switchboard::{Pattern, Switchboard};
use crate::text;
use crate::traps::{self, Population, Trap, DECLS};

/// The generator's dials, besides the patch.
#[derive(Clone, Debug)]
pub struct Wiring {
    pub seed: u64,
    pub sets: u64,
    pub chunk: u64,
    pub builders: u64,
    pub rows_per_builder: u64,
    pub lettered_ppm: u32,
    pub rerelease_ppm: u32,
    pub second_version_ppm: u32,
    pub recolour_ppm: u32,
    /// A nesting cord joins a pack to a child of another root theme.
    pub cross_nesting: f64,
    /// A `-2` record sits in another socket than its `-1`.
    pub cross_versions: f64,
    /// A bare record sits in another socket than its `-1`.
    pub cross_twins: f64,
    /// A builder's collection row falls outside the builder's home socket.
    pub cross_collections: f64,
}

/// Purchases per set on offer, on average; each set's share follows its demand.
pub const PURCHASES_PER_SET: u64 = 50;
pub const ROWS_PER_BUILDER: u64 = 40;

/// The builders a catalogue of `sets` sets needs for `PURCHASES_PER_SET` purchases each (a
/// collection row buys 1.25 copies on average), and at least one per home zone.
pub fn builders_for(sets: u64) -> u64 {
    (sets * PURCHASES_PER_SET * 4 / (5 * ROWS_PER_BUILDER)).max(calendar::ZONE_NAMES.len() as u64)
}
pub const LAST_REAL_YEAR: i32 = 2017;
pub const FIRST_PURCHASE_YEAR: i64 = 1950;
pub const TODAY: (i64, u32, u32) = (2026, 9, 25);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Real,
    Synthetic,
    Plan,
}

impl Origin {
    pub fn name(self) -> &'static str {
        match self {
            Origin::Real => "real",
            Origin::Synthetic => "synthetic",
            Origin::Plan => "plan",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetOut {
    pub set_num: String,
    pub name: String,
    pub year: Option<i32>,
    pub theme_id: Option<i32>,
    pub num_parts: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvOut {
    pub id: i32,
    pub version: i32,
    pub set_num: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LineOut {
    pub inventory_id: i32,
    pub part: u32,
    pub color_id: i32,
    pub quantity: i32,
    pub is_spare: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NestOut {
    pub inventory_id: i32,
    pub set_num: String,
    pub quantity: i32,
}

/// A builder, and where they live: a house on a street, and the home's point in microdegrees.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuilderOut {
    pub builder_id: i32,
    pub name: String,
    pub street_id: i32,
    pub house_number: i32,
    pub latitude: i32,
    pub longitude: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionOut {
    pub builder_id: i32,
    pub row_no: i32,
    pub set_num: String,
    /// The set number as the builder typed it, on the rows that keep the builder's own spelling
    /// (traps K1, K2 and K5).
    pub typed_set_num: Option<String>,
    /// The set's name as the builder typed it (trap K3).
    pub typed_name: Option<String>,
}

/// An instant with the offset it is written in, or the open end of time: `t` whole seconds since
/// 1970-01-01 UTC, and `ms` the milliseconds past them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stamp {
    At { t: i64, ms: u16, offset_min: i32 },
    Infinity,
}

/// One copy a collection row holds, bought: the row is `(builder_id, row_no)`, and names the set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PurchaseOut {
    pub purchase_id: i64,
    pub builder_id: i32,
    pub row_no: i32,
    pub store: &'static str,
    pub ordered_at: Stamp,
    pub ordered_local: String,
    pub delivered_at: Stamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestOut {
    pub trap: Trap,
    pub origin: Origin,
    pub table: &'static str,
    pub row_key: String,
    pub phase: String,
    pub wave: i64,
    pub socket: Option<String>,
    pub detail: String,
}

/// One wave of the set hierarchy: sets, then inventories, then lines and nesting, then the manifest.
#[derive(Clone, Debug, Default)]
pub struct SetWave {
    pub sets: Vec<SetOut>,
    pub inventories: Vec<InvOut>,
    pub lines: Vec<LineOut>,
    pub nests: Vec<NestOut>,
    pub manifest: Vec<ManifestOut>,
    /// Per socket label (or `outside`): sets and lines.
    pub by_socket: HashMap<String, (u64, u64)>,
    /// Per phase label: sets and lines.
    pub by_phase: HashMap<String, (u64, u64)>,
}

/// One wave of the builder hierarchy: builders, then collection rows, then purchases, then the manifest.
#[derive(Clone, Debug, Default)]
pub struct BuilderWave {
    pub builders: Vec<BuilderOut>,
    pub collection: Vec<CollectionOut>,
    pub purchases: Vec<PurchaseOut>,
    pub manifest: Vec<ManifestOut>,
    /// Per collection row, the set it names (see `World::on_sale`) and its purchases.
    pub bought: Vec<(u64, u32)>,
}

/// A pack of the `paired` phase: one of a pair.
#[derive(Clone, Debug)]
pub struct PairedPack {
    pub pair: u64,
    pub first: bool,
    pub pattern: [i32; 5],
    pub partner: [i32; 5],
    pub window: i32,
    pub mirrored: bool,
}

/// What a synthesized set is, before its lines are drawn.
#[derive(Clone, Debug)]
pub struct Head {
    pub set_num: String,
    pub base: String,
    pub name: String,
    pub year: Option<i32>,
    pub theme_id: Option<i32>,
    pub template: u32,
    pub socket: Option<u16>,
    pub trap: Option<(Trap, u64)>,
    pub paired: Option<PairedPack>,
    /// For a pack of the wavy pattern: the year its children's offsets are measured from, and
    /// whether the offsets are mirrored.
    pub wavy_shift: Option<(i32, bool)>,
}

const FIRST_NAMES: [&str; 32] = [
    "Ada", "Bo", "Cy", "Di", "Ed", "Flo", "Gus", "Hana", "Ivo", "Jun", "Kai", "Lea", "Mo", "Nia",
    "Ole", "Pia", "Quin", "Rui", "Sif", "Teo", "Uma", "Vic", "Wen", "Xan", "Yara", "Zeb", "Ana",
    "Ben", "Cleo", "Dag", "Eli", "Fay",
];

/// Stores, with the share of purchases each takes (per thousand) and its opening hours.
const STORES: [(&str, u32, u32, u32, bool); 5] = [
    ("LEGO Store", 300, 10, 19, false),
    ("LEGO online shop", 400, 7, 23, true),
    ("toy shop", 150, 9, 18, false),
    ("second-hand market", 100, 9, 13, false),
    ("LEGO House", 50, 10, 17, false),
];

pub struct World {
    pub real: Real,
    pub wiring: Wiring,
    pub board: Switchboard,
    /// The cities, postcodes and streets the builders live in.
    pub places: Places,
    pub set_traps: HashMap<u64, (Trap, u64)>,
    pub row_traps: HashMap<u64, (Trap, u64)>,
    pub planted: Vec<(Trap, u64)>,
    /// Synthesized unreleased or announced sets, which pre-orders name.
    pub unreleased: Vec<u64>,
    pub real_waves: u64,
    /// Real sets with an inventory, by release year.
    templates_by_year: HashMap<i32, Vec<u32>>,
    /// Real sets by (root theme, year), and by year: where a pack's children are drawn from.
    children_by_root_year: HashMap<(i32, i32), Vec<u32>>,
    children_by_year: HashMap<i32, Vec<u32>>,
    /// Real sets released no later than the first purchase year, or else the earliest real sets:
    /// the sets the clock traps' rows name, so that their purchases can fall in any year after.
    old_sets: Vec<u32>,
    /// Per home zone, the clock's folds and gaps from the release of the old sets to today.
    folds: Vec<Vec<Shift>>,
    gaps: Vec<Vec<Shift>>,
    /// The first years of the windows a paired pack's children are drawn from: eight years, each
    /// with five real sets or more.
    pack_windows: Vec<i32>,
    /// Per real pack, its children's year offsets from its own year, one per nesting row.
    pack_offsets: HashMap<u32, Vec<i32>>,
    /// Per set (a real set by its index, synthesized set `i` at the real sets' count plus `i`), its
    /// release year and root theme, when it is on sale by `TODAY`.
    on_sale: Vec<Option<(i32, Option<i32>)>>,
    /// The sets on sale, which collection rows draw by demand: all of them, and per socket the ones
    /// there.
    pub buyable: Vec<u64>,
    demand_all: Alias,
    demand_by_socket: HashMap<u16, (Vec<u64>, Alias)>,
    /// Per root theme, its spikes.
    spikes: HashMap<i32, Vec<Spike>>,
}

fn zone_index(builder: u64) -> usize {
    (builder % calendar::ZONE_NAMES.len() as u64) as usize
}

fn zone_of(builder: u64) -> &'static Zone {
    &calendar::zones()[zone_index(builder)]
}

/// The first instant of a year, UTC.
fn year_start(y: i64) -> i64 {
    calendar::days_from_civil(y, 1, 1) * 86_400
}

/// The first instant of `TODAY`, UTC.
fn today_start() -> i64 {
    calendar::days_from_civil(TODAY.0, TODAY.1, TODAY.2) * 86_400
}

/// The local midnight a fold reads twice, as a day number, if it reads one: that day begins twice.
fn day_begun_twice(fold: &Shift) -> Option<i64> {
    let repeated =
        fold.at + i64::from(fold.after_min) * 60..fold.at + i64::from(fold.before_min) * 60;
    let day = repeated.start.div_euclid(86_400) + i64::from(repeated.start.rem_euclid(86_400) != 0);
    (fold.is_fold() && repeated.contains(&(day * 86_400))).then_some(day)
}

/// The first synthesized inventory id: past the real ones, on a round hundred thousand.
fn inventory_base(real: &Real) -> i64 {
    (i64::from(real.max_inventory_id) / 100_000 + 1) * 100_000
}

/// The id past every inventory `sets` synthesized sets number.
pub fn inventory_ceiling(real: &Real, sets: u64) -> i64 {
    inventory_base(real) + 2 * sets as i64
}

impl World {
    pub fn new(real: Real, wiring: Wiring, board: Switchboard) -> Result<World, String> {
        let last_inventory = inventory_ceiling(&real, wiring.sets);
        if last_inventory > i64::from(i32::MAX) {
            return Err(format!(
                "{} sets number their inventories up to {last_inventory}, past {}, the largest integer \
                 the tables hold",
                wiring.sets,
                i32::MAX
            ));
        }
        let mut templates_by_year: HashMap<i32, Vec<u32>> = HashMap::new();
        for &t in &real.templates {
            if let (Some(y), Some(_)) =
                (real.cat.sets[t as usize].year, real.set_socket[t as usize])
            {
                templates_by_year.entry(y).or_default().push(t);
            }
        }
        let mut children_by_root_year: HashMap<(i32, i32), Vec<u32>> = HashMap::new();
        let mut children_by_year: HashMap<i32, Vec<u32>> = HashMap::new();
        for (i, s) in real.cat.sets.iter().enumerate() {
            let (Some(y), Some(t)) = (s.year, s.theme_id) else {
                continue;
            };
            if split_version(&s.set_num).is_none() {
                continue;
            }
            children_by_year.entry(y).or_default().push(i as u32);
            if let Some(&root) = real.root_of_theme.get(&t) {
                children_by_root_year
                    .entry((root, y))
                    .or_default()
                    .push(i as u32);
            }
        }
        let year_of = |t: u32| real.cat.sets[t as usize].year.map(i64::from);
        let old_year = real
            .templates
            .iter()
            .filter_map(|&t| year_of(t))
            .min()
            .map_or(FIRST_PURCHASE_YEAR, |y| y.max(FIRST_PURCHASE_YEAR));
        let old_sets: Vec<u32> = real
            .templates
            .iter()
            .copied()
            .filter(|&t| year_of(t).is_some_and(|y| y <= old_year))
            .collect();
        let (folds, gaps): (Vec<Vec<Shift>>, Vec<Vec<Shift>>) = calendar::zones()
            .iter()
            .map(|z| {
                z.shifts(year_start(old_year), today_start())
                    .into_iter()
                    .filter(|s| s.at + s.len_secs() <= today_start())
                    .partition(|s| s.is_fold())
            })
            .unzip();
        let pack_windows: Vec<i32> = (DECADE_LO..=LAST_REAL_YEAR - 7)
            .filter(|w| {
                (*w..*w + 8).all(|y| children_by_year.get(&y).is_some_and(|v| v.len() >= 5))
            })
            .collect();
        let mut pack_offsets: HashMap<u32, Vec<i32>> = HashMap::new();
        for &t in &real.templates {
            let Some(inv) = real.first_inventory(t) else {
                continue;
            };
            let Some(pack_year) = real.cat.sets[t as usize].year else {
                continue;
            };
            let mut offs = Vec::new();
            for n in real.nests(inv.id) {
                let child_year = real
                    .set_by_num
                    .get(&n.set_num)
                    .and_then(|&c| real.cat.sets[c as usize].year);
                offs.push(child_year.map_or(0, |y| y - pack_year));
            }
            if !offs.is_empty() {
                pack_offsets.insert(t, offs);
            }
        }
        let real_waves = (real.cat.sets.len() as u64).div_ceil(wiring.chunk.max(1));
        let places = Places::new(wiring.seed, wiring.builders);
        let mut w = World {
            real,
            wiring,
            board,
            places,
            set_traps: HashMap::new(),
            row_traps: HashMap::new(),
            planted: Vec::new(),
            unreleased: Vec::new(),
            real_waves,
            templates_by_year,
            children_by_root_year,
            children_by_year,
            old_sets,
            folds,
            gaps,
            pack_windows,
            pack_offsets,
            on_sale: Vec::new(),
            buyable: Vec::new(),
            demand_all: Alias::default(),
            demand_by_socket: HashMap::new(),
            spikes: HashMap::new(),
        };
        w.assign_traps()?;
        w.build_demand();
        Ok(w)
    }

    pub fn collection_rows(&self) -> u64 {
        self.wiring.builders * self.wiring.rows_per_builder
    }

    /// The theme a set of trap B3 carries: none for an even ordinal, else a root theme the real
    /// catalogue does not hold, which the generated theme list adds (`added_themes`).
    fn unlisted_theme(&self, k: u64) -> Option<i32> {
        (!k.is_multiple_of(2)).then(|| self.real.max_theme_id + 1 + (k % 7) as i32)
    }

    /// The root themes the generated theme list holds beside the real catalogue's: the ones the
    /// sets of trap B3 carry, each named by its id.
    pub fn added_themes(&self) -> Vec<Theme> {
        let ids: BTreeSet<i32> = self
            .set_traps
            .values()
            .filter(|(t, _)| *t == Trap::B3)
            .filter_map(|&(_, k)| self.unlisted_theme(k))
            .collect();
        ids.into_iter()
            .map(|id| Theme {
                id,
                name: format!("Unlisted theme {id}"),
                parent_id: None,
            })
            .collect()
    }

    fn assign_traps(&mut self) -> Result<(), String> {
        let seed = self.wiring.seed;
        for d in DECLS {
            let (population, map_is_sets) = match d.population {
                Population::Sets => (self.wiring.sets, true),
                Population::CollectionRows => (self.collection_rows(), false),
                Population::Natural | Population::Phase => continue,
            };
            let want = d.planted(population);
            let mut r = Rng::stream(seed, Purpose::TrapPick, d.trap as u64);
            let mut got = 0u64;
            let mut tries = 0u64;
            while got < want {
                tries += 1;
                if tries > 1000 * want + 100_000 {
                    return Err(format!(
                        "trap {}: found {got} of {want} eligible rows in a population of {population}",
                        d.trap
                    ));
                }
                let k = r.below(population.max(1));
                let taken = if map_is_sets {
                    self.set_traps.contains_key(&k)
                } else {
                    self.row_traps.contains_key(&k)
                };
                if taken || !self.eligible(d.trap, k) {
                    continue;
                }
                if map_is_sets {
                    self.set_traps.insert(k, (d.trap, got));
                    if matches!(d.trap, Trap::B2 | Trap::B5) {
                        self.unreleased.push(k);
                    }
                } else {
                    self.row_traps.insert(k, (d.trap, got));
                }
                got += 1;
            }
            self.planted.push((d.trap, got));
        }
        self.unreleased.sort_unstable();
        if self.row_traps.values().any(|(t, _)| *t == Trap::D5) && self.unreleased.is_empty() {
            return Err("pre-orders need an unreleased or announced set".into());
        }
        Ok(())
    }

    fn eligible(&self, trap: Trap, k: u64) -> bool {
        match traps::decl(trap).population {
            Population::Sets => {
                let (lettered, rerelease) = self.kind_raw(k);
                let (base, v) = self.plain_number(k);
                !lettered
                    && !rerelease
                    && !self.in_paired(k)
                    && v == 1
                    && !self.real.keys.contains(&base)
                    && (trap != Trap::K4 || !self.real.non_ascii.is_empty())
                    && (trap != Trap::B4 || !self.real.early.is_empty())
                    && (trap != Trap::B5 || !self.real.modern.is_empty())
            }
            Population::CollectionRows => {
                let zi = zone_index(k / self.wiring.rows_per_builder);
                match trap {
                    Trap::D1 => !self.folds[zi].is_empty(),
                    Trap::D2 => !self.gaps[zi].is_empty(),
                    Trap::D8 => self.folds[zi].iter().any(|f| day_begun_twice(f).is_some()),
                    Trap::K1 => !self.real.lettered.is_empty(),
                    Trap::K3 => self.real.cafe_corner.is_some(),
                    _ => true,
                }
            }
            Population::Natural | Population::Phase => false,
        }
    }

    /// The raw kind draw of synthesized set `i`: lettered, and whether it asks to be a re-release.
    fn kind_raw(&self, i: u64) -> (bool, bool) {
        let mut r = Rng::stream(self.wiring.seed, Purpose::SetKind, i);
        let lettered = r.chance_ppm(self.wiring.lettered_ppm);
        let rerelease = !lettered && r.chance_ppm(self.wiring.rerelease_ppm);
        (lettered, rerelease)
    }

    fn in_paired(&self, i: u64) -> bool {
        self.board.phase_of(i).pattern == Pattern::Paired
    }

    /// Whether synthesized set `i` is the `-2` record of set `i - 1`'s number.
    fn is_rerelease(&self, i: u64) -> bool {
        let (lettered, rerelease) = self.kind_raw(i);
        if lettered || !rerelease || i == 0 || self.in_paired(i) || self.set_traps.contains_key(&i)
        {
            return false;
        }
        let (pl, pr) = self.kind_raw(i - 1);
        !pl && !pr && !self.in_paired(i - 1) && !self.set_traps.contains_key(&(i - 1))
    }

    /// The number a plain synthesized set carries, with its version bumped past any real record. A
    /// number whose bare form is itself a real record moves to the eight-digit range, so that no
    /// synthesized `-1` stands beside a real bare number.
    fn plain_number(&self, i: u64) -> (String, u32) {
        let mut base = base_number(self.wiring.seed, i).to_string();
        if self.real.keys.contains(&base) {
            base = (base_number(self.wiring.seed, i) + 9_000_000).to_string();
        }
        let mut v = 1;
        while self.real.keys.contains(&format!("{base}-{v}")) {
            v += 1;
        }
        (base, v)
    }

    /// The synthesized set `i`, without its lines.
    pub fn head(&self, i: u64) -> Head {
        let seed = self.wiring.seed;
        let real = &self.real;
        let trap = self.set_traps.get(&i).copied();
        let (lettered, _) = self.kind_raw(i);
        let rerelease = self.is_rerelease(i);
        let phase = self.board.phase_of(i);

        if self.in_paired(i) {
            return self.paired_head(i);
        }

        // The socket, and the real set this one is modelled on.
        let mut rs = Rng::stream(seed, Purpose::SetSocket, i);
        let mut cord = Rng::stream(seed, Purpose::SetCord, i);
        let stays = rerelease && cord.unit() >= self.wiring.cross_versions;
        let previous = if stays { self.head(i - 1).socket } else { None };
        let socket = match previous {
            Some(prev) if real.sockets[usize::from(prev)].weight() > 0 => prev,
            _ => self.board.socket_for(i, &mut rs),
        };
        let mut rt = Rng::stream(seed, Purpose::SetTemplate, i);
        let real_year = |t: u32| (t, real.cat.sets[t as usize].year);
        let (template, slot_year) = match trap {
            Some((Trap::B4, _)) => real_year(*rt.pick(&real.early)),
            Some((Trap::B5, _)) => real_year(*rt.pick(&real.modern)),
            Some((Trap::K4, _)) => real_year(*rt.pick(&real.non_ascii)),
            // An inventory filed under the box number must hold lines, or the misfiling loses nothing.
            Some((Trap::K8, _)) => {
                let mut pick = self.slot(socket, &mut rt);
                for _ in 0..64 {
                    if real.first_inventory(pick.0).is_some_and(|inv| {
                        real.lines(inv.id)
                            .iter()
                            .any(|l| real.cat.lists_part(l.part))
                    }) {
                        break;
                    }
                    pick = self.slot(socket, &mut rt);
                }
                pick
            }
            _ => self.slot(socket, &mut rt),
        };
        let t = &real.cat.sets[template as usize];

        // The number.
        let mut rn = Rng::stream(seed, Purpose::SetNumber, i);
        let (set_num, base) = if let Some((Trap::B4, k)) = trap {
            const LETTERS: [&str; 8] = ["G", "H", "J", "K", "L", "M", "N", "P"];
            let mut n = k;
            loop {
                let base = format!("700.{}.{}", LETTERS[(n % 8) as usize], n / 8 + 1);
                let num = format!("{base}-1");
                if !real.keys.contains(&num) {
                    break (num, base);
                }
                n += 8;
            }
        } else if rerelease {
            let (base, v) = self.plain_number(i - 1);
            let mut v = v + 1;
            while real.keys.contains(&format!("{base}-{v}")) {
                v += 1;
            }
            (format!("{base}-{v}"), base)
        } else if lettered {
            let total: u64 = real.prefixes.iter().map(|p| u64::from(p.1)).sum();
            let mut pick = rn.below(total.max(1));
            let mut prefix = "K";
            for (p, c) in &real.prefixes {
                if pick < u64::from(*c) {
                    prefix = p;
                    break;
                }
                pick -= u64::from(*c);
            }
            let base = format!("{prefix}{}", base_number(seed, i));
            let mut v = 1;
            while real.keys.contains(&format!("{base}-{v}")) {
                v += 1;
            }
            (format!("{base}-{v}"), base)
        } else {
            let (base, v) = self.plain_number(i);
            (format!("{base}-{v}"), base)
        };

        // The name: the model's own, or the front of it joined to the back of another set's name
        // from the same theme.
        let mut rname = Rng::stream(seed, Purpose::SetName, i);
        let mut name = t.name.clone();
        if trap.is_none() && rname.unit() < 0.5 {
            if let Some(peers) = t.theme_id.and_then(|th| real.theme_sets.get(&th)) {
                let other = &real.cat.sets[*rname.pick(peers) as usize].name;
                let word = |w: &&str| !w.is_empty() && w.chars().any(char::is_alphanumeric);
                let a: Vec<&str> = t.name.split(' ').filter(word).collect();
                let b: Vec<&str> = other.split(' ').filter(word).collect();
                if a.len() >= 2 && b.len() >= 2 {
                    let front = &a[..a.len().div_ceil(2)];
                    let mut back = &b[b.len() / 2..];
                    if front.last() == back.first() {
                        back = &back[1..];
                    }
                    let joined = front
                        .iter()
                        .chain(back.iter())
                        .copied()
                        .collect::<Vec<_>>()
                        .join(" ");
                    let repeats = joined
                        .split(' ')
                        .collect::<Vec<_>>()
                        .windows(2)
                        .any(|w| w[0] == w[1]);
                    if joined.len() <= 255 && !back.is_empty() && !repeats {
                        name = joined;
                    }
                }
            }
        }
        let mut year = slot_year;
        let mut theme_id = t.theme_id;
        match trap {
            Some((Trap::K4, _)) => name = text::latin1_round_trip(&name),
            Some((Trap::B2, _)) => year = None,
            Some((Trap::B3, k)) => theme_id = self.unlisted_theme(k),
            Some((Trap::B4, _)) => year = Some(1949),
            Some((Trap::B5, _)) => year = Some(2031),
            _ => {}
        }

        // A pack of the wavy pattern: its year moves within its decade with the wave, and its
        // children's year offsets are mirrored on the way back.
        let mut wavy_shift = None;
        if trap.is_none()
            && phase.pattern == Pattern::Wavy
            && self.pack_offsets.contains_key(&template)
        {
            if let Some(y) = slot_year {
                let period = self.board.dials.wavy_period.max(1);
                let wave = (i - phase.start) / self.board.chunk;
                let pos = (wave % (2 * period)) as f64 / period as f64;
                let (centre, mirrored) = if pos < 1.0 {
                    (pos, false)
                } else {
                    (2.0 - pos, true)
                };
                let digit = ((centre * 10.0) as i32).min(9);
                let moved = y - y.rem_euclid(10) + digit;
                let offsets = &self.pack_offsets[&template];
                let fits = offsets.iter().all(|d| {
                    let c = if mirrored { moved - d } else { moved + d };
                    self.children_by_year.contains_key(&c)
                });
                if fits && (DECADE_LO..=LAST_REAL_YEAR).contains(&moved) && is_release_year(moved) {
                    year = Some(moved);
                    wavy_shift = Some((moved, mirrored));
                }
            }
        }

        let socket_final = match (
            theme_id.and_then(|th| real.root_of_theme.get(&th)),
            year.and_then(decade_of),
        ) {
            (Some(&root), Some(dec)) => real
                .sockets
                .iter()
                .position(|s| s.root == root && s.decade == dec)
                .map(|p| p as u16),
            _ => real.set_socket[template as usize],
        };

        Head {
            set_num,
            base,
            name,
            year,
            theme_id,
            template,
            socket: socket_final,
            trap,
            paired: None,
            wavy_shift,
        }
    }

    fn paired_head(&self, i: u64) -> Head {
        let seed = self.wiring.seed;
        let real = &self.real;
        let phase = self.board.phase_of(i);
        let k = i - phase.start;
        let pair = k / 2;
        let first = k.is_multiple_of(2);
        let (a, b) = YEAR_PAIRS[(pair % 3) as usize];
        let (pattern, partner) = if first { (a, b) } else { (b, a) };
        let wave = (pair * 2) / self.board.chunk;
        let period = self.board.dials.wavy_period.max(1);
        let pos = (wave % (2 * period)) as f64 / period as f64;
        let (centre, mirrored) = if pos < 1.0 {
            (pos, false)
        } else {
            (2.0 - pos, true)
        };
        let window = if self.pack_windows.is_empty() {
            DECADE_LO
        } else {
            let n = self.pack_windows.len();
            self.pack_windows[((centre * n as f64) as usize).min(n - 1)]
        };
        let offsets = paired::placed(&pattern, mirrored);
        let pack_year = window + offsets.iter().copied().max().unwrap_or(0);
        let mut rt = Rng::stream(seed, Purpose::SetTemplate, i);
        let pool = self
            .templates_by_year
            .get(&pack_year)
            .map(|v| v.as_slice())
            .filter(|v| !v.is_empty())
            .unwrap_or(&real.templates);
        let packs: Vec<u32> = pool
            .iter()
            .copied()
            .filter(|t| self.pack_offsets.contains_key(t))
            .collect();
        let template = if packs.is_empty() {
            *rt.pick(pool)
        } else {
            *rt.pick(&packs)
        };
        let t = &real.cat.sets[template as usize];
        let (base, v) = self.plain_number(i);
        let socket = match (
            t.theme_id.and_then(|th| real.root_of_theme.get(&th)),
            decade_of(pack_year),
        ) {
            (Some(&root), Some(dec)) => real
                .sockets
                .iter()
                .position(|s| s.root == root && s.decade == dec)
                .map(|p| p as u16),
            _ => None,
        };
        Head {
            set_num: format!("{base}-{v}"),
            base,
            name: t.name.clone(),
            year: Some(pack_year),
            theme_id: t.theme_id,
            template,
            socket,
            trap: None,
            paired: Some(PairedPack {
                pair,
                first,
                pattern,
                partner,
                window,
                mirrored,
            }),
            wavy_shift: None,
        }
    }

    /// The real set a synthesized set in `socket` is modelled on, and the year it takes, drawn by the
    /// socket's weight: one of the socket's real sets with its own year, or one of its modelled years
    /// with a real set of its root theme from the years they are modelled on.
    fn slot(&self, socket: u16, r: &mut Rng) -> (u32, Option<i32>) {
        let s = &self.real.sockets[usize::from(socket)];
        let mut k = r.below(s.weight());
        if let Some(&t) = s.templates.get(k as usize) {
            return (t, self.real.cat.sets[t as usize].year);
        }
        k -= s.templates.len() as u64;
        for &(year, w) in &s.modelled {
            if k < w {
                return (*r.pick(&self.real.model_pool[&s.root]), Some(year));
            }
            k -= w;
        }
        unreachable!("a draw below the socket's weight names a slot")
    }

    /// The real child set of a given year: from the pack's root theme
    /// unless the nesting cord crosses.
    fn child_for(
        &self,
        root: Option<i32>,
        year: i32,
        r: &mut Rng,
        taken: &HashSet<u32>,
    ) -> Option<u32> {
        let crosses = r.unit() < self.wiring.cross_nesting;
        let own = root.and_then(|rt| self.children_by_root_year.get(&(rt, year)));
        let pool = match own {
            Some(v) if !crosses && !v.is_empty() => v,
            _ => self.children_by_year.get(&year)?,
        };
        for _ in 0..16 {
            let c = *r.pick(pool);
            if !taken.contains(&c) {
                return Some(c);
            }
        }
        pool.iter().copied().find(|c| !taken.contains(c))
    }

    fn socket_label(&self, s: Option<u16>) -> Option<String> {
        s.map(|s| self.real.sockets[usize::from(s)].label())
    }

    fn phase_label(&self, i: u64) -> String {
        self.board.phase_of(i).label()
    }

    /// The real catalogue's wave `w`: real sets `w * chunk ..`, passed through unchanged.
    pub fn real_wave(&self, w: u64) -> SetWave {
        let real = &self.real;
        let chunk = self.wiring.chunk.max(1) as usize;
        let lo = (w as usize) * chunk;
        let hi = (lo + chunk).min(real.cat.sets.len());
        let mut out = SetWave::default();
        let wave_no = w as i64;
        for s in lo..hi {
            let set = &real.cat.sets[s];
            out.sets.push(SetOut {
                set_num: set.set_num.clone(),
                name: set.name.clone(),
                year: set.year,
                theme_id: set.theme_id,
                num_parts: set.num_parts,
            });
            let sock = real.set_socket[s];
            let label = self.socket_label(sock).unwrap_or_else(|| "outside".into());
            let mut lines = 0u64;
            for &k in &real.inventories_of[s] {
                let inv = &real.cat.inventories[k as usize];
                out.inventories.push(InvOut {
                    id: inv.id,
                    version: inv.version,
                    set_num: inv.set_num.clone(),
                });
                for l in real.lines(inv.id) {
                    out.lines.push(LineOut {
                        inventory_id: l.inventory_id,
                        part: l.part,
                        color_id: l.color_id,
                        quantity: l.quantity,
                        is_spare: l.is_spare,
                    });
                    lines += 1;
                    self.classify_line(
                        &mut out.manifest,
                        Origin::Real,
                        "real",
                        wave_no,
                        &sock,
                        l.inventory_id,
                        l.part,
                        l.color_id,
                        l.is_spare,
                    );
                }
                for n in real.nests(inv.id) {
                    out.nests.push(NestOut {
                        inventory_id: n.inventory_id,
                        set_num: n.set_num.clone(),
                        quantity: n.quantity,
                    });
                }
            }
            let e = out.by_socket.entry(label).or_default();
            e.0 += 1;
            e.1 += lines;
            let p = out.by_phase.entry("0:real".into()).or_default();
            p.0 += 1;
            p.1 += lines;
            self.classify_set(
                &mut out.manifest,
                Origin::Real,
                "0:real",
                wave_no,
                sock,
                set,
            );
        }
        // Real traps a set carries by itself: a bare number beside its -1, a part count of -1, a
        // name after a Latin-1 round trip.
        for s in lo..hi {
            let set = &real.cat.sets[s];
            let sock = self.socket_label(real.set_socket[s]);
            if split_version(&set.set_num).is_none()
                && real.keys.contains(&format!("{}-1", set.set_num))
            {
                out.manifest.push(manifest(
                    Trap::K7,
                    Origin::Real,
                    "lego_sets",
                    &set.set_num,
                    "0:real",
                    wave_no,
                    sock.clone(),
                    format!("twin of {}-1", set.set_num),
                ));
            }
            if set.num_parts == Some(-1) {
                out.manifest.push(manifest(
                    Trap::B6,
                    Origin::Real,
                    "lego_sets",
                    &set.set_num,
                    "0:real",
                    wave_no,
                    sock.clone(),
                    String::new(),
                ));
            }
            if looks_round_tripped(&set.name) {
                out.manifest.push(manifest(
                    Trap::K4,
                    Origin::Real,
                    "lego_sets",
                    &set.set_num,
                    "0:real",
                    wave_no,
                    sock.clone(),
                    String::new(),
                ));
            }
            if let Ok(k) = real.redated.binary_search_by_key(&(s as u32), |r| r.0) {
                out.manifest.push(manifest(
                    Trap::B9,
                    Origin::Real,
                    "lego_sets",
                    &set.set_num,
                    "0:real",
                    wave_no,
                    sock,
                    format!("released in {}", real.redated[k].1),
                ));
            }
        }
        let mut r = Rng::stream(self.wiring.seed, Purpose::Shuffle, w);
        r.shuffle(&mut out.sets);
        r.shuffle(&mut out.inventories);
        r.shuffle(&mut out.lines);
        out
    }

    /// Natural traps a set carries: a release year on a decade endpoint, a lettered number.
    fn classify_set(
        &self,
        m: &mut Vec<ManifestOut>,
        origin: Origin,
        phase: &str,
        wave: i64,
        sock: Option<u16>,
        set: &crate::catalogue::SetRec,
    ) {
        let label = self.socket_label(sock);
        if let Some(y) = set.year {
            if y > DECADE_LO && y < 2030 && y % 10 == 0 {
                m.push(manifest(
                    Trap::B1,
                    origin,
                    "lego_sets",
                    &set.set_num,
                    phase,
                    wave,
                    label.clone(),
                    format!("year {y}"),
                ));
            }
        }
        if text::is_lettered(&set.set_num) {
            m.push(manifest(
                Trap::O2,
                origin,
                "lego_sets",
                &set.set_num,
                phase,
                wave,
                label,
                String::new(),
            ));
        }
    }

    /// Natural traps a line carries: a sentinel colour, a part number whose text and number readings
    /// disagree about the basic bricks 3001 to 3010, a part number the parts list does not hold.
    #[allow(clippy::too_many_arguments)]
    fn classify_line(
        &self,
        m: &mut Vec<ManifestOut>,
        origin: Origin,
        phase: &str,
        wave: i64,
        sock: &Option<u16>,
        inv: i32,
        part: u32,
        colour: i32,
        spare: bool,
    ) {
        let p = &self.real.cat.part_pool[part as usize];
        let key = || format!("{inv}|{p}|{colour}|{}", if spare { 't' } else { 'f' });
        if colour == -1 || colour == 9999 {
            m.push(manifest(
                Trap::B7,
                origin,
                "lego_inventory_parts",
                &key(),
                phase,
                wave,
                self.socket_label(*sock),
                format!("colour {colour}"),
            ));
        }
        if basic_brick_disagreement(p) {
            m.push(manifest(
                Trap::K6,
                origin,
                "lego_inventory_parts",
                &key(),
                phase,
                wave,
                self.socket_label(*sock),
                String::new(),
            ));
        }
        if !self.real.cat.lists_part(part) {
            m.push(manifest(
                Trap::B10,
                origin,
                "lego_inventory_parts",
                &key(),
                phase,
                wave,
                self.socket_label(*sock),
                String::new(),
            ));
        }
    }

    /// Synthesized sets `lo..hi` as one wave.
    pub fn set_wave(&self, w: u64, lo: u64, hi: u64) -> SetWave {
        let real = &self.real;
        let seed = self.wiring.seed;
        let mut out = SetWave::default();
        let wave_no = w as i64;
        let inv_base = inventory_base(real);
        let mut scratch: Vec<(u32, i32, bool, i32)> = Vec::new();
        for i in lo..hi {
            let h = self.head(i);
            let phase = self.phase_label(i);
            let sock_label = self.socket_label(h.socket);
            let t = h.template;
            let tinv = real.first_inventory(t);
            let inv_id = (inv_base + 2 * i as i64) as i32;
            let mut rl = Rng::stream(seed, Purpose::SetLines, i);

            // Lines: the model's lowest-version inventory, a share recoloured into a colour the part
            // is known in, merged back onto the line key. A line naming a part number the parts
            // list does not hold is the real catalogue's own, and is not copied.
            scratch.clear();
            if let Some(inv) = tinv {
                for l in real.lines(inv.id) {
                    if !real.cat.lists_part(l.part) {
                        continue;
                    }
                    let mut colour = l.color_id;
                    if rl.chance_ppm(self.wiring.recolour_ppm) {
                        if let Some(cs) = real.part_colours.get(&l.part) {
                            colour = *rl.pick(cs);
                        }
                    }
                    scratch.push((l.part, colour, l.is_spare, l.quantity));
                }
            }
            scratch.sort_unstable_by(|a, b| {
                real.cat.part_pool[a.0 as usize]
                    .as_bytes()
                    .cmp(real.cat.part_pool[b.0 as usize].as_bytes())
                    .then(a.1.cmp(&b.1))
                    .then(a.2.cmp(&b.2))
            });
            let mut merged: Vec<(u32, i32, bool, i32)> = Vec::with_capacity(scratch.len());
            for &(p, c, s, q) in &scratch {
                match merged.last_mut() {
                    Some(last) if last.0 == p && last.1 == c && last.2 == s => last.3 += q,
                    _ => merged.push((p, c, s, q)),
                }
            }

            // Nesting: the model's children. Each keeps its year offset from the pack (moved with the
            // wave, and mirrored on its way back) and is drawn from
            // the real sets of that year.
            let root = h
                .theme_id
                .and_then(|th| real.root_of_theme.get(&th))
                .copied();
            let mut nests: Vec<NestOut> = Vec::new();
            let mut rc = Rng::stream(seed, Purpose::SetCord, i ^ 0x5eed);
            let mut taken: HashSet<u32> = HashSet::new();
            if let Some(p) = &h.paired {
                for offset in paired::placed(&p.pattern, p.mirrored) {
                    if let Some(c) = self.child_for(root, p.window + offset, &mut rc, &taken) {
                        taken.insert(c);
                        nests.push(NestOut {
                            inventory_id: inv_id,
                            set_num: real.cat.sets[c as usize].set_num.clone(),
                            quantity: 1,
                        });
                    }
                }
            } else if let Some(inv) = tinv {
                let offsets = self.pack_offsets.get(&t);
                for (k, n) in real.nests(inv.id).iter().enumerate() {
                    let d = offsets.map_or(0, |o| o[k]);
                    let child_year = match (h.wavy_shift, h.year) {
                        (Some((moved, mirrored)), _) => {
                            Some(if mirrored { moved - d } else { moved + d })
                        }
                        (None, Some(y)) if real.set_by_num.contains_key(&n.set_num) => Some(y + d),
                        _ => None,
                    };
                    let child = child_year.and_then(|y| self.child_for(root, y, &mut rc, &taken));
                    let set_num = match child {
                        Some(c) => {
                            taken.insert(c);
                            real.cat.sets[c as usize].set_num.clone()
                        }
                        None => n.set_num.clone(),
                    };
                    match nests.iter_mut().find(|x| x.set_num == set_num) {
                        Some(x) => x.quantity += n.quantity,
                        None => nests.push(NestOut {
                            inventory_id: inv_id,
                            set_num,
                            quantity: n.quantity,
                        }),
                    }
                }
            }

            let non_spare: i64 = merged.iter().filter(|l| !l.2).map(|l| i64::from(l.3)).sum();
            let nested: i64 = nests.iter().map(|n| i64::from(n.quantity)).sum();
            let mut num_parts = Some((non_spare + nested).min(i64::from(i32::MAX)) as i32);
            if matches!(h.trap, Some((Trap::B6, _))) {
                num_parts = Some(-1);
            }

            let set = SetOut {
                set_num: h.set_num.clone(),
                name: h.name.clone(),
                year: h.year,
                theme_id: h.theme_id,
                num_parts,
            };
            let inv_set_num = if matches!(h.trap, Some((Trap::K8, _))) {
                h.base.clone()
            } else {
                h.set_num.clone()
            };
            out.inventories.push(InvOut {
                id: inv_id,
                version: 1,
                set_num: inv_set_num.clone(),
            });
            for &(p, c, s, q) in &merged {
                out.lines.push(LineOut {
                    inventory_id: inv_id,
                    part: p,
                    color_id: c,
                    quantity: q,
                    is_spare: s,
                });
                self.classify_line(
                    &mut out.manifest,
                    Origin::Synthetic,
                    &phase,
                    wave_no,
                    &h.socket,
                    inv_id,
                    p,
                    c,
                    s,
                );
            }
            let mut lines = merged.len() as u64;
            // A second, revised inventory: one line's count corrected.
            if h.trap.is_none()
                && h.paired.is_none()
                && rl.chance_ppm(self.wiring.second_version_ppm)
                && !merged.is_empty()
            {
                let v2 = inv_id + 1;
                out.inventories.push(InvOut {
                    id: v2,
                    version: 2,
                    set_num: h.set_num.clone(),
                });
                let fix = rl.below(merged.len() as u64) as usize;
                for (k, &(p, c, s, q)) in merged.iter().enumerate() {
                    let q = if k == fix { q + 1 } else { q };
                    out.lines.push(LineOut {
                        inventory_id: v2,
                        part: p,
                        color_id: c,
                        quantity: q,
                        is_spare: s,
                    });
                    self.classify_line(
                        &mut out.manifest,
                        Origin::Synthetic,
                        &phase,
                        wave_no,
                        &h.socket,
                        v2,
                        p,
                        c,
                        s,
                    );
                }
                lines += merged.len() as u64;
            }
            out.nests.extend(nests.iter().cloned());

            let label = sock_label.clone().unwrap_or_else(|| "outside".into());
            let placed_label = match (
                h.theme_id.and_then(|th| real.root_of_theme.get(&th)),
                h.year.and_then(decade_of),
            ) {
                (Some(_), Some(_)) => label,
                _ => "outside".into(),
            };
            let e = out.by_socket.entry(placed_label).or_default();
            e.0 += 1;
            e.1 += lines;
            let p = out.by_phase.entry(phase.clone()).or_default();
            p.0 += 1;
            p.1 += lines;

            let rec = crate::catalogue::SetRec {
                set_num: set.set_num.clone(),
                name: set.name.clone(),
                year: set.year,
                theme_id: set.theme_id,
                num_parts: set.num_parts,
            };
            self.classify_set(
                &mut out.manifest,
                Origin::Synthetic,
                &phase,
                wave_no,
                h.socket,
                &rec,
            );
            if let Some((trap, k)) = h.trap {
                let (table, key, detail) = match trap {
                    Trap::K8 => (
                        "lego_inventories",
                        inv_id.to_string(),
                        format!("filed under {} for {}", h.base, h.set_num),
                    ),
                    _ => ("lego_sets", h.set_num.clone(), format!("ordinal {k}")),
                };
                out.manifest.push(manifest(
                    trap,
                    Origin::Synthetic,
                    table,
                    &key,
                    &phase,
                    wave_no,
                    sock_label.clone(),
                    detail,
                ));
                if trap == Trap::B3 && h.theme_id.is_none() {
                    out.manifest.push(manifest(
                        Trap::B8,
                        Origin::Synthetic,
                        "lego_sets",
                        &h.set_num,
                        &phase,
                        wave_no,
                        sock_label.clone(),
                        "theme_id is NULL".into(),
                    ));
                }
            }
            if let Some(p) = &h.paired {
                out.manifest.push(manifest(
                    Trap::O5,
                    Origin::Synthetic,
                    "lego_sets",
                    &h.set_num,
                    &phase,
                    wave_no,
                    sock_label.clone(),
                    format!(
                        "pair={} member={} offsets={} partner_offsets={} window={} mirrored={}",
                        p.pair,
                        if p.first { "first" } else { "second" },
                        paired::label(&p.pattern),
                        paired::label(&p.partner),
                        p.window,
                        p.mirrored,
                    ),
                ));
            }
            out.sets.push(set);
            // A bare record beside the -1: the number on the box, filed as a set of its own. A K8
            // set's inventory is filed under it, with the set's own theme and part count.
            let twin = match h.trap {
                Some((Trap::K7, k)) => {
                    let mut rtw = Rng::stream(seed, Purpose::SetCord, i ^ 0x7719);
                    let crosses = rtw.unit() < self.wiring.cross_twins;
                    let theme = match (crosses, h.theme_id) {
                        (false, Some(th)) => real
                            .parent_of_theme
                            .get(&th)
                            .copied()
                            .flatten()
                            .or(Some(th)),
                        (true, _) => real.cat.sets[*rtw.pick(&real.templates) as usize].theme_id,
                        (false, None) => None,
                    };
                    Some((
                        theme,
                        if k.is_multiple_of(2) {
                            Some(0)
                        } else {
                            num_parts.map(|n| (n - 1).max(0))
                        },
                        format!("twin of {}", h.set_num),
                    ))
                }
                Some((Trap::K8, _)) => Some((
                    h.theme_id,
                    num_parts,
                    format!("twin of {}, holding its inventory", h.set_num),
                )),
                _ => None,
            };
            if let Some((theme, twin_parts, detail)) = twin {
                let twin = crate::catalogue::SetRec {
                    set_num: h.base.clone(),
                    name: h.name.clone(),
                    year: h.year,
                    theme_id: theme,
                    num_parts: twin_parts,
                };
                // The bare record is a set row of its own: it carries the traps its year and
                // number carry, and counts in its phase and socket.
                self.classify_set(
                    &mut out.manifest,
                    Origin::Synthetic,
                    &phase,
                    wave_no,
                    h.socket,
                    &twin,
                );
                let twin_socket = match (
                    twin.theme_id.and_then(|th| real.root_of_theme.get(&th)),
                    twin.year.and_then(decade_of),
                ) {
                    (Some(&root), Some(dec)) => real
                        .sockets
                        .iter()
                        .find(|s| s.root == root && s.decade == dec)
                        .map(|s| s.label()),
                    _ => None,
                };
                out.by_socket
                    .entry(twin_socket.unwrap_or_else(|| "outside".into()))
                    .or_default()
                    .0 += 1;
                out.by_phase.entry(phase.clone()).or_default().0 += 1;
                out.sets.push(SetOut {
                    set_num: twin.set_num,
                    name: twin.name,
                    year: twin.year,
                    theme_id: twin.theme_id,
                    num_parts: twin.num_parts,
                });
                out.manifest.push(manifest(
                    Trap::K7,
                    Origin::Synthetic,
                    "lego_sets",
                    &h.base,
                    &phase,
                    wave_no,
                    sock_label,
                    detail,
                ));
            }
        }
        let mut r = Rng::stream(seed, Purpose::Shuffle, w);
        r.shuffle(&mut out.sets);
        r.shuffle(&mut out.inventories);
        r.shuffle(&mut out.lines);
        out
    }

    /// Which sets are on sale, their demand, and the tables collection rows draw them from.
    fn build_demand(&mut self) {
        let seed = self.wiring.seed;
        let today = calendar::days_from_civil(TODAY.0, TODAY.1, TODAY.2);
        let first_day = calendar::days_from_civil(FIRST_PURCHASE_YEAR, 1, 1);
        let mut roots: Vec<i32> = self.real.sockets.iter().map(|s| s.root).collect();
        roots.dedup();
        self.spikes = roots
            .into_iter()
            .map(|root| {
                let s = demand::theme_spikes(seed, root, FIRST_PURCHASE_YEAR, TODAY.0);
                (root, s)
            })
            .collect();
        let placed: Vec<(Option<i32>, Option<i32>, Option<u16>)> = {
            let real = &self.real;
            let root = |theme: Option<i32>| theme.and_then(|t| real.root_of_theme.get(&t)).copied();
            // A bare record beside its -1 is the same set filed twice, and it is sold as its -1.
            let reals = (0..real.cat.sets.len()).map(|s| {
                let set = &real.cat.sets[s];
                let twin = split_version(&set.set_num).is_none()
                    && real.keys.contains(&format!("{}-1", set.set_num));
                let year = set.year.filter(|_| !twin);
                (year, root(set.theme_id), real.set_socket[s])
            });
            let synthetic = (0..self.wiring.sets).map(|i| {
                let h = self.head(i);
                (h.year, root(h.theme_id), h.socket)
            });
            reals.chain(synthetic).collect()
        };
        let mut on_sale = Vec::with_capacity(placed.len());
        let (mut buyable, mut weights) = (Vec::new(), Vec::new());
        let mut by_socket: HashMap<u16, (Vec<u64>, Vec<f64>)> = HashMap::new();
        for (set, &(year, root, socket)) in placed.iter().enumerate() {
            let set = set as u64;
            let spikes = root
                .and_then(|r| self.spikes.get(&r))
                .map_or(&[][..], Vec::as_slice);
            let timeline = year
                .filter(|&y| i64::from(y) <= TODAY.0)
                .and_then(|y| demand::timeline(seed, set, y, spikes, first_day, today));
            let Some(t) = timeline else {
                on_sale.push(None);
                continue;
            };
            on_sale.push(Some((year.expect("on sale"), root)));
            let w = demand::popularity(seed, set) * t.amount();
            buyable.push(set);
            weights.push(w);
            if let Some(s) = socket {
                let e = by_socket.entry(s).or_default();
                e.0.push(set);
                e.1.push(w);
            }
        }
        self.on_sale = on_sale;
        self.demand_all = Alias::new(&weights);
        self.buyable = buyable;
        self.demand_by_socket = by_socket
            .into_iter()
            .map(|(s, (sets, w))| (s, (sets, Alias::new(&w))))
            .collect();
    }

    /// The timeline of set `set` (see `on_sale`), if it is on sale by `TODAY`.
    pub fn timeline_of(&self, set: u64) -> Option<Timeline> {
        let (year, root) = (*self.on_sale.get(set as usize)?)?;
        let spikes = root
            .and_then(|r| self.spikes.get(&r))
            .map_or(&[][..], Vec::as_slice);
        demand::timeline(
            self.wiring.seed,
            set,
            year,
            spikes,
            calendar::days_from_civil(FIRST_PURCHASE_YEAR, 1, 1),
            calendar::days_from_civil(TODAY.0, TODAY.1, TODAY.2),
        )
    }

    /// The number and year of set `set` (see `on_sale`).
    fn set_named(&self, set: u64) -> (String, Option<i32>) {
        let reals = self.real.cat.sets.len() as u64;
        if set < reals {
            let s = &self.real.cat.sets[set as usize];
            (s.set_num.clone(), s.year)
        } else {
            let h = self.head(set - reals);
            (h.set_num, h.year)
        }
    }

    /// A set a builder buys, drawn by demand: from the builder's home socket unless the collection
    /// cord crosses.
    fn bought_set(&self, home: u16, r: &mut Rng) -> u64 {
        let crosses = r.unit() < self.wiring.cross_collections;
        match (crosses, self.demand_by_socket.get(&home)) {
            (false, Some((sets, alias))) => sets[alias.draw(r)],
            _ => self.buyable[self.demand_all.draw(r)],
        }
    }

    /// Builders `lo..hi` as one wave.
    pub fn builder_wave(&self, w: u64, lo: u64, hi: u64) -> BuilderWave {
        let real = &self.real;
        let seed = self.wiring.seed;
        let rpb = self.wiring.rows_per_builder;
        let mut out = BuilderWave::default();
        let wave_no = w as i64;
        for j in lo..hi {
            let zone = zone_of(j);
            let mut rb = Rng::stream(seed, Purpose::Builder, j);
            let home = self.board_natural(&mut rb);
            let builder_id = (j + 1) as i32;
            let name = format!(
                "{} {}.",
                FIRST_NAMES[(j % 32) as usize],
                char::from(b'A' + ((j / 32) % 26) as u8)
            );
            let lives = self
                .places
                .home(zone_index(j), &mut Rng::stream(seed, Purpose::Address, j));
            out.builders.push(BuilderOut {
                builder_id,
                name,
                street_id: lives.street_id,
                house_number: lives.house_number,
                latitude: lives.latitude,
                longitude: lives.longitude,
            });
            for row in 0..rpb {
                let c = j * rpb + row;
                let row_no = (row + 1) as i32;
                let trap = self.row_traps.get(&c).copied();
                let mut r = Rng::stream(seed, Purpose::Collection, c);
                let set = match trap {
                    Some((Trap::K1, _)) => u64::from(*r.pick(&real.lettered)),
                    Some((Trap::K3, _)) => u64::from(real.cafe_corner.expect("eligible")),
                    Some((Trap::D5, _)) => real.cat.sets.len() as u64 + *r.pick(&self.unreleased),
                    Some((
                        Trap::D1 | Trap::D2 | Trap::D3 | Trap::D4 | Trap::D6 | Trap::D7 | Trap::D8,
                        _,
                    )) => u64::from(*r.pick(&self.old_sets)),
                    _ => self.bought_set(home, &mut r),
                };
                let (set_num, set_year) = self.set_named(set);
                let typed_set_num = match trap {
                    Some((Trap::K1, k)) => Some(text::case_variant(&set_num, k)),
                    Some((Trap::K2, k)) => Some(text::whitespace_variant(&set_num, k)),
                    Some((Trap::K5, k)) => Some(text::dash_variant(&set_num, k)),
                    _ => None,
                };
                let typed_name = match trap {
                    Some((Trap::K3, k)) => Some(if k % 2 == 0 {
                        "Café Corner".to_string()
                    } else {
                        text::nfd_latin1("Café Corner")
                    }),
                    _ => None,
                };
                // The copies the row holds, each one a purchase.
                let copies: u64 = match trap {
                    Some((Trap::D1, _)) => 2,
                    Some((Trap::D5, _)) => 1,
                    _ => match r.below(100) {
                        0..=79 => 1,
                        80..=94 => 2,
                        _ => 3,
                    },
                };
                out.collection.push(CollectionOut {
                    builder_id,
                    row_no,
                    set_num: set_num.clone(),
                    typed_set_num,
                    typed_name,
                });
                let row_key = format!("{builder_id}|{row_no}");
                if let Some((t @ (Trap::K1 | Trap::K2 | Trap::K3 | Trap::K5), _)) = trap {
                    out.manifest.push(manifest(
                        t,
                        Origin::Synthetic,
                        "lego_collection",
                        &row_key,
                        "builders",
                        wave_no,
                        None,
                        format!("canonical {set_num}"),
                    ));
                }
                let mut rp = Rng::stream(seed, Purpose::Purchase, c);
                let mut rs = Rng::stream(seed, Purpose::Seconds, c);
                let first_year = set_year.map_or(FIRST_PURCHASE_YEAR, |y| {
                    i64::from(y).max(FIRST_PURCHASE_YEAR)
                });
                let stamps = self.purchase_times(
                    zone_index(j),
                    trap,
                    set,
                    first_year,
                    copies,
                    &mut rp,
                    &mut rs,
                );
                out.bought.push((set, stamps.len() as u32));
                for (k, (store, ordered, local, delivered)) in stamps.into_iter().enumerate() {
                    let purchase_id = (c * 4 + k as u64 + 1) as i64;
                    let p = PurchaseOut {
                        purchase_id,
                        builder_id,
                        row_no,
                        store,
                        ordered_at: ordered,
                        ordered_local: local,
                        delivered_at: delivered,
                    };
                    let key = purchase_id.to_string();
                    if let Some((t @ (Trap::D1 | Trap::D2 | Trap::D4 | Trap::D5), _)) = trap {
                        if k == 0 || t == Trap::D1 {
                            out.manifest.push(manifest(
                                t,
                                Origin::Synthetic,
                                "lego_purchases",
                                &key,
                                "builders",
                                wave_no,
                                None,
                                zone.name.to_string(),
                            ));
                        }
                    }
                    if let Stamp::At { t, offset_min, .. } = p.ordered_at {
                        let local_days = (t + i64::from(offset_min) * 60).div_euclid(86_400);
                        let (ly, lm, ld) = calendar::civil_from_days(local_days);
                        let (uy, um, _) = calendar::civil_from_days(t.div_euclid(86_400));
                        if (ly, lm) != (uy, um) {
                            out.manifest.push(manifest(
                                Trap::D3,
                                Origin::Synthetic,
                                "lego_purchases",
                                &key,
                                "builders",
                                wave_no,
                                None,
                                zone.name.to_string(),
                            ));
                        }
                        if lm == 2 && ld == 29 {
                            out.manifest.push(manifest(
                                Trap::D6,
                                Origin::Synthetic,
                                "lego_purchases",
                                &key,
                                "builders",
                                wave_no,
                                None,
                                zone.name.to_string(),
                            ));
                        }
                        if calendar::iso_week(local_days).0 != ly {
                            out.manifest.push(manifest(
                                Trap::D7,
                                Origin::Synthetic,
                                "lego_purchases",
                                &key,
                                "builders",
                                wave_no,
                                None,
                                zone.name.to_string(),
                            ));
                        }
                        if let LocalTime::Twice(..) =
                            zone.instants_of(calendar::local_secs(local_days, 0, 0))
                        {
                            out.manifest.push(manifest(
                                Trap::D8,
                                Origin::Synthetic,
                                "lego_purchases",
                                &key,
                                "builders",
                                wave_no,
                                None,
                                zone.name.to_string(),
                            ));
                        }
                    }
                    out.purchases.push(p);
                }
            }
        }
        let mut r = Rng::stream(seed, Purpose::Shuffle, w ^ 0xB11D);
        r.shuffle(&mut out.collection);
        r.shuffle(&mut out.purchases);
        out
    }

    /// Traps that belong to a whole table or to an absence rather than to a row, and the root themes
    /// the generated theme list adds.
    pub fn plan_manifest(&self) -> Vec<ManifestOut> {
        let mut m = Vec::new();
        for table in [
            "lego_sets",
            "lego_inventories",
            "lego_inventory_parts",
            "lego_inventory_sets",
            "lego_collection",
            "lego_purchases",
        ] {
            m.push(manifest(
                Trap::O1,
                Origin::Plan,
                table,
                "*",
                "plan",
                -1,
                None,
                "rows written in a shuffled order within each wave".into(),
            ));
        }
        m.push(manifest(
            Trap::B9,
            Origin::Plan,
            "lego_sets",
            "*",
            "plan",
            -1,
            None,
            "no set released in 1994 to 1996 or 2001 to 2011: the decade 2001 to 2010 holds none, \
             the decade [2000, 2010) only 2000"
                .into(),
        ));
        m.push(manifest(
            Trap::O3,
            Origin::Plan,
            "lego_sets",
            "*",
            "plan",
            -1,
            None,
            "the lettered set numbers (trap O2) against a range split".into(),
        ));
        m.push(manifest(
            Trap::O4,
            Origin::Plan,
            "lego_inventory_parts",
            "*",
            "plan",
            -1,
            None,
            "synthesized sets modelled on one real set tie on their brick count".into(),
        ));
        for t in self.added_themes() {
            m.push(manifest(
                Trap::B3,
                Origin::Synthetic,
                "lego_themes",
                &t.id.to_string(),
                "plan",
                -1,
                None,
                "a root theme the real catalogue does not hold".into(),
            ));
        }
        m
    }

    fn board_natural(&self, r: &mut Rng) -> u16 {
        let t = *r.pick(&self.real.templates);
        self.real.set_socket[t as usize].unwrap_or(0)
    }

    /// The purchases of one collection row: store, when ordered, the wall clock the builder wrote,
    /// and when delivered. `r` draws each reading's minute, and `rs` where in its minute it falls,
    /// so the minutes are the same whether or not a reading carries its seconds.
    #[allow(clippy::too_many_arguments)]
    pub fn purchase_times(
        &self,
        zi: usize,
        trap: Option<(Trap, u64)>,
        set: u64,
        first_year: i64,
        copies: u64,
        r: &mut Rng,
        rs: &mut Rng,
    ) -> Vec<(&'static str, Stamp, String, Stamp)> {
        let zone = &calendar::zones()[zi];
        let today = calendar::days_from_civil(TODAY.0, TODAY.1, TODAY.2);
        let since = |shifts: &[Shift]| -> Vec<Shift> {
            shifts
                .iter()
                .copied()
                .filter(|s| s.at >= year_start(first_year))
                .collect()
        };
        // Milliseconds into a minute.
        let mut within = || rs.below(60_000) as i64;
        let at = |t: i64, into: i64| into_minute(t, into, zone.offset_min_at(t));
        let mut out = Vec::new();
        let online = STORES[1].0;
        let deliver = |ordered: i64, r: &mut Rng, into: i64| -> Stamp {
            let local_days = (ordered + i64::from(zone.offset_min_at(ordered)) * 60)
                .div_euclid(86_400)
                + r.range_i64(2, 9);
            let hour = r.range_i64(10, 17) as u32;
            let minute = r.below(60) as u32;
            at(
                zone.instant_of(calendar::local_secs(local_days, hour, minute)),
                into,
            )
        };
        match trap {
            Some((Trap::D1, _)) => {
                // Both readings of one clock time in the hour a fold reads twice.
                let folds = since(&self.folds[zi]);
                let fold = *r.pick(&folds);
                let len = fold.len_secs();
                let a = fold.at - len + 60 * r.below((len / 60) as u64) as i64;
                let b = a + len;
                let into = within();
                let local = reading(a + i64::from(fold.before_min) * 60, into);
                out.push((online, at(a, into), local.clone(), deliver(a, r, within())));
                out.push((online, at(b, into), local, deliver(b, r, within())));
                return out;
            }
            Some((Trap::D2, _)) => {
                // An instant in a gap, written in the offset before the clock went forward.
                let gaps = since(&self.gaps[zi]);
                let gap = *r.pick(&gaps);
                let t = gap.at + 60 * r.below((gap.len_secs() / 60) as u64) as i64;
                let before = gap.before_min;
                let into = within();
                out.push((
                    online,
                    into_minute(t, into, before),
                    reading(t + i64::from(before) * 60, into),
                    deliver(t, r, within()),
                ));
            }
            Some((Trap::D8, _)) => {
                // An order in the first reading of a midnight the clock read twice.
                let folds: Vec<(Shift, i64)> = since(&self.folds[zi])
                    .into_iter()
                    .filter_map(|f| day_begun_twice(&f).map(|day| (f, day)))
                    .collect();
                let (fold, day) = *r.pick(&folds);
                let midnight = day * 86_400;
                let repeated_until = fold.at + i64::from(fold.before_min) * 60;
                let local =
                    midnight + 60 * r.below(((repeated_until - midnight) / 60) as u64) as i64;
                let t = local - i64::from(fold.before_min) * 60;
                let into = within();
                out.push((
                    online,
                    at(t, into),
                    reading(local, into),
                    deliver(t, r, within()),
                ));
            }
            Some((Trap::D3, _)) => {
                // Late on the last day of a month west of UTC; early on the first day east of it.
                loop {
                    let y = r.range_i64(first_year.min(2025), 2025);
                    let m = r.range_i64(1, 12) as u32;
                    let first = calendar::days_from_civil(y, m, 1);
                    let east = match zone.offset_min_at(first * 86_400) {
                        0 => continue,
                        offset => offset > 0,
                    };
                    let (days, hour) = if east {
                        (calendar::days_from_civil(y, m, 1), 0)
                    } else {
                        (
                            calendar::days_from_civil(y, m, calendar::days_in_month(y, m)),
                            23,
                        )
                    };
                    let minute = r.below(60) as u32;
                    if let LocalTime::Unique(t) =
                        zone.instants_of(calendar::local_secs(days, hour, minute))
                    {
                        let local_days =
                            (t + i64::from(zone.offset_min_at(t)) * 60).div_euclid(86_400);
                        let (ly, lm, _) = calendar::civil_from_days(local_days);
                        let (uy, um, _) = calendar::civil_from_days(t.div_euclid(86_400));
                        if (ly, lm) != (uy, um) {
                            let into = within();
                            let local = reading(calendar::local_secs(days, hour, minute), into);
                            out.push((online, at(t, into), local, deliver(t, r, within())));
                            break;
                        }
                    }
                }
            }
            Some((Trap::D4, _)) => loop {
                // A midnight the clock skipped has no instant to write as 24:00 of the eve. The
                // reading is the midnight itself, so nothing falls past it.
                let eve = r.range_i64(calendar::days_from_civil(first_year, 1, 1), today - 1);
                if let LocalTime::Unique(t) | LocalTime::Twice(t, _) =
                    zone.instants_of(calendar::local_secs(eve + 1, 0, 0))
                {
                    let (y, m, d) = calendar::civil_from_days(eve);
                    out.push((
                        STORES[0].0,
                        at(t, 0),
                        format!("{y:04}-{m:02}-{d:02} 24:00:00.000"),
                        at(t, 0),
                    ));
                    break;
                }
            },
            Some((Trap::D5, _)) => loop {
                let day = r.range_i64(calendar::days_from_civil(2026, 1, 1), today);
                let hour = r.range_i64(8, 23) as u32;
                let minute = r.below(60) as u32;
                if let LocalTime::Unique(t) | LocalTime::Twice(t, _) =
                    zone.instants_of(calendar::local_secs(day, hour, minute))
                {
                    let into = within();
                    out.push((
                        online,
                        at(t, into),
                        reading(calendar::local_secs(day, hour, minute), into),
                        Stamp::Infinity,
                    ));
                    break;
                }
            },
            Some((Trap::D6, _)) => loop {
                let leaps: Vec<i64> = (FIRST_PURCHASE_YEAR..=2024)
                    .filter(|y| calendar::is_leap(*y) && *y >= first_year)
                    .collect();
                let y = if leaps.is_empty() {
                    2024
                } else {
                    *r.pick(&leaps)
                };
                let day = calendar::days_from_civil(y, 2, 29);
                let (hour, minute) = (r.range_i64(10, 19) as u32, r.below(60) as u32);
                if let LocalTime::Unique(t) =
                    zone.instants_of(calendar::local_secs(day, hour, minute))
                {
                    let into = within();
                    out.push((
                        STORES[0].0,
                        at(t, into),
                        reading(calendar::local_secs(day, hour, minute), into),
                        at(t, into),
                    ));
                    break;
                }
            },
            Some((Trap::D7, _)) => {
                let mut days = Vec::new();
                for y in first_year.min(2025)..=2025 {
                    for (yy, m, d) in [
                        (y, 12, 29),
                        (y, 12, 30),
                        (y, 12, 31),
                        (y + 1, 1, 1),
                        (y + 1, 1, 2),
                        (y + 1, 1, 3),
                    ] {
                        let z = calendar::days_from_civil(yy, m, d);
                        if calendar::iso_week(z).0 != yy && z <= today {
                            days.push(z);
                        }
                    }
                }
                loop {
                    let day = *r.pick(&days);
                    let (hour, minute) = (r.range_i64(10, 19) as u32, r.below(60) as u32);
                    if let LocalTime::Unique(t) =
                        zone.instants_of(calendar::local_secs(day, hour, minute))
                    {
                        let into = within();
                        out.push((
                            STORES[0].0,
                            at(t, into),
                            reading(calendar::local_secs(day, hour, minute), into),
                            at(t, into),
                        ));
                        break;
                    }
                }
            }
            _ => {}
        }
        // The remaining copies: bought on a day of the set's timeline, in opening hours. A set not on
        // sale by today has no timeline, and its copies fall on any day from its first year.
        let timeline = self.timeline_of(set);
        let lo = calendar::days_from_civil(first_year.min(2026), 1, 1).min(today);
        while (out.len() as u64) < copies {
            let mut pick = r.below(1000) as u32;
            let mut store = STORES[0];
            for s in STORES {
                if pick < s.1 {
                    store = s;
                    break;
                }
                pick -= s.1;
            }
            let day = match &timeline {
                Some(t) => t.day(r),
                None => r.range_i64(lo, today),
            };
            let hour = r.range_i64(i64::from(store.2), i64::from(store.3)) as u32;
            let minute = r.below(60) as u32;
            // The clock the builder saw: a reading the clock skipped is read on past the gap.
            let t = zone.instant_of(calendar::local_secs(day, hour, minute));
            let into = within();
            let delivered = if store.4 {
                deliver(t, r, within())
            } else {
                at(t, into)
            };
            out.push((
                store.0,
                at(t, into),
                reading(t + i64::from(zone.offset_min_at(t)) * 60, into),
                delivered,
            ));
        }
        out
    }
}

/// The instant `into` milliseconds past the whole minute `t`, written at `offset_min`.
fn into_minute(t: i64, into: i64, offset_min: i32) -> Stamp {
    Stamp::At {
        t: t + into / 1000,
        ms: (into % 1000) as u16,
        offset_min,
    }
}

/// The wall clock `into` milliseconds past the whole minute `local`.
fn reading(local: i64, into: i64) -> String {
    calendar::render_local(local + into / 1000, (into % 1000) as u16)
}

/// A synthesized set's number: seven digits for the first nine million sets, then each further
/// nine million in a band of its own, above the numbers the band before moves to when its bare
/// form is a real record.
pub fn base_number(seed: u64, i: u64) -> u64 {
    const SPAN: u64 = 9_000_000;
    const MULT: u64 = 7_654_321;
    1_000_000 + i / SPAN * 2 * SPAN + (i.wrapping_mul(MULT) + seed % SPAN) % SPAN
}

/// A name whose UTF-8 was read back as Latin-1: a lead byte of a two-byte sequence (`Ã`, `Â`,
/// `â`) followed by a character in the C1 or Latin-1 Supplement range.
pub fn looks_round_tripped(name: &str) -> bool {
    let cs: Vec<char> = name.chars().collect();
    cs.windows(2)
        .any(|w| matches!(w[0], 'Ã' | 'Â' | 'â') && ('\u{0080}'..='\u{00BF}').contains(&w[1]))
}

/// A part number the basic bricks 3001 to 3010 read differently as text and as a number: inside the
/// byte range '3001' to '3010', and not a whole number from 3001 to 3010.
pub fn basic_brick_disagreement(p: &str) -> bool {
    let in_text = p.as_bytes() >= b"3001".as_slice() && p.as_bytes() <= b"3010".as_slice();
    let in_number = p.bytes().all(|b| b.is_ascii_digit())
        && p.parse::<u64>().is_ok_and(|n| (3001..=3010).contains(&n));
    in_text != in_number
}

#[allow(clippy::too_many_arguments)]
fn manifest(
    trap: Trap,
    origin: Origin,
    table: &'static str,
    key: &str,
    phase: &str,
    wave: i64,
    socket: Option<String>,
    detail: String,
) -> ManifestOut {
    ManifestOut {
        trap,
        origin,
        table,
        row_key: key.to_string(),
        phase: phase.to_string(),
        wave,
        socket,
        detail,
    }
}
