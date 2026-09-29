//! The real LEGO catalogue the generator draws from: reference tables, sets, inventories, inventory lines
//! and nesting, read once from the source schema, with the indices synthesis needs.
//!
//! A SOCKET is one cell of the product of the root themes and the half-open decades
//! `[1950, 1960) … [2020, 2030)`: the place a set lands in a root-theme partition, in a decade
//! partition, and in the product of the two. The sockets are read from the reference tables, every root
//! theme times every decade, whether or not a real set sits there.
//!
//! No set is released in the years of `NO_RELEASE`: a real set of one of them is re-dated to the
//! nearest year outside them. The years after the real catalogue (`MODELLED`) are modelled on its
//! last years: each root theme keeps its share of them, and the count of releases grows at
//! `GROWTH_FACTOR` times the rate fitted to them.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::RangeInclusive;

use sqlx::PgConnection;

use crate::text;

pub const DECADE_LO: i32 = 1950;
pub const DECADE_HI: i32 = 2030;
pub const DECADES: u8 = ((DECADE_HI - DECADE_LO) / 10) as u8;

/// Years in which no set is released.
pub const NO_RELEASE: [RangeInclusive<i32>; 2] = [1994..=1996, 2001..=2011];
/// The real years the modelled years copy: their root themes' shares and their sets.
pub const MODEL_ON: RangeInclusive<i32> = 2010..=2017;
/// The real years the growth rate is fitted to. The dump ends part way through 2017, so its count
/// is not a year's releases.
pub const MODEL_FIT: RangeInclusive<i32> = 2010..=2016;
/// The years after the real catalogue that synthesized sets are released in.
pub const MODELLED: RangeInclusive<i32> = 2018..=2026;
/// How many times the fitted growth rate the modelled years grow at.
pub const GROWTH_FACTOR: f64 = 3.0;

/// Whether sets are released in year `y`.
pub fn is_release_year(y: i32) -> bool {
    !NO_RELEASE.iter().any(|r| r.contains(&y))
}

/// The nearest release year to `y`: `y` itself outside `NO_RELEASE`, and the later of two equally
/// near years.
pub fn nearest_release_year(y: i32) -> i32 {
    match NO_RELEASE.iter().find(|r| r.contains(&y)) {
        Some(r) => {
            let (before, after) = (r.start() - 1, r.end() + 1);
            if y - before < after - y {
                before
            } else {
                after
            }
        }
        None => y,
    }
}

/// The releases per year a series of real counts gives the modelled years: the real years' growth,
/// followed to the last year of `MODEL_ON`, then sped up `GROWTH_FACTOR` times. Also returns the real
/// years' growth rate.
pub fn model_releases(counts: &[(i32, u64)]) -> (Vec<(i32, f64)>, f64) {
    let points: Vec<(f64, f64)> = counts
        .iter()
        .filter(|&&(_, n)| n > 0)
        .map(|&(y, n)| (f64::from(y), (n as f64).ln()))
        .collect();
    if points.is_empty() {
        return (MODELLED.map(|y| (y, 0.0)).collect(), 0.0);
    }
    let len = points.len() as f64;
    let mx = points.iter().map(|p| p.0).sum::<f64>() / len;
    let my = points.iter().map(|p| p.1).sum::<f64>() / len;
    let sxx: f64 = points.iter().map(|p| (p.0 - mx).powi(2)).sum();
    let sxy: f64 = points.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum();
    let growth = if sxx > 0.0 { sxy / sxx } else { 0.0 };
    let last = f64::from(*MODEL_ON.end());
    let base = my + growth * (last - mx);
    let releases = MODELLED
        .map(|y| {
            let years = f64::from(y) - last;
            (y, (base + GROWTH_FACTOR * growth * years).exp())
        })
        .collect();
    (releases, growth)
}

#[derive(Clone, Debug)]
pub struct Colour {
    pub id: i32,
    pub name: String,
    pub rgb: String,
    pub is_trans: String,
}

#[derive(Clone, Debug)]
pub struct Theme {
    pub id: i32,
    pub name: String,
    pub parent_id: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct Category {
    pub id: i32,
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct Part {
    pub part_num: String,
    pub name: String,
    pub part_cat_id: i32,
}

#[derive(Clone, Debug)]
pub struct SetRec {
    pub set_num: String,
    pub name: String,
    pub year: Option<i32>,
    pub theme_id: Option<i32>,
    pub num_parts: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct InvRec {
    pub id: i32,
    pub version: i32,
    pub set_num: String,
}

/// An inventory line. The part is an index into the catalogue's part-number pool, because a line
/// may name a part number the parts table does not hold.
#[derive(Clone, Copy, Debug)]
pub struct LineRec {
    pub inventory_id: i32,
    pub part: u32,
    pub color_id: i32,
    pub quantity: i32,
    pub is_spare: bool,
}

#[derive(Clone, Debug)]
pub struct NestRec {
    pub inventory_id: i32,
    pub set_num: String,
    pub quantity: i32,
}

/// The catalogue as read, in a fixed order: reference tables by id, sets by set number in byte order,
/// inventories by id, lines and nesting by inventory then key.
#[derive(Clone, Debug, Default)]
pub struct Catalogue {
    pub colours: Vec<Colour>,
    pub themes: Vec<Theme>,
    pub categories: Vec<Category>,
    pub parts: Vec<Part>,
    pub sets: Vec<SetRec>,
    pub inventories: Vec<InvRec>,
    pub lines: Vec<LineRec>,
    pub nests: Vec<NestRec>,
    /// Every part number a line or the parts table names, indexed by `LineRec::part`.
    pub part_pool: Vec<String>,
}

/// The eight real tables `Catalogue::read` reads.
pub const TABLES: [&str; 8] = [
    "lego_colors",
    "lego_themes",
    "lego_part_categories",
    "lego_parts",
    "lego_sets",
    "lego_inventories",
    "lego_inventory_parts",
    "lego_inventory_sets",
];

/// The real tables `schema` does not hold, in the order of `TABLES`.
pub async fn missing(
    conn: &mut PgConnection,
    schema: &str,
) -> Result<Vec<&'static str>, sqlx::Error> {
    let mut absent = Vec::new();
    for t in TABLES {
        let found: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(format!("{schema}.{t}"))
            .fetch_one(&mut *conn)
            .await?;
        if found.is_none() {
            absent.push(t);
        }
    }
    Ok(absent)
}

impl Catalogue {
    /// Reads the eight real tables from `schema`.
    pub async fn read(pool: &mut PgConnection, schema: &str) -> Result<Catalogue, sqlx::Error> {
        let q = |t: &str| format!("{schema}.{t}");
        let colours = sqlx::query_as::<_, (i32, String, String, String)>(&format!(
            "SELECT id, name, rgb, is_trans::text FROM {} ORDER BY id",
            q("lego_colors")
        ))
        .fetch_all(&mut *pool)
        .await?
        .into_iter()
        .map(|(id, name, rgb, is_trans)| Colour {
            id,
            name,
            rgb,
            is_trans,
        })
        .collect();
        let themes = sqlx::query_as::<_, (i32, String, Option<i32>)>(&format!(
            "SELECT id, name, parent_id FROM {} ORDER BY id",
            q("lego_themes")
        ))
        .fetch_all(&mut *pool)
        .await?
        .into_iter()
        .map(|(id, name, parent_id)| Theme {
            id,
            name,
            parent_id,
        })
        .collect();
        let categories = sqlx::query_as::<_, (i32, String)>(&format!(
            "SELECT id, name FROM {} ORDER BY id",
            q("lego_part_categories")
        ))
        .fetch_all(&mut *pool)
        .await?
        .into_iter()
        .map(|(id, name)| Category { id, name })
        .collect();
        let parts: Vec<Part> = sqlx::query_as::<_, (String, String, i32)>(&format!(
            "SELECT part_num, name, part_cat_id FROM {} ORDER BY part_num COLLATE \"C\"",
            q("lego_parts")
        ))
        .fetch_all(&mut *pool)
        .await?
        .into_iter()
        .map(|(part_num, name, part_cat_id)| Part {
            part_num,
            name,
            part_cat_id,
        })
        .collect();
        let sets = sqlx::query_as::<_, (String, String, Option<i32>, Option<i32>, Option<i32>)>(
            &format!(
                "SELECT set_num, name, year, theme_id, num_parts FROM {} ORDER BY set_num COLLATE \"C\"",
                q("lego_sets")
            ),
        )
        .fetch_all(&mut *pool)
        .await?
        .into_iter()
        .map(|(set_num, name, year, theme_id, num_parts)| SetRec {
            set_num,
            name,
            year,
            theme_id,
            num_parts,
        })
        .collect();
        let inventories = sqlx::query_as::<_, (i32, i32, String)>(&format!(
            "SELECT id, version, set_num FROM {} ORDER BY id",
            q("lego_inventories")
        ))
        .fetch_all(&mut *pool)
        .await?
        .into_iter()
        .map(|(id, version, set_num)| InvRec {
            id,
            version,
            set_num,
        })
        .collect();
        let raw_lines = sqlx::query_as::<_, (i32, String, i32, i32, bool)>(&format!(
            "SELECT inventory_id, part_num, color_id, quantity, is_spare FROM {} \
             ORDER BY inventory_id, part_num COLLATE \"C\", color_id, is_spare, quantity",
            q("lego_inventory_parts")
        ))
        .fetch_all(&mut *pool)
        .await?;
        let nests = sqlx::query_as::<_, (i32, String, i32)>(&format!(
            "SELECT inventory_id, set_num, quantity FROM {} ORDER BY inventory_id, set_num COLLATE \"C\", quantity",
            q("lego_inventory_sets")
        ))
        .fetch_all(&mut *pool)
        .await?
        .into_iter()
        .map(|(inventory_id, set_num, quantity)| NestRec { inventory_id, set_num, quantity })
        .collect();
        let mut cat = Catalogue {
            colours,
            themes,
            categories,
            parts,
            sets,
            inventories,
            nests,
            ..Default::default()
        };
        let mut pool_index: HashMap<String, u32> = HashMap::new();
        for p in &cat.parts {
            intern(&mut cat.part_pool, &mut pool_index, &p.part_num);
        }
        cat.lines = raw_lines
            .into_iter()
            .map(
                |(inventory_id, part_num, color_id, quantity, is_spare)| LineRec {
                    inventory_id,
                    part: intern(&mut cat.part_pool, &mut pool_index, &part_num),
                    color_id,
                    quantity,
                    is_spare,
                },
            )
            .collect();
        Ok(cat)
    }
}

pub fn intern(pool: &mut Vec<String>, index: &mut HashMap<String, u32>, s: &str) -> u32 {
    if let Some(&i) = index.get(s) {
        return i;
    }
    let i = pool.len() as u32;
    pool.push(s.to_string());
    index.insert(s.to_string(), i);
    i
}

/// One cell of root theme × decade.
#[derive(Clone, Debug)]
pub struct Socket {
    pub root: i32,
    /// 0 for `[1950, 1960)`, up to `DECADES - 1` for `[2020, 2030)`.
    pub decade: u8,
    /// Real sets with an inventory whose root theme and year place them here.
    pub templates: Vec<u32>,
    /// The modelled years in this decade, each with the releases the root theme has in it.
    pub modelled: Vec<(i32, u64)>,
}

impl Socket {
    pub fn label(&self) -> String {
        format!(
            "r{}/{}s",
            self.root,
            DECADE_LO + 10 * i32::from(self.decade)
        )
    }

    /// The socket's weight in the switchboard: its real sets and its modelled years' releases.
    pub fn weight(&self) -> u64 {
        self.templates.len() as u64 + self.modelled.iter().map(|m| m.1).sum::<u64>()
    }
}

/// How often a cord joins two different sockets, counted on the real catalogue.
#[derive(Clone, Copy, Debug, Default)]
pub struct Crossing {
    pub crossed: u32,
    pub total: u32,
}

impl Crossing {
    pub fn rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            f64::from(self.crossed) / f64::from(self.total)
        }
    }
}

/// The real catalogue with the indices synthesis reads.
pub struct Real {
    pub cat: Catalogue,
    pub root_of_theme: HashMap<i32, i32>,
    pub parent_of_theme: HashMap<i32, Option<i32>>,
    /// Every root × every decade, ordered by root theme id then decade.
    pub sockets: Vec<Socket>,
    pub set_socket: Vec<Option<u16>>,
    pub set_by_num: HashMap<String, u32>,
    pub keys: HashSet<String>,
    /// Per real set, its inventories (indices into `cat.inventories`) by version.
    pub inventories_of: Vec<Vec<u32>>,
    pub lines_of_inv: HashMap<i32, (u32, u32)>,
    pub nests_of_inv: HashMap<i32, (u32, u32)>,
    pub part_colours: HashMap<u32, Vec<i32>>,
    pub theme_sets: HashMap<i32, Vec<u32>>,
    /// Real sets with an inventory: the templates of synthesis.
    pub templates: Vec<u32>,
    pub early: Vec<u32>,
    pub modern: Vec<u32>,
    pub non_ascii: Vec<u32>,
    /// Real lettered sets released by 2026.
    pub lettered: Vec<u32>,
    pub prefixes: Vec<(String, u32)>,
    pub max_theme_id: i32,
    pub max_inventory_id: i32,
    pub cafe_corner: Option<u32>,
    pub nesting_crossing: Crossing,
    pub nesting_root_crossing: Crossing,
    pub version_crossing: Crossing,
    pub twin_crossing: Crossing,
    /// Real sets re-dated out of `NO_RELEASE`, with the year the dump gives them.
    pub redated: Vec<(u32, i32)>,
    /// Releases per modelled year, summed over the root themes.
    pub modelled: Vec<(i32, f64)>,
    /// The real years' growth rate, fitted to `MODEL_FIT`.
    pub fitted_growth: f64,
    /// Per root theme, its real sets with an inventory released in `MODEL_ON`: the models of its
    /// sets in the modelled years.
    pub model_pool: HashMap<i32, Vec<u32>>,
}

/// The base number and version of a set number `<base>-<version>`, when it has that shape.
pub fn split_version(set_num: &str) -> Option<(&str, u32)> {
    let (base, v) = set_num.rsplit_once('-')?;
    if base.is_empty() || v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    v.parse().ok().map(|v| (base, v))
}

pub fn decade_of(year: i32) -> Option<u8> {
    if (DECADE_LO..DECADE_HI).contains(&year) {
        Some(((year - DECADE_LO) / 10) as u8)
    } else {
        None
    }
}

impl Real {
    pub fn new(mut cat: Catalogue) -> Real {
        // The years the dump gives, before any is re-dated: the modelled years are fitted to them.
        let dump_year: Vec<Option<i32>> = cat.sets.iter().map(|s| s.year).collect();
        let mut redated = Vec::new();
        for (i, s) in cat.sets.iter_mut().enumerate() {
            if let Some(y) = s.year.filter(|&y| !is_release_year(y)) {
                s.year = Some(nearest_release_year(y));
                redated.push((i as u32, y));
            }
        }
        let parent_of_theme: HashMap<i32, Option<i32>> =
            cat.themes.iter().map(|t| (t.id, t.parent_id)).collect();
        let mut root_of_theme = HashMap::new();
        for t in &cat.themes {
            let mut cur = t.id;
            let mut steps = 0;
            let root = loop {
                match parent_of_theme.get(&cur) {
                    Some(Some(p)) if steps < 64 => {
                        cur = *p;
                        steps += 1;
                    }
                    Some(None) => break Some(cur),
                    _ => break None,
                }
            };
            if let Some(r) = root {
                root_of_theme.insert(t.id, r);
            }
        }
        let mut roots: Vec<i32> = cat
            .themes
            .iter()
            .filter(|t| t.parent_id.is_none())
            .map(|t| t.id)
            .collect();
        roots.sort_unstable();
        let mut sockets = Vec::with_capacity(roots.len() * usize::from(DECADES));
        let mut socket_of: HashMap<(i32, u8), u16> = HashMap::new();
        for &root in &roots {
            for decade in 0..DECADES {
                socket_of.insert((root, decade), sockets.len() as u16);
                sockets.push(Socket {
                    root,
                    decade,
                    templates: Vec::new(),
                    modelled: Vec::new(),
                });
            }
        }
        let set_by_num: HashMap<String, u32> = cat
            .sets
            .iter()
            .enumerate()
            .map(|(i, s)| (s.set_num.clone(), i as u32))
            .collect();
        let keys: HashSet<String> = cat.sets.iter().map(|s| s.set_num.clone()).collect();
        let socket_for = |s: &SetRec| -> Option<u16> {
            let root = root_of_theme.get(&s.theme_id?)?;
            let decade = decade_of(s.year?)?;
            socket_of.get(&(*root, decade)).copied()
        };
        let set_socket: Vec<Option<u16>> = cat.sets.iter().map(socket_for).collect();

        let mut inventories_of: Vec<Vec<u32>> = vec![Vec::new(); cat.sets.len()];
        for (k, inv) in cat.inventories.iter().enumerate() {
            if let Some(&s) = set_by_num.get(&inv.set_num) {
                inventories_of[s as usize].push(k as u32);
            }
        }
        for list in &mut inventories_of {
            list.sort_by_key(|&k| {
                (
                    cat.inventories[k as usize].version,
                    cat.inventories[k as usize].id,
                )
            });
        }
        let lines_of_inv = ranges(cat.lines.iter().map(|l| l.inventory_id));
        let nests_of_inv = ranges(cat.nests.iter().map(|n| n.inventory_id));
        let mut colour_sets: HashMap<u32, Vec<i32>> = HashMap::new();
        for l in &cat.lines {
            colour_sets.entry(l.part).or_default().push(l.color_id);
        }
        let part_colours = colour_sets
            .into_iter()
            .map(|(p, mut cs)| {
                cs.sort_unstable();
                cs.dedup();
                (p, cs)
            })
            .collect();

        let mut theme_sets: HashMap<i32, Vec<u32>> = HashMap::new();
        let mut templates = Vec::new();
        let (mut early, mut modern, mut non_ascii, mut lettered) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (i, s) in cat.sets.iter().enumerate() {
            let i = i as u32;
            if let Some(t) = s.theme_id {
                theme_sets.entry(t).or_default().push(i);
            }
            let has_inventory = !inventories_of[i as usize].is_empty();
            if has_inventory {
                templates.push(i);
                if let Some(sock) = set_socket[i as usize] {
                    sockets[usize::from(sock)].templates.push(i);
                }
                let first = cat.inventories[inventories_of[i as usize][0] as usize].id;
                let has_lines = lines_of_inv.contains_key(&first);
                match s.year {
                    Some(y) if y <= 1955 && has_lines => early.push(i),
                    Some(y) if y >= 2010 && has_lines => modern.push(i),
                    _ => {}
                }
                if text::has_non_ascii(&s.name) && has_lines && set_socket[i as usize].is_some() {
                    non_ascii.push(i);
                }
            }
            if text::is_lettered(&s.set_num)
                && s.year.is_some_and(|y| y <= 2026)
                && split_version(&s.set_num).is_some()
            {
                lettered.push(i);
            }
        }
        let mut prefix_counts: BTreeMap<String, u32> = BTreeMap::new();
        for s in &cat.sets {
            if let Some((base, _)) = split_version(&s.set_num) {
                let letters: String = base
                    .chars()
                    .take_while(|c| c.is_ascii_alphabetic())
                    .collect();
                let rest = &base[letters.len()..];
                if !letters.is_empty()
                    && !rest.is_empty()
                    && rest.bytes().all(|b| b.is_ascii_digit())
                {
                    *prefix_counts.entry(letters).or_default() += 1;
                }
            }
        }
        let prefixes: Vec<(String, u32)> = prefix_counts.into_iter().collect();

        // The modelled years: releases fitted to the dump's own years, shared among the root themes
        // as they share `MODEL_ON`.
        let mut fit_counts: BTreeMap<i32, u64> = MODEL_FIT.map(|y| (y, 0)).collect();
        let mut model_pool: HashMap<i32, Vec<u32>> = HashMap::new();
        for &t in &templates {
            let Some(y) = dump_year[t as usize] else {
                continue;
            };
            if let Some(n) = fit_counts.get_mut(&y) {
                *n += 1;
            }
            let root = cat.sets[t as usize]
                .theme_id
                .and_then(|th| root_of_theme.get(&th));
            if let (true, Some(&root)) = (MODEL_ON.contains(&y), root) {
                model_pool.entry(root).or_default().push(t);
            }
        }
        let fit_counts: Vec<(i32, u64)> = fit_counts.into_iter().collect();
        let (modelled, fitted_growth) = model_releases(&fit_counts);
        let pooled: usize = model_pool.values().map(Vec::len).sum();
        for (&root, pool) in &model_pool {
            let share = pool.len() as f64 / pooled as f64;
            for &(y, releases) in &modelled {
                let w = (releases * share).round() as u64;
                let socket = decade_of(y).and_then(|d| socket_of.get(&(root, d)));
                if let (true, Some(&s)) = (w > 0, socket) {
                    sockets[usize::from(s)].modelled.push((y, w));
                }
            }
        }
        for s in &mut sockets {
            s.modelled.sort_unstable();
        }

        let max_theme_id = cat.themes.iter().map(|t| t.id).max().unwrap_or(0);
        let max_inventory_id = cat.inventories.iter().map(|i| i.id).max().unwrap_or(0);
        let cafe_corner = set_by_num.get("10182-1").copied();

        // Crossing rates of the three cords the catalogue files.
        let mut nesting_crossing = Crossing::default();
        let mut nesting_root_crossing = Crossing::default();
        let inv_set: HashMap<i32, u32> = cat
            .inventories
            .iter()
            .filter_map(|inv| set_by_num.get(&inv.set_num).map(|&s| (inv.id, s)))
            .collect();
        for n in &cat.nests {
            let (Some(&pack), Some(&child)) =
                (inv_set.get(&n.inventory_id), set_by_num.get(&n.set_num))
            else {
                continue;
            };
            if let (Some(a), Some(b)) = (set_socket[pack as usize], set_socket[child as usize]) {
                nesting_crossing.total += 1;
                nesting_crossing.crossed += u32::from(a != b);
                nesting_root_crossing.total += 1;
                nesting_root_crossing.crossed +=
                    u32::from(sockets[usize::from(a)].root != sockets[usize::from(b)].root);
            }
        }
        let mut by_base: BTreeMap<&str, Vec<(u32, u32)>> = BTreeMap::new();
        for (i, s) in cat.sets.iter().enumerate() {
            if let Some((base, v)) = split_version(&s.set_num) {
                by_base.entry(base).or_default().push((v, i as u32));
            }
        }
        let mut version_crossing = Crossing::default();
        for list in by_base.values_mut() {
            list.sort_unstable();
            if let Some(&(_, first)) = list.first() {
                for &(_, other) in &list[1..] {
                    if let (Some(a), Some(b)) =
                        (set_socket[first as usize], set_socket[other as usize])
                    {
                        version_crossing.total += 1;
                        version_crossing.crossed += u32::from(a != b);
                    }
                }
            }
        }
        let mut twin_crossing = Crossing::default();
        for (i, s) in cat.sets.iter().enumerate() {
            if split_version(&s.set_num).is_none() {
                if let Some(&dashed) = set_by_num.get(&format!("{}-1", s.set_num)) {
                    if let (Some(a), Some(b)) = (set_socket[i], set_socket[dashed as usize]) {
                        twin_crossing.total += 1;
                        twin_crossing.crossed += u32::from(a != b);
                    }
                }
            }
        }

        Real {
            cat,
            root_of_theme,
            parent_of_theme,
            sockets,
            set_socket,
            set_by_num,
            keys,
            inventories_of,
            lines_of_inv,
            nests_of_inv,
            part_colours,
            theme_sets,
            templates,
            early,
            modern,
            non_ascii,
            lettered,
            prefixes,
            max_theme_id,
            max_inventory_id,
            cafe_corner,
            nesting_crossing,
            nesting_root_crossing,
            version_crossing,
            twin_crossing,
            redated,
            modelled,
            fitted_growth,
            model_pool,
        }
    }

    /// The lowest-version inventory of a real set, if it has one.
    pub fn first_inventory(&self, set: u32) -> Option<&InvRec> {
        self.inventories_of[set as usize]
            .first()
            .map(|&k| &self.cat.inventories[k as usize])
    }

    pub fn lines(&self, inventory_id: i32) -> &[LineRec] {
        match self.lines_of_inv.get(&inventory_id) {
            Some(&(a, b)) => &self.cat.lines[a as usize..b as usize],
            None => &[],
        }
    }

    pub fn nests(&self, inventory_id: i32) -> &[NestRec] {
        match self.nests_of_inv.get(&inventory_id) {
            Some(&(a, b)) => &self.cat.nests[a as usize..b as usize],
            None => &[],
        }
    }
}

/// Contiguous index ranges of a sorted key sequence.
fn ranges(keys: impl Iterator<Item = i32>) -> HashMap<i32, (u32, u32)> {
    let mut out: HashMap<i32, (u32, u32)> = HashMap::new();
    for (i, k) in keys.enumerate() {
        let i = i as u32;
        out.entry(k)
            .and_modify(|r| r.1 = i + 1)
            .or_insert((i, i + 1));
    }
    out
}
