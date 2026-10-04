//! Tests over a small real-shaped catalogue built in memory: the generation is a function of the
//! wiring, and the manifest names exactly the rows that carry each trap.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::calendar::{self, LocalTime, ZONE_NAMES};
use crate::catalogue::{
    is_release_year, model_releases, nearest_release_year, split_version, Catalogue, Category,
    Colour, InvRec, LineRec, NestRec, Part, Real, SetRec, Theme, GROWTH_FACTOR, MODELLED,
    MODEL_FIT,
};
use crate::encode::{Enc, Format};
use crate::load::{index_name, CompositeIndex, Part as KeyPart, COMPOSITE_INDEXES, NAME_BYTES};
use crate::paired;
use crate::switchboard::{Dials, Switchboard};
use crate::traps::{Trap, DECLS};
use crate::world::{self, BuilderWave, SetWave, Stamp, Wiring, World};

/// Themes, colours and parts of the real catalogue, and sets shaped like it: one per theme in
/// every year from 1953 to 2017, plus the real sets the traps name.
fn fixture() -> Catalogue {
    let themes = vec![
        (1, "Technic", None),
        (50, "Town", None),
        (52, "City", Some(50)),
        (126, "Space", None),
        (155, "Modular Buildings", None),
        (158, "Star Wars", None),
        (185, "Star Wars Rogue One", Some(158)),
        (186, "Castle", None),
        (365, "Classic", None),
        (366, "Basic Set", Some(365)),
        (371, "Supplemental", Some(365)),
        (408, "LEGO Brand Store", None),
        (482, "Super Heroes", None),
        (484, "The LEGO Batman Movie", Some(482)),
    ];
    let colours = [
        (-1, "Unknown", "0033B2", "f"),
        (0, "Black", "05131D", "f"),
        (4, "Red", "C91A09", "f"),
        (15, "White", "FFFFFF", "f"),
        (36, "Trans-Red", "C91A09", "t"),
        (9999, "[No Color]", "05131D", "f"),
    ];
    let parts = [
        "3001", "3001a", "3002", "3003", "3004", "3010", "30010", "3020", "973c01", "3626b",
    ];
    let mut cat = Catalogue {
        colours: colours
            .iter()
            .map(|&(id, n, rgb, t)| Colour {
                id,
                name: n.into(),
                rgb: rgb.into(),
                is_trans: t.into(),
            })
            .collect(),
        themes: themes
            .iter()
            .map(|&(id, n, p)| Theme {
                id,
                name: n.into(),
                parent_id: p,
            })
            .collect(),
        categories: vec![Category {
            id: 11,
            name: "Bricks".into(),
        }],
        parts: parts
            .iter()
            .map(|p| Part {
                part_num: (*p).into(),
                name: format!("Brick {p}"),
                part_cat_id: 11,
            })
            .collect(),
        ..Default::default()
    };
    let mut sets: Vec<SetRec> = Vec::new();
    let add = |sets: &mut Vec<SetRec>, num: &str, name: &str, year: i32, theme: i32, np: i32| {
        sets.push(SetRec {
            set_num: num.into(),
            name: name.into(),
            year: Some(year),
            theme_id: Some(theme),
            num_parts: Some(np),
        });
    };
    let names = [
        "Police Station",
        "Fire Truck",
        "Space Cruiser",
        "Castle Tower",
        "Café Racer",
        "Brick Box",
    ];
    let yearly_themes = [52, 126, 186, 366, 185, 1];
    for year in 1953..=2017 {
        for (k, th) in yearly_themes.iter().enumerate() {
            add(&mut sets, &format!("{year}{k}-1"), names[k], year, *th, 4);
        }
    }
    add(&mut sets, "10182-1", "Cafe Corner", 2007, 155, 6);
    add(
        &mut sets,
        "700.A-1",
        "Automatic Binding Bricks Small Brick Set (Lego Mursten)",
        1953,
        366,
        4,
    );
    add(&mut sets, "75160", "U-wing", 2017, 158, 5);
    add(&mut sets, "75160-1", "U-wing", 2017, 185, 6);
    add(&mut sets, "70904", "Clayface Splat Attack", 2017, 484, 0);
    add(
        &mut sets,
        "70904-1",
        "Clayface\" Splat Attack",
        2017,
        484,
        6,
    );
    add(
        &mut sets,
        "Vancouver-1",
        "LEGO Store Grand Opening Exclusive Set, Vancouver",
        2012,
        408,
        -1,
    );
    add(&mut sets, "vwkit-1", "Volkswagen Kit", 1959, 366, 4);
    add(
        &mut sets,
        "K8672-1",
        "Ferrari Racing Collection",
        2006,
        1,
        2,
    );
    add(
        &mut sets,
        "Copenhagen-1",
        "LEGO Store Grand Opening, Copenhagen (KÃ¸benhavn)",
        2014,
        408,
        4,
    );
    sets.sort_by(|a, b| a.set_num.as_bytes().cmp(b.set_num.as_bytes()));
    let mut inv_id = 1;
    for s in &sets {
        if s.set_num == "75160" || s.set_num == "70904" {
            continue;
        }
        cat.inventories.push(InvRec {
            id: inv_id,
            version: 1,
            set_num: s.set_num.clone(),
        });
        if s.set_num == "K8672-1" {
            let kids = ["20060-1", "20061-1"];
            for k in kids {
                cat.nests.push(NestRec {
                    inventory_id: inv_id,
                    set_num: k.into(),
                    quantity: 1,
                });
            }
        } else {
            for (k, p) in parts.iter().enumerate().take(4 + (inv_id as usize % 5)) {
                let colour = [0, 4, 15, 9999, -1, 36][(inv_id as usize + k) % 6];
                cat.lines.push(LineRec {
                    inventory_id: inv_id,
                    part: k as u32,
                    color_id: colour,
                    quantity: 1 + (k as i32 % 3),
                    is_spare: false,
                });
                if k == 0 {
                    let _ = p;
                    cat.lines.push(LineRec {
                        inventory_id: inv_id,
                        part: 0,
                        color_id: colour,
                        quantity: 1,
                        is_spare: true,
                    });
                }
            }
        }
        inv_id += 1;
    }
    cat.sets = sets;
    cat.part_pool = parts.iter().map(|p| (*p).to_string()).collect();
    // A part number some lines name and the parts table does not hold, as the real catalogue has.
    let unlisted = cat.part_pool.len() as u32;
    cat.part_pool.push("rb00164".into());
    for inv in cat.inventories.iter().filter(|i| i.id % 7 == 0) {
        cat.lines.push(LineRec {
            inventory_id: inv.id,
            part: unlisted,
            color_id: 0,
            quantity: 1,
            is_spare: false,
        });
    }
    cat.lines
        .sort_by_key(|l| (l.inventory_id, l.part, l.color_id, l.is_spare));
    cat
}

fn wiring(sets: u64, chunk: u64) -> Wiring {
    Wiring {
        seed: 7,
        sets,
        chunk,
        builders: (sets / 1000).max(ZONE_NAMES.len() as u64),
        rows_per_builder: world::ROWS_PER_BUILDER,
        lettered_ppm: 30_000,
        rerelease_ppm: 20_000,
        second_version_ppm: 5_000,
        recolour_ppm: 150_000,
        cross_nesting: 0.05,
        cross_versions: 0.2,
        cross_twins: 0.0,
        cross_collections: 0.6,
        traps: crate::traps::all_planted(),
    }
}

fn dials() -> Dials {
    Dials {
        wavy_period: 4,
        wavy_amplitude: 0.25,
        hotspot_share: 0.9,
        hotspot_socket: None,
        swing_period: 2,
        swing_a: "50,126,186@1980-2000".into(),
        swing_b: "158,482@2010-2030".into(),
    }
}

fn world(sets: u64, chunk: u64, patch: &str) -> World {
    world_planting(sets, chunk, patch, crate::traps::all_planted()).expect("world")
}

/// A world that plants only `traps` of the traps `--traps` can leave out.
fn world_planting(
    sets: u64,
    chunk: u64,
    patch: &str,
    traps: BTreeSet<Trap>,
) -> Result<World, String> {
    let real = Real::new(fixture());
    let board = Switchboard::new(&real, patch, dials(), sets, chunk).expect("patch");
    let mut wiring = wiring(sets, chunk);
    wiring.traps = traps;
    World::new(real, wiring, board)
}

struct Everything {
    sets: Vec<SetWave>,
    builders: Vec<BuilderWave>,
    plan: Vec<world::ManifestOut>,
}

fn generate(w: &World) -> Everything {
    let chunk = w.wiring.chunk;
    let mut sets: Vec<SetWave> = (0..w.real_waves).map(|k| w.real_wave(k)).collect();
    for s in 0..w.wiring.sets.div_ceil(chunk) {
        sets.push(w.set_wave(
            w.real_waves + s,
            s * chunk,
            ((s + 1) * chunk).min(w.wiring.sets),
        ));
    }
    let builders = (0..w.wiring.builders.div_ceil(chunk))
        .map(|b| w.builder_wave(b, b * chunk, ((b + 1) * chunk).min(w.wiring.builders)))
        .collect();
    Everything {
        sets,
        builders,
        plan: w.plan_manifest(),
    }
}

/// One generated table as the load writes it: its columns, and its rows as the text COPY format
/// reads them back, NULL as `None`.
struct Written {
    columns: Vec<&'static str>,
    rows: Vec<Vec<Option<String>>>,
}

impl Written {
    fn at(&self, column: &str) -> usize {
        self.columns
            .iter()
            .position(|c| *c == column)
            .unwrap_or_else(|| panic!("no column {column}"))
    }

    /// A row's values in `columns`, or `None` where one of them is NULL.
    fn project(&self, row: &[Option<String>], columns: &[&str]) -> Option<Vec<String>> {
        columns.iter().map(|c| row[self.at(c)].clone()).collect()
    }
}

fn copy_text_rows(buf: &[u8]) -> Vec<Vec<Option<String>>> {
    let unescape = |f: &str| {
        let mut out = String::with_capacity(f.len());
        let mut cs = f.chars();
        while let Some(c) = cs.next() {
            match (c, c == '\\') {
                (_, true) => match cs.next() {
                    Some('t') => out.push('\t'),
                    Some('n') => out.push('\n'),
                    Some('r') => out.push('\r'),
                    Some(o) => out.push(o),
                    None => {}
                },
                (c, false) => out.push(c),
            }
        }
        out
    };
    std::str::from_utf8(buf)
        .expect("utf-8")
        .split_terminator('\n')
        .map(|line| {
            line.split('\t')
                .map(|f| (f != "\\N").then(|| unescape(f)))
                .collect()
        })
        .collect()
}

/// Every table the load writes, each row as the load's own batches encode it.
fn written(w: &World, e: &Everything) -> BTreeMap<&'static str, Written> {
    let mut out: BTreeMap<&'static str, Written> = crate::load::TABLES
        .iter()
        .map(|t| {
            (
                t.name,
                Written {
                    columns: t.columns.split(", ").collect(),
                    rows: Vec::new(),
                },
            )
        })
        .collect();
    let mut enc = Enc::new(Format::Text);
    let mut take = |name: &'static str, enc: &mut Enc| {
        enc.finish();
        out.get_mut(name)
            .expect("declared table")
            .rows
            .extend(copy_text_rows(&enc.buf));
        enc.reset();
    };
    let added = w.added_themes();
    for name in crate::load::REFERENCE_TABLES {
        crate::load::encode_reference(name, &w.real.cat, &added, &w.places, &mut enc);
        take(name, &mut enc);
    }
    let pool = &w.real.cat.part_pool;
    let mut batches: Vec<crate::load::Batches> = e
        .sets
        .iter()
        .map(|x| crate::load::set_wave_batches(String::new(), x, pool))
        .collect();
    batches.extend(
        e.builders
            .iter()
            .map(|x| crate::load::builder_wave_batches(String::new(), x)),
    );
    batches.push(crate::load::Batches {
        label: String::new(),
        levels: vec![("trap_manifest", crate::load::Level::Manifest(&e.plan))],
    });
    for b in &batches {
        for (name, level) in &b.levels {
            level.encode(&mut enc);
            take(name, &mut enc);
        }
    }
    out
}

/// The key the manifest names a row of `table` by, where the manifest names that table's rows.
fn manifest_key(t: &Written, table: &str, row: &[Option<String>]) -> Option<String> {
    let cols: &[&str] = match table {
        "lego_sets" => &["set_num"],
        "lego_themes" | "lego_inventories" => &["id"],
        "lego_collection" => &["builder_id", "row_no"],
        "lego_purchases" => &["purchase_id"],
        "lego_inventory_parts" => &["inventory_id", "part_num", "color_id", "is_spare"],
        _ => return None,
    };
    t.project(row, cols).map(|v| v.join("|"))
}

/// A reference: a table, its columns, and the table and key they name. A NULL names no row.
type Reference = (&'static str, &'static str, &'static str, &'static str);

/// Every reference among the generated tables.
const REFERENCES: &[Reference] = &[
    ("lego_themes", "parent_id", "lego_themes", "id"),
    ("lego_parts", "part_cat_id", "lego_part_categories", "id"),
    ("lego_sets", "theme_id", "lego_themes", "id"),
    ("lego_inventories", "set_num", "lego_sets", "set_num"),
    (
        "lego_inventory_parts",
        "inventory_id",
        "lego_inventories",
        "id",
    ),
    ("lego_inventory_parts", "part_num", "lego_parts", "part_num"),
    ("lego_inventory_parts", "color_id", "lego_colors", "id"),
    (
        "lego_inventory_sets",
        "inventory_id",
        "lego_inventories",
        "id",
    ),
    ("lego_inventory_sets", "set_num", "lego_sets", "set_num"),
    (
        "lego_collection",
        "builder_id",
        "lego_builders",
        "builder_id",
    ),
    ("lego_collection", "set_num", "lego_sets", "set_num"),
    (
        "lego_purchases",
        "builder_id, row_no",
        "lego_collection",
        "builder_id, row_no",
    ),
    ("lego_postcodes", "city_id", "lego_cities", "city_id"),
    (
        "lego_streets",
        "postcode_id",
        "lego_postcodes",
        "postcode_id",
    ),
    ("lego_builders", "street_id", "lego_streets", "street_id"),
];

fn cols(list: &str) -> Vec<&str> {
    list.split(", ").collect()
}

/// The manifest's rows as written: (trap, origin, table, row key).
fn manifest_rows(db: &BTreeMap<&str, Written>) -> Vec<(String, String, String, String)> {
    let m = &db["trap_manifest"];
    m.rows
        .iter()
        .map(|r| {
            let v = m
                .project(r, &["trap", "origin", "tbl", "row_key"])
                .expect("no NULL");
            (v[0].clone(), v[1].clone(), v[2].clone(), v[3].clone())
        })
        .collect()
}

fn sorted<T: Ord + Clone>(v: impl Iterator<Item = T>) -> Vec<T> {
    let mut v: Vec<T> = v.collect();
    v.sort();
    v
}

#[test]
fn the_same_wiring_generates_the_same_rows() {
    let patch = "natural:60,wavy:20,interleaved:10,paired:10";
    let a = generate(&world(3000, 100, patch));
    let b = generate(&world(3000, 100, patch));
    for (x, y) in a.sets.iter().zip(&b.sets) {
        assert_eq!(x.sets, y.sets);
        assert_eq!(x.inventories, y.inventories);
        assert_eq!(x.lines, y.lines);
        assert_eq!(x.nests, y.nests);
        assert_eq!(x.manifest, y.manifest);
    }
    for (x, y) in a.builders.iter().zip(&b.builders) {
        assert_eq!(x.collection, y.collection);
        assert_eq!(x.purchases, y.purchases);
        assert_eq!(x.manifest, y.manifest);
    }
}

#[test]
fn a_natural_patch_does_not_depend_on_the_wave_size() {
    let a = generate(&world(1500, 100, "natural:100"));
    let b = generate(&world(1500, 37, "natural:100"));
    let key = |e: &Everything| {
        (
            sorted(
                e.sets
                    .iter()
                    .flat_map(|w| w.sets.iter().map(|s| format!("{s:?}"))),
            ),
            sorted(
                e.sets
                    .iter()
                    .flat_map(|w| w.lines.iter().map(|l| format!("{l:?}"))),
            ),
            sorted(
                e.builders
                    .iter()
                    .flat_map(|w| w.purchases.iter().map(|p| format!("{p:?}"))),
            ),
        )
    };
    assert_eq!(key(&a), key(&b));
}

#[test]
fn a_different_seed_generates_different_rows() {
    let a = generate(&world(500, 100, "natural:100"));
    let real = Real::new(fixture());
    let board = Switchboard::new(&real, "natural:100", dials(), 500, 100).unwrap();
    let mut w = wiring(500, 100);
    w.seed = 8;
    let b = generate(&World::new(real, w, board).unwrap());
    assert_ne!(a.sets.last().unwrap().sets, b.sets.last().unwrap().sets);
}

/// Every manifest row names a written row that carries its trap, and every written row that
/// carries a trap the generator classifies is in the manifest.
#[test]
fn the_manifest_names_exactly_the_trapped_rows() {
    let w = world(3000, 100, "natural:60,wavy:20,interleaved:10,paired:10");
    let e = generate(&w);
    let pool = &w.real.cat.part_pool;
    let sets: HashMap<String, &world::SetOut> = e
        .sets
        .iter()
        .flat_map(|x| x.sets.iter())
        .map(|s| (s.set_num.clone(), s))
        .collect();
    assert_eq!(
        sets.len(),
        e.sets.iter().map(|x| x.sets.len()).sum::<usize>(),
        "set numbers are keys"
    );
    let invs: HashMap<String, &world::InvOut> = e
        .sets
        .iter()
        .flat_map(|x| x.inventories.iter())
        .map(|i| (i.id.to_string(), i))
        .collect();
    let line_key = |l: &world::LineOut| {
        format!(
            "{}|{}|{}|{}",
            l.inventory_id,
            pool[l.part as usize],
            l.color_id,
            if l.is_spare { 't' } else { 'f' }
        )
    };
    let lines: HashMap<String, &world::LineOut> = e
        .sets
        .iter()
        .flat_map(|x| x.lines.iter())
        .map(|l| (line_key(l), l))
        .collect();
    let coll: HashMap<String, &world::CollectionOut> = e
        .builders
        .iter()
        .flat_map(|x| x.collection.iter())
        .map(|c| (format!("{}|{}", c.builder_id, c.row_no), c))
        .collect();
    let purchases: HashMap<String, &world::PurchaseOut> = e
        .builders
        .iter()
        .flat_map(|x| x.purchases.iter())
        .map(|p| (p.purchase_id.to_string(), p))
        .collect();
    let builders: HashMap<i32, &world::BuilderOut> = e
        .builders
        .iter()
        .flat_map(|x| x.builders.iter())
        .map(|b| (b.builder_id, b))
        .collect();
    // A builder's zone is their street's city's.
    let zone_of = |p: &world::PurchaseOut| {
        let street = builders[&p.builder_id].street_id;
        &calendar::zones()[w.places.cities[w.places.city_of_street(street)].zone]
    };
    let themes: HashSet<i32> = w.real.cat.themes.iter().map(|t| t.id).collect();
    let parts: HashSet<&str> = w
        .real
        .cat
        .parts
        .iter()
        .map(|p| p.part_num.as_str())
        .collect();
    let local_day = |p: &world::PurchaseOut| match p.ordered_at {
        Stamp::At { t, offset_min, .. } => (t + i64::from(offset_min) * 60).div_euclid(86_400),
        Stamp::Infinity => unreachable!(),
    };

    let mut manifest: BTreeMap<Trap, HashSet<String>> = BTreeMap::new();
    for m in e
        .sets
        .iter()
        .flat_map(|x| x.manifest.iter())
        .chain(e.builders.iter().flat_map(|x| x.manifest.iter()))
    {
        manifest
            .entry(m.trap)
            .or_default()
            .insert(m.row_key.clone());
        // A collection row of a K trap names its set, and keeps what the builder typed beside it.
        let typed = |m: &world::ManifestOut| -> Option<(String, String)> {
            let c = coll[&m.row_key];
            let canonical = m.detail.trim_start_matches("canonical ");
            let named = c.set_num == canonical && sets.contains_key(canonical);
            match (&c.typed_set_num, &c.typed_name) {
                (Some(t), None) if named => Some((t.clone(), canonical.to_string())),
                _ => None,
            }
        };
        let ok = match m.trap {
            Trap::K1 => typed(m).is_some_and(|(t, canonical)| {
                t != canonical && t.eq_ignore_ascii_case(&canonical) && !sets.contains_key(&t)
            }),
            Trap::K2 => {
                typed(m).is_some_and(|(t, canonical)| t != canonical && t.trim() == canonical)
            }
            Trap::K3 => {
                let c = coll[&m.row_key];
                c.set_num == "10182-1"
                    && c.typed_set_num.is_none()
                    && matches!(
                        c.typed_name.as_deref(),
                        Some("Café Corner" | "Cafe\u{301} Corner")
                    )
            }
            Trap::K5 => typed(m).is_some_and(|(t, _)| {
                t.contains(['\u{2013}', '\u{2011}', '\u{00A0}']) && !sets.contains_key(&t)
            }),
            Trap::K4 => world::looks_round_tripped(&sets[&m.row_key].name),
            Trap::K6 => {
                world::basic_brick_disagreement(m.row_key.split('|').nth(1).unwrap())
                    && lines.contains_key(&m.row_key)
            }
            Trap::K7 => sets.contains_key(&m.row_key),
            // Filed under the bare record beside the -1, which holds no inventory of its own.
            Trap::K8 => {
                let inv = invs[&m.row_key];
                let has_lines = lines.values().any(|l| l.inventory_id == inv.id);
                let dash_one = format!("{}-1", inv.set_num);
                split_version(&inv.set_num).is_none()
                    && sets.contains_key(&inv.set_num)
                    && sets.contains_key(&dash_one)
                    && !invs.values().any(|i| i.set_num == dash_one)
                    && has_lines
            }
            Trap::B1 => sets[&m.row_key]
                .year
                .is_some_and(|y| y % 10 == 0 && y > 1950 && y < 2030),
            Trap::B2 => sets[&m.row_key].year.is_none(),
            Trap::B3 => sets[&m.row_key]
                .theme_id
                .is_none_or(|t| !themes.contains(&t)),
            Trap::B4 => sets[&m.row_key].year == Some(1949),
            Trap::B5 => sets[&m.row_key].year == Some(2031),
            Trap::B6 => sets[&m.row_key].num_parts == Some(-1),
            Trap::B7 => lines
                .get(&m.row_key)
                .is_some_and(|l| l.color_id == -1 || l.color_id == 9999),
            Trap::B8 => sets[&m.row_key].theme_id.is_none(),
            Trap::O2 => sets[&m.row_key]
                .set_num
                .bytes()
                .any(|b| b.is_ascii_alphabetic()),
            Trap::O5 => sets.contains_key(&m.row_key),
            Trap::B10 => {
                m.origin == world::Origin::Real
                    && lines.contains_key(&m.row_key)
                    && !parts.contains(m.row_key.split('|').nth(1).unwrap())
            }
            Trap::B9 | Trap::O1 | Trap::O3 | Trap::O4 => true,
            Trap::D1 => {
                let p = purchases[&m.row_key];
                purchases.values().any(|q| {
                    q.purchase_id != p.purchase_id
                        && q.ordered_local == p.ordered_local
                        && q.builder_id == p.builder_id
                })
            }
            Trap::D2 => {
                let p = purchases[&m.row_key];
                let (d, rest) = p.ordered_local.split_once(' ').unwrap();
                let ymd: Vec<u32> = d.split('-').map(|x| x.parse().unwrap()).collect();
                let hm: Vec<u32> = rest
                    .split(':')
                    .take(2)
                    .map(|x| x.parse().unwrap())
                    .collect();
                let local = calendar::local_secs(
                    calendar::days_from_civil(i64::from(ymd[0]), ymd[1], ymd[2]),
                    hm[0],
                    hm[1],
                );
                zone_of(p).instants_of(local) == LocalTime::Gap
            }
            Trap::D3 => {
                let p = purchases[&m.row_key];
                let Stamp::At { t, .. } = p.ordered_at else {
                    unreachable!()
                };
                let (ly, lm, _) = calendar::civil_from_days(local_day(p));
                let (uy, um, _) = calendar::civil_from_days(t.div_euclid(86_400));
                (ly, lm) != (uy, um)
            }
            Trap::D4 => purchases[&m.row_key]
                .ordered_local
                .ends_with(" 24:00:00.000"),
            Trap::D5 => purchases[&m.row_key].delivered_at == Stamp::Infinity,
            Trap::D6 => {
                let (_, mo, d) = calendar::civil_from_days(local_day(purchases[&m.row_key]));
                (mo, d) == (2, 29)
            }
            Trap::D7 => {
                let day = local_day(purchases[&m.row_key]);
                calendar::iso_week(day).0 != calendar::civil_from_days(day).0
            }
            Trap::D8 => {
                let p = purchases[&m.row_key];
                let midnight = calendar::local_secs(local_day(p), 0, 0);
                matches!(zone_of(p).instants_of(midnight), LocalTime::Twice(..))
            }
        };
        assert!(ok, "manifest row does not carry its trap: {m:?}");
    }
    // Conversely, for the traps the generator classifies row by row.
    let all_sets: Vec<&world::SetOut> = sets.values().copied().collect();
    let expect = |t: Trap, rows: HashSet<String>| {
        assert_eq!(
            manifest.get(&t).cloned().unwrap_or_default(),
            rows,
            "trap {t}"
        )
    };
    expect(
        Trap::B2,
        all_sets
            .iter()
            .filter(|s| s.year.is_none())
            .map(|s| s.set_num.clone())
            .collect(),
    );
    expect(
        Trap::B4,
        all_sets
            .iter()
            .filter(|s| s.year == Some(1949))
            .map(|s| s.set_num.clone())
            .collect(),
    );
    expect(
        Trap::B5,
        all_sets
            .iter()
            .filter(|s| s.year == Some(2031))
            .map(|s| s.set_num.clone())
            .collect(),
    );
    expect(
        Trap::B6,
        all_sets
            .iter()
            .filter(|s| s.num_parts == Some(-1))
            .map(|s| s.set_num.clone())
            .collect(),
    );
    expect(
        Trap::B1,
        all_sets
            .iter()
            .filter(|s| s.year.is_some_and(|y| y % 10 == 0 && y > 1950 && y < 2030))
            .map(|s| s.set_num.clone())
            .collect(),
    );
    expect(
        Trap::B7,
        lines
            .iter()
            .filter(|(_, l)| l.color_id == -1 || l.color_id == 9999)
            .map(|(k, _)| k.clone())
            .collect(),
    );
    expect(
        Trap::K6,
        lines
            .iter()
            .filter(|(_, l)| world::basic_brick_disagreement(&pool[l.part as usize]))
            .map(|(k, _)| k.clone())
            .collect(),
    );
    expect(
        Trap::B10,
        lines
            .iter()
            .filter(|(_, l)| !parts.contains(pool[l.part as usize].as_str()))
            .map(|(k, _)| k.clone())
            .collect(),
    );
    assert!(manifest
        .get(&Trap::B10)
        .is_some_and(|rows| !rows.is_empty()));
    // The builder's own spelling is kept on the K traps' collection rows, and on no other.
    let kept = |f: fn(&world::CollectionOut) -> bool| -> HashSet<String> {
        coll.iter()
            .filter(|(_, c)| f(c))
            .map(|(k, _)| k.clone())
            .collect()
    };
    let listed = |ts: &[Trap]| -> HashSet<String> {
        ts.iter()
            .flat_map(|t| manifest.get(t).cloned().unwrap_or_default())
            .collect()
    };
    assert!(!listed(&[Trap::K3]).is_empty());
    assert_eq!(
        kept(|c| c.typed_set_num.is_some()),
        listed(&[Trap::K1, Trap::K2, Trap::K5])
    );
    assert_eq!(kept(|c| c.typed_name.is_some()), listed(&[Trap::K3]));
    let twins: HashSet<String> = all_sets
        .iter()
        .filter(|s| {
            split_version(&s.set_num).is_none() && sets.contains_key(&format!("{}-1", s.set_num))
        })
        .flat_map(|s| [s.set_num.clone(), format!("{}-1", s.set_num)])
        .collect();
    assert!(manifest[&Trap::K7].iter().all(|k| twins.contains(k)));
    expect(
        Trap::D6,
        purchases
            .iter()
            .filter(|(_, p)| {
                p.ordered_at != Stamp::Infinity
                    && calendar::civil_from_days(local_day(p)).1 == 2
                    && calendar::civil_from_days(local_day(p)).2 == 29
            })
            .map(|(k, _)| k.clone())
            .collect(),
    );
    expect(
        Trap::D8,
        purchases
            .iter()
            .filter(|(_, p)| {
                p.ordered_at != Stamp::Infinity
                    && matches!(
                        zone_of(p).instants_of(calendar::local_secs(local_day(p), 0, 0)),
                        LocalTime::Twice(..)
                    )
            })
            .map(|(k, _)| k.clone())
            .collect(),
    );
    assert!(manifest.get(&Trap::D8).is_some_and(|rows| !rows.is_empty()));
    // The planted counts are the declared ones.
    for d in DECLS {
        let population = match d.population {
            crate::traps::Population::Sets => w.wiring.sets,
            crate::traps::Population::CollectionRows => w.collection_rows(),
            _ => 0,
        };
        let planted = w.planted.iter().find(|p| p.0 == d.trap).map_or(0, |p| p.1);
        assert_eq!(planted, d.planted(population), "trap {}", d.trap);
    }
}

/// Every purchase writes its clock as its instant read in the builder's home zone, and every
/// instant with the offset in force then, except where a trap writes them otherwise: D2's receipt
/// in the offset before a gap, and D4's 24:00 of the eve.
#[test]
fn every_written_clock_reads_its_instant_but_the_traps_that_write_it_otherwise() {
    let w = world(3000, 100, "natural:100");
    let e = generate(&w);
    let zone: HashMap<i32, &calendar::Zone> = e
        .builders
        .iter()
        .flat_map(|x| x.builders.iter())
        .map(|b| {
            let z = &calendar::zones()[w.places.cities[w.places.city_of_street(b.street_id)].zone];
            (b.builder_id, z)
        })
        .collect();
    let trapped = |t: Trap| -> HashSet<String> {
        e.builders
            .iter()
            .flat_map(|x| x.manifest.iter())
            .filter(|m| m.trap == t)
            .map(|m| m.row_key.clone())
            .collect()
    };
    let (d2, d4) = (trapped(Trap::D2), trapped(Trap::D4));
    assert!(!d2.is_empty() && !d4.is_empty());
    let mut read = 0;
    let (mut seconds, mut millis) = (HashSet::new(), HashSet::new());
    for p in e.builders.iter().flat_map(|x| x.purchases.iter()) {
        let z = zone[&p.builder_id];
        let key = p.purchase_id.to_string();
        let Stamp::At { t, ms, offset_min } = p.ordered_at else {
            panic!("{key}: no instant")
        };
        let reading = calendar::render_local(t + i64::from(z.offset_min_at(t)) * 60, ms);
        let in_force = offset_min == z.offset_min_at(t);
        if d2.contains(&key) {
            assert!(!in_force && p.ordered_local != reading, "{p:?}");
        } else {
            assert!(in_force, "{p:?}");
            assert_eq!(p.ordered_local != reading, d4.contains(&key), "{p:?}");
        }
        if let Stamp::At { t, offset_min, .. } = p.delivered_at {
            assert_eq!(offset_min, z.offset_min_at(t), "{p:?}");
        }
        seconds.insert(t.rem_euclid(60));
        millis.insert(ms);
        read += 1;
    }
    assert!(read > 100, "{read} purchases");
    // The clocks run to the millisecond.
    assert!(seconds.len() > 1, "{seconds:?}");
    assert!(millis.len() > 1, "{millis:?}");
}

/// Where in its minute a reading falls is drawn apart from the minute: a second draw of it moves
/// every instant and every reading within its minute, and none out of it.
#[test]
fn the_seconds_move_every_reading_within_its_minute_only() {
    let w = world(3000, 100, "natural:100");
    let minute = |s: &Stamp| match *s {
        Stamp::At { t, .. } => Some(t.div_euclid(60)),
        Stamp::Infinity => None,
    };
    let lisbon = ZONE_NAMES
        .iter()
        .position(|z| *z == "Europe/Lisbon")
        .unwrap();
    let traps = [
        None,
        Some(Trap::D1),
        Some(Trap::D2),
        Some(Trap::D3),
        Some(Trap::D4),
        Some(Trap::D5),
        Some(Trap::D6),
        Some(Trap::D7),
    ];
    let (mut read, mut moved) = (0, 0);
    for trap in traps {
        for k in 0..20 {
            let times = |s: u64| {
                w.purchase_times(
                    lisbon,
                    trap.map(|t| (t, 0)),
                    k,
                    1990,
                    4,
                    &mut crate::rng::Rng::new(k),
                    &mut crate::rng::Rng::new(s),
                )
            };
            let (a, b) = (times(1), times(2));
            assert_eq!(a.len(), b.len(), "{trap:?}");
            for (x, y) in a.iter().zip(&b) {
                assert_eq!(x.0, y.0, "{trap:?}");
                assert_eq!(minute(&x.1), minute(&y.1), "{trap:?} {x:?} {y:?}");
                assert_eq!(x.2[..16], y.2[..16], "{trap:?} {x:?} {y:?}");
                assert_eq!(minute(&x.3), minute(&y.3), "{trap:?} {x:?} {y:?}");
                read += 1;
                moved += usize::from(x.1 != y.1);
            }
        }
    }
    // D4's midnight is the only reading nothing falls past.
    assert_eq!(read - moved, 20, "{moved} of {read} moved");
}

#[test]
fn a_year_without_releases_moves_to_the_nearest_release_year_the_later_on_a_tie() {
    let moved: Vec<(i32, i32)> = [
        1993, 1994, 1995, 1996, 1997, 2000, 2001, 2005, 2006, 2007, 2011,
    ]
    .iter()
    .map(|&y| (y, nearest_release_year(y)))
    .collect();
    assert_eq!(
        moved,
        [
            (1993, 1993),
            (1994, 1993),
            (1995, 1997),
            (1996, 1997),
            (1997, 1997),
            (2000, 2000),
            (2001, 2000),
            (2005, 2000),
            (2006, 2012),
            (2007, 2012),
            (2011, 2012)
        ]
    );
    assert!((1950..=2026).all(|y| is_release_year(nearest_release_year(y))));
}

/// The modelled years keep the real years' growth to the end of the real years, then grow
/// `GROWTH_FACTOR` times as fast: a series doubling every year is followed by one growing eightfold
/// a year, and a flat series stays flat.
#[test]
fn the_modelled_years_grow_at_the_factor_times_the_fitted_rate() {
    let doubling: Vec<(i32, u64)> = MODEL_FIT.map(|y| (y, 1u64 << (y - 2000))).collect();
    let (modelled, growth) = model_releases(&doubling);
    assert!((growth - 2f64.ln()).abs() < 1e-9);
    assert_eq!(
        modelled.iter().map(|m| m.0).collect::<Vec<_>>(),
        MODELLED.collect::<Vec<_>>()
    );
    let last_fitted = (1u64 << (MODEL_FIT.end() - 2000)) as f64;
    let at_2017 = last_fitted * 2.0;
    for (y, n) in &modelled {
        let want = at_2017 * 2f64.powf(GROWTH_FACTOR * f64::from(y - 2017));
        assert!((n / want - 1.0).abs() < 1e-9, "{y}: {n} against {want}");
    }
    let (flat, growth) = model_releases(&MODEL_FIT.map(|y| (y, 6)).collect::<Vec<_>>());
    assert_eq!(growth, 0.0);
    assert!(flat.iter().all(|&(_, n)| (n - 6.0).abs() < 1e-9));
}

/// No set is released in a year of `NO_RELEASE`: the real sets of those years are re-dated to the
/// nearest release year and listed under B9 with the year the dump gives them, and the synthesized
/// sets reach every modelled year.
#[test]
fn no_set_is_released_in_a_gap_and_the_modelled_years_are_reached() {
    let w = world(3000, 100, "natural:60,wavy:20,interleaved:10,paired:10");
    let e = generate(&w);
    let fixture_years: HashMap<String, i32> = fixture()
        .sets
        .iter()
        .filter_map(|s| s.year.map(|y| (s.set_num.clone(), y)))
        .collect();
    let mut synthetic_years: HashSet<i32> = HashSet::new();
    let mut redated: HashMap<String, String> = HashMap::new();
    for x in &e.sets {
        for s in &x.sets {
            assert!(s.year.is_none_or(is_release_year), "{s:?}");
            match fixture_years.get(&s.set_num) {
                Some(&dump) => {
                    assert_eq!(s.year, Some(nearest_release_year(dump)), "{s:?}");
                    if !is_release_year(dump) {
                        redated.insert(s.set_num.clone(), format!("released in {dump}"));
                    }
                }
                None => {
                    synthetic_years.extend(s.year);
                }
            }
        }
    }
    let listed: HashMap<String, String> = e
        .sets
        .iter()
        .flat_map(|x| x.manifest.iter())
        .filter(|m| m.trap == Trap::B9 && m.origin == world::Origin::Real)
        .map(|m| (m.row_key.clone(), m.detail.clone()))
        .collect();
    assert!(!redated.is_empty());
    assert_eq!(listed, redated);
    let missing: Vec<i32> = MODELLED.filter(|y| !synthetic_years.contains(y)).collect();
    assert!(
        missing.is_empty(),
        "modelled years no synthesized set reaches: {missing:?}"
    );
}

/// At `PURCHASES_PER_SET`, almost every set on sale is bought, the most bought tenth of the sets
/// takes about half the purchases, and a real set is bought only on the days of its timeline.
#[test]
fn buying_reaches_almost_every_set_skewed_and_within_each_timeline() {
    let real = Real::new(fixture());
    let sets = 3000;
    let board = Switchboard::new(&real, "natural:100", dials(), sets, 100).expect("patch");
    let mut wiring = wiring(sets, 100);
    wiring.builders = world::builders_for(sets + real.cat.sets.len() as u64);
    let w = World::new(real, wiring, board).expect("world");
    let e = generate(&w);
    let mut bought: HashMap<u64, u64> = HashMap::new();
    for (set, n) in e.builders.iter().flat_map(|x| x.bought.iter()) {
        *bought.entry(*set).or_default() += u64::from(*n);
    }
    let mut counts: Vec<u64> = w
        .buyable
        .iter()
        .map(|s| bought.get(s).copied().unwrap_or(0))
        .collect();
    counts.sort_unstable_by(|a, b| b.cmp(a));
    let once = counts.iter().filter(|&&n| n > 0).count() as f64 / counts.len() as f64;
    let total: u64 = counts.iter().sum();
    let top: u64 = counts[..counts.len() / 10].iter().sum();
    let top_share = top as f64 / total as f64;
    assert!(once > 0.9, "{once} of the sets on sale bought");
    assert!((0.4..0.7).contains(&top_share), "top tenth {top_share}");

    // A purchase's set is its collection row's.
    let row_set: HashMap<(i32, i32), &str> = e
        .builders
        .iter()
        .flat_map(|x| x.collection.iter())
        .map(|c| ((c.builder_id, c.row_no), c.set_num.as_str()))
        .collect();
    let set_of = |p: &world::PurchaseOut| row_set[&(p.builder_id, p.row_no)];
    let today = calendar::days_from_civil(world::TODAY.0, world::TODAY.1, world::TODAY.2);
    let mut read = 0;
    for p in e.builders.iter().flat_map(|x| x.purchases.iter()) {
        let (Some(&s), Stamp::At { t, offset_min, .. }) =
            (w.real.set_by_num.get(set_of(p)), p.ordered_at)
        else {
            continue;
        };
        // A trap's row writes its own instants.
        if w.row_traps.contains_key(&((p.purchase_id as u64 - 1) / 4)) {
            continue;
        }
        let Some(tl) = w.timeline_of(u64::from(s)) else {
            continue;
        };
        let day = (t + i64::from(offset_min) * 60).div_euclid(86_400);
        assert!(
            day >= tl.lo && day <= today,
            "{p:?} outside {}..={today}",
            tl.lo
        );
        read += 1;
    }
    assert!(read > 1000, "{read} purchases of real sets");

    // A bare record beside its -1 is sold as its -1: no purchase names it.
    let keys: HashSet<&str> = w.real.cat.sets.iter().map(|s| s.set_num.as_str()).collect();
    let twins: Vec<&str> = keys
        .iter()
        .copied()
        .filter(|k| split_version(k).is_none() && keys.contains(format!("{k}-1").as_str()))
        .collect();
    assert!(!twins.is_empty());
    let named: Vec<&world::PurchaseOut> = e
        .builders
        .iter()
        .flat_map(|x| x.purchases.iter())
        .filter(|p| twins.contains(&set_of(p)))
        .collect();
    assert!(
        named.is_empty(),
        "{} purchases name a bare twin",
        named.len()
    );
}

/// A bare record beside its -1 carries the -1's year, so a twin of a set released in a decade's
/// first year is itself such a set, and is in the manifest as one.
#[test]
fn a_twin_record_is_classified_like_any_set() {
    // Search seeds until a twin lands in a decade's first year, so the check has something to check.
    let mut on_endpoint = 0;
    for seed in 1..200u64 {
        let real = Real::new(fixture());
        let board = Switchboard::new(&real, "natural:100", dials(), 3000, 100).unwrap();
        let mut wiring = wiring(3000, 100);
        wiring.seed = seed;
        let w = World::new(real, wiring, board).unwrap();
        let e = generate(&w);
        let manifest: HashSet<(Trap, String)> = e
            .sets
            .iter()
            .flat_map(|x| x.manifest.iter())
            .map(|m| (m.trap, m.row_key.clone()))
            .collect();
        for s in e.sets.iter().flat_map(|x| x.sets.iter()) {
            if split_version(&s.set_num).is_none()
                && manifest.contains(&(Trap::K7, s.set_num.clone()))
            {
                let endpoint = s.year.is_some_and(|y| y % 10 == 0 && y > 1950 && y < 2030);
                assert_eq!(
                    manifest.contains(&(Trap::B1, s.set_num.clone())),
                    endpoint,
                    "seed {seed} twin {}",
                    s.set_num
                );
                on_endpoint += u32::from(endpoint);
            }
        }
        if on_endpoint > 0 {
            break;
        }
    }
    assert!(
        on_endpoint > 0,
        "no seed put a twin in a decade's first year"
    );
}

#[test]
fn a_target_schema_is_refused_when_it_names_the_source_schema() {
    assert!(crate::same_schema("public", "public"));
    assert!(crate::same_schema("Public", "public"));
    assert!(crate::same_schema("\"public\"", "PUBLIC"));
    assert!(!crate::same_schema("\"Public\"", "public"));
    assert!(!crate::same_schema("lego", "public"));
}

/// The paired packs: each pair is a first and a second pack, whose offsets are the two patterns of
/// a declared pair and name each other as partner, drawn from one window, mirrored alike. Each
/// pack's child sets are released in the years its offsets place from the window, one to a year.
#[test]
fn paired_packs_hold_the_years_their_manifest_rows_name() {
    let w = world(3000, 100, "natural:80,paired:20");
    let e = generate(&w);
    let years: HashMap<&str, i32> = w
        .real
        .cat
        .sets
        .iter()
        .filter_map(|s| s.year.map(|y| (s.set_num.as_str(), y)))
        .collect();
    let offsets = |s: &str| -> Vec<i32> { s.split(',').map(|x| x.parse().unwrap()).collect() };
    // Per pair, each member's manifest fields and its child sets' years, in the order written.
    struct Member {
        member: String,
        offsets: Vec<i32>,
        partner: Vec<i32>,
        window: i32,
        mirrored: bool,
        years: Vec<i32>,
    }
    let mut packs: BTreeMap<u64, Vec<Member>> = BTreeMap::new();
    for wave in &e.sets {
        let inv_of: HashMap<i32, &str> = wave
            .inventories
            .iter()
            .map(|i| (i.id, i.set_num.as_str()))
            .collect();
        let mut kids: HashMap<&str, Vec<i32>> = HashMap::new();
        for n in &wave.nests {
            kids.entry(inv_of[&n.inventory_id])
                .or_default()
                .push(years[n.set_num.as_str()]);
        }
        for m in wave.manifest.iter().filter(|m| m.trap == Trap::O5) {
            let field = |name: &str| -> String {
                m.detail
                    .split_whitespace()
                    .find_map(|f| f.strip_prefix(name))
                    .unwrap_or_else(|| panic!("{}: no {name}", m.detail))
                    .to_string()
            };
            let mut ys = kids[m.row_key.as_str()].clone();
            ys.sort_unstable();
            packs
                .entry(field("pair=").parse().unwrap())
                .or_default()
                .push(Member {
                    member: field("member="),
                    offsets: offsets(&field("offsets=")),
                    partner: offsets(&field("partner_offsets=")),
                    window: field("window=").parse().unwrap(),
                    mirrored: field("mirrored=").parse().unwrap(),
                    years: ys,
                });
        }
    }
    assert!(packs.len() >= 3);
    let declared: Vec<(Vec<i32>, Vec<i32>)> = paired::YEAR_PAIRS
        .iter()
        .map(|(a, b)| (a.to_vec(), b.to_vec()))
        .collect();
    for (pair, members) in &packs {
        assert_eq!(members.len(), 2, "pair {pair}");
        let (a, b) = (&members[0], &members[1]);
        assert_eq!(
            (a.member.as_str(), b.member.as_str()),
            ("first", "second"),
            "pair {pair}"
        );
        assert_eq!(
            (&a.partner, &b.partner),
            (&b.offsets, &a.offsets),
            "pair {pair}"
        );
        assert!(
            declared.contains(&(a.offsets.clone(), b.offsets.clone())),
            "pair {pair}: {:?} and {:?} are not a declared pair",
            a.offsets,
            b.offsets
        );
        assert_eq!(
            (a.window, a.mirrored),
            (b.window, b.mirrored),
            "pair {pair}"
        );
        for m in members {
            let placed: Vec<i32> = paired::placed(&m.offsets, m.mirrored)
                .iter()
                .map(|o| m.window + o)
                .collect();
            assert_eq!(m.years, placed, "pair {pair} {}", m.member);
        }
    }
}

/// Every reference lands in a row of its owner: through each reference, every row names a row the
/// named table holds, and every row the manifest names exists. The one exception is the real
/// catalogue's own lines whose part number its parts list does not hold, which the manifest lists
/// under B10, every one of them real.
#[test]
fn every_reference_lands_in_a_row_of_its_owner() {
    let w = world(3000, 100, "natural:60,wavy:20,interleaved:10,paired:10");
    let e = generate(&w);
    let db = written(&w, &e);
    let manifest = manifest_rows(&db);
    let declared: BTreeSet<String> = manifest
        .iter()
        .filter(|m| m.0 == "B10")
        .map(|m| {
            assert_eq!(m.1, "real", "{m:?}");
            m.3.clone()
        })
        .collect();
    assert!(!declared.is_empty(), "no line names an unlisted part");
    for &(table, columns, owner, key) in REFERENCES {
        let o = &db[owner];
        let owner_keys: HashSet<Vec<String>> = o
            .rows
            .iter()
            .filter_map(|r| o.project(r, &cols(key)))
            .collect();
        let t = &db[table];
        let (mut naming, mut dangling) = (0, BTreeSet::new());
        for r in &t.rows {
            let Some(named) = t.project(r, &cols(columns)) else {
                continue;
            };
            naming += 1;
            if !owner_keys.contains(&named) {
                dangling.insert(manifest_key(t, table, r).unwrap_or_else(|| named.join("|")));
            }
        }
        let refers = format!("{table} ({columns}) -> {owner} ({key})");
        assert!(naming > 0, "{refers}: no row names one");
        let allowed = if (table, columns) == ("lego_inventory_parts", "part_num") {
            declared.clone()
        } else {
            BTreeSet::new()
        };
        assert_eq!(
            dangling, allowed,
            "{refers}: the rows, of {naming}, that name no row of the owner"
        );
    }
    let mut keys: HashMap<&str, HashSet<String>> = HashMap::new();
    for (table, t) in &db {
        for r in &t.rows {
            if let Some(k) = manifest_key(t, table, r) {
                keys.entry(table).or_default().insert(k);
            }
        }
    }
    let unnamed: Vec<&(String, String, String, String)> = manifest
        .iter()
        .filter(|m| m.3 != "*")
        .filter(|m| !keys.get(m.2.as_str()).is_some_and(|k| k.contains(&m.3)))
        .collect();
    assert!(
        unnamed.is_empty(),
        "manifest rows naming no row: {unnamed:?}"
    );
}

/// No column repeats a value its references decide: outside its key and its references, no column
/// of a generated table equals a column of the row it reaches through one, two or three references,
/// on every row that no trap lists, unless the column holds one value throughout.
#[test]
fn no_column_repeats_a_value_its_references_decide() {
    let w = world(3000, 100, "natural:60,wavy:20,interleaved:10,paired:10");
    let e = generate(&w);
    let db = written(&w, &e);
    let listed: HashSet<(String, String)> =
        manifest_rows(&db).into_iter().map(|m| (m.2, m.3)).collect();
    // Each owner's rows by the key a reference names them by.
    let mut index: HashMap<(&str, &str), HashMap<Vec<String>, usize>> = HashMap::new();
    for &(_, _, owner, key) in REFERENCES {
        let o = &db[owner];
        index.entry((owner, key)).or_insert_with(|| {
            o.rows
                .iter()
                .enumerate()
                .filter_map(|(i, r)| o.project(r, &cols(key)).map(|k| (k, i)))
                .collect()
        });
    }
    let (mut compared, mut repeats) = (0usize, Vec::new());
    for def in crate::load::TABLES
        .iter()
        .filter(|t| !matches!(t.name, "trap_manifest" | "generator_run"))
    {
        let table = def.name;
        let t = &db[table];
        let refs: Vec<&Reference> = REFERENCES.iter().filter(|r| r.0 == table).collect();
        let key: Vec<&str> = def.key.map_or(Vec::new(), |k| k.split(", ").collect());
        let own: Vec<&str> = t
            .columns
            .iter()
            .copied()
            .filter(|c| !key.contains(c) && !refs.iter().any(|r| cols(r.1).contains(c)))
            .collect();
        let mut paths: Vec<Vec<&Reference>> = refs.iter().map(|r| vec![*r]).collect();
        for _ in 1..3 {
            let longer: Vec<Vec<&Reference>> = paths
                .iter()
                .filter(|p| p.len() == paths.iter().map(Vec::len).max().unwrap_or(0))
                .flat_map(|p| {
                    let end = p.last().expect("a path").2;
                    REFERENCES.iter().filter(move |r| r.0 == end).map(move |r| {
                        let mut q = p.clone();
                        q.push(r);
                        q
                    })
                })
                .collect();
            paths.extend(longer);
        }
        for path in &paths {
            let owner = path.last().expect("a path").2;
            let o = &db[owner];
            let mut stat: HashMap<(&str, &str), (bool, usize, HashSet<&str>)> = HashMap::new();
            for row in &t.rows {
                if manifest_key(t, table, row)
                    .is_some_and(|k| listed.contains(&(table.to_string(), k)))
                {
                    continue;
                }
                let mut at: Option<&Vec<Option<String>>> = Some(row);
                let mut from = table;
                for r in path {
                    at = at
                        .and_then(|cur| db[from].project(cur, &cols(r.1)))
                        .and_then(|k| index[&(r.2, r.3)].get(&k).copied())
                        .map(|i| &db[r.2].rows[i]);
                    from = r.2;
                }
                let Some(reached) = at else {
                    continue;
                };
                for c in &own {
                    let Some(v) = &row[t.at(c)] else {
                        continue;
                    };
                    for a in &o.columns {
                        let Some(u) = &reached[o.at(a)] else {
                            continue;
                        };
                        let s = stat.entry((*c, *a)).or_insert((true, 0, HashSet::new()));
                        s.0 &= v == u;
                        s.1 += 1;
                        s.2.insert(v.as_str());
                    }
                }
            }
            for ((c, a), (equal, n, values)) in stat {
                compared += n;
                if equal && values.len() > 1 {
                    let through: Vec<&str> = path.iter().map(|r| r.2).collect();
                    repeats.push(format!(
                        "{table}.{c} = {owner}.{a} on all {n} rows, through {}",
                        through.join(" -> ")
                    ));
                }
            }
        }
    }
    assert!(compared > 0);
    assert!(repeats.is_empty(), "{repeats:#?}");
}

/// The column names an expression in an index's key reads, without the words of SQL beside them.
fn expression_columns(sql: &str) -> Vec<String> {
    const SQL_WORDS: [&str; 4] = ["extract", "month", "from", "smallint"];
    sql.replace("{clock}", " ")
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .filter(|w| !w.is_empty() && !w.bytes().all(|b| b.is_ascii_digit()))
        .map(str::to_ascii_lowercase)
        .filter(|w| !SQL_WORDS.contains(&w.as_str()))
        .collect()
}

/// Each composite index reads only its own table: every column its key names, directly or in an
/// expression, and every column of its `INCLUDE` list is a column of the index's table.
#[test]
fn each_composite_index_reads_only_its_own_table() {
    for s in COMPOSITE_INDEXES {
        let columns = cols(crate::load::table(s.table).columns);
        let mut named: Vec<String> = s.include.iter().map(|c| c.to_string()).collect();
        for p in s.parts {
            match p {
                KeyPart::Column(c) => named.push(c.to_string()),
                KeyPart::Expression { sql, .. } => named.extend(expression_columns(sql)),
            }
        }
        for n in &named {
            assert!(
                columns.contains(&n.as_str()),
                "{} names {n}, which is not a column of {}",
                index_name(s.table, s.parts),
                s.table
            );
        }
    }
}

/// A column a composite index's key reads only through an expression is also in the index as the
/// column itself, in its key or its `INCLUDE` list, so the index returns what a query on that
/// column reads. Some index has an expression to check.
#[test]
fn each_composite_index_includes_the_columns_its_expressions_read() {
    let mut expressions = 0;
    for s in COMPOSITE_INDEXES {
        let plain: Vec<&str> = s
            .parts
            .iter()
            .filter_map(|p| match p {
                KeyPart::Column(c) => Some(*c),
                _ => None,
            })
            .chain(s.include.iter().copied())
            .collect();
        for p in s.parts {
            let KeyPart::Expression { sql, .. } = p else {
                continue;
            };
            expressions += 1;
            for c in expression_columns(sql) {
                assert!(
                    plain.contains(&c.as_str()),
                    "{} reads {c} through an expression and does not include it",
                    index_name(s.table, s.parts)
                );
            }
        }
    }
    assert!(expressions > 0);
}

/// Each composite index lists its plain columns before its expressions: no column of the table
/// follows an expression in its key. Some index has an expression, so the order is checked on one.
#[test]
fn each_composite_index_lists_its_plain_columns_before_its_expressions() {
    let mut with_expressions = 0;
    for s in COMPOSITE_INDEXES {
        let first_expression = s
            .parts
            .iter()
            .position(|p| matches!(p, KeyPart::Expression { .. }));
        let Some(at) = first_expression else {
            continue;
        };
        with_expressions += 1;
        assert!(
            s.parts[at..]
                .iter()
                .all(|p| matches!(p, KeyPart::Expression { .. })),
            "{} lists a column after an expression",
            index_name(s.table, s.parts)
        );
    }
    assert!(with_expressions > 0);
}

/// The example's migration builds on the dump's tables exactly the composite indexes the generator
/// builds on them, in the same order and under the same names, none with an expression, since the
/// example makes no clock function.
#[test]
fn the_example_migration_builds_the_composite_indexes_of_the_dump_tables() {
    let sql = include_str!("../../lego/migrations/20260930000000_composite_indexes.sql");
    let text = sql
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join(" ");
    let built: Vec<String> = text
        .split(';')
        .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|s| !s.is_empty())
        .collect();
    let dump: Vec<&CompositeIndex> = COMPOSITE_INDEXES
        .iter()
        .filter(|s| crate::catalogue::TABLES.contains(&s.table))
        .collect();
    assert!(dump
        .iter()
        .all(|s| s.parts.iter().all(|p| matches!(p, KeyPart::Column(_)))));
    let expected: Vec<String> = dump
        .iter()
        .map(|s| {
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} {}",
                index_name(s.table, s.parts),
                s.table,
                s.definition("public")
            )
        })
        .collect();
    assert_eq!(built, expected);
}

/// A composite index's name longer than the server keeps is cut to fit, keeps its front, and stays
/// apart from the names of the table's other composite indexes.
#[test]
fn an_index_name_past_the_limit_is_cut_to_fit_and_stays_distinct() {
    let long = "lego_sets_a_class_whose_name_is_long_enough_to_pass_the_limit";
    let names: Vec<String> = COMPOSITE_INDEXES
        .iter()
        .filter(|s| s.table == "lego_sets")
        .map(|s| index_name(long, s.parts))
        .collect();
    assert!(names.len() > 1);
    for n in &names {
        assert!(
            n.len() <= NAME_BYTES && n.starts_with("lego_sets_a_class"),
            "{n}"
        );
    }
    assert_eq!(names.iter().collect::<HashSet<_>>().len(), names.len());
}

/// Every synthesized set number, and the eight-digit number it moves to when its bare form is a
/// real record, is distinct from every other through three times nine million sets; the numbers
/// below nine million are the ones earlier runs wrote.
#[test]
fn set_numbers_stay_distinct_past_nine_million_sets() {
    let seed = 20_260_926;
    assert_eq!(world::base_number(seed, 0), 3_260_926);
    assert_eq!(world::base_number(seed, 8_999_999), 4_606_605);
    let mut seen = vec![0u64; 1 << 20];
    for i in 0..27_000_000 {
        let b = world::base_number(seed, i);
        for n in [b, b + 9_000_000] {
            let (word, bit) = ((n / 64) as usize, n % 64);
            assert!(seen[word] >> bit & 1 == 0, "set {i} repeats the number {n}");
            seen[word] |= 1 << bit;
        }
    }
}

/// A set count whose inventories would number past the largest integer the tables hold is refused
/// before anything is generated.
#[test]
fn a_set_count_past_the_tables_integers_is_refused() {
    let sets = 1_100_000_000;
    let real = Real::new(fixture());
    let board = Switchboard::new(&real, "natural:100", dials(), sets, 1000).expect("patch");
    match World::new(real, wiring(sets, 1000), board) {
        Ok(_) => panic!("{sets} sets were accepted"),
        Err(e) => assert!(e.contains(&i32::MAX.to_string()), "{e}"),
    }
}

/// The build timeout is an hour for every two million sets, and never less than an hour.
#[test]
fn the_build_timeout_grows_an_hour_for_every_two_million_sets() {
    assert_eq!(crate::build_timeout_for(20_000), "3600s");
    assert_eq!(crate::build_timeout_for(2_000_000), "3600s");
    assert_eq!(crate::build_timeout_for(8_000_000), "14400s");
    assert_eq!(crate::build_timeout_for(800_000_000), "1440000s");
}

/// Every named size is read back from its name, and numbers its inventories within the tables'
/// integers.
#[test]
fn every_named_size_fits_the_tables_integers() {
    use clap::ValueEnum;
    let real = Real::new(fixture());
    for &size in crate::Size::value_variants() {
        assert_eq!(crate::Size::from_str(&size.name(), false), Ok(size));
        assert!(
            world::inventory_ceiling(&real, size.sets()) <= i64::from(i32::MAX),
            "{}",
            size.name()
        );
    }
}

/// A trap left out of `--traps` is planted on no row, and every other trap is planted as often as
/// when all are. Each left out trap is planted when all are, so the run has something to leave out.
#[test]
fn a_trap_left_out_is_planted_nowhere_and_every_other_as_often() {
    let patch = "natural:60,wavy:20,interleaved:10,paired:10";
    let left_out = [Trap::K3, Trap::K7, Trap::B3, Trap::D3, Trap::O5];
    let mut traps = crate::traps::all_planted();
    for t in left_out {
        assert!(traps.remove(&t), "{t}");
    }
    let all = world(3000, 100, patch);
    let some = world_planting(3000, 100, patch, traps).expect("world");
    let planted = |w: &World, trap: Trap| w.planted.iter().find(|p| p.0 == trap).map(|p| p.1);
    for d in DECLS {
        let (a, s) = (planted(&all, d.trap), planted(&some, d.trap));
        if left_out.contains(&d.trap) && d.trap != Trap::O5 {
            assert!(a.unwrap() > 0, "{}", d.trap);
            assert_eq!(s, Some(0), "{}", d.trap);
        } else {
            assert_eq!(s, a, "{}", d.trap);
        }
    }
    for (trap, _) in some.set_traps.values().chain(some.row_traps.values()) {
        assert!(!left_out.contains(trap), "{trap} planted");
    }
    assert!(!all.added_themes().is_empty());
    assert!(some.added_themes().is_empty());
    let o5 = |w: &World| {
        let e = generate(w);
        e.sets
            .iter()
            .flat_map(|wave| wave.manifest.iter())
            .filter(|m| m.trap == Trap::O5)
            .count()
    };
    assert!(o5(&all) > 0);
    assert_eq!(o5(&some), 0);
    let db = written(&some, &generate(&some));
    let c = &db["lego_collection"];
    let typed = c.at("typed_name");
    assert!(c.rows.iter().all(|r| r[typed].is_none()));
}

/// With no trap planted, no row is picked for one and the paired phase writes no pair, while the
/// real catalogue's own trapped rows are listed exactly as when every trap is planted.
#[test]
fn with_no_trap_planted_the_real_catalogue_keeps_its_own_trapped_rows() {
    let patch = "natural:90,paired:10";
    let all = world(1500, 100, patch);
    let none = world_planting(1500, 100, patch, BTreeSet::new()).expect("world");
    assert!(none.set_traps.is_empty() && none.row_traps.is_empty());
    assert!(none.planted.iter().all(|p| p.1 == 0));
    let rows = |w: &World| manifest_rows(&written(w, &generate(w)));
    let (all_rows, none_rows) = (rows(&all), rows(&none));
    assert!(!none_rows.iter().any(|m| m.0 == "O5"));
    let real = |rows: &[(String, String, String, String)]| {
        sorted(rows.iter().filter(|m| m.1 == "real").cloned())
    };
    assert!(!real(&none_rows).is_empty());
    assert_eq!(real(&none_rows), real(&all_rows));
}

/// Pre-orders (D5) name a set that is not out yet, which only traps B2 and B5 plant, so D5 alone is
/// refused before anything is generated.
#[test]
fn pre_orders_without_an_unreleased_set_are_refused() {
    let only_d5: BTreeSet<Trap> = [Trap::D5].into_iter().collect();
    match world_planting(500, 100, "natural:100", only_d5) {
        Ok(_) => panic!("D5 was planted without an unreleased set"),
        Err(e) => assert!(e.contains("B2") && e.contains("--traps"), "{e}"),
    }
    let with_b2: BTreeSet<Trap> = [Trap::D5, Trap::B2].into_iter().collect();
    assert!(world_planting(500, 100, "natural:100", with_b2).is_ok());
}

/// A catalogue that repeats a line's or a nesting row's natural key is found when it is read, by
/// the line and the row that repeat; the fixture repeats neither.
#[test]
fn a_catalogue_repeating_a_natural_key_is_found() {
    assert!(fixture().repeated_keys().is_empty());
    let mut cat = fixture();
    let line = cat.lines[3];
    cat.lines.insert(4, line);
    let nest = cat.nests[0].clone();
    cat.nests.insert(1, nest.clone());
    let found = cat.repeated_keys();
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(found[0].starts_with(&format!("lego_inventory_parts ({}, ", line.inventory_id)));
    assert_eq!(
        found[1],
        format!(
            "lego_inventory_sets ({}, {})",
            nest.inventory_id, nest.set_num
        )
    );
}

/// Every key a generated table declares, its primary key and the unique keys the load adds, holds
/// on every row the load writes, the synthesized rows of every pattern among them. The run's own
/// `generator_run` is written apart, from the run's settings.
#[test]
fn every_declared_key_holds_on_every_written_row() {
    let w = world(3000, 100, "natural:60,wavy:20,interleaved:10,paired:10");
    let db = written(&w, &generate(&w));
    let mut checked = 0;
    for t in crate::load::TABLES
        .iter()
        .filter(|t| t.name != "generator_run")
    {
        let declared = t.key.into_iter().chain(
            crate::load::UNIQUE_KEYS
                .iter()
                .filter(|u| u.0 == t.name)
                .map(|u| u.2),
        );
        for key in declared {
            let rows = &db[t.name];
            let mut seen = HashSet::new();
            for r in &rows.rows {
                let value = rows.project(r, &cols(key)).expect("a key has no NULL");
                assert!(
                    seen.insert(value.clone()),
                    "{} repeats ({key}) = {value:?}",
                    t.name
                );
            }
            assert!(!seen.is_empty(), "{} has no rows", t.name);
            checked += 1;
        }
    }
    assert!(checked >= crate::load::UNIQUE_KEYS.len());
}
