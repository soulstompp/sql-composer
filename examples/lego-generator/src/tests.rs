//! Tests over a small real-shaped catalogue built in memory: the generation is a function of the
//! wiring, and the manifest names exactly the rows that carry each trap.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::calendar::{self, LocalTime, ZONE_NAMES};
use crate::catalogue::{
    is_release_year, model_releases, nearest_release_year, split_version, Catalogue, Category,
    Colour, InvRec, LineRec, NestRec, Part, Real, SetRec, Theme, GROWTH_FACTOR, MODELLED,
    MODEL_FIT,
};
use crate::chords;
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
    let real = Real::new(fixture());
    let board = Switchboard::new(&real, patch, dials(), sets, chunk).expect("patch");
    World::new(real, wiring(sets, chunk), board).expect("world")
}

struct Everything {
    sets: Vec<SetWave>,
    builders: Vec<BuilderWave>,
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
    Everything { sets, builders }
}

fn sorted<T: Ord + Clone>(v: impl Iterator<Item = T>) -> Vec<T> {
    let mut v: Vec<T> = v.collect();
    v.sort();
    v
}

#[test]
fn the_same_wiring_generates_the_same_rows() {
    let patch = "natural:60,wavy:20,interleaved:10,zchord:10";
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
    let w = world(3000, 100, "natural:60,wavy:20,interleaved:10,zchord:10");
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
    let zone_of = |p: &world::PurchaseOut| {
        calendar::zones()
            .iter()
            .find(|z| z.name == builders[&p.builder_id].home_zone)
            .unwrap()
    };
    let themes: HashSet<i32> = w.real.cat.themes.iter().map(|t| t.id).collect();
    let local_day = |p: &world::PurchaseOut| match p.ordered_at {
        Stamp::At { t, offset_min } => (t + i64::from(offset_min) * 60).div_euclid(86_400),
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
        let ok = match m.trap {
            Trap::K1 => {
                let c = coll[&m.row_key];
                let canonical = m.detail.trim_start_matches("canonical ");
                c.set_num != canonical
                    && c.set_num.eq_ignore_ascii_case(canonical)
                    && sets.contains_key(canonical)
                    && !sets.contains_key(&c.set_num)
            }
            Trap::K2 => {
                let c = coll[&m.row_key];
                c.set_num.trim() != c.set_num && sets.contains_key(c.set_num.trim())
            }
            Trap::K3 => {
                let c = coll[&m.row_key];
                c.set_num == "10182-1"
                    && (c.set_name == "Café Corner" || c.set_name == "Cafe\u{301} Corner")
            }
            Trap::K5 => {
                let c = coll[&m.row_key];
                c.set_num.contains(['\u{2013}', '\u{2011}', '\u{00A0}'])
                    && !sets.contains_key(&c.set_num)
            }
            Trap::K4 => world::looks_round_tripped(&sets[&m.row_key].name),
            Trap::K6 => {
                world::basic_brick_disagreement(m.row_key.split('|').nth(1).unwrap())
                    && lines.contains_key(&m.row_key)
            }
            Trap::K7 => sets.contains_key(&m.row_key),
            Trap::K8 => {
                let inv = invs[&m.row_key];
                let has_lines = lines.values().any(|l| l.inventory_id == inv.id);
                split_version(&inv.set_num).is_none()
                    && !sets.contains_key(&inv.set_num)
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
                let hm: Vec<u32> = rest.split(':').map(|x| x.parse().unwrap()).collect();
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
            Trap::D4 => purchases[&m.row_key].ordered_local.ends_with(" 24:00"),
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
            let z = calendar::zones()
                .iter()
                .find(|z| z.name == b.home_zone)
                .unwrap();
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
    for p in e.builders.iter().flat_map(|x| x.purchases.iter()) {
        let z = zone[&p.builder_id];
        let key = p.purchase_id.to_string();
        let Stamp::At { t, offset_min } = p.ordered_at else {
            panic!("{key}: no instant")
        };
        let reading = calendar::render_local_minute(t + i64::from(z.offset_min_at(t)) * 60);
        let in_force = offset_min == z.offset_min_at(t);
        if d2.contains(&key) {
            assert!(!in_force && p.ordered_local != reading, "{p:?}");
        } else {
            assert!(in_force, "{p:?}");
            assert_eq!(p.ordered_local != reading, d4.contains(&key), "{p:?}");
        }
        if let Stamp::At { t, offset_min } = p.delivered_at {
            assert_eq!(offset_min, z.offset_min_at(t), "{p:?}");
        }
        read += 1;
    }
    assert!(read > 100, "{read} purchases");
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
    let w = world(3000, 100, "natural:60,wavy:20,interleaved:10,zchord:10");
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

    let today = calendar::days_from_civil(world::TODAY.0, world::TODAY.1, world::TODAY.2);
    let mut read = 0;
    for p in e.builders.iter().flat_map(|x| x.purchases.iter()) {
        let (Some(&s), Stamp::At { t, offset_min }) =
            (w.real.set_by_num.get(&p.set_num), p.ordered_at)
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
        .filter(|p| twins.contains(&p.set_num.as_str()))
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

/// The paired packs: equal counts of year gaps between their child sets, different counts of three
/// consecutive child years.
#[test]
fn paired_packs_agree_on_gaps_and_differ_on_consecutive_years() {
    let w = world(3000, 100, "natural:80,zchord:20");
    let e = generate(&w);
    let years: HashMap<&str, i32> = w
        .real
        .cat
        .sets
        .iter()
        .filter_map(|s| s.year.map(|y| (s.set_num.as_str(), y)))
        .collect();
    let mut packs: BTreeMap<u64, Vec<(String, Vec<i32>)>> = BTreeMap::new();
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
            let pair: u64 = m
                .detail
                .split_whitespace()
                .next()
                .unwrap()
                .trim_start_matches("pair=")
                .parse()
                .unwrap();
            let class = m
                .detail
                .split_whitespace()
                .find(|w| w.starts_with("class="))
                .unwrap()
                .to_string();
            packs
                .entry(pair)
                .or_default()
                .push((class, kids[m.row_key.as_str()].clone()));
        }
    }
    assert!(packs.len() >= 3);
    let triples_by_class: HashMap<&str, u32> = [
        ("0,1,2,5,7", 1),
        ("0,1,3,5,6", 0),
        ("0,1,2,4,7", 1),
        ("0,1,3,4,6", 0),
        ("0,1,2,3,6", 2),
        ("0,1,2,4,5", 1),
    ]
    .into_iter()
    .collect();
    // A year pattern's key, read from the years alone: each year by its last digit, and the least of
    // the pattern's turns round the decade and their mirror images.
    let pattern_key = |ys: &[i32]| -> String {
        let pcs: Vec<i32> = ys.iter().map(|y| y.rem_euclid(10)).collect();
        let mut best: Option<Vec<i32>> = None;
        for inv in [false, true] {
            for n in 0..10 {
                let mut img: Vec<i32> = pcs
                    .iter()
                    .map(|&x| ((if inv { -x } else { x }) + n).rem_euclid(10))
                    .collect();
                img.sort_unstable();
                img.dedup();
                if best.as_ref().is_none_or(|b| img < *b) {
                    best = Some(img);
                }
            }
        }
        best.unwrap()
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(",")
    };
    let z_pairs: Vec<(String, String)> = chords::Z_PAIRS
        .iter()
        .map(|(a, b)| (chords::class_label(a), chords::class_label(b)))
        .collect();
    for (pair, members) in &packs {
        assert_eq!(members.len(), 2, "pair {pair}");
        let pcs = |ys: &[i32]| ys.iter().map(|y| y.rem_euclid(10)).collect::<Vec<_>>();
        assert_eq!(members[0].1.len(), 5);
        assert_eq!(
            chords::gap_counts(&pcs(&members[0].1)),
            chords::gap_counts(&pcs(&members[1].1)),
            "pair {pair}"
        );
        let (c0, c1) = (pattern_key(&members[0].1), pattern_key(&members[1].1));
        assert!(
            z_pairs
                .iter()
                .any(|(a, b)| (a == &c0 && b == &c1) || (a == &c1 && b == &c0)),
            "pair {pair}: {c0} and {c1} are not a declared pair"
        );
        for (class, ys) in members {
            assert_eq!(
                class.trim_start_matches("class="),
                pattern_key(ys),
                "pair {pair}: the manifest's pattern is the years' pattern"
            );
            assert_eq!(
                chords::consecutive_triples(ys),
                triples_by_class[pattern_key(ys).as_str()],
                "pair {pair} class {class}"
            );
        }
        assert_ne!(
            chords::consecutive_triples(&members[0].1),
            chords::consecutive_triples(&members[1].1),
            "pair {pair}"
        );
    }
}
