//! What the tests share: the brick example's directories, a source the respelling rewrites, and
//! how two sets of files are held to each other. Each test uses what it needs of it.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The brick example's directory `name`.
pub fn brick(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/brick")
        .join(name)
}

/// Every file under `dir`, by its path under it, its parts joined by `/`, with its bytes.
pub fn files_under(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    walkdir::WalkDir::new(dir)
        .into_iter()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| {
            let rel = entry.path().strip_prefix(dir).unwrap();
            let slashed = rel
                .components()
                .map(|part| part.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            (slashed, std::fs::read(entry.path()).unwrap())
        })
        .collect()
}

/// The paths `composed` and `committed` disagree on: missing, extra, or with other bytes.
pub fn differences(
    composed: &BTreeMap<String, Vec<u8>>,
    committed: &BTreeMap<String, Vec<u8>>,
) -> Vec<String> {
    let mut differences = Vec::new();
    for (path, bytes) in committed {
        match composed.get(path) {
            None => differences.push(format!("not composed: {path}")),
            Some(made) if made != bytes => differences.push(format!("other bytes: {path}")),
            Some(_) => {}
        }
    }
    for path in composed
        .keys()
        .filter(|path| !committed.contains_key(*path))
    {
        differences.push(format!("not committed: {path}"));
    }
    differences
}

/// The brick example's committed files, by their paths under the target, as composing with or
/// without `--respell` writes them. The brick templates hold no union the respelling rewrites, so
/// the files are the same either way, and the manifest says which.
pub fn committed_brick(respell: bool) -> BTreeMap<String, Vec<u8>> {
    let mut committed = files_under(&brick(".sql"));
    assert!(
        committed.len() > 1,
        "the committed files are there to compare"
    );
    if respell {
        let manifest = String::from_utf8(committed["manifest.json"].clone()).unwrap();
        assert_eq!(
            manifest.matches(r#""respell": false"#).count(),
            1,
            "{manifest}"
        );
        let respelled = manifest.replace(r#""respell": false"#, r#""respell": true"#);
        committed.insert("manifest.json".to_string(), respelled.into_bytes());
    }
    committed
}

/// A source of templates the respelling rewrites, written under `dir`: `laws.sqlc`, a
/// `:union(ALL …)` of three arms under `arms/`, each reading `roster.sqlc`, cut to its own rule,
/// and left joined `ON true` to its own law. Its composed file reads the roster once for each arm.
pub fn roster_source(dir: &Path) -> PathBuf {
    let source = dir.join("sqlc");
    std::fs::create_dir_all(source.join("arms")).unwrap();
    std::fs::write(source.join("roster.sqlc"), "SELECT * FROM roster\n").unwrap();
    for n in 1..=3 {
        std::fs::write(
            source.join(format!("arms/rule_{n}.sqlc")),
            format!(
                "SELECT r.rule, p.filing, p.violates\n\
                 FROM (\n    :compose(roster.sqlc)\n) r\n\
                 LEFT JOIN (\n    SELECT l.filing, l.bad AS violates FROM law_{n} l\n) p ON true\n\
                 WHERE r.slug = 'rule_{n}'\n"
            ),
        )
        .unwrap();
    }
    std::fs::write(
        source.join("laws.sqlc"),
        ":union(ALL arms/rule_1.sqlc, arms/rule_2.sqlc, arms/rule_3.sqlc)\n",
    )
    .unwrap();
    source
}

/// How many times `sql` reads the roster.
pub fn roster_reads(sql: &[u8]) -> usize {
    String::from_utf8_lossy(sql)
        .matches("SELECT * FROM roster")
        .count()
}

/// Holds the roster source composed without the respelling, `composed`, and with it,
/// `respelled`, each by its files' paths under the target: as composed, `laws.sql` reads the
/// roster once for each arm, and respelled, once, its text what `respell::respell` gives for the
/// composed text. Every other file is written as composed, and the manifest records the flag.
pub fn assert_roster_respelled(
    composed: &BTreeMap<String, Vec<u8>>,
    respelled: &BTreeMap<String, Vec<u8>>,
) {
    assert_eq!(roster_reads(&composed["laws.sql"]), 3);
    assert_eq!(roster_reads(&respelled["laws.sql"]), 1);
    let laws = String::from_utf8(composed["laws.sql"].clone()).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&respelled["laws.sql"]),
        sql_composer::respell::respell(&laws).sql
    );
    for (file, bytes) in composed {
        match file.as_str() {
            "laws.sql" => {}
            "manifest.json" => assert_eq!(
                String::from_utf8_lossy(&respelled[file]),
                String::from_utf8_lossy(bytes).replace(r#""respell": false"#, r#""respell": true"#)
            ),
            _ => assert_eq!(&respelled[file], bytes, "{file}"),
        }
    }
    assert_eq!(composed.len(), respelled.len());
}
