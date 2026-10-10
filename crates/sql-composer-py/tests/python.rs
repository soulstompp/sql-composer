//! The module as Python sees it: Python code, run in an interpreter these tests start, composes
//! through `sql_composer` and reads what it gives with Python's own `pathlib`, `json` and
//! `sqlite3`.

use std::collections::BTreeMap;
use std::ffi::CString;
use std::sync::Once;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use sql_composer::types::{Dialect, TemplateSource};

mod common;
use common::{
    assert_roster_respelled, brick, committed_brick, differences, files_under, roster_source,
};

/// Start the interpreter, once, with the module among its built-in modules.
fn interpreter() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        sql_composer_py::append_to_inittab();
        Python::initialize();
    });
}

/// Run the Python `code`, each of `names` a string it reads, and fail with Python's traceback
/// when it raises.
fn run(code: &str, names: &[(&str, String)]) {
    interpreter();
    Python::attach(|py| {
        let globals = PyDict::new(py);
        for (name, value) in names {
            globals.set_item(name, value).unwrap();
        }
        let code = CString::new(code).unwrap();
        if let Err(error) = py.run(&code, Some(&globals), None) {
            let traceback = error
                .traceback(py)
                .and_then(|traceback| traceback.format().ok())
                .unwrap_or_default();
            panic!("{traceback}{error}");
        }
    });
}

/// Writes a tree as cargo sqlc writes it: each composed file under the target, and the manifest
/// beside them.
const WRITE: &str = r#"
import pathlib

import sql_composer


def write(tree, target):
    target = pathlib.Path(target)
    for name, composed in tree.files.items():
        path = target / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(composed.sql.encode())
    (target / sql_composer.MANIFEST).write_bytes(tree.manifest_json.encode())
"#;

#[test]
fn test_the_brick_templates_compose_to_the_committed_files() {
    // Composing for PostgreSQL without `--with`, as the committed files were composed, and with
    // `--respell` too: the brick templates hold no union it rewrites, so it writes the same files
    // and a manifest that records it.
    for respell in ["False", "True"] {
        let target = tempfile::tempdir().unwrap();
        let code = WRITE.to_string()
            + &r#"
tree = sql_composer.compose_all(source, "postgres", respell=RESPELL)
write(tree, target)

for name, standing in tree.standings.items():
    written = standing.kind == "written"
    assert written == (standing.file is not None), (name, standing)
    assert written == (standing.composed is not None), (name, standing)
    assert written == (not standing.slots), (name, standing)
assert len(tree.files) == len([s for s in tree.standings.values() if s.kind == "written"])
"#
            .replace("RESPELL", respell);
        run(
            &code,
            &[
                ("source", brick("sqlc").display().to_string()),
                ("target", target.path().display().to_string()),
            ],
        );

        let committed = committed_brick(respell == "True");
        let differences = differences(&files_under(target.path()), &committed);
        assert!(
            differences.is_empty(),
            "respell={respell}: {differences:#?}"
        );
    }
}

#[test]
fn test_a_union_of_roster_arms_is_respelled_to_read_the_roster_once() {
    let dir = tempfile::tempdir().unwrap();
    let source = roster_source(dir.path());
    let (composed, respelled) = (dir.path().join("composed"), dir.path().join("respelled"));
    let code = WRITE.to_string()
        + r#"
write(sql_composer.compose_all(source, "postgres"), composed)
write(sql_composer.compose_all(source, "postgres", respell=True), respelled)
"#;
    run(
        &code,
        &[
            ("source", source.display().to_string()),
            ("composed", composed.display().to_string()),
            ("respelled", respelled.display().to_string()),
        ],
    );
    assert_roster_respelled(&files_under(&composed), &files_under(&respelled));
}

#[test]
fn test_python_runs_a_composed_file_by_its_manifest() {
    let code = WRITE.to_string()
        + r#"
import json
import sqlite3
import tempfile


def statements(sql):
    """The statements of `sql`, each ending where SQLite's own reading ends one."""
    found, start = [], 0
    for end, char in enumerate(sql):
        if char == ";" and sqlite3.complete_statement(sql[start:end + 1]):
            found.append(sql[start:end + 1])
            start = end + 1
    if sql[start:].strip():
        found.append(sql[start:])
    return found


with tempfile.TemporaryDirectory() as root:
    source = pathlib.Path(root, "sqlc")
    (source / "parts").mkdir(parents=True)
    (source / "parts" / "by_kind.sqlc").write_text(
        "SELECT id FROM parts WHERE kind = :bind(kind) OR alt_kind = :bind(kind) ORDER BY id;\n"
    )
    (source / "parts" / "relabel.sqlc").write_text(
        "UPDATE parts SET kind = :bind(to_kind) WHERE kind = :bind(from_kind);\n"
        "SELECT id FROM parts WHERE kind = :bind(to_kind) AND id > :bind(after) ORDER BY id;\n"
    )
    target = pathlib.Path(root, ".sql")
    write(sql_composer.compose_all(source, "sqlite"), target)

    manifest = json.loads((target / "manifest.json").read_text())
    assert manifest["dialect"] == "sqlite", manifest
    assert sorted(manifest["files"]) == ["parts/by_kind.sql", "parts/relabel.sql"], manifest
    # A name used twice in a statement takes one placeholder, and is named once.
    assert manifest["files"]["parts/by_kind.sql"]["bind_params"] == ["kind"], manifest
    assert manifest["files"]["parts/relabel.sql"]["statement_starts"] == [0, 2], manifest

    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE parts (id INTEGER PRIMARY KEY, kind TEXT, alt_kind TEXT)")
    db.executemany(
        "INSERT INTO parts VALUES (?, ?, ?)",
        [(1, "brick", None), (2, "plate", "brick"), (3, "brick", None), (4, "tile", None),
         (5, "brick", "tile")],
    )

    def run(file, values):
        """Each statement of the composed `file` run, given its values in the order the manifest
        names them; the last one's rows."""
        names = manifest["files"][file]
        bind_params, starts = names["bind_params"], names["statement_starts"]
        found = statements((target / file).read_text())
        assert len(found) == len(starts), found
        rows = None
        for statement, start, end in zip(found, starts, starts[1:] + [len(bind_params)]):
            given = [values[name] for name in bind_params[start:end]]
            rows = db.execute(statement, given).fetchall()
        return rows

    assert run("parts/by_kind.sql", {"kind": "brick"}) == [(1,), (2,), (3,), (5,)]
    relabelled = run("parts/relabel.sql", {"from_kind": "brick", "to_kind": "slope", "after": 1})
    assert relabelled == [(3,), (5,)], relabelled
"#;
    run(&code, &[]);
}

#[test]
fn test_a_composer_composes_with_counts_and_mock_tables() {
    run(
        r#"
import sqlite3

import sql_composer

composer = sql_composer.Composer("sqlite")
template = sql_composer.parse(
    "SELECT id FROM parts WHERE id IN (:bind(ids)) AND kind = :bind(kind) ORDER BY id", name="parts"
)
composed = composer.compose(template, counts={"ids": 3})
assert composed.sql == "SELECT id FROM parts WHERE id IN (?1, ?2, ?3) AND kind = ?4 ORDER BY id", composed
assert composed.bind_params == ["ids", "ids", "ids", "kind"], composed
assert composed.statement_starts == [0], composed
assert repr(composed) == (
    "Composed(sql='SELECT id FROM parts WHERE id IN (?1, ?2, ?3) AND kind = ?4 ORDER BY id', "
    "bind_params=['ids', 'ids', 'ids', 'kind'], statement_starts=[0])"
), repr(composed)
assert repr(template) == "Template('parts')", repr(template)
assert composer.compose(template).bind_params == ["ids", "kind"]

composer.add_mock_table(
    "parts",
    [("id", "integer"), ("kind", "text")],
    [["1", "brick"], ["2", "plate"], ["3", "brick"], ["4", None]],
)
mocked = composer.compose(template, counts={"ids": 3})
assert mocked.sql.startswith("WITH parts AS ("), mocked
values = {"ids": iter([1, 3, 4]), "kind": iter(["brick"])}
given = [next(values[name]) for name in mocked.bind_params]
rows = sqlite3.connect(":memory:").execute(mocked.sql, given).fetchall()
assert rows == [(1,), (3,)], rows

try:
    composer.add_mock_table("parts", [("id", "integer")], [["1", "brick"]])
except ValueError as error:
    assert str(error) == "a row of the mock parts holds 2 values for 1 columns", error
else:
    raise AssertionError("a row longer than the columns is refused")

assert sql_composer.DIALECTS == ("postgres", "mysql", "mariadb", "sqlite", "duckdb")
assert composer.dialect == "sqlite" and not composer.name_relations
"#,
        &[],
    );
}

#[test]
fn test_each_refusal_raises_with_the_cores_message() {
    let literal = |text: &str, name: &str| {
        sql_composer::parser::parse_template(text, TemplateSource::Literal(name.into()))
    };
    let bad = "SELECT * FROM parts WHERE id = :bind(";
    let shape = "SELECT * FROM (:compose(@rows)) r";
    let counted = "SELECT * FROM parts WHERE id IN (:bind(ids EXPECTING 1..2))";
    let missing = tempfile::tempdir().unwrap().path().join("missing.sqlc");
    let postgres = sql_composer::Composer::new(Dialect::Postgres);

    let respelling = |dialect: &str| {
        let mut flags = sql_composer::source::Flags::new(dialect.parse().unwrap());
        flags.respell = true;
        let message = sql_composer::source::compose_all(&brick("sqlc"), &flags)
            .unwrap_err()
            .to_string();
        assert!(message.contains("--respell"), "{message}");
        (format!("respell_{dialect}_message"), message)
    };
    let respellings: Vec<(String, String)> = ["mysql", "mariadb", "sqlite", "duckdb"]
        .into_iter()
        .map(respelling)
        .collect();

    let mut names = vec![
        ("brick", brick("sqlc").display().to_string()),
        ("bad", bad.to_string()),
        ("bad_message", literal(bad, "bad").unwrap_err().to_string()),
        (
            "dialect_message",
            "postgresql".parse::<Dialect>().unwrap_err().to_string(),
        ),
        ("shape", shape.to_string()),
        (
            "slot_message",
            postgres
                .compose(&literal(shape, "shape").unwrap())
                .unwrap_err()
                .to_string(),
        ),
        ("counted", counted.to_string()),
        (
            "count_message",
            postgres
                .compose_with_values(
                    &literal(counted, "counted").unwrap(),
                    &BTreeMap::from([("ids".to_string(), vec![(); 3])]),
                )
                .unwrap_err()
                .to_string(),
        ),
        ("missing", missing.display().to_string()),
        (
            "missing_message",
            sql_composer::parser::parse_template_file(&missing)
                .unwrap_err()
                .to_string(),
        ),
    ];
    names.extend(
        respellings
            .iter()
            .map(|(name, message)| (name.as_str(), message.clone())),
    );
    run(
        r#"
import sql_composer


def refusal(call, raised=sql_composer.Error):
    try:
        call()
    except raised as error:
        return str(error)
    raise AssertionError("not refused")


assert refusal(lambda: sql_composer.parse(bad, name="bad")) == bad_message
assert refusal(lambda: sql_composer.Composer("postgresql")) == dialect_message
assert refusal(lambda: sql_composer.compose_all(".", "postgresql")) == dialect_message
postgres = sql_composer.Composer("postgres")
assert refusal(lambda: postgres.compose(sql_composer.parse(shape, name="shape"))) == slot_message
counted = sql_composer.parse(counted, name="counted")
assert refusal(lambda: postgres.compose(counted, counts={"ids": 3})) == count_message
assert refusal(lambda: sql_composer.parse_file(missing), FileNotFoundError) == missing_message
for dialect in ("mysql", "mariadb", "sqlite", "duckdb"):
    respelled = refusal(lambda: sql_composer.compose_all(brick, dialect, respell=True))
    assert respelled == globals()[f"respell_{dialect}_message"], respelled
assert issubclass(sql_composer.Error, Exception)
"#,
        &names,
    );
}
