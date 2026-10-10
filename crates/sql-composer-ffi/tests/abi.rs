//! The C ABI driven as a C caller drives it: C strings in, handles and borrowed pointers out,
//! everything the library made freed by the library.

use std::collections::BTreeMap;
use std::ffi::{c_char, CStr, CString};
use std::path::Path;
use std::ptr;

use sql_composer::types::{Dialect, TemplateSource};
use sql_composer_ffi::*;

mod common;
use common::{assert_roster_respelled, brick, committed_brick, differences, roster_source};

/// `text` as a C string.
fn c(text: &str) -> CString {
    CString::new(text).unwrap()
}

/// The borrowed string at `ptr`.
fn read(ptr: *const c_char) -> String {
    assert!(!ptr.is_null());
    // SAFETY: a string the library lent, read while its handle lives.
    unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_string()
}

/// The message a failed call left in `error`, freed.
fn taken(error: *mut c_char) -> String {
    let message = read(error);
    // SAFETY: a message the library gave this caller to keep, freed once.
    unsafe { sql_composer_string_free(error) };
    message
}

/// The bind names of `composed`.
fn bind_params(composed: *const SqlComposerComposed) -> Vec<String> {
    let mut len = 0;
    // SAFETY: a live composed template, and a length this test can write.
    let names = unsafe { sql_composer_composed_bind_params(composed, &mut len) };
    // SAFETY: `len` names, lent while `composed` lives.
    (0..len).map(|i| read(unsafe { *names.add(i) })).collect()
}

/// Where each statement's names begin in `composed`.
fn statement_starts(composed: *const SqlComposerComposed) -> Vec<usize> {
    let mut len = 0;
    // SAFETY: a live composed template, and a length this test can write.
    let starts = unsafe { sql_composer_composed_statement_starts(composed, &mut len) };
    // SAFETY: `len` starts, lent while `composed` lives.
    unsafe { std::slice::from_raw_parts(starts, len) }.to_vec()
}

/// The template `text`, parsed under `name`.
fn parsed(text: &str, name: &str) -> *mut SqlComposerTemplate {
    let mut template = ptr::null_mut();
    let mut error = ptr::null_mut();
    // SAFETY: C strings and pointers this test can write.
    let status = unsafe {
        sql_composer_template_parse(
            c(text).as_ptr(),
            c(name).as_ptr(),
            &mut template,
            &mut error,
        )
    };
    assert_eq!(status, SqlComposerStatus::Ok, "{text}");
    assert!(error.is_null());
    template
}

/// A composer for `dialect`.
fn composer(dialect: &str) -> *mut SqlComposer {
    let mut composer = ptr::null_mut();
    // SAFETY: a C string and a pointer this test can write; no message wanted.
    let status = unsafe { sql_composer_new(c(dialect).as_ptr(), &mut composer, ptr::null_mut()) };
    assert_eq!(status, SqlComposerStatus::Ok);
    composer
}

/// Flags for `dialect`, with `--with` when `with` and `--respell` when `respell`.
fn flags(dialect: &str, with: bool, respell: bool) -> *mut SqlComposerFlags {
    let mut flags = ptr::null_mut();
    // SAFETY: a C string and pointers this test can write; no message wanted.
    unsafe {
        assert_eq!(
            sql_composer_flags_new(c(dialect).as_ptr(), &mut flags, ptr::null_mut()),
            SqlComposerStatus::Ok
        );
        assert_eq!(
            sql_composer_flags_set_with(flags, with, ptr::null_mut()),
            SqlComposerStatus::Ok
        );
        assert_eq!(
            sql_composer_flags_set_respell(flags, respell, ptr::null_mut()),
            SqlComposerStatus::Ok
        );
    }
    flags
}

/// The files composing `source` with `flags` gives through the ABI: a file for each template
/// written, and `manifest.json`, each by its path under the target.
fn composed_files(source: &Path, flags: *const SqlComposerFlags) -> BTreeMap<String, Vec<u8>> {
    let mut tree = ptr::null_mut();
    let mut error = ptr::null_mut();
    let source = c(source.to_str().unwrap());
    // SAFETY: a C string, live flags, and pointers this test can write.
    let status = unsafe { sql_composer_compose_all(source.as_ptr(), flags, &mut tree, &mut error) };
    assert_eq!(status, SqlComposerStatus::Ok, "{}", taken(error));

    let mut files = BTreeMap::new();
    // SAFETY: a live tree, read while it lives, freed once at the end.
    unsafe {
        for index in 0..sql_composer_tree_len(tree) {
            let mut entry: SqlComposerEntry = std::mem::zeroed();
            assert_eq!(
                sql_composer_tree_entry(tree, index, &mut entry, ptr::null_mut()),
                SqlComposerStatus::Ok
            );
            match entry.standing {
                SqlComposerStanding::Written => {
                    let sql = sql_composer_composed_sql(entry.composed);
                    let previous = files.insert(read(entry.file), read(sql).into_bytes());
                    assert!(previous.is_none(), "{} written twice", read(entry.file));
                }
                _ => {
                    assert!(entry.file.is_null(), "{} has no file", read(entry.path));
                    assert!(
                        entry.composed.is_null(),
                        "{} is not composed",
                        read(entry.path)
                    );
                    assert!(
                        entry.slot_count > 0,
                        "{} leaves a slot open",
                        read(entry.path)
                    );
                }
            }
        }
        files.insert(
            "manifest.json".to_string(),
            read(sql_composer_tree_manifest_json(tree)).into_bytes(),
        );
        sql_composer_tree_free(tree);
    }
    files
}

#[test]
fn test_the_brick_templates_compose_to_the_committed_files() {
    // Composing for PostgreSQL without `--with`, as the committed files were composed, and with
    // `--respell` too: the brick templates hold no union it rewrites, so it writes the same files
    // and a manifest that records it.
    for respell in [false, true] {
        let postgres = flags("postgres", false, respell);
        let composed = composed_files(&brick("sqlc"), postgres);
        // SAFETY: flags this test made, freed once.
        unsafe { sql_composer_flags_free(postgres) };

        let differences = differences(&composed, &committed_brick(respell));
        assert!(
            differences.is_empty(),
            "respell: {respell}: {differences:#?}"
        );
    }
}

#[test]
fn test_a_union_of_roster_arms_is_respelled_to_read_the_roster_once() {
    let dir = tempfile::tempdir().unwrap();
    let source = roster_source(dir.path());
    let composed_with = |respell: bool| {
        let postgres = flags("postgres", false, respell);
        let files = composed_files(&source, postgres);
        // SAFETY: flags this test made, freed once.
        unsafe { sql_composer_flags_free(postgres) };
        files
    };
    assert_roster_respelled(&composed_with(false), &composed_with(true));
}

#[test]
fn test_respelling_another_dialect_is_refused_with_the_cores_message() {
    for dialect in ["mysql", "mariadb", "sqlite", "duckdb"] {
        let mut core = sql_composer::source::Flags::new(dialect.parse().unwrap());
        core.respell = true;
        let expected = sql_composer::source::compose_all(&brick("sqlc"), &core)
            .unwrap_err()
            .to_string();
        assert!(expected.contains("--respell"), "{expected}");

        let flags = flags(dialect, false, true);
        let source = c(brick("sqlc").to_str().unwrap());
        let mut tree = ptr::null_mut();
        let mut error = ptr::null_mut();
        // SAFETY: a C string, live flags, and pointers this test can write; the flags freed once.
        let status =
            unsafe { sql_composer_compose_all(source.as_ptr(), flags, &mut tree, &mut error) };
        unsafe { sql_composer_flags_free(flags) };
        assert_eq!(status, SqlComposerStatus::Error, "{dialect}");
        assert!(tree.is_null());
        assert_eq!(taken(error), expected);
    }
}
#[test]
fn test_a_template_composes_with_its_names_and_counts() {
    let template = parsed(
        "SELECT * FROM parts WHERE id IN (:bind(ids)) AND kind = :bind(kind);\n\
         SELECT * FROM sets WHERE kind = :bind(kind) OR alt = :bind(kind);\n",
        "two",
    );
    let postgres = composer("postgres");
    let names = [c("ids")];
    let name_ptrs = [names[0].as_ptr()];
    let counts = [3usize];
    let mut composed = ptr::null_mut();
    let mut error = ptr::null_mut();
    // SAFETY: live handles, arrays of one, and pointers this test can write.
    let status = unsafe {
        sql_composer_compose_with_counts(
            postgres,
            template,
            name_ptrs.as_ptr(),
            counts.as_ptr(),
            1,
            &mut composed,
            &mut error,
        )
    };
    assert_eq!(status, SqlComposerStatus::Ok);
    assert_eq!(
        read(unsafe { sql_composer_composed_sql(composed) }),
        "SELECT * FROM parts WHERE id IN ($1, $2, $3) AND kind = $4;\n\
         SELECT * FROM sets WHERE kind = $1 OR alt = $1;\n"
    );
    assert_eq!(bind_params(composed), ["ids", "ids", "ids", "kind", "kind"]);
    assert_eq!(statement_starts(composed), [0, 4]);

    // SAFETY: handles this test made, each freed once.
    unsafe {
        sql_composer_composed_free(composed);
        sql_composer_free(postgres);
        sql_composer_template_free(template);
    }
}

#[test]
fn test_a_mock_table_stands_for_its_table() {
    let template = parsed("SELECT name FROM people WHERE id = :bind(id)", "people");
    let sqlite = composer("sqlite");
    let (columns, types) = ([c("id"), c("name")], [c("integer"), c("text")]);
    let column_ptrs = [columns[0].as_ptr(), columns[1].as_ptr()];
    let type_ptrs = [types[0].as_ptr(), types[1].as_ptr()];
    let one = [c("1"), c("Alice")];
    let values = [one[0].as_ptr(), one[1].as_ptr(), c"2".as_ptr(), ptr::null()];
    let mut composed = ptr::null_mut();
    // SAFETY: live handles, arrays as long as they are said to be, and pointers this test can
    // write; no message wanted.
    unsafe {
        let status = sql_composer_add_mock_table(
            sqlite,
            c"people".as_ptr(),
            column_ptrs.as_ptr(),
            type_ptrs.as_ptr(),
            2,
            values.as_ptr(),
            2,
            ptr::null_mut(),
        );
        assert_eq!(status, SqlComposerStatus::Ok);
        let status = sql_composer_compose(sqlite, template, &mut composed, ptr::null_mut());
        assert_eq!(status, SqlComposerStatus::Ok);
    }
    let sql = read(unsafe { sql_composer_composed_sql(composed) });
    assert!(
        sql.starts_with("WITH people AS (\nSELECT CAST('1' AS integer) AS id"),
        "{sql}"
    );
    assert!(
        sql.contains("SELECT CAST('2' AS integer), CAST(NULL AS text)"),
        "{sql}"
    );
    assert!(
        sql.ends_with("SELECT name FROM people WHERE id = ?1"),
        "{sql}"
    );
    assert_eq!(bind_params(composed), ["id"]);

    // SAFETY: handles this test made, each freed once.
    unsafe {
        sql_composer_composed_free(composed);
        sql_composer_free(sqlite);
        sql_composer_template_free(template);
    }
}

#[test]
fn test_a_template_that_does_not_parse_is_refused_with_the_cores_message() {
    let text = "SELECT * FROM parts WHERE id = :bind(";
    let expected =
        sql_composer::parser::parse_template(text, TemplateSource::Literal("bad".into()))
            .unwrap_err()
            .to_string();

    let mut template = ptr::null_mut();
    let mut error = ptr::null_mut();
    // SAFETY: C strings and pointers this test can write.
    let status = unsafe {
        sql_composer_template_parse(c(text).as_ptr(), c"bad".as_ptr(), &mut template, &mut error)
    };
    assert_eq!(status, SqlComposerStatus::Error);
    assert!(template.is_null());
    assert_eq!(taken(error), expected);
}

#[test]
fn test_an_unknown_dialect_is_refused_with_the_cores_message() {
    let expected = "postgresql".parse::<Dialect>().unwrap_err().to_string();

    let mut composer = ptr::null_mut();
    let mut flags = ptr::null_mut();
    let mut error = ptr::null_mut();
    // SAFETY: C strings and pointers this test can write.
    unsafe {
        let status = sql_composer_new(c"postgresql".as_ptr(), &mut composer, &mut error);
        assert_eq!(status, SqlComposerStatus::Error);
        assert!(composer.is_null());
        assert_eq!(taken(error), expected);

        let status = sql_composer_flags_new(c"postgresql".as_ptr(), &mut flags, &mut error);
        assert_eq!(status, SqlComposerStatus::Error);
        assert!(flags.is_null());
        assert_eq!(taken(error), expected);
    }
}

#[test]
fn test_a_missing_slot_is_refused_with_the_cores_message() {
    let text = "SELECT * FROM (:compose(@rows)) r";
    let core = sql_composer::parser::parse_template(text, TemplateSource::Literal("shape".into()))
        .unwrap();
    let expected = sql_composer::Composer::new(Dialect::Postgres)
        .compose(&core)
        .unwrap_err()
        .to_string();
    assert!(expected.contains("@rows"), "{expected}");

    let template = parsed(text, "shape");
    let postgres = composer("postgres");
    let mut composed = ptr::null_mut();
    let mut error = ptr::null_mut();
    // SAFETY: live handles and pointers this test can write.
    let status = unsafe { sql_composer_compose(postgres, template, &mut composed, &mut error) };
    assert_eq!(status, SqlComposerStatus::Error);
    assert!(composed.is_null());
    assert_eq!(taken(error), expected);

    // SAFETY: handles this test made, each freed once.
    unsafe {
        sql_composer_free(postgres);
        sql_composer_template_free(template);
    }
}

#[test]
fn test_a_count_its_expecting_refuses_is_refused_with_the_cores_message() {
    let text = "SELECT * FROM parts WHERE id IN (:bind(ids EXPECTING 1..2))";
    let core =
        sql_composer::parser::parse_template(text, TemplateSource::Literal("ids".into())).unwrap();
    let expected = sql_composer::Composer::new(Dialect::Postgres)
        .compose_with_values(&core, &BTreeMap::from([("ids".to_string(), vec![(); 3])]))
        .unwrap_err()
        .to_string();

    let template = parsed(text, "ids");
    let postgres = composer("postgres");
    let names = [c"ids".as_ptr()];
    let mut composed = ptr::null_mut();
    let mut error = ptr::null_mut();
    // SAFETY: live handles, arrays of one, and pointers this test can write.
    let status = unsafe {
        sql_composer_compose_with_counts(
            postgres,
            template,
            names.as_ptr(),
            [3usize].as_ptr(),
            1,
            &mut composed,
            &mut error,
        )
    };
    assert_eq!(status, SqlComposerStatus::Error);
    assert_eq!(taken(error), expected);

    // SAFETY: handles this test made, each freed once.
    unsafe {
        sql_composer_free(postgres);
        sql_composer_template_free(template);
    }
}

#[test]
fn test_a_null_argument_is_refused_by_name() {
    let mut template = ptr::null_mut();
    let mut error = ptr::null_mut();
    // SAFETY: NULL where the library says it may be, and pointers this test can write.
    unsafe {
        let status =
            sql_composer_template_parse(ptr::null(), ptr::null(), &mut template, &mut error);
        assert_eq!(status, SqlComposerStatus::InvalidArgument);
        assert_eq!(taken(error), "text is NULL");

        let status = sql_composer_compose(ptr::null(), ptr::null(), ptr::null_mut(), &mut error);
        assert_eq!(status, SqlComposerStatus::InvalidArgument);
        assert_eq!(taken(error), "out is NULL");

        // Every free and every accessor takes NULL.
        sql_composer_template_free(ptr::null_mut());
        sql_composer_free(ptr::null_mut());
        sql_composer_composed_free(ptr::null_mut());
        sql_composer_flags_free(ptr::null_mut());
        sql_composer_tree_free(ptr::null_mut());
        sql_composer_string_free(ptr::null_mut());
        assert!(sql_composer_composed_sql(ptr::null()).is_null());
        let mut len = 9;
        assert!(sql_composer_composed_bind_params(ptr::null(), &mut len).is_null());
        assert_eq!(len, 0);
        assert_eq!(sql_composer_tree_len(ptr::null()), 0);
        assert!(sql_composer_tree_manifest_json(ptr::null()).is_null());
    }
}

#[test]
fn test_the_version_is_the_crates() {
    assert_eq!(read(sql_composer_version()), env!("CARGO_PKG_VERSION"));
}
