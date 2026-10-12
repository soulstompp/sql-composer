//! A C program, `tests/c/compose_tree.c`, compiled against `sql_composer.h` with every warning an
//! error, linked to this library's `cdylib`, and run: it writes the brick example's templates as
//! cargo sqlc writes them, with and without `--respell`, and the files it writes are the committed
//! ones.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;

mod common;
use common::{brick, committed_brick, differences, files_under};

/// The C program, built into `dir`.
fn compose_tree(dir: &Path) -> PathBuf {
    // The test runs from `target/<profile>/deps`, where cargo leaves this library's `cdylib`.
    let exe = std::env::current_exe().unwrap();
    let libraries = exe.parent().unwrap();
    let library = format!(
        "{}sql_composer_ffi{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    assert!(
        libraries.join(&library).is_file(),
        "{library} is beside the test in {}",
        libraries.display()
    );

    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let program = dir.join("compose_tree");
    let output = Command::new("cc")
        .args(["-std=c99", "-Wall", "-Wextra", "-Werror", "-pedantic"])
        .arg("-I")
        .arg(crate_dir)
        .arg(crate_dir.join("tests/c/compose_tree.c"))
        .arg("-o")
        .arg(&program)
        .arg("-L")
        .arg(libraries)
        .arg("-lsql_composer_ffi")
        .arg(format!("-Wl,-rpath,{}", libraries.display()))
        .output()
        .expect("cc runs");
    assert!(
        output.status.success(),
        "cc refused the program:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    program
}

#[test]
fn test_a_c_program_writes_the_committed_brick_files() {
    let dir = tempfile::tempdir().unwrap();
    let program = compose_tree(dir.path());

    // Without `--respell` and with it: it rewrites the union of the checks, `checks/all.sql`.
    for respell in [false, true] {
        let target = dir
            .path()
            .join(if respell { "respelled" } else { "composed" });
        let mut run = Command::new(&program);
        run.arg(brick("sqlc")).arg(&target).arg("postgres");
        if respell {
            run.arg("--respell");
        }
        let output = run.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );

        let differences = differences(&files_under(&target), &committed_brick(respell));
        assert!(
            differences.is_empty(),
            "respell: {respell}: {differences:#?}"
        );
    }
}

#[test]
fn test_a_c_program_reads_the_cores_message() {
    let dir = tempfile::tempdir().unwrap();
    let program = compose_tree(dir.path());

    let output = Command::new(&program)
        .arg(brick("sqlc"))
        .arg(dir.path().join(".sql"))
        .arg("postgresql")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "postgresql"
            .parse::<sql_composer::Dialect>()
            .unwrap_err()
            .to_string()
    );
    assert!(!dir.path().join(".sql").exists(), "nothing is written");
}
