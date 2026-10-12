//! The committed `sql_composer.h`, the header callers read, is the one cbindgen writes from the
//! crate's source.

use std::path::Path;

#[test]
fn test_the_committed_header_is_the_one_cbindgen_writes() {
    let written = Path::new(env!("OUT_DIR")).join("sql_composer.h");
    let committed = Path::new(env!("CARGO_MANIFEST_DIR")).join("sql_composer.h");
    let fresh = std::fs::read_to_string(&written).unwrap();
    assert!(
        std::fs::read_to_string(&committed).ok().as_deref() == Some(fresh.as_str()),
        "{} is not the header cbindgen writes from the source. Refresh it with\n\n    cp {} {}\n",
        committed.display(),
        written.display(),
        committed.display()
    );
}
