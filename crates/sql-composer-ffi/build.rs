//! Writes the C header, `sql_composer.h`, from this crate's source into `OUT_DIR`. The copy beside
//! the manifest is the one callers read, and a test holds it to this one.

use std::path::PathBuf;

fn main() {
    let crate_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets it"));
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("cargo sets it"));
    let config = cbindgen::Config::from_file(crate_dir.join("cbindgen.toml"))
        .expect("cbindgen.toml is readable");

    println!("cargo:rerun-if-changed=cbindgen.toml");
    println!("cargo:rerun-if-changed=src");

    // From `src/lib.rs` and the modules it declares, read as files: nothing here asks cargo to
    // resolve the dependencies again, as reading the crate whole would.
    cbindgen::Builder::new()
        .with_src(crate_dir.join("src/lib.rs"))
        .with_config(config)
        .generate()
        .expect("the header is written from the crate's source")
        .write_to_file(out_dir.join("sql_composer.h"));
}
