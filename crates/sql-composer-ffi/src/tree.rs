//! Composing a directory of templates as `cargo sqlc compose` does.

use std::ffi::{c_char, CString};
use std::ptr;

use sql_composer::source::{self, Flags, Manifest, Standing};
use sql_composer::types::Dialect;

use crate::composer::SqlComposerComposed;
use crate::{
    arg_mut, arg_out, arg_path, arg_ref, arg_str, c_string, entry, free_handle, guarded,
    into_handle, slashed, Failure, SqlComposerStatus,
};

/// How a directory of templates is composed: the flags `cargo sqlc compose` takes for it. Made by
/// [`sql_composer_flags_new`], freed by [`sql_composer_flags_free`].
pub struct SqlComposerFlags {
    pub(crate) inner: Flags,
}

/// What composing made of one template.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlComposerStanding {
    /// Composed, and written to its file.
    Written = 0,
    /// A shape: it leaves slots of its own for a caller to fill, and is composed only inside that
    /// caller. It gets no file.
    Shape = 1,
    /// A caller that leaves a slot of a template it composes open. It gets no file.
    Skipped = 2,
}

/// One template under the source, as [`sql_composer_tree_entry`] gives it. Every pointer is
/// borrowed from the tree, and lives as long as it does.
#[repr(C)]
pub struct SqlComposerEntry {
    /// The template's path under the source, its parts joined by `/`.
    pub path: *const c_char,
    /// What composing made of it.
    pub standing: SqlComposerStanding,
    /// When written: its file's path under the target, its parts joined by `/`. `NULL` otherwise.
    pub file: *const c_char,
    /// When written: what its file holds, and its names. `NULL` otherwise.
    pub composed: *const SqlComposerComposed,
    /// A shape's open slots, in order, or the one slot a skipped template leaves open. None when
    /// written.
    pub slots: *const *const c_char,
    /// How many `slots` holds.
    pub slot_count: usize,
    /// When skipped: the template the open slot stands in, as `:compose` names it. `NULL`
    /// otherwise.
    pub slot_template: *const c_char,
}

/// Every template under a source directory, with what composing made of it, and the manifest of
/// the composed files. Made by [`sql_composer_compose_all`], freed by [`sql_composer_tree_free`].
pub struct SqlComposerTree {
    entries: Vec<Entry>,
    manifest_json: CString,
}

/// One template of a tree, held as the C strings its [`SqlComposerEntry`] points into.
struct Entry {
    path: CString,
    standing: SqlComposerStanding,
    file: Option<CString>,
    composed: Option<SqlComposerComposed>,
    slots: Vec<CString>,
    slot_ptrs: Vec<*const c_char>,
    slot_template: Option<CString>,
}

impl Entry {
    /// The template at `rel_path` under the source, standing as `standing`.
    fn new(rel_path: &std::path::Path, standing: &Standing) -> Result<Self, Failure> {
        let path = slashed(rel_path);
        let (kind, file, composed, slots, slot_template) = match standing {
            Standing::Written(composed) => (
                SqlComposerStanding::Written,
                Some(c_string(source::composed_file(rel_path), "a file's path")?),
                Some(SqlComposerComposed::new(composed.clone())?),
                Vec::new(),
                None,
            ),
            Standing::Shape(slots) => (SqlComposerStanding::Shape, None, None, slots.clone(), None),
            Standing::Skipped { slot, template } => (
                SqlComposerStanding::Skipped,
                None,
                None,
                vec![slot.clone()],
                Some(c_string(template.clone(), "a template's path")?),
            ),
            other => {
                return Err(Failure::error(format!(
                    "{path}: {other:?} is a standing this library does not know"
                )))
            }
        };
        let slots = slots
            .into_iter()
            .map(|slot| c_string(slot, "a slot's name"))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Entry {
            path: c_string(path, "a template's path")?,
            standing: kind,
            file,
            composed,
            slot_ptrs: slots.iter().map(|slot| slot.as_ptr()).collect(),
            slots,
            slot_template,
        })
    }

    /// The entry as C reads it, borrowed from `self`.
    fn borrowed(&self) -> SqlComposerEntry {
        let as_ptr = |text: &Option<CString>| text.as_ref().map_or(ptr::null(), |t| t.as_ptr());
        SqlComposerEntry {
            path: self.path.as_ptr(),
            standing: self.standing,
            file: as_ptr(&self.file),
            composed: self
                .composed
                .as_ref()
                .map_or(ptr::null(), |composed| composed as *const _),
            slots: self.slot_ptrs.as_ptr(),
            slot_count: self.slots.len(),
            slot_template: as_ptr(&self.slot_template),
        }
    }
}

/// Make flags for composing for the dialect named `dialect` (`postgres`, `mysql`, `mariadb`,
/// `sqlite` or `duckdb`) into `*out`, with nothing else: no `--with`, no `--views`, no
/// `--search-path`, no `--respell`.
///
/// # Safety
///
/// `dialect` is `NULL` or a NUL-terminated string; `out` and `error` are `NULL` or point to
/// pointers the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_flags_new(
    dialect: *const c_char,
    out: *mut *mut SqlComposerFlags,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let out = arg_out(out, "out")?;
            let dialect: Dialect = arg_str(dialect, "dialect")?.parse()?;
            *out = into_handle(SqlComposerFlags {
                inner: Flags::new(dialect),
            });
            Ok(())
        })
    }
}

/// `--with`: every relation a statement reads named once at its head.
///
/// # Safety
///
/// `flags` is `NULL` or live flags no other call is using; `error` is `NULL` or points to a
/// pointer the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_flags_set_with(
    flags: *mut SqlComposerFlags,
    with: bool,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            arg_mut(flags, "flags")?.inner.with = with;
            Ok(())
        })
    }
}

/// `--respell`: each composed file respelled as `cargo sqlc compose --respell` respells it. A
/// union whose arms each read one roster, cut to their own rows of it and left joined `ON true`
/// to what each reads, is written as one join that reads the roster once; every other byte is
/// written as composed. PostgreSQL alone is respelled: [`sql_composer_compose_all`] refuses flags
/// that ask for it with any other dialect.
///
/// # Safety
///
/// `flags` is `NULL` or live flags no other call is using; `error` is `NULL` or points to a
/// pointer the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_flags_set_respell(
    flags: *mut SqlComposerFlags,
    respell: bool,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            arg_mut(flags, "flags")?.inner.respell = respell;
            Ok(())
        })
    }
}

/// `--views`: the view registry, as the manifest records it. `NULL` takes it away.
///
/// # Safety
///
/// `flags` is `NULL` or live flags no other call is using; `registry` is `NULL` or a
/// NUL-terminated string; `error` is `NULL` or points to a pointer the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_flags_set_views(
    flags: *mut SqlComposerFlags,
    registry: *const c_char,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let flags = arg_mut(flags, "flags")?;
            flags.inner.views = if registry.is_null() {
                None
            } else {
                Some(arg_path(registry, "registry")?)
            };
            Ok(())
        })
    }
}

/// `--search-path`: another directory to look in for templates, after the source and the ones
/// already added.
///
/// # Safety
///
/// `flags` is `NULL` or live flags no other call is using; `path` is `NULL` or a NUL-terminated
/// string; `error` is `NULL` or points to a pointer the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_flags_add_search_path(
    flags: *mut SqlComposerFlags,
    path: *const c_char,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let flags = arg_mut(flags, "flags")?;
            flags.inner.search_paths.push(arg_path(path, "path")?);
            Ok(())
        })
    }
}

/// Free flags. `NULL` does nothing.
///
/// # Safety
///
/// `flags` is `NULL`, or flags this library made and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_flags_free(flags: *mut SqlComposerFlags) {
    // SAFETY: the caller's word.
    unsafe { free_handle(flags) }
}

/// Compose every `.sqlc` file under `source_dir`, at any depth, with `flags`, into `*out`: what
/// composing made of each, and the text of the `manifest.json` cargo sqlc writes beside the
/// composed files.
///
/// # Safety
///
/// `source_dir` is `NULL` or a NUL-terminated string; `flags` is `NULL` or live flags; `out` and
/// `error` are `NULL` or point to pointers the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_compose_all(
    source_dir: *const c_char,
    flags: *const SqlComposerFlags,
    out: *mut *mut SqlComposerTree,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let out = arg_out(out, "out")?;
            let source_dir = arg_path(source_dir, "source_dir")?;
            let flags = &arg_ref(flags, "flags")?.inner;
            let standings = source::compose_all(&source_dir, flags)?;
            let manifest_json = Manifest::new(flags, &standings).to_json()?;
            let entries = standings
                .iter()
                .map(|(rel_path, standing)| Entry::new(rel_path, standing))
                .collect::<Result<Vec<_>, _>>()?;
            *out = into_handle(SqlComposerTree {
                entries,
                manifest_json: c_string(manifest_json, "the manifest")?,
            });
            Ok(())
        })
    }
}

/// How many templates the tree holds. 0 when `tree` is `NULL`.
///
/// # Safety
///
/// `tree` is `NULL` or a live tree.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_tree_len(tree: *const SqlComposerTree) -> usize {
    // SAFETY: the caller's word.
    guarded(0, || {
        unsafe { tree.as_ref() }.map_or(0, |tree| tree.entries.len())
    })
}

/// The template at `index` into `*out`, in the order of their paths under the source.
///
/// # Safety
///
/// `tree` is `NULL` or a live tree; `out` is `NULL` or points to an entry the caller can write;
/// `error` is `NULL` or points to a pointer the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_tree_entry(
    tree: *const SqlComposerTree,
    index: usize,
    out: *mut SqlComposerEntry,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let tree = arg_ref(tree, "tree")?;
            let out = out
                .as_mut()
                .ok_or_else(|| Failure::invalid("out is NULL"))?;
            let held = tree.entries.get(index).ok_or_else(|| {
                Failure::invalid(format!(
                    "index {index} is past the tree's {} templates",
                    tree.entries.len()
                ))
            })?;
            *out = held.borrowed();
            Ok(())
        })
    }
}

/// The text of the `manifest.json` cargo sqlc writes beside the composed files: the flags, and
/// each composed file's bind names and where its statements' names begin. `NULL` when `tree` is.
///
/// # Safety
///
/// `tree` is `NULL` or a live tree. The string lives as long as it does.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_tree_manifest_json(
    tree: *const SqlComposerTree,
) -> *const c_char {
    // SAFETY: the caller's word.
    guarded(ptr::null(), || {
        unsafe { tree.as_ref() }.map_or(ptr::null(), |tree| tree.manifest_json.as_ptr())
    })
}

/// Free a tree, and every composed template it holds. `NULL` does nothing.
///
/// # Safety
///
/// `tree` is `NULL`, or a tree this library made and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_tree_free(tree: *mut SqlComposerTree) {
    // SAFETY: the caller's word.
    unsafe { free_handle(tree) }
}
