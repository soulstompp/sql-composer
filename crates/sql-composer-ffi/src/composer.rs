//! Composing a template for a dialect, and what composing gives back.

use std::collections::BTreeMap;
use std::ffi::{c_char, CString};
use std::ptr;

use sql_composer::types::Dialect;
use sql_composer::{ComposedSql, Composer, MockTable};

use crate::template::SqlComposerTemplate;
use crate::{
    arg_items, arg_mut, arg_out, arg_path, arg_ref, arg_str, arg_strs, c_string, entry,
    free_handle, guarded, into_handle, Failure, SqlComposerStatus,
};

/// A composer: the dialect it composes for, where it finds the templates a `:compose` names,
/// whether it names the relations a statement reads, its views and its mock tables. Made by
/// [`sql_composer_new`], freed by [`sql_composer_free`].
pub struct SqlComposer {
    pub(crate) inner: Composer,
}

/// A composed template: its SQL, its bind names in placeholder order, and where each statement's
/// names begin. Made by [`sql_composer_compose`] or [`sql_composer_compose_with_counts`], freed by
/// [`sql_composer_composed_free`].
pub struct SqlComposerComposed {
    sql: CString,
    bind_params: Vec<CString>,
    bind_param_ptrs: Vec<*const c_char>,
    statement_starts: Vec<usize>,
}

impl SqlComposerComposed {
    /// `composed` as C strings.
    pub(crate) fn new(composed: ComposedSql) -> Result<Self, Failure> {
        let bind_params = composed
            .bind_params
            .into_iter()
            .map(|name| c_string(name, "a bind name"))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SqlComposerComposed {
            sql: c_string(composed.sql, "the composed SQL")?,
            bind_param_ptrs: bind_params.iter().map(|name| name.as_ptr()).collect(),
            bind_params,
            statement_starts: composed.statement_starts,
        })
    }
}

/// Make a composer for the dialect named `dialect` (`postgres`, `mysql`, `mariadb`, `sqlite` or
/// `duckdb`) into `*out`, with no search path, no views, no mock tables, and relations written
/// where they are composed.
///
/// # Safety
///
/// `dialect` is `NULL` or a NUL-terminated string; `out` and `error` are `NULL` or point to
/// pointers the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_new(
    dialect: *const c_char,
    out: *mut *mut SqlComposer,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let out = arg_out(out, "out")?;
            let dialect: Dialect = arg_str(dialect, "dialect")?.parse()?;
            *out = into_handle(SqlComposer {
                inner: Composer::new(dialect),
            });
            Ok(())
        })
    }
}

/// Add `path` to the directories the composer looks in for the templates a `:compose` names,
/// after the ones already added.
///
/// # Safety
///
/// `composer` is `NULL` or a live composer no other call is using; `path` is `NULL` or a
/// NUL-terminated string; `error` is `NULL` or points to a pointer the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_add_search_path(
    composer: *mut SqlComposer,
    path: *const c_char,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let composer = arg_mut(composer, "composer")?;
            composer.inner.add_search_path(arg_path(path, "path")?);
            Ok(())
        })
    }
}

/// Whether the composer writes every relation a statement reads once, named, at the head of that
/// statement, as `cargo sqlc compose --with` does. Off when the composer is made.
///
/// # Safety
///
/// `composer` is `NULL` or a live composer no other call is using; `error` is `NULL` or points to
/// a pointer the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_set_name_relations(
    composer: *mut SqlComposer,
    name_relations: bool,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            arg_mut(composer, "composer")?.inner.name_relations = name_relations;
            Ok(())
        })
    }
}

/// Register every `CREATE VIEW <name> AS :define(<path>)` in the registry template at `registry`:
/// wherever `<path>` is composed after this, the composed SQL names the view instead. The registry
/// is found on the composer's search paths.
///
/// # Safety
///
/// `composer` is `NULL` or a live composer no other call is using; `registry` is `NULL` or a
/// NUL-terminated string; `error` is `NULL` or points to a pointer the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_load_view_registry(
    composer: *mut SqlComposer,
    registry: *const c_char,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let composer = arg_mut(composer, "composer")?;
            composer
                .inner
                .load_view_registry(&arg_path(registry, "registry")?)?;
            Ok(())
        })
    }
}

/// Give the table `name` rows of the caller's own: every statement composed after this that names
/// the table reads them instead. The table has `column_count` columns, named by `column_names`
/// and typed by `column_types` as the dialect spells its types. `values` holds `row_count` rows of
/// `column_count` values each, one row after another, each value written as SQL text would give
/// it, and `NULL` for SQL's `NULL`. A mock of a name already added replaces it.
///
/// # Safety
///
/// `composer` is `NULL` or a live composer no other call is using; `name` is `NULL` or a
/// NUL-terminated string; `column_names` and `column_types` are `NULL` or point to `column_count`
/// NUL-terminated strings; `values` is `NULL` or points to `row_count * column_count` pointers,
/// each `NULL` or a NUL-terminated string; `error` is `NULL` or points to a pointer the caller can
/// write.
// A C caller passes the table as its name and three arrays with their lengths.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub unsafe extern "C" fn sql_composer_add_mock_table(
    composer: *mut SqlComposer,
    name: *const c_char,
    column_names: *const *const c_char,
    column_types: *const *const c_char,
    column_count: usize,
    values: *const *const c_char,
    row_count: usize,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let composer = arg_mut(composer, "composer")?;
            let mut mock = MockTable::new(arg_str(name, "name")?);
            let names = arg_strs(column_names, column_count, "column_names")?;
            let types = arg_strs(column_types, column_count, "column_types")?;
            for (name, sql_type) in names.into_iter().zip(types) {
                mock = mock.column(name, sql_type);
            }
            let cells = row_count.checked_mul(column_count).ok_or_else(|| {
                Failure::invalid(format!(
                    "{row_count} rows of {column_count} columns is more values than memory holds"
                ))
            })?;
            let values = arg_items(values, cells, "values")?;
            if column_count > 0 {
                for (r, row) in values.chunks(column_count).enumerate() {
                    let row = row
                        .iter()
                        .enumerate()
                        .map(|(c, &value)| {
                            if value.is_null() {
                                return Ok(None);
                            }
                            arg_str(value, &format!("values[{}]", r * column_count + c)).map(Some)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    mock = mock.row(&row);
                }
            }
            composer.inner.add_mock_table(mock);
            Ok(())
        })
    }
}

/// Compose the template `parsed` into `*out`, each binding one placeholder.
///
/// # Safety
///
/// `composer` and `parsed` are `NULL` or live handles; `out` and `error` are `NULL` or point to
/// pointers the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_compose(
    composer: *const SqlComposer,
    parsed: *const SqlComposerTemplate,
    out: *mut *mut SqlComposerComposed,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let out = arg_out(out, "out")?;
            let composer = arg_ref(composer, "composer")?;
            let parsed = arg_ref(parsed, "parsed")?;
            let composed = composer.inner.compose(&parsed.inner)?;
            *out = into_handle(SqlComposerComposed::new(composed)?);
            Ok(())
        })
    }
}

/// Compose the template `parsed` into `*out`, the binding `names[i]` given `counts[i]` values: a
/// placeholder for each, and its name that many times in the bind names. A binding not named takes
/// one placeholder. A count outside what the binding's `EXPECTING` allows is refused.
///
/// # Safety
///
/// `composer` and `parsed` are `NULL` or live handles; `names` is `NULL` or points to `len`
/// NUL-terminated strings, and `counts` is `NULL` or points to `len` counts; `out` and `error` are
/// `NULL` or point to pointers the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_compose_with_counts(
    composer: *const SqlComposer,
    parsed: *const SqlComposerTemplate,
    names: *const *const c_char,
    counts: *const usize,
    len: usize,
    out: *mut *mut SqlComposerComposed,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let out = arg_out(out, "out")?;
            let composer = arg_ref(composer, "composer")?;
            let parsed = arg_ref(parsed, "parsed")?;
            let names = arg_strs(names, len, "names")?;
            let counts = arg_items(counts, len, "counts")?;
            let mut values: BTreeMap<String, Vec<()>> = BTreeMap::new();
            for (name, &count) in names.into_iter().zip(counts) {
                if values.insert(name.to_string(), vec![(); count]).is_some() {
                    return Err(Failure::invalid(format!("{name} is given a count twice")));
                }
            }
            let composed = composer.inner.compose_with_values(&parsed.inner, &values)?;
            *out = into_handle(SqlComposerComposed::new(composed)?);
            Ok(())
        })
    }
}

/// Free a composer. `NULL` does nothing.
///
/// # Safety
///
/// `composer` is `NULL`, or a composer this library made and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_free(composer: *mut SqlComposer) {
    // SAFETY: the caller's word.
    unsafe { free_handle(composer) }
}

/// The composed SQL, with the dialect's placeholders. `NULL` when `composed` is.
///
/// # Safety
///
/// `composed` is `NULL` or a live composed template. The string lives as long as it does.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_composed_sql(
    composed: *const SqlComposerComposed,
) -> *const c_char {
    // SAFETY: the caller's word.
    guarded(ptr::null(), || match unsafe { composed.as_ref() } {
        Some(composed) => composed.sql.as_ptr(),
        None => ptr::null(),
    })
}

/// The bind names, one for each placeholder in order, and their number in `*len`. On PostgreSQL,
/// SQLite and DuckDB a statement's names are in alphabetical order, each once; on MySQL and
/// MariaDB they are in the order the statement uses them. `NULL`, and `*len` 0, when `composed` is
/// `NULL`.
///
/// # Safety
///
/// `composed` is `NULL` or a live composed template; `len` is `NULL` or points to a `size_t` the
/// caller can write. The array and its strings live as long as `composed` does.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_composed_bind_params(
    composed: *const SqlComposerComposed,
    len: *mut usize,
) -> *const *const c_char {
    // SAFETY: the caller's word.
    guarded(ptr::null(), || unsafe {
        let (array, count) = match composed.as_ref() {
            Some(composed) => (
                composed.bind_param_ptrs.as_ptr(),
                composed.bind_params.len(),
            ),
            None => (ptr::null(), 0),
        };
        if let Some(len) = len.as_mut() {
            *len = count;
        }
        array
    })
}

/// Where each statement's names begin among the bind names, one for each statement, the first 0,
/// and their number in `*len`. Each statement numbers its placeholders from the first. `NULL`, and
/// `*len` 0, when `composed` is `NULL`.
///
/// # Safety
///
/// `composed` is `NULL` or a live composed template; `len` is `NULL` or points to a `size_t` the
/// caller can write. The array lives as long as `composed` does.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_composed_statement_starts(
    composed: *const SqlComposerComposed,
    len: *mut usize,
) -> *const usize {
    // SAFETY: the caller's word.
    guarded(ptr::null(), || unsafe {
        let (array, count) = match composed.as_ref() {
            Some(composed) => (
                composed.statement_starts.as_ptr(),
                composed.statement_starts.len(),
            ),
            None => (ptr::null(), 0),
        };
        if let Some(len) = len.as_mut() {
            *len = count;
        }
        array
    })
}

/// Free a composed template. `NULL` does nothing.
///
/// # Safety
///
/// `composed` is `NULL`, or a composed template this library made and not yet freed, and not one
/// a tree holds.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_composed_free(composed: *mut SqlComposerComposed) {
    // SAFETY: the caller's word.
    unsafe { free_handle(composed) }
}
