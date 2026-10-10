//! The C ABI of [sql-composer](https://docs.rs/sql-composer): parse SQL templates, compose them
//! for a dialect, and compose a whole directory of them as `cargo sqlc compose` does, from any
//! language that calls C. The header, `sql_composer.h`, stands beside this crate's manifest.
//!
//! # Conventions
//!
//! - Every string in and out is UTF-8 and NUL-terminated.
//! - Everything the library makes is an opaque handle, freed by the library's function named for
//!   it: [`sql_composer_template_free`], [`sql_composer_free`], [`sql_composer_composed_free`],
//!   [`sql_composer_flags_free`], [`sql_composer_tree_free`]. Each takes `NULL` and does nothing.
//! - An error message is the one string the caller keeps, and frees with
//!   [`sql_composer_string_free`]. Every other string or array the library gives out is borrowed
//!   from the handle it came from, and lives as long as that handle.
//! - Every call that can fail returns a [`SqlComposerStatus`] and takes `char **error` last. On
//!   failure `*error` is set to the reason; on success it is set to `NULL`. `error` may itself be
//!   `NULL` when the caller does not want the reason.
//! - A call that makes a handle puts it in its `out` argument, and sets `*out` to `NULL` when it
//!   fails.
//! - No panic crosses the boundary: one inside the library is caught where the call entered and
//!   returned as [`SqlComposerStatus::Panic`], with its message.
//! - Values stay the caller's. A call that expands a binding takes only how many values each name
//!   is given, and the caller binds its values in the order the composed bind names list them.
//!
//! # Example
//!
//! ```c
//! #include <stdio.h>
//! #include "sql_composer.h"
//!
//! int main(void) {
//!     char *error = NULL;
//!     SqlComposerTemplate *template = NULL;
//!     SqlComposer *composer = NULL;
//!     SqlComposerComposed *composed = NULL;
//!
//!     if (sql_composer_template_parse("SELECT * FROM users WHERE id = :bind(user_id);",
//!                                     "users", &template, &error) != SQL_COMPOSER_STATUS_OK
//!         || sql_composer_new("postgres", &composer, &error) != SQL_COMPOSER_STATUS_OK
//!         || sql_composer_compose(composer, template, &composed, &error)
//!                != SQL_COMPOSER_STATUS_OK) {
//!         fprintf(stderr, "%s\n", error);
//!         sql_composer_string_free(error);
//!     } else {
//!         size_t count = 0;
//!         const char *const *names = sql_composer_composed_bind_params(composed, &count);
//!         printf("%s\n", sql_composer_composed_sql(composed)); /* ... WHERE id = $1; */
//!         for (size_t i = 0; i < count; i++) printf("$%zu is %s\n", i + 1, names[i]);
//!     }
//!     sql_composer_composed_free(composed);
//!     sql_composer_free(composer);
//!     sql_composer_template_free(template);
//!     return error == NULL ? 0 : 1;
//! }
//! ```

#![warn(missing_docs, unsafe_op_in_unsafe_fn)]

use std::any::Any;
use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::ptr;

mod composer;
mod template;
mod tree;

pub use composer::*;
pub use template::*;
pub use tree::*;

/// What a call came to.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlComposerStatus {
    /// The call did what it was asked.
    Ok = 0,
    /// sql-composer refused, and the message is its own; or what it made holds a NUL, which a C
    /// string cannot carry.
    Error = 1,
    /// An argument was `NULL` where a value is needed, or out of range, or a string was not UTF-8.
    InvalidArgument = 2,
    /// The library panicked. The panic was caught where the call entered, and the message is the
    /// panic's.
    Panic = 3,
}

/// The version of this library, as its crate is numbered.
#[no_mangle]
pub extern "C" fn sql_composer_version() -> *const c_char {
    guarded(ptr::null(), || {
        concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
    })
}

/// Free a string the library gave the caller to keep: an error message. `NULL` does nothing.
///
/// # Safety
///
/// `string` is `NULL`, or a string this library gave the caller to keep and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_string_free(string: *mut c_char) {
    if string.is_null() {
        return;
    }
    // SAFETY: the caller hands back a string `owned_message` made with `CString::into_raw`.
    guarded((), || drop(unsafe { CString::from_raw(string) }));
}

/// Why a call failed: the status it returns, and the message the caller reads.
#[derive(Debug)]
pub(crate) struct Failure {
    status: SqlComposerStatus,
    message: String,
}

impl Failure {
    /// An argument the call cannot take.
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Failure {
            status: SqlComposerStatus::InvalidArgument,
            message: message.into(),
        }
    }

    /// A refusal that is not an argument's fault.
    pub(crate) fn error(message: impl Into<String>) -> Self {
        Failure {
            status: SqlComposerStatus::Error,
            message: message.into(),
        }
    }
}

impl From<sql_composer::Error> for Failure {
    fn from(error: sql_composer::Error) -> Self {
        Failure::error(error.to_string())
    }
}

/// Run `call` with any panic caught, and return its status. The reason it failed, or the panic's
/// message, goes into `*error` as a string the caller frees.
///
/// # Safety
///
/// `error` is `NULL` or points to a `char *` the caller can write.
pub(crate) unsafe fn entry(
    error: *mut *mut c_char,
    call: impl FnOnce() -> Result<(), Failure>,
) -> SqlComposerStatus {
    if !error.is_null() {
        // SAFETY: the caller's pointer, which it says is writable.
        unsafe { *error = ptr::null_mut() };
    }
    let failure = match catch_unwind(AssertUnwindSafe(call)) {
        Ok(Ok(())) => return SqlComposerStatus::Ok,
        Ok(Err(failure)) => failure,
        Err(payload) => Failure {
            status: SqlComposerStatus::Panic,
            message: panic_message(payload.as_ref()),
        },
    };
    if !error.is_null() {
        // SAFETY: as above.
        unsafe { *error = owned_message(&failure.message) };
    }
    failure.status
}

/// `call`'s value with any panic caught, and `fallback` in its place when one is.
pub(crate) fn guarded<T>(fallback: T, call: impl FnOnce() -> T) -> T {
    catch_unwind(AssertUnwindSafe(call)).unwrap_or(fallback)
}

/// What a panic said.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    let said = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a panic with no message");
    format!("sql-composer panicked: {said}")
}

/// `message` as a string the caller keeps and frees with [`sql_composer_string_free`]. A NUL
/// inside it, which a C string cannot hold, is written `\0`.
pub(crate) fn owned_message(message: &str) -> *mut c_char {
    CString::new(message.replace('\0', "\\0"))
        .expect("every NUL is replaced")
        .into_raw()
}

/// `text` as a C string the library keeps, or a refusal naming `what` when it holds a NUL.
pub(crate) fn c_string(text: impl Into<String>, what: &str) -> Result<CString, Failure> {
    CString::new(text.into())
        .map_err(|_| Failure::error(format!("{what} holds a NUL, which a C string cannot carry")))
}

/// The string at `ptr`, the argument `what`.
///
/// # Safety
///
/// `ptr` is `NULL` or a NUL-terminated string that outlives `'a`.
pub(crate) unsafe fn arg_str<'a>(ptr: *const c_char, what: &str) -> Result<&'a str, Failure> {
    if ptr.is_null() {
        return Err(Failure::invalid(format!("{what} is NULL")));
    }
    // SAFETY: not NULL, and NUL-terminated by the caller's word.
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|_| Failure::invalid(format!("{what} is not UTF-8")))
}

/// The path at `ptr`, the argument `what`.
///
/// # Safety
///
/// As [`arg_str`].
pub(crate) unsafe fn arg_path(ptr: *const c_char, what: &str) -> Result<PathBuf, Failure> {
    // SAFETY: the caller's word, passed on.
    unsafe { arg_str(ptr, what) }.map(PathBuf::from)
}

/// The `len` strings at `ptr`, the argument `what`, none of them `NULL`.
///
/// # Safety
///
/// `ptr` is `NULL` only when `len` is 0, and otherwise points to `len` strings as [`arg_str`]
/// takes them.
pub(crate) unsafe fn arg_strs<'a>(
    ptr: *const *const c_char,
    len: usize,
    what: &str,
) -> Result<Vec<&'a str>, Failure> {
    // SAFETY: the caller's word, passed on.
    unsafe { arg_items(ptr, len, what) }?
        .iter()
        .enumerate()
        // SAFETY: each one a string, by the caller's word.
        .map(|(i, &item)| unsafe { arg_str(item, &format!("{what}[{i}]")) })
        .collect()
}

/// The `len` items at `ptr`, the argument `what`.
///
/// # Safety
///
/// `ptr` is `NULL` only when `len` is 0, and otherwise points to `len` items that outlive `'a`.
pub(crate) unsafe fn arg_items<'a, T>(
    ptr: *const T,
    len: usize,
    what: &str,
) -> Result<&'a [T], Failure> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(Failure::invalid(format!("{what} is NULL and holds {len}")));
    }
    // SAFETY: not NULL, and `len` items long by the caller's word.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// The handle `ptr`, the argument `what`.
///
/// # Safety
///
/// `ptr` is `NULL` or a live handle of this library's that outlives `'a`.
pub(crate) unsafe fn arg_ref<'a, T>(ptr: *const T, what: &str) -> Result<&'a T, Failure> {
    // SAFETY: the caller's word.
    unsafe { ptr.as_ref() }.ok_or_else(|| Failure::invalid(format!("{what} is NULL")))
}

/// The handle `ptr`, the argument `what`, to change.
///
/// # Safety
///
/// As [`arg_ref`], and nothing else reads the handle during the call.
pub(crate) unsafe fn arg_mut<'a, T>(ptr: *mut T, what: &str) -> Result<&'a mut T, Failure> {
    // SAFETY: the caller's word.
    unsafe { ptr.as_mut() }.ok_or_else(|| Failure::invalid(format!("{what} is NULL")))
}

/// The place `ptr` a call puts what it makes, set to `NULL` until it has made it.
///
/// # Safety
///
/// `ptr` is `NULL` or points to a pointer the caller can write.
pub(crate) unsafe fn arg_out<'a, T>(
    ptr: *mut *mut T,
    what: &str,
) -> Result<&'a mut *mut T, Failure> {
    // SAFETY: the caller's word.
    let out = unsafe { ptr.as_mut() }.ok_or_else(|| Failure::invalid(format!("{what} is NULL")))?;
    *out = ptr::null_mut();
    Ok(out)
}

/// `value` as a handle the caller frees.
pub(crate) fn into_handle<T>(value: T) -> *mut T {
    Box::into_raw(Box::new(value))
}

/// Free the handle `ptr` made by [`into_handle`]. `NULL` does nothing.
///
/// # Safety
///
/// `ptr` is `NULL`, or a handle [`into_handle`] made and not yet freed.
pub(crate) unsafe fn free_handle<T>(ptr: *mut T) {
    if !ptr.is_null() {
        // SAFETY: a handle `into_handle` made, by the caller's word.
        guarded((), || drop(unsafe { Box::from_raw(ptr) }));
    }
}

/// `path` with its parts joined by `/`, as the manifest names files.
pub(crate) fn slashed(path: &Path) -> String {
    path.components()
        .map(|part| part.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The message `error` holds, freed.
    fn taken(error: *mut c_char) -> String {
        assert!(!error.is_null(), "a failure gives its reason");
        // SAFETY: a string `entry` made, freed once.
        let message = unsafe { CStr::from_ptr(error) }
            .to_str()
            .unwrap()
            .to_string();
        unsafe { sql_composer_string_free(error) };
        message
    }

    #[test]
    fn test_a_panic_is_caught_where_the_call_entered() {
        let mut error = ptr::null_mut();
        // SAFETY: `error` is a pointer this test can write.
        let status = unsafe { entry(&mut error, || panic!("the composer fell over")) };
        assert_eq!(status, SqlComposerStatus::Panic);
        assert_eq!(
            taken(error),
            "sql-composer panicked: the composer fell over"
        );

        let fallback = guarded(7, || -> i32 { panic!("an accessor fell over") });
        assert_eq!(fallback, 7);
    }

    #[test]
    fn test_a_success_leaves_no_message() {
        let mut error = owned_message("left from an earlier call");
        let earlier = error;
        // SAFETY: `error` is a pointer this test can write.
        let status = unsafe { entry(&mut error, || Ok(())) };
        assert_eq!(status, SqlComposerStatus::Ok);
        assert!(error.is_null());
        // The caller freed nothing yet: the earlier message is still its own to free.
        assert_eq!(taken(earlier), "left from an earlier call");
    }

    #[test]
    fn test_a_nul_in_a_message_is_written_out() {
        assert_eq!(taken(owned_message("a\0b")), "a\\0b");
    }
}
