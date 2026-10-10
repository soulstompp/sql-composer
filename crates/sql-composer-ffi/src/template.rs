//! Parsing templates.

use std::ffi::c_char;

use sql_composer::parser;
use sql_composer::types::{Template, TemplateSource};

use crate::{arg_out, arg_path, arg_str, entry, free_handle, into_handle, SqlComposerStatus};

/// A parsed template. Made by [`sql_composer_template_parse`] or
/// [`sql_composer_template_parse_file`], freed by [`sql_composer_template_free`].
pub struct SqlComposerTemplate {
    pub(crate) inner: Template,
}

/// Parse the template `text` into `*out`. `name` is what an error calls the template; `NULL` calls
/// it `template`. A template's `:compose` paths are found on the search paths of the composer that
/// composes it.
///
/// # Safety
///
/// `text` and `name` are `NULL` or NUL-terminated strings; `out` and `error` are `NULL` or point
/// to pointers the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_template_parse(
    text: *const c_char,
    name: *const c_char,
    out: *mut *mut SqlComposerTemplate,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let out = arg_out(out, "out")?;
            let text = arg_str(text, "text")?;
            let name = if name.is_null() {
                "template"
            } else {
                arg_str(name, "name")?
            };
            let template = parser::parse_template(text, TemplateSource::Literal(name.into()))?;
            *out = into_handle(SqlComposerTemplate { inner: template });
            Ok(())
        })
    }
}

/// Parse the template in the file at `path` into `*out`. Its `:compose` paths are found on the
/// search paths of the composer that composes it.
///
/// # Safety
///
/// `path` is `NULL` or a NUL-terminated string; `out` and `error` are `NULL` or point to pointers
/// the caller can write.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_template_parse_file(
    path: *const c_char,
    out: *mut *mut SqlComposerTemplate,
    error: *mut *mut c_char,
) -> SqlComposerStatus {
    // SAFETY: every pointer is the caller's, read as its documentation says.
    unsafe {
        entry(error, || {
            let out = arg_out(out, "out")?;
            let path = arg_path(path, "path")?;
            let template = parser::parse_template_file(&path)?;
            *out = into_handle(SqlComposerTemplate { inner: template });
            Ok(())
        })
    }
}

/// Free the template `parsed`. `NULL` does nothing.
///
/// # Safety
///
/// `parsed` is `NULL`, or a template this library made and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn sql_composer_template_free(parsed: *mut SqlComposerTemplate) {
    // SAFETY: the caller's word.
    unsafe { free_handle(parsed) }
}
