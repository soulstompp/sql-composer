# sql-composer-ffi

> **Também disponível em português europeu: [`README.pt.md`](
> https://github.com/soulstompp/sql-composer/blob/HEAD/crates/sql-composer-ffi/README.pt.md).**

The C ABI of [sql-composer](https://crates.io/crates/sql-composer): parse SQL templates, compose
them for a dialect, and compose a whole directory of them as `cargo sqlc compose` does, from any
language that calls C.

The composer gives back SQL and the names its placeholders stand for. The values stay the
caller's: it runs the SQL with its own database library, binding its values in the order the
names are listed.

## Building

```sh
cargo build --release -p sql-composer-ffi
```

This builds the shared library `sql_composer_ffi` (`libsql_composer_ffi.so`, or as the platform
names its shared libraries) and the static `libsql_composer_ffi.a` under `target/release/`. The
header is [`sql_composer.h`][header], beside this file. cbindgen writes it from the
crate's source into the build's `OUT_DIR` on every build, and `cargo test -p sql-composer-ffi`
fails, saying what to copy, when the copy here differs from it. A program linking the static
library also links the system libraries Rust's standard library needs, which this lists:

```sh
cargo rustc -p sql-composer-ffi --crate-type staticlib -- --print native-static-libs
```

## How it is called

- Every string in and out is UTF-8 and NUL-terminated.
- What the library makes is an opaque handle (`SqlComposerTemplate`, `SqlComposer`,
  `SqlComposerComposed`, `SqlComposerFlags`, `SqlComposerTree`), freed by the function of the same
  name ending `_free`.
- Every call that can fail returns a `SqlComposerStatus` and takes `char **error` last. On failure
  `*error` holds the reason, sql-composer's own message, which the caller frees with
  `sql_composer_string_free`. Every other string or array the library gives out is borrowed from
  its handle.
- No panic crosses the boundary: one is returned as `SQL_COMPOSER_STATUS_PANIC`.

## Composing a template

```c
#include <stdio.h>
#include "sql_composer.h"

int main(void) {
    char *error = NULL;
    SqlComposerTemplate *template = NULL;
    SqlComposer *composer = NULL;
    SqlComposerComposed *composed = NULL;
    const char *names[] = {"theme_ids"};
    size_t counts[] = {3};

    if (sql_composer_template_parse(
            "SELECT set_num, name FROM lego_sets WHERE theme_id IN (:bind(theme_ids))",
            "sets", &template, &error) != SQL_COMPOSER_STATUS_OK
        || sql_composer_new("postgres", &composer, &error) != SQL_COMPOSER_STATUS_OK
        || sql_composer_compose_with_counts(composer, template, names, counts, 1, &composed,
                                            &error) != SQL_COMPOSER_STATUS_OK) {
        fprintf(stderr, "%s\n", error);
        sql_composer_string_free(error);
    } else {
        size_t count = 0;
        const char *const *params = sql_composer_composed_bind_params(composed, &count);
        /* SELECT set_num, name FROM lego_sets WHERE theme_id IN ($1, $2, $3) */
        printf("%s\n", sql_composer_composed_sql(composed));
        for (size_t i = 0; i < count; i++) {
            printf("$%zu is %s\n", i + 1, params[i]); /* theme_ids, three times */
        }
    }
    sql_composer_composed_free(composed);
    sql_composer_free(composer);
    sql_composer_template_free(template);
    return error == NULL ? 0 : 1;
}
```

`sql_composer_compose` composes each binding with one placeholder; `sql_composer_add_search_path`,
`sql_composer_set_name_relations`, `sql_composer_load_view_registry` and
`sql_composer_add_mock_table` set the composer up first.

## Composing a directory

`sql_composer_compose_all` composes every `.sqlc` file under a directory with the flags
`cargo sqlc compose` takes (`sql_composer_flags_new`, `_set_with`, `_set_views`,
`_add_search_path`, `_set_respell`). Each template of the tree it gives (`sql_composer_tree_entry`)
is written, with its file's path and its composed statements, or is a shape or skipped, with no
file. `sql_composer_tree_manifest_json` is the text of the `manifest.json` cargo sqlc writes
beside the files. [`tests/c/compose_tree.c`][compose_tree] writes a whole target with them.

`sql_composer_flags_set_respell` respells each composed file as `cargo sqlc compose --respell`
does: a union whose arms each read one roster, cut to their own rows of it and left joined
`ON true` to what each reads, is written as one join that reads the roster once, and every other
byte as composed. It respells PostgreSQL alone; flags for any other dialect that ask for it are
refused with sql-composer's message.

## License

MIT, as [LICENSE][LICENSE] says.

[header]: https://github.com/soulstompp/sql-composer/blob/HEAD/crates/sql-composer-ffi/sql_composer.h
[compose_tree]: https://github.com/soulstompp/sql-composer/blob/HEAD/crates/sql-composer-ffi/tests/c/compose_tree.c
[LICENSE]: https://github.com/soulstompp/sql-composer/blob/HEAD/crates/sql-composer-ffi/LICENSE
