#ifndef SQL_COMPOSER_H
#define SQL_COMPOSER_H

/* Written by cbindgen from the source of the sql-composer-ffi crate. Edits here are overwritten. */

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

/*
 What a call came to.
 */
typedef enum SqlComposerStatus {
  /*
   The call did what it was asked.
   */
  SQL_COMPOSER_STATUS_OK = 0,
  /*
   sql-composer refused, and the message is its own; or what it made holds a NUL, which a C
   string cannot carry.
   */
  SQL_COMPOSER_STATUS_ERROR = 1,
  /*
   An argument was `NULL` where a value is needed, or out of range, or a string was not UTF-8.
   */
  SQL_COMPOSER_STATUS_INVALID_ARGUMENT = 2,
  /*
   The library panicked. The panic was caught where the call entered, and the message is the
   panic's.
   */
  SQL_COMPOSER_STATUS_PANIC = 3,
} SqlComposerStatus;

/*
 What composing made of one template.
 */
typedef enum SqlComposerStanding {
  /*
   Composed, and written to its file.
   */
  SQL_COMPOSER_STANDING_WRITTEN = 0,
  /*
   A shape: it leaves slots of its own for a caller to fill, and is composed only inside that
   caller. It gets no file.
   */
  SQL_COMPOSER_STANDING_SHAPE = 1,
  /*
   A caller that leaves a slot of a template it composes open. It gets no file.
   */
  SQL_COMPOSER_STANDING_SKIPPED = 2,
} SqlComposerStanding;

/*
 A composer: the dialect it composes for, where it finds the templates a `:compose` names,
 whether it names the relations a statement reads, its views and its mock tables. Made by
 [`sql_composer_new`], freed by [`sql_composer_free`].
 */
typedef struct SqlComposer SqlComposer;

/*
 A composed template: its SQL, its bind names in placeholder order, and where each statement's
 names begin. Made by [`sql_composer_compose`] or [`sql_composer_compose_with_counts`], freed by
 [`sql_composer_composed_free`].
 */
typedef struct SqlComposerComposed SqlComposerComposed;

/*
 How a directory of templates is composed: the flags `cargo sqlc compose` takes for it. Made by
 [`sql_composer_flags_new`], freed by [`sql_composer_flags_free`].
 */
typedef struct SqlComposerFlags SqlComposerFlags;

/*
 A parsed template. Made by [`sql_composer_template_parse`] or
 [`sql_composer_template_parse_file`], freed by [`sql_composer_template_free`].
 */
typedef struct SqlComposerTemplate SqlComposerTemplate;

/*
 Every template under a source directory, with what composing made of it, and the manifest of
 the composed files. Made by [`sql_composer_compose_all`], freed by [`sql_composer_tree_free`].
 */
typedef struct SqlComposerTree SqlComposerTree;

/*
 One template under the source, as [`sql_composer_tree_entry`] gives it. Every pointer is
 borrowed from the tree, and lives as long as it does.
 */
typedef struct SqlComposerEntry {
  /*
   The template's path under the source, its parts joined by `/`.
   */
  const char *path;
  /*
   What composing made of it.
   */
  enum SqlComposerStanding standing;
  /*
   When written: its file's path under the target, its parts joined by `/`. `NULL` otherwise.
   */
  const char *file;
  /*
   When written: what its file holds, and its names. `NULL` otherwise.
   */
  const struct SqlComposerComposed *composed;
  /*
   A shape's open slots, in order, or the one slot a skipped template leaves open. None when
   written.
   */
  const char *const *slots;
  /*
   How many `slots` holds.
   */
  size_t slot_count;
  /*
   When skipped: the template the open slot stands in, as `:compose` names it. `NULL`
   otherwise.
   */
  const char *slot_template;
} SqlComposerEntry;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/*
 The version of this library, as its crate is numbered.
 */
const char *sql_composer_version(void);

/*
 Free a string the library gave the caller to keep: an error message. `NULL` does nothing.

 # Safety

 `string` is `NULL`, or a string this library gave the caller to keep and not yet freed.
 */
void sql_composer_string_free(char *string);

/*
 Make a composer for the dialect named `dialect` (`postgres`, `mysql`, `mariadb`, `sqlite` or
 `duckdb`) into `*out`, with no search path, no views, no mock tables, and relations written
 where they are composed.

 # Safety

 `dialect` is `NULL` or a NUL-terminated string; `out` and `error` are `NULL` or point to
 pointers the caller can write.
 */
enum SqlComposerStatus sql_composer_new(const char *dialect,
                                        struct SqlComposer **out,
                                        char **error);

/*
 Add `path` to the directories the composer looks in for the templates a `:compose` names,
 after the ones already added.

 # Safety

 `composer` is `NULL` or a live composer no other call is using; `path` is `NULL` or a
 NUL-terminated string; `error` is `NULL` or points to a pointer the caller can write.
 */
enum SqlComposerStatus sql_composer_add_search_path(struct SqlComposer *composer,
                                                    const char *path,
                                                    char **error);

/*
 Whether the composer writes every relation a statement reads once, named, at the head of that
 statement, as `cargo sqlc compose --with` does. Off when the composer is made.

 # Safety

 `composer` is `NULL` or a live composer no other call is using; `error` is `NULL` or points to
 a pointer the caller can write.
 */
enum SqlComposerStatus sql_composer_set_name_relations(struct SqlComposer *composer,
                                                       bool name_relations,
                                                       char **error);

/*
 Register every `CREATE VIEW <name> AS :define(<path>)` in the registry template at `registry`:
 wherever `<path>` is composed after this, the composed SQL names the view instead. The registry
 is found on the composer's search paths.

 # Safety

 `composer` is `NULL` or a live composer no other call is using; `registry` is `NULL` or a
 NUL-terminated string; `error` is `NULL` or points to a pointer the caller can write.
 */
enum SqlComposerStatus sql_composer_load_view_registry(struct SqlComposer *composer,
                                                       const char *registry,
                                                       char **error);

/*
 Give the table `name` rows of the caller's own: every statement composed after this that names
 the table reads them instead. The table has `column_count` columns, named by `column_names`
 and typed by `column_types` as the dialect spells its types. `values` holds `row_count` rows of
 `column_count` values each, one row after another, each value written as SQL text would give
 it, and `NULL` for SQL's `NULL`. A mock of a name already added replaces it.

 # Safety

 `composer` is `NULL` or a live composer no other call is using; `name` is `NULL` or a
 NUL-terminated string; `column_names` and `column_types` are `NULL` or point to `column_count`
 NUL-terminated strings; `values` is `NULL` or points to `row_count * column_count` pointers,
 each `NULL` or a NUL-terminated string; `error` is `NULL` or points to a pointer the caller can
 write.
 */
enum SqlComposerStatus sql_composer_add_mock_table(struct SqlComposer *composer,
                                                   const char *name,
                                                   const char *const *column_names,
                                                   const char *const *column_types,
                                                   size_t column_count,
                                                   const char *const *values,
                                                   size_t row_count,
                                                   char **error);

/*
 Compose the template `parsed` into `*out`, each binding one placeholder.

 # Safety

 `composer` and `parsed` are `NULL` or live handles; `out` and `error` are `NULL` or point to
 pointers the caller can write.
 */
enum SqlComposerStatus sql_composer_compose(const struct SqlComposer *composer,
                                            const struct SqlComposerTemplate *parsed,
                                            struct SqlComposerComposed **out,
                                            char **error);

/*
 Compose the template `parsed` into `*out`, the binding `names[i]` given `counts[i]` values: a
 placeholder for each, and its name that many times in the bind names. A binding not named takes
 one placeholder. A count outside what the binding's `EXPECTING` allows is refused.

 # Safety

 `composer` and `parsed` are `NULL` or live handles; `names` is `NULL` or points to `len`
 NUL-terminated strings, and `counts` is `NULL` or points to `len` counts; `out` and `error` are
 `NULL` or point to pointers the caller can write.
 */
enum SqlComposerStatus sql_composer_compose_with_counts(const struct SqlComposer *composer,
                                                        const struct SqlComposerTemplate *parsed,
                                                        const char *const *names,
                                                        const size_t *counts,
                                                        size_t len,
                                                        struct SqlComposerComposed **out,
                                                        char **error);

/*
 Free a composer. `NULL` does nothing.

 # Safety

 `composer` is `NULL`, or a composer this library made and not yet freed.
 */
void sql_composer_free(struct SqlComposer *composer);

/*
 The composed SQL, with the dialect's placeholders. `NULL` when `composed` is.

 # Safety

 `composed` is `NULL` or a live composed template. The string lives as long as it does.
 */
const char *sql_composer_composed_sql(const struct SqlComposerComposed *composed);

/*
 The bind names, one for each placeholder in order, and their number in `*len`. On PostgreSQL,
 SQLite and DuckDB a statement's names are in alphabetical order, each once; on MySQL and
 MariaDB they are in the order the statement uses them. `NULL`, and `*len` 0, when `composed` is
 `NULL`.

 # Safety

 `composed` is `NULL` or a live composed template; `len` is `NULL` or points to a `size_t` the
 caller can write. The array and its strings live as long as `composed` does.
 */
const char *const *sql_composer_composed_bind_params(const struct SqlComposerComposed *composed,
                                                     size_t *len);

/*
 Where each statement's names begin among the bind names, one for each statement, the first 0,
 and their number in `*len`. Each statement numbers its placeholders from the first. `NULL`, and
 `*len` 0, when `composed` is `NULL`.

 # Safety

 `composed` is `NULL` or a live composed template; `len` is `NULL` or points to a `size_t` the
 caller can write. The array lives as long as `composed` does.
 */
const size_t *sql_composer_composed_statement_starts(const struct SqlComposerComposed *composed,
                                                     size_t *len);

/*
 Free a composed template. `NULL` does nothing.

 # Safety

 `composed` is `NULL`, or a composed template this library made and not yet freed, and not one
 a tree holds.
 */
void sql_composer_composed_free(struct SqlComposerComposed *composed);

/*
 Parse the template `text` into `*out`. `name` is what an error calls the template; `NULL` calls
 it `template`. A template's `:compose` paths are found on the search paths of the composer that
 composes it.

 # Safety

 `text` and `name` are `NULL` or NUL-terminated strings; `out` and `error` are `NULL` or point
 to pointers the caller can write.
 */
enum SqlComposerStatus sql_composer_template_parse(const char *text,
                                                   const char *name,
                                                   struct SqlComposerTemplate **out,
                                                   char **error);

/*
 Parse the template in the file at `path` into `*out`. Its `:compose` paths are found on the
 search paths of the composer that composes it.

 # Safety

 `path` is `NULL` or a NUL-terminated string; `out` and `error` are `NULL` or point to pointers
 the caller can write.
 */
enum SqlComposerStatus sql_composer_template_parse_file(const char *path,
                                                        struct SqlComposerTemplate **out,
                                                        char **error);

/*
 Free the template `parsed`. `NULL` does nothing.

 # Safety

 `parsed` is `NULL`, or a template this library made and not yet freed.
 */
void sql_composer_template_free(struct SqlComposerTemplate *parsed);

/*
 Make flags for composing for the dialect named `dialect` (`postgres`, `mysql`, `mariadb`,
 `sqlite` or `duckdb`) into `*out`, with nothing else: no `--with`, no `--views`, no
 `--search-path`, no `--respell`.

 # Safety

 `dialect` is `NULL` or a NUL-terminated string; `out` and `error` are `NULL` or point to
 pointers the caller can write.
 */
enum SqlComposerStatus sql_composer_flags_new(const char *dialect,
                                              struct SqlComposerFlags **out,
                                              char **error);

/*
 `--with`: every relation a statement reads named once at its head.

 # Safety

 `flags` is `NULL` or live flags no other call is using; `error` is `NULL` or points to a
 pointer the caller can write.
 */
enum SqlComposerStatus sql_composer_flags_set_with(struct SqlComposerFlags *flags,
                                                   bool with,
                                                   char **error);

/*
 `--respell`: each composed file respelled as `cargo sqlc compose --respell` respells it. A
 union whose arms each read one roster, cut to their own rows of it and left joined `ON true`
 to what each reads, is written as one join that reads the roster once; every other byte is
 written as composed. PostgreSQL alone is respelled: [`sql_composer_compose_all`] refuses flags
 that ask for it with any other dialect.

 # Safety

 `flags` is `NULL` or live flags no other call is using; `error` is `NULL` or points to a
 pointer the caller can write.
 */
enum SqlComposerStatus sql_composer_flags_set_respell(struct SqlComposerFlags *flags,
                                                      bool respell,
                                                      char **error);

/*
 `--views`: the view registry, as the manifest records it. `NULL` takes it away.

 # Safety

 `flags` is `NULL` or live flags no other call is using; `registry` is `NULL` or a
 NUL-terminated string; `error` is `NULL` or points to a pointer the caller can write.
 */
enum SqlComposerStatus sql_composer_flags_set_views(struct SqlComposerFlags *flags,
                                                    const char *registry,
                                                    char **error);

/*
 `--search-path`: another directory to look in for templates, after the source and the ones
 already added.

 # Safety

 `flags` is `NULL` or live flags no other call is using; `path` is `NULL` or a NUL-terminated
 string; `error` is `NULL` or points to a pointer the caller can write.
 */
enum SqlComposerStatus sql_composer_flags_add_search_path(struct SqlComposerFlags *flags,
                                                          const char *path,
                                                          char **error);

/*
 Free flags. `NULL` does nothing.

 # Safety

 `flags` is `NULL`, or flags this library made and not yet freed.
 */
void sql_composer_flags_free(struct SqlComposerFlags *flags);

/*
 Compose every `.sqlc` file under `source_dir`, at any depth, with `flags`, into `*out`: what
 composing made of each, and the text of the `manifest.json` cargo sqlc writes beside the
 composed files.

 # Safety

 `source_dir` is `NULL` or a NUL-terminated string; `flags` is `NULL` or live flags; `out` and
 `error` are `NULL` or point to pointers the caller can write.
 */
enum SqlComposerStatus sql_composer_compose_all(const char *source_dir,
                                                const struct SqlComposerFlags *flags,
                                                struct SqlComposerTree **out,
                                                char **error);

/*
 How many templates the tree holds. 0 when `tree` is `NULL`.

 # Safety

 `tree` is `NULL` or a live tree.
 */
size_t sql_composer_tree_len(const struct SqlComposerTree *tree);

/*
 The template at `index` into `*out`, in the order of their paths under the source.

 # Safety

 `tree` is `NULL` or a live tree; `out` is `NULL` or points to an entry the caller can write;
 `error` is `NULL` or points to a pointer the caller can write.
 */
enum SqlComposerStatus sql_composer_tree_entry(const struct SqlComposerTree *tree,
                                               size_t index,
                                               struct SqlComposerEntry *out,
                                               char **error);

/*
 The text of the `manifest.json` cargo sqlc writes beside the composed files: the flags, and
 each composed file's bind names and where its statements' names begin. `NULL` when `tree` is.

 # Safety

 `tree` is `NULL` or a live tree. The string lives as long as it does.
 */
const char *sql_composer_tree_manifest_json(const struct SqlComposerTree *tree);

/*
 Free a tree, and every composed template it holds. `NULL` does nothing.

 # Safety

 `tree` is `NULL`, or a tree this library made and not yet freed.
 */
void sql_composer_tree_free(struct SqlComposerTree *tree);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* SQL_COMPOSER_H */
