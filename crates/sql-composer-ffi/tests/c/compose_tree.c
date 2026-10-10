/*
 * Composes every template under a source directory through the C ABI of sql-composer, and writes
 * what cargo sqlc compose writes into a target directory: a file for each template written, and
 * manifest.json beside them.
 *
 *     compose_tree <source> <target> <dialect> [--respell]
 *
 * Exits 0 when every file is written, 1 with the library's message on stderr when it refuses.
 */
#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "sql_composer.h"

/* Write `text` to the file `rel` under `target`, making the directories on the way. */
static int write_file(const char *target, const char *rel, const char *text) {
    size_t len = strlen(target) + 1 + strlen(rel) + 1;
    char *path = malloc(len);
    if (path == NULL) {
        return -1;
    }
    snprintf(path, len, "%s/%s", target, rel);
    for (char *slash = path + strlen(target) + 1; (slash = strchr(slash, '/')) != NULL; slash++) {
        *slash = '\0';
        if (mkdir(path, 0777) != 0 && errno != EEXIST) {
            free(path);
            return -1;
        }
        *slash = '/';
    }
    FILE *file = fopen(path, "wb");
    free(path);
    if (file == NULL) {
        return -1;
    }
    size_t size = strlen(text);
    int written = fwrite(text, 1, size, file) == size;
    return fclose(file) == 0 && written ? 0 : -1;
}

/* Print the library's message and free it. */
static int refused(char *error) {
    fprintf(stderr, "%s", error);
    sql_composer_string_free(error);
    return 1;
}

int main(int argc, char **argv) {
    int respell = argc == 5 && strcmp(argv[4], "--respell") == 0;
    if (argc != 4 && !respell) {
        fprintf(stderr, "usage: %s <source> <target> <dialect> [--respell]\n", argv[0]);
        return 2;
    }
    const char *source = argv[1];
    const char *target = argv[2];
    const char *dialect = argv[3];

    char *error = NULL;
    SqlComposerFlags *flags = NULL;
    SqlComposerTree *tree = NULL;

    if (sql_composer_flags_new(dialect, &flags, &error) != SQL_COMPOSER_STATUS_OK
        || sql_composer_flags_set_respell(flags, respell, &error) != SQL_COMPOSER_STATUS_OK) {
        sql_composer_flags_free(flags);
        return refused(error);
    }
    SqlComposerStatus status = sql_composer_compose_all(source, flags, &tree, &error);
    sql_composer_flags_free(flags);
    if (status != SQL_COMPOSER_STATUS_OK) {
        return refused(error);
    }

    int failed = mkdir(target, 0777) != 0 && errno != EEXIST;
    for (size_t i = 0; i < sql_composer_tree_len(tree) && !failed; i++) {
        SqlComposerEntry entry;
        if (sql_composer_tree_entry(tree, i, &entry, &error) != SQL_COMPOSER_STATUS_OK) {
            sql_composer_tree_free(tree);
            return refused(error);
        }
        if (entry.standing == SQL_COMPOSER_STANDING_WRITTEN) {
            failed = write_file(target, entry.file, sql_composer_composed_sql(entry.composed));
        }
    }
    if (!failed) {
        failed = write_file(target, "manifest.json", sql_composer_tree_manifest_json(tree));
    }
    sql_composer_tree_free(tree);
    if (failed) {
        perror(target);
    }
    return failed ? 1 : 0;
}
