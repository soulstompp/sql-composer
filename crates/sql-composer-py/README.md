# sql-composer for Python

Python bindings for [sql-composer](https://crates.io/crates/sql-composer): parse SQL templates,
compose them for a dialect, and compose a whole directory of them as `cargo sqlc compose` does.
The distribution is `sql-composer`, the module `sql_composer`.

The composer gives back SQL and the names its placeholders stand for. The values stay yours: you
pass them to your own database module in the order `bind_params` lists the names.

```python
import sqlite3
import sql_composer

template = sql_composer.parse(
    "SELECT id FROM parts WHERE id IN (:bind(ids)) AND kind = :bind(kind)", name="parts"
)
composer = sql_composer.Composer("sqlite")
composed = composer.compose(template, counts={"ids": 3})

composed.sql               # 'SELECT id FROM parts WHERE id IN (?1, ?2, ?3) AND kind = ?4'
composed.bind_params       # ['ids', 'ids', 'ids', 'kind']
composed.statement_starts  # [0]

values = {"ids": iter([1, 3, 4]), "kind": iter(["brick"])}
db = sqlite3.connect("parts.db")
rows = db.execute(composed.sql, [next(values[name]) for name in composed.bind_params]).fetchall()
```

## Composing

```python
sql_composer.parse(text, name="template")      # a Template
sql_composer.parse_file(path)                  # a Template

composer = sql_composer.Composer(
    dialect,                 # "postgres", "mysql", "mariadb", "sqlite" or "duckdb"
    *,
    search_paths=(),         # where a :compose is looked for, in order
    name_relations=False,    # each relation a statement reads named once at its head (--with)
    views=None,              # a view registry, found on the search paths
)
composer.add_search_path(path)
composer.load_view_registry(path)
composer.add_mock_table(name, [("id", "integer"), ("kind", "text")], [["1", "brick"], ["2", None]])
composer.compose(template, *, counts=None)     # a Composed
```

`counts` gives a binding a number of values: a placeholder for each, and its name that many times
in `bind_params`. A template of several statements numbers each statement's placeholders from the
first, and `statement_starts` says where each statement's names begin in `bind_params`.

## Composing a directory

```python
tree = sql_composer.compose_all(
    source_dir,              # "sqlc", say
    dialect,                 # "postgres", say
    *,
    name_relations=False,    # --with
    views=None,              # --views
    search_paths=(),         # --search-path, each looked in after the source
    respell=False,           # --respell
)
tree.files           # {"parts/by_kind.sql": Composed, ...}: one for each template written
tree.standings       # {"parts/by_kind.sqlc": Standing, ...}: every template under the source
tree.manifest_json   # the text of the manifest.json cargo sqlc writes beside the files
```

A `Standing` is `kind` `"written"`, with its `file` and `composed`; `"shape"`, with the `slots` it
leaves open; or `"skipped"`, with the slot left open in `slots` and the template it stands in,
`slot_template`. Only the written ones get a file. Writing each of `tree.files` under a target
directory, and `tree.manifest_json` as `manifest.json` beside them, gives the files
`cargo sqlc compose` writes.

`respell=True` respells each composed file as `cargo sqlc compose --respell` does: a union whose
arms each read one roster, cut to their own rows of it and left joined `ON true` to what each
reads, is written as one join that reads the roster once, and every other byte as composed. It
respells PostgreSQL alone.

## Errors

A refusal raises `sql_composer.Error`, with sql-composer's own message: a template that does not
parse, a dialect that is not one, a slot left open, a count its `EXPECTING` refuses, a respelling
asked for another dialect than PostgreSQL. A file that cannot be read raises the `OSError` of its
kind, a missing one `FileNotFoundError`.

## Building

```sh
maturin build --release      # from this directory; the wheel lands in target/wheels/
```

maturin sets `PYO3_BUILD_EXTENSION_MODULE` itself, so the crate names no `extension-module`
feature, and `cargo test -p sql-composer-py` embeds Python and links to libpython.

## License

MIT, as [LICENSE](LICENSE) says.
