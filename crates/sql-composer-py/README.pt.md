# sql-composer para Python

> **Português europeu, grafia do AO90.** Também disponível em inglês: [`README.md`](README.md).

O módulo Python do [sql-composer](https://crates.io/crates/sql-composer), que analisa modelos de
SQL, os compõe para um dialeto e compõe uma pasta inteira deles como o `cargo sqlc compose` a
compõe. A distribuição chama-se `sql-composer`, e o módulo `sql_composer`.

O compositor devolve o SQL e os nomes que os seus marcadores representam, e os valores ficam com o
programa, que os passa ao seu próprio módulo de base de dados pela ordem em que o `bind_params`
lista os nomes.

```python
import sqlite3
import sql_composer

template = sql_composer.parse(
    "SELECT set_num, name FROM lego_sets WHERE theme_id IN (:bind(theme_ids)) AND year = :bind(year)",
    name="sets",
)
composer = sql_composer.Composer("sqlite")
composed = composer.compose(template, counts={"theme_ids": 3})

composed.sql  # 'SELECT set_num, name FROM lego_sets WHERE theme_id IN (?1, ?2, ?3) AND year = ?4'
composed.bind_params       # ['theme_ids', 'theme_ids', 'theme_ids', 'year']
composed.statement_starts  # [0]

values = {"theme_ids": iter([50, 126, 186]), "year": iter([1990])}
db = sqlite3.connect("sets.db")
rows = db.execute(composed.sql, [next(values[name]) for name in composed.bind_params]).fetchall()
```

## Compor

```python
sql_composer.parse(text, name="template")      # um Template
sql_composer.parse_file(path)                  # um Template

composer = sql_composer.Composer(
    dialect,                 # "postgres", "mysql", "mariadb", "sqlite" ou "duckdb"
    *,
    search_paths=(),         # onde se procura um :compose, por ordem
    name_relations=False,    # cada relação que uma instrução lê, nomeada uma vez à cabeça (--with)
    views=None,              # um registo de vistas, encontrado nos caminhos de pesquisa
)
composer.add_search_path(path)
composer.load_view_registry(path)
composer.add_mock_table(name, [("id", "integer"), ("kind", "text")], [["1", "brick"], ["2", None]])
composer.compose(template, *, counts=None)     # um Composed
```

O `counts` dá a um parâmetro um número de valores, com um marcador por valor e o seu nome repetido
esse número de vezes no `bind_params`. Num modelo com várias instruções, cada uma numera os seus
marcadores a partir do primeiro, e o `statement_starts` diz onde começam, no `bind_params`, os
nomes de cada uma.

## Compor uma pasta

```python
tree = sql_composer.compose_all(
    source_dir,              # "sqlc", por exemplo
    dialect,                 # "postgres", por exemplo
    *,
    name_relations=False,    # --with
    views=None,              # --views
    search_paths=(),         # --search-path, cada um procurado depois da fonte
    respell=False,           # --respell
)
tree.files           # {"sets/select_set_parts.sql": Composed, ...}: um por cada modelo escrito
tree.standings       # {"sets/select_set_parts.sqlc": Standing, ...}: todos os modelos da fonte
tree.manifest_json   # o texto do manifest.json que o cargo sqlc escreve ao lado dos ficheiros
```

Um `Standing` tem o `kind` `"written"`, com o seu `file` e o seu `composed`; `"shape"`, com as
vagas que deixa abertas, em `slots`; ou `"skipped"`, com a vaga deixada aberta em `slots` e o
modelo onde ela está, `slot_template`. Só os escritos têm ficheiro, e escrever cada um dos
`tree.files` numa pasta de destino, com o `tree.manifest_json` ao lado deles como `manifest.json`,
dá os ficheiros que o `cargo sqlc compose` escreve.

O `respell=True` escreve cada ficheiro composto noutra grafia, como o `cargo sqlc compose --respell`
o faz: uma união cujos braços leem todos um mesmo rol, cada braço cortado às linhas que são suas e
juntado à esquerda, `ON true`, ao que lê, passa a ser escrita como uma só junção que lê o rol uma
única vez, e todos os outros bytes ficam como foram compostos. Só o faz para o PostgreSQL.

## Erros

Uma recusa lança `sql_composer.Error`, com a própria mensagem do sql-composer: um modelo que não se
consegue analisar, um dialeto que não existe, uma vaga deixada aberta, uma contagem que o seu
`EXPECTING` recusa, ou a nova grafia, escrita no ficheiro composto, pedida para outro dialeto que
não o PostgreSQL. Um ficheiro que não se consegue ler lança o `OSError` do seu tipo, e um que não
existe, `FileNotFoundError`.

## Compilar

```sh
maturin build --release      # a partir desta pasta; o ficheiro .whl fica em target/wheels/
```

O maturin define ele próprio o `PYO3_BUILD_EXTENSION_MODULE`, pelo que o pacote não nomeia a
funcionalidade `extension-module`, e o `cargo test -p sql-composer-py` incorpora o Python e liga-se
à libpython.

## Licença

MIT, como diz o [LICENSE][LICENSE].

[LICENSE]: https://github.com/soulstompp/sql-composer/blob/HEAD/crates/sql-composer-py/LICENSE
