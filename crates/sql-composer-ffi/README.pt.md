# sql-composer-ffi

> **Português europeu, grafia do AO90.** Também disponível em inglês: [`README.md`](README.md).

A ABI de C do [sql-composer](https://crates.io/crates/sql-composer), com que qualquer linguagem que
saiba chamar C analisa modelos de SQL, os compõe para um dialeto e compõe uma pasta inteira deles
como o `cargo sqlc compose` a compõe.

O compositor devolve o SQL e os nomes que os seus marcadores representam, e os valores ficam com
quem chama, que executa o SQL com a sua própria biblioteca de base de dados e lhe dá os valores pela
ordem em que os nomes vêm listados.

## Compilar

```sh
cargo build --release -p sql-composer-ffi
```

Isto compila a biblioteca partilhada `sql_composer_ffi` (`libsql_composer_ffi.so`, ou o nome que a
plataforma der às suas bibliotecas partilhadas) e a estática `libsql_composer_ffi.a`, em
`target/release/`. O cabeçalho é o [`sql_composer.h`][header], ao lado deste ficheiro: o cbindgen
escreve-o a partir do código do pacote, no `OUT_DIR` de cada compilação, e o
`cargo test -p sql-composer-ffi` falha, dizendo o que copiar, quando a cópia daqui for diferente
dele. Um programa que se ligue à biblioteca estática liga-se também às bibliotecas de sistema de
que a biblioteca padrão do Rust precisa, e é isto que as lista:

```sh
cargo rustc -p sql-composer-ffi --crate-type staticlib -- --print native-static-libs
```

## Como se chama

- Todas as cadeias de caracteres que entram e saem são UTF-8 e terminam em NUL.
- O que a biblioteca cria é um identificador opaco (`SqlComposerTemplate`, `SqlComposer`,
  `SqlComposerComposed`, `SqlComposerFlags`, `SqlComposerTree`), que se liberta com a função do
  mesmo nome terminada em `_free`.
- Cada chamada que possa falhar devolve um `SqlComposerStatus` e recebe `char **error` em último
  lugar. Em caso de falha, o `*error` traz o motivo, a própria mensagem do sql-composer, que quem
  chama liberta com `sql_composer_string_free`; qualquer outra cadeia ou vetor que a biblioteca dê
  é emprestado pelo identificador de onde vem.
- Nenhum pânico atravessa a fronteira, e um que aconteça é devolvido como
  `SQL_COMPOSER_STATUS_PANIC`.

## Compor um modelo

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

O `sql_composer_compose` compõe cada parâmetro com um só marcador, e o
`sql_composer_add_search_path`, o `sql_composer_set_name_relations`, o
`sql_composer_load_view_registry` e o `sql_composer_add_mock_table` preparam o compositor antes
disso.

## Compor uma pasta

O `sql_composer_compose_all` compõe cada ficheiro `.sqlc` de uma pasta com as opções que o
`cargo sqlc compose` aceita (`sql_composer_flags_new`, `_set_with`, `_set_views`,
`_add_search_path`, `_set_respell`). Cada modelo da árvore que devolve (`sql_composer_tree_entry`)
ou ficou escrito, com o caminho do seu ficheiro e as suas instruções compostas, ou é uma forma ou
foi saltado, sem ficheiro, e o `sql_composer_tree_manifest_json` é o texto do `manifest.json` que o
cargo sqlc escreve ao lado dos ficheiros. O [`tests/c/compose_tree.c`][compose_tree] escreve com
eles um destino inteiro.

O `sql_composer_flags_set_respell` escreve cada ficheiro composto noutra grafia, como o
`cargo sqlc compose --respell` o faz: uma união cujos braços leem todos um mesmo rol, cada braço
cortado às linhas que são suas e juntado à esquerda, `ON true`, ao que lê, passa a ser escrita como
uma só junção que lê o rol uma única vez, e todos os outros bytes ficam como foram compostos. Só o
faz para o PostgreSQL, e as opções de qualquer outro dialeto que o peçam são recusadas com a
mensagem do sql-composer.

## Licença

MIT, como diz o [LICENSE][LICENSE].

[header]: https://github.com/soulstompp/sql-composer/blob/HEAD/crates/sql-composer-ffi/sql_composer.h
[compose_tree]: https://github.com/soulstompp/sql-composer/blob/HEAD/crates/sql-composer-ffi/tests/c/compose_tree.c
[LICENSE]: https://github.com/soulstompp/sql-composer/blob/HEAD/crates/sql-composer-ffi/LICENSE
