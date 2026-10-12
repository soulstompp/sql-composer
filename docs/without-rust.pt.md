# Compor sem Rust

> **Português europeu, grafia do AO90.** Também disponível em inglês:
> [`without-rust.md`](without-rust.md).

O SQL de um projeto continua a ser SQL. Cada consulta é um modelo, um ficheiro de SQL simples que
os outros modelos compõem pelo seu caminho, e o `cargo sqlc compose` escreve cada instrução já
composta num ficheiro `.sql`, que o `psql` executa tal como está e que um programa em qualquer
linguagem executa com os nomes que o `manifest.json` dá aos seus marcadores. Do projeto, nada é
Rust: as ferramentas do Rust instalam um programa, um só, e é tudo o que o projeto lhes pede.

## Instalar

Instalam-se primeiro as ferramentas do Rust, com o [rustup](https://rustup.rs), e depois o
programa:

```sh
cargo install cargo-sqlc
```

O programa fica na pasta `bin` do Cargo (`~/.cargo/bin`, a não ser que se tenha escolhido outra), e
o Cargo executa-o como `cargo sqlc`. Compor lê ficheiros `.sqlc` e escreve ficheiros `.sql` sem se
ligar a base de dados nenhuma, e o `cargo uninstall cargo-sqlc` volta a tirar o programa.

## Entram os modelos

Uma árvore de modelos é uma pasta, `sqlc` por omissão, com ficheiros `.sqlc` a qualquer
profundidade, e o [`reports/release_years.sqlc`](../examples/brick/sqlc/reports/release_years.sqlc)
do exemplo brick conta as caixas lançadas em cada ano:

```sql
# Every year from the first release to the last, with the number of sets released in it: one row
# per year, keyed by year. generate_series writes the calendar and the sets are counted against it,
# so a year with no release is a year examined and found empty, with 0, and never a year left out.
SELECT y.year, count(s.set_num) AS sets
FROM (
    SELECT min(year) AS first_year, max(year) AS last_year
    FROM lego_sets
) span
CROSS JOIN generate_series(span.first_year, span.last_year) AS y(year)
LEFT JOIN lego_sets s ON s.year = y.year
GROUP BY y.year
ORDER BY y.year
```

As linhas `#` são comentários do próprio modelo, que nunca chegam à base de dados; a linguagem de
modelos, o [README](../README.pt.md) percorre-a uma diretiva de cada vez: uma relação posta dentro
de outra, um valor dado pelo nome, uma relação passada como argumento e relações combinadas como
conjuntos.

## Saem os ficheiros compostos

A partir da pasta onde está a `sqlc`:

```sh
cargo sqlc compose --source sqlc --target .sql --skip-prepare
```

O `--skip-prepare` deixa de fora o `cargo sqlx prepare`, que guarda os dados das consultas com que
o SQLx de um programa em Rust as confere. Sem ele, o comando corre esse passo assim que os ficheiros
estão escritos e sai com 1 se o passo falhar, como falha onde o SQLx não está instalado; os
ficheiros, esses, ficam escritos na mesma.

Cada modelo tem uma linha sua, e a última conta-os a todos; no exemplo brick, é esta:

```text
Composed 51 of 55 template(s) into .sql: 4 shape(s), 0 skipped
```

O ficheiro de cada modelo mantém o caminho dele, pelo que `sqlc/reports/release_years.sqlc` dá
`.sql/reports/release_years.sql`, o mesmo SQL já sem os comentários `#`; já uma forma, ou seja, um
modelo com uma vaga que fica para quem o compõe preencher, não tem ficheiro seu. Como o destino é
substituído por inteiro em cada composição, o `--target` deve apontar para uma pasta que não tenha
mais nada, e essa pasta fica no repositório, ao lado dos modelos.

## Executar um ficheiro com o psql

Um ficheiro sem marcadores executa-se tal como está:

```sh
psql -d sqlc_brick -f .sql/reports/release_years.sql
```

Um ficheiro com marcadores recebe os valores pela ordem em que o manifesto lista os nomes (mais
abaixo). O [`sets/select_colored_parts.sql`](../examples/brick/.sql/sets/select_colored_parts.sql),
que dá as entradas de uma caixa numa só cor, recebe o nome da cor como `$1` e a caixa como `$2`, e
o psql 16, ou mais recente, dá-lhos com o `\bind`, escrito depois da instrução:

```sh
{ cat .sql/sets/select_colored_parts.sql; echo '\bind Black 10182-1 \g'; } | psql -d sqlc_brick
```

Em qualquer versão, o `PREPARE` e o `EXECUTE` fazem o mesmo:

```sh
{ echo 'PREPARE colored AS'; cat .sql/sets/select_colored_parts.sql; echo ';'
  echo "EXECUTE colored('Black', '10182-1');"; } | psql -d sqlc_brick
```

## Os nomes, no manifesto

O `manifest.json`, na raiz do destino, regista como foram compostos os ficheiros e, a seguir, os
nomes de cada um. O do exemplo brick começa assim:

```json
{
  "dialect": "postgres",
  "with": false,
  "views": null,
  "search_path": [],
  "respell": false,
  "files": {
```

e, da lista por cor, diz isto:

```json
    "sets/select_colored_parts.sql": {
      "bind_params": [
        "color_name",
        "set_num"
      ],
      "statement_starts": [
        0
      ]
    },
```

Um programa em qualquer linguagem lê-o com o leitor de JSON que tiver, encontra a entrada de um
ficheiro pelo caminho dele dentro do destino e dá ao controlador da sua base de dados o valor de
cada nome pela ordem em que o `bind_params` os lista: aqui, o `$1` é o `color_name` e o `$2` o
`set_num`. Um nome que o modelo use duas vezes é um só marcador no PostgreSQL, no DuckDB e no
SQLite, e aparece uma vez na lista; no MySQL e no MariaDB, cada `?` é um nome à parte, e um nome
usado duas vezes aparece duas vezes. Num ficheiro com várias instruções, cada uma numera os seus
marcadores a partir do primeiro, e o `statement_starts` diz onde começam, no `bind_params`, os nomes
de cada uma; o programa envia então as instruções uma a uma, cada qual terminada onde a sua base de
dados a termina.

## Manter os ficheiros em dia

```sh
cargo sqlc compose --source sqlc --target .sql --verify
```

O `--verify` compõe em memória, com as opções que o manifesto regista, e compara com o destino: dá
cada ficheiro como `CHANGED`, `MISSING` ou `STALE`, não escreve nada e sai com 1 se houver algum.
Corrido na integração contínua, garante que os ficheiros guardados no repositório continuam a ser o
que os seus modelos compõem.

## Até onde vai

Compor nunca confere o SQL: um erro no SQL compõe-se sem queixa e só a base de dados o recusa, pelo
que cada ficheiro se executa uma vez, pelo `psql` ou por um teste. O manifesto guarda nomes, nunca
tipos nem valores, e os valores, esses, são do programa.

Para compor dentro do próprio programa, em vez de o fazer de antemão, o Python tem o módulo
`sql_composer`, distribuído como `sql-composer`, e o C tem o `sql-composer-ffi`; cada um compõe uma
árvore inteira como o `cargo sqlc compose` a compõe e devolve os mesmos ficheiros e o mesmo
`manifest.json`, e as chamadas, dão-nas as páginas do
[sql-composer-py](../crates/sql-composer-py/README.pt.md) e do
[sql-composer-ffi](../crates/sql-composer-ffi/README.pt.md).
