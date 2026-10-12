# Contribuir

> **Português europeu, grafia do AO90.** Também disponível em inglês:
> [`CONTRIBUTING.md`](CONTRIBUTING.md).

Sugestões, relatórios de erros e pedidos de integração (*pull requests*): todos são bem-vindos no
sql-composer.

## Comunicar um erro

Procura-se primeiro na [lista de questões][issue tracker], e, se nenhuma questão de lá, aberta ou
fechada, descrever o problema ou outro muito parecido, abre-se uma nova, com estes pormenores:

- Que versões do Rust e do sql-composer estão em uso?
- O que se pretende fazer?
- Qual é o erro, por inteiro, que aparece?
- Como se pode reproduzir?
  - Cita-se todo o código de que se precise para o reproduzir (de preferência num repositório
    público ou num [Gist]).
  - Junta-se toda a parte do esquema da base de dados que tenha a ver com o erro.

[issue tracker]: https://github.com/soulstompp/sql-composer/issues
[Gist]: https://gist.github.com

Obrigado.


## Pedir uma funcionalidade

Se nenhuma questão da [lista de questões][issue tracker], aberta ou fechada, descrever a ideia,
abre-se uma, e ajuda que a sua descrição responda a estas perguntas:

- O que se quer fazer, e de que maneira se espera que o sql-composer ajude a fazê-lo?
- Como se poderia acrescentar isto ao sql-composer?
- Que alternativas há?
- Há desvantagens?

Obrigado.


## Contribuir com código para o sql-composer

### Preparar o sql-composer na própria máquina

1. Instala-se o Rust com o [rustup], que permite mudar facilmente de uma versão do Rust para
   outra; o sql-composer compila com o Rust estável, e o `rust-version` de cada pacote diz qual é a
   versão mais antiga com que compila.

2. Instalam-se as bibliotecas de sistema de que se precisa para falar com as bases de dados que se
   queira usar (p. ex., `libpq-dev` para o PostgreSQL e `libmysqlclient-dev` para o MySQL).

3. Clona-se este repositório, que se abre depois no editor preferido.

4. O projeto é um espaço de trabalho do Cargo com os pacotes que estão em `crates/` e os exemplos
   que estão em `examples/`:

   ```
   crates/
     sql-composer/           # a biblioteca central
     sql-composer-rusqlite/  # controlador rusqlite
     sql-composer-duckdb/    # controlador DuckDB
     sql-composer-postgres/  # controlador PostgreSQL (síncrono e assíncrono)
     sql-composer-mysql/     # controlador MySQL (síncrono e assíncrono)
     sql-composer-sqlx/      # integração com o sqlx
     sql-composer-ffi/       # a ABI de C (sql_composer.h)
     sql-composer-py/        # o módulo Python (sql_composer)
     cargo-sqlc/             # a ferramenta de linha de comandos
   examples/
     brick/                  # um exemplo que corre na base de dados do sqlc-brickgen (brick-example)
     sqlc-brickgen/          # o gerador de um catálogo grande para o PostgreSQL (sqlc-brickgen)
   ```

5. **Bases de dados para os testes de integração** (opcional)

   Os testes que precisam de uma base de dados a funcionar têm-na com o docker-compose, que arranca
   contentores do PostgreSQL e do MySQL:

   ```bash
   docker-compose up             # arranca os dois
   docker-compose up postgres    # arranca só o PostgreSQL
   docker-compose up mysql       # arranca só o MySQL
   ```

   As portas e as palavras-passe mudam-se com variáveis de ambiente ou no ficheiro `.env`, e os
   dados das bases de dados ficam guardados em `./db/`, pelo que, para as começar do zero, basta
   apagá-los:

   ```bash
   sudo rm -rf db/*
   ```

6. Correm-se os testes:

   ```bash
   # Os testes todos (núcleo, rusqlite, duckdb, unitários dos controladores, exemplos)
   cargo test --workspace

   # Os testes de um só pacote
   cargo test -p sql-composer
   cargo test -p sql-composer-rusqlite
   cargo test -p sql-composer-duckdb
   cargo test -p sql-composer-postgres
   cargo test -p sql-composer-mysql
   cargo test -p sql-composer-sqlx --features validate
   cargo test -p sql-composer-ffi
   cargo test -p sql-composer-py   # incorpora o Python, pelo que se liga à libpython
   cargo test -p cargo-sqlc

   # Os testes do gerador do catálogo, que não precisam de base de dados
   cargo test -p sqlc-brickgen

   # Confere que os .sql do exemplo brick no repositório são o que os seus modelos compõem
   cargo run -p cargo-sqlc -- sqlc compose --source examples/brick/sqlc --target examples/brick/.sql --verify
   ```

   Depois de se mudar um modelo em `examples/brick/sqlc`, volta-se a compor o exemplo (o mesmo
   comando, sem `--verify` e com `--skip-prepare`), e os ficheiros `.sql` vão no mesmo commit que
   o modelo.

[rustup]: https://rustup.rs/

### Acrescentar uma identidade

Uma identidade escreve um modelo de outra maneira que devolve as mesmas linhas, decidida só pelo
texto do modelo e por nada que um catálogo guarde. Cada uma é uma linha da tabela de baixo, com o
eixo que faz variar, as suas duas grafias, onde vale, a sua armadilha (a quase identidade, uma
grafia parecida que devolve outras linhas, e as linhas que as distinguem) e a sua lei, o teste que
corre ambas sobre essas linhas. As cinco primeiras são os eixos do `cargo sqlc spellings` e do
`cargo sqlc spell`, em `crates/sql-composer/src/spellings/`, e a última é a nova grafia que o
`cargo sqlc compose --respell` escreve no ficheiro composto, em
`crates/sql-composer/src/respell.rs`.

| eixo, e a sua chave | uma grafia, e a outra | vale onde | a sua armadilha | a sua lei |
|---|---|---|---|---|
| onde fica uma condição, `conditions` | no `ON` de uma junção interna, ou no `WHERE` | a junção, e todas as que vêm depois dela no mesmo item do `FROM`, é interna; cada coluna é nomeada com a sua relação; a condição não contém nenhuma consulta | a condição passada para o `ON` de um `LEFT JOIN`: as linhas exteriores voltam | `conditions.rs`: `law_a_condition_in_an_inner_join_s_on_or_in_the_where_returns_the_same_rows` |
| a negação, `negation` | `NOT` sobre um `AND` ou um `OR`, ou levado a cada operando; `NOT NOT p` como `p` | sempre | `NOT (p AND q)` como `NOT p AND NOT q`; `NOT IN` como `NOT EXISTS`, sobre uma subconsulta que tenha um NULL | `negation.rs`: `law_each_spelling_of_a_not_returns_the_same_rows_on_every_pair_of_truths` |
| a ordem das junções, `join_order` | as relações de uma lista de junções noutra ordem, cada condição no primeiro `ON` em que todas as relações que nomeia já estão juntas | todas as junções internas ou `CROSS`, nenhuma natural; cada coluna nomeada com a sua relação; nenhum `*` sozinho; 8 relações, no máximo; uma coluna de `USING` que só uma das relações juntas antes dela tenha, o que se lê nas listas dos seus `SELECT` | `SELECT *`: as colunas voltam noutra ordem; uma coluna de `USING` não qualificada, que lê a relação junta em primeiro lugar: o `1.0` contra o `1.00` do numeric | `join_order.rs`: `law_every_order_of_inner_joins_returns_the_same_rows` |
| como se nomeia uma relação lida uma só vez, `naming` | no próprio sítio, como consulta `WITH` à cabeça da instrução, ou como modelo à parte | é lida uma vez, não é recursiva, não é `MATERIALIZED` e não escreve nada | uma relação lida duas vezes que chame `random()`: como consulta `WITH`, é calculada uma só vez | `naming.rs`: `law_a_relation_read_once_returns_the_same_rows_under_each_name` |
| a forma de uma semijunção, `semi_join` | `x IN (SELECT y …)`, ou `EXISTS (SELECT … WHERE x = y)` | um dos termos do `AND` de um `WHERE`, sob nenhum `NOT`, sobre um `SELECT` simples | o mesmo sob um `NOT`, com um NULL na subconsulta | `semi_join.rs`: `law_a_semi_join_in_either_form_returns_the_same_rows_under_no_not` |
| uma união dos braços de um rol, `--respell` | cada braço a ler o rol, ou o rol lido uma vez e as linhas de cada braço marcadas com o seu número | todos os braços leem o mesmo rol, juntam-no `ON true`, cortam-no com um `WHERE` que só lê o rol e devolvem colunas do rol iguais em todos os braços ou colunas simples das suas próprias linhas | as linhas dos braços juntas por junção interna, e não à esquerda: um braço que não encontre nada perde a sua linha do rol | `respell.rs`: `law_a_union_of_roster_arms_respelled_returns_the_same_rows` |

Para acrescentar um eixo:

1. **O seu módulo**, `crates/sql-composer/src/spellings/<axis>.rs`, com uma constante por cada
   escolha e o valor dela no `spellings.toml`, e com o `places`, que encontra cada sítio a que o
   eixo se aplica num modelo lido para as suas grafias (`read::Read`). Cada sítio lista as suas
   alternativas: primeiro o sítio tal como está escrito (`Choice::written`) e depois cada grafia,
   como edições do próprio texto do modelo, com a instrução que a sua leitura tem de dar. Uma
   alternativa cuja leitura não dê essa instrução nunca é oferecida (`place::checked`), e cada byte
   fora das suas edições fica tal como estava escrito; onde o texto não diga se uma condição se
   cumpre, o sítio fica tal como está.
2. **O seu eixo:** uma variante de `Axis` em `spellings/mod.rs`, com a sua chave, as suas
   escolhas, as suas palavras e o seu `places`, e o seu lugar em `Axis::ALL`, a ordem por que o
   `spell` escreve os eixos; e um campo de `Picks` em `spellings/picks.rs`, com o seu braço em
   `Picks::pick`. É daí que o `cargo sqlc spellings`, o `--axis` e o `spell` o leem.
3. **Os seus testes**, no módulo: cada grafia escrita, e escrita de volta pela outra escolha; o que
   fica tal como está escrito quando cada condição falha; e os comentários e as diretivas mantidos
   onde estão.
4. **A sua lei**, um teste chamado `law_…` e marcado com
   `#[ignore = "needs a PostgreSQL server: set SQLC_TEST_POSTGRES_URL"]`, que constrói uma `Trap`,
   tabelas simuladas com as linhas em que a quase identidade falha, e chama
   `laws::holds(written, spelled, near, &trap)`: a grafia devolve as linhas que o modelo, tal como
   está escrito, devolve, cada uma o mesmo número de vezes, e pelo menos algumas, em tabelas com
   essas linhas e de novo nas mesmas linhas postas em tabelas simuladas, e a quase identidade
   devolve outras linhas. Antes de mais, vê-se a lei falhar, com a quase identidade no lugar da
   grafia.
5. **Os modelos do exemplo brick.** Os testes sobre o exemplo brick continuam a passar: os sítios
   de cada eixo contados em `shared/scope_sets.sqlc`, com o novo eixo entre eles
   (`each_place_is_counted_and_its_count_is_the_spellings_written`); cada modelo lido e escrito de
   volta byte a byte sem nada escolhido
   (`every_brick_template_reads_and_is_written_back_byte_for_byte`); cada modelo escrito em cada
   escolha a ler-se, a compor-se e a ficar escrito
   (`every_template_written_in_every_pick_reads_composes_and_stays_written`, com a nova escolha
   dada); e cada ficheiro composto a manter as suas consultas `WITH`
   (`the_brick_templates_written_in_every_pick_keep_their_with_queries`). O
   `spellings_counts_each_place_before_it_writes_each_spelling` do cargo-sqlc escreve a linha de
   contagem de cada eixo, a do novo entre elas.
6. **O plano.** O `cargo run -p brick-example -- fit`, na base de dados que
   [o exemplo brick](examples/brick/README.pt.md) carrega, confere, só pelo `EXPLAIN (COSTS OFF)`,
   que cada grafia que o `spell` sabe escrever deixa cada ficheiro composto que guarda o seu modelo
   com o plano que tem tal como está escrito, sem e com `--with`. Um eixo cuja grafia mude um plano
   é estreitado até nenhuma o mudar.
7. **As páginas.** A sua linha na tabela do `spellings.toml` do README, e a sua linha no CHANGELOG.

```bash
cargo test -p sql-composer --all-features spellings::
cargo test -p cargo-sqlc spell
SQLC_TEST_POSTGRES_URL=postgres://localhost/postgres \
  cargo test -p sql-composer --all-features -- --ignored law_
cargo run -p brick-example -- fit
```

Uma nova grafia escrita no ficheiro composto, como a da última linha da tabela, vai para o
`respell.rs`, por trás da funcionalidade `respell`. Os seus testes quebram cada uma das suas
condições à vez e veem a instrução escrita tal como foi composta, e veem um ficheiro sem nada para
escrever noutra grafia escrito byte a byte; a sua lei, pelo `laws::holds`, obriga a instrução
escrita noutra grafia a devolver as linhas que a instrução composta devolve.

### O estilo do código

O código segue o
[Rust Style Guide](https://github.com/rust-dev-tools/fmt-rfcs/blob/master/guide/guide.md),
que o [rustfmt](https://github.com/rust-lang/rustfmt) faz cumprir, e as verificações correm-se na
própria máquina assim:

1. Instalam-se o rustfmt e o clippy:
   ```
   rustup component add rustfmt
   rustup component add clippy
   ```

2. Corre-se o clippy:
   ```
   cargo clippy --workspace --all-targets -- -D warnings
   ```
   Cada pedido de integração tem de compilar sem avisos.

3. Corre-se o rustfmt:

   Para verificar a formatação:
   ```
   cargo fmt --all -- --check
   ```

   Para aplicar a formatação:
   ```
   cargo fmt --all
   ```

O rustfmt também pode corrigir o código, ou assinalar o que estiver mal, no próprio editor, e o
[README dele](https://github.com/rust-lang/rustfmt) dá os pormenores.
