# sql-composer

> **Português europeu, grafia do AO90.** Também disponível em inglês: [`README.md`][README.md].

Uma junção copiada para uma segunda consulta já são duas junções, que nascem iguais e acabam por
divergir.

O sql-composer fica com uma só: cada consulta escreve-se uma vez, em SQL simples, e é composta em
todas as instruções que precisem dela, para PostgreSQL, MySQL, MariaDB, SQLite ou DuckDB. Uma
alteração faz-se, assim, num só ficheiro, e a composição seguinte leva-a a todas as instruções que a
contêm. O que chega à base de dados é um ficheiro de SQL simples, e esse ficheiro, o DBA lê-o, o
`EXPLAIN` executa-o, o `query_file!()` do SQLx confere-o quando o código compila e um programa em
qualquer linguagem corre-o pelos nomes que o manifesto dá a cada um dos seus marcadores.

Cada consulta vive num ficheiro seu, um modelo, e os outros modelos compõem-na pelo caminho desse
ficheiro; o `cargo sqlc compose` escreve depois cada instrução composta sem diretiva nenhuma lá
dentro, e nada fica por montar em tempo de execução.

A composição funciona porque toda a consulta devolve uma relação, isto é, linhas que partilham as
mesmas colunas, e uma relação pode estar em qualquer sítio onde estaria uma tabela, de modo que uma
consulta composta dentro de outra é lida aí como uma tabela. A isto chama-se fecho, e é no fecho
que assenta toda a linguagem de modelos: uma relação posta dentro de outra, uma relação passada como
argumento, relações combinadas como conjuntos e cada relação nomeada uma só vez.

A instrução composta, quem a planeia continua a ser a base de dados; o sql-composer limita-se a
escrevê-la. No PostgreSQL, a extensão do [warren-surveyor][warren-surveyor] age enquanto ele
planeia: o speller dela escreve a instrução noutra grafia onde o catálogo diz que as linhas
continuam as mesmas, e o surveyor lê nos índices do DBA as linhas e as estatísticas que, de
outro modo, o planeador do PostgreSQL estimaria. Essa mesma instrução, o
[warren-snail][warren-snail] também a lê, na sintaxe do PostgreSQL, numa base de dados que guarda
num só ficheiro e que percorre uma coluna de cada vez.

## Instalar

```sh
cargo install cargo-sqlc
```

Isto instala um único programa, o `cargo-sqlc`, na pasta `bin` do Cargo (`~/.cargo/bin`, a não ser
que se tenha escolhido outra), e o Cargo passa a executá-lo como `cargo sqlc`. Compor lê ficheiros
`.sqlc` e escreve ficheiros `.sql` sem se ligar a base de dados nenhuma.

Terminada a composição, o comando executa o `cargo sqlx prepare`, que guarda os dados das consultas
com que o SQLx as confere sem ligação. É um comando do próprio SQLx, que se instala à parte com
`cargo install sqlx-cli`, e esse, sim, liga-se à base de dados indicada em `DATABASE_URL`. O
`--skip-prepare` deixa-o de fora, e o `cargo uninstall cargo-sqlc` volta a tirar o programa.

Para compor a partir de Rust, basta juntar a biblioteca:

```toml
[dependencies]
sql-composer = "0.1.0"
```

Em C ou em Python, há pacotes que compõem dentro do programa que os usa (*C e Python*, mais
abaixo); e um projeto sem Rust nenhum compõe só com o `cargo sqlc`, como mostra
[Compor sem Rust][docs/without-rust.md], que faz os mesmos passos com o `psql` e o manifesto.

## Uma primeira composição

Os modelos desta página leem as tabelas do [exemplo brick][examples/brick/], um catálogo de caixas
de construção com as suas peças, cores e temas e com os construtores que as compram, tudo feito pelo
[sqlc-brickgen][examples/sqlc-brickgen/].

As entradas de uma caixa juntam cinco tabelas: cada entrada dos inventários da caixa, com a sua
peça, a categoria dessa peça e a sua cor. Escrita uma vez, essa junção fica num ficheiro seu,
[`shared/set_part_details.sqlc`][examples/brick/sqlc/shared/set_part_details.sqlc]:

```sql
# What parts are in a set: every line of every inventory the set has, one row per line, keyed by the
# line (inventory_id, part_num, color_id, is_spare). A set sold in more than one version has one
# inventory per version, and each line carries its version. Nothing here adds two versions
# together: a caller that totals a set totals it per version.
#
# A line is never dropped for what the catalogue lacks. A line whose part is not in the parts
# catalogue keeps its row, with part_status 'uncatalogued' and no part name or category.
SELECT
    ip.inventory_id,
    i.version,
    ip.part_num,
    ip.color_id,
    ip.is_spare,
    ip.quantity,
    CASE WHEN p.part_num IS NULL THEN 'uncatalogued' ELSE 'catalogued' END AS part_status,
    p.name AS part_name,
    pc.name AS category_name,
    c.name AS color_name,
    c.rgb AS color_rgb,
    c.is_trans
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
LEFT JOIN lego_parts p ON p.part_num = ip.part_num
LEFT JOIN lego_part_categories pc ON pc.id = p.part_cat_id
LEFT JOIN lego_colors c ON c.id = ip.color_id
WHERE i.set_num = :bind(set_num)
```

É uma consulta completa, que corre sozinha desde que receba um `set_num`, e o seu comentário diz o
que são as linhas que devolve e qual é a chave delas. O que vem depois de um `#` é comentário do
próprio modelo e nunca chega à base de dados.

A lista das peças de uma caixa compõe essa consulta, em
[`sets/select_set_parts.sqlc`][examples/brick/sqlc/sets/select_set_parts.sqlc]:

```sql
# A set's parts, one row per line, version by version, each line with its part, category and
# colour. The ORDER BY ends on the line key, so the order is total and a truncated listing is always
# the same rows. A line with no category, part name or colour name comes after the named ones; the
# ORDER BY says so, because databases disagree on where a NULL sorts.
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
)
SELECT
    inventory_id,
    version,
    part_num,
    color_id,
    is_spare,
    part_status,
    part_name,
    category_name,
    color_name,
    color_rgb,
    quantity
FROM set_part_details
ORDER BY
    version,
    inventory_id,
    category_name IS NULL, category_name,
    part_name IS NULL, part_name,
    color_name IS NULL, color_name,
    part_num,
    color_id,
    is_spare
```

Na pasta do exemplo, o `cargo sqlc compose --source sqlc --target .sql --skip-prepare` escreve
[`.sql/sets/select_set_parts.sql`][examples/brick/.sql/sets/select_set_parts.sql], onde a junção
ficou no lugar do `:compose` e um marcador numerado no do `:bind`:

```sql
WITH set_part_details AS (
    SELECT
    ip.inventory_id,
    i.version,
    ip.part_num,
    ip.color_id,
    ip.is_spare,
    ip.quantity,
    CASE WHEN p.part_num IS NULL THEN 'uncatalogued' ELSE 'catalogued' END AS part_status,
    p.name AS part_name,
    pc.name AS category_name,
    c.name AS color_name,
    c.rgb AS color_rgb,
    c.is_trans
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
LEFT JOIN lego_parts p ON p.part_num = ip.part_num
LEFT JOIN lego_part_categories pc ON pc.id = p.part_cat_id
LEFT JOIN lego_colors c ON c.id = ip.color_id
WHERE i.set_num = $1

)
SELECT
    inventory_id,
    version,
    part_num,
    color_id,
    is_spare,
    part_status,
    part_name,
    category_name,
    color_name,
    color_rgb,
    quantity
FROM set_part_details
ORDER BY
    version,
    inventory_id,
    category_name IS NULL, category_name,
    part_name IS NULL, part_name,
    color_name IS NULL, color_name,
    part_num,
    color_id,
    is_spare
```

A lista filtrada por cor, mais abaixo, compõe o mesmo ficheiro, e o mesmo fazem os totais por
categoria que um `INSERT` e um `MERGE` escrevem e as contagens de peças sobresselentes que um
`UPDATE` mantém em dia. Uma alteração à junção faz-se, por isso, num só sítio, e a composição
seguinte leva-a a todos eles.

O compositor nunca analisa o SQL que insere, a não ser sob o `--respell`, e um erro no SQL compõe-se
sem queixa: só a base de dados o recusa. É por isso que os ficheiros compostos ficam no repositório
ao lado dos modelos, com o `manifest.json` que a composição escreve junto deles e que guarda as
opções com que foram compostos e, de cada ficheiro, os nomes a que correspondem os seus marcadores.
O `cargo sqlc compose --verify` confirma que os ficheiros continuam a ser o que os modelos compõem,
com as opções que o manifesto regista, e o SQLx, ou um teste, executa-os.

## A linguagem de modelos

As diretivas são os movimentos da álgebra relacional, escritos em SQL. Para cada uma, a secção que
lhe cabe diz o que um modelo afirma com ela, a cópia que poupa, como aparece na instrução composta e
até onde vai.

### `:compose`: uma relação dentro de outra

O `:compose(path)` põe a relação que está em `path` onde a diretiva estiver: num `FROM`, num `JOIN`,
num `WITH`, num `EXISTS`, num `INSERT … SELECT` ou num `UPDATE … FROM`. O caminho lê-se a partir de
`--source`, ou dos caminhos de pesquisa que se tiverem dado a um `Composer`.

Poupa a consulta colada à mão, que é a cópia que acaba por divergir. Na instrução composta, o corpo
da relação fica onde estava a diretiva, já sem os comentários `#`; convém dar um nome a cada relação
inserida, como faz o exemplo, para que duas relações compostas com colunas do mesmo nome não se
confundam.

Um modelo que se componha a si próprio, por qualquer cadeia de ficheiros, é recusado como
referência circular, e o mesmo acontece a um caminho que não leve a ficheiro nenhum.

### `:bind`: os valores que a instrução recebe

O `:bind(name)` é um valor que a instrução recebe quando é executada, e um nome pode receber também
uma lista de valores, para um `IN`.

Poupa o `$3` numerado à mão, que tem de mudar sempre que se acrescenta um filtro: aqui, um valor
encontra o seu marcador pelo nome, e nunca pela posição.

Na instrução composta, o compositor escreve o marcador próprio do dialeto:

```sql
SELECT set_num, name, year FROM lego_sets WHERE year = :bind(year) AND theme_id = :bind(theme_id)
-- PostgreSQL: ... WHERE year = $2 AND theme_id = $1
-- DuckDB:     ... WHERE year = $2 AND theme_id = $1
-- SQLite:     ... WHERE year = ?2 AND theme_id = ?1
-- MySQL:      ... WHERE year = ? AND theme_id = ?
-- MariaDB:    ... WHERE year = ? AND theme_id = ?
```

No PostgreSQL, no DuckDB e no SQLite, cada nome é numerado pelo lugar que ocupa, por ordem
alfabética, entre os nomes da instrução, e um nome usado duas vezes fica um só marcador; no MySQL e
no MariaDB, cada uso é um `?` à parte, pela ordem em que a instrução os usa. O `bind_params` da
instrução composta lista os nomes pela ordem dos marcadores, que é também a ordem em que o
`query_file!()` recebe os argumentos. Num modelo com várias instruções, cada uma numera os seus
marcadores a partir do primeiro, e o `statement_starts` diz onde começam, no `bind_params`, os
nomes de cada uma. O `cargo sqlc compose` escreve as duas listas de cada ficheiro composto no
`manifest.json` do destino (*Referência*).

Um nome que recebe uma lista de valores dá um marcador por valor. Composta com três valores em
`color_ids` (`compose_with_values`), a consulta

```sql
SELECT name, rgb FROM lego_colors WHERE id IN (:bind(color_ids)) AND is_trans = :bind(is_trans)
```

fica `... WHERE id IN ($1, $2, $3) AND is_trans = $4`, e o `bind_params` passa a ser
`["color_ids", "color_ids", "color_ids", "is_trans"]`.

O `:bind(name EXPECTING 1..20)` diz quantos valores um parâmetro aceita, e o `EXPECTING 2`, que
aceita pelo menos dois; uma contagem fora disso, o `compose_with_values` recusa-a (`ValueCount`).
Já o `:bind(name NULL)` diz que o parâmetro aceita `NULL`, e os controladores do SQLite, do DuckDB
e do MySQL recusam um NULL dado a um parâmetro que não o diga (`NullValue`); nos do PostgreSQL, os
valores só mostram que são NULL depois de enviados, pelo que aí o `NULL` fica apenas registado no
`Binding` analisado (`nullable`).

### Vagas: uma relação passada como argumento

Um modelo pode deixar uma relação em aberto, numa vaga, e cada modelo que o componha preenche-a à
sua maneira. O
[`shared/filtered_set_parts.sqlc`][examples/brick/sqlc/shared/filtered_set_parts.sqlc] dá as
entradas de uma caixa que passam no filtro dado em `@filter`, seja ele qual for:

```sql
# A set's lines that match a filter. The caller decides which filter to use (the @filter slot).
#
# A filter returns patterns with two columns, part_num and color_id, in which NULL means "any
# value". A line matches when some pattern agrees with it on every column the pattern names, and it
# comes back once however many patterns it matches.
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
),
filter AS (
    :compose(@filter)
)
SELECT p.*
FROM set_part_details p
WHERE EXISTS (
    SELECT 1
    FROM filter f
    WHERE (f.part_num IS NULL OR f.part_num = p.part_num)
      AND (f.color_id IS NULL OR f.color_id = p.color_id)
)
```

Um filtro é uma consulta à parte,
[`filters/by_color.sqlc`][examples/brick/sqlc/filters/by_color.sqlc]:

```sql
# A colour, as a filter pattern: (any part, this colour). The colour is chosen by its name; a
# filter's contract is in shared/filtered_set_parts.sqlc.
SELECT DISTINCT
    NULL::varchar AS part_num,
    c.id AS color_id
FROM lego_colors c
WHERE c.name = :bind(color_name)
```

e o [`sets/select_colored_parts.sqlc`][examples/brick/sqlc/sets/select_colored_parts.sqlc] preenche
a vaga com ele:

```sql
# A set's lines in the colour color_name, one row per line, keyed by the line: the set's lines with
# the colour's filter in the @filter slot.
:compose(shared/filtered_set_parts.sqlc, @filter = filters/by_color.sqlc)
```

Uma vaga, só a preenche o `:compose` que a nomeia. Um modelo composto dentro de outro não herda
nenhuma das vagas de quem o compõe, pelo que cada nível passa adiante aquilo de que precisa; e o `@`
só quer dizer vaga dentro de `:compose(…)`, já que fora daí `@name` é SQL e passa tal como está.

Poupa a consulta de base escrita uma vez por filtro: a mesma vaga recebe uma categoria
(`filters/by_category.sqlc`), as palavras de uma pesquisa (`filters/by_words.sqlc`) e uma expressão
regular (`filters/by_pattern.sqlc`), e a base continua a ser um único ficheiro.

Na instrução composta, o corpo do filtro fica onde estava o `:compose(@filter)`.

Um modelo com uma vaga por preencher é uma forma, e não uma consulta: a biblioteca recusa-se a
compô-lo sozinho (`MissingSlot`, que nomeia a vaga e o modelo onde ela está), e o
`cargo sqlc compose` não escreve ficheiro nenhum para ele, mas di-lo, com as vagas que a forma deixa
abertas: `Shape sqlc/shared/filtered_set_parts.sqlc: @filter open`.

Também fica sem ficheiro um modelo que componha outro e lhe deixe uma vaga por preencher, e esse já
nada o pode compor, uma vez que uma vaga só a preenche o `:compose` que a nomeia. O
`cargo sqlc compose` dá-o como saltado, na saída de erro, com a vaga e o modelo onde ela está:
`Skipped sqlc/sets/select_colored_parts.sqlc: @filter of shared/filtered_set_parts.sqlc left open`.
O `--verify` dá-o da mesma maneira, ao lado do `STALE` que o ficheiro dele recebe se já estiver no
repositório, e nada disto muda aquilo com que o comando sai.

### União, interseção e diferença

O `:union`, o `:intersect` e o `:except` combinam consultas inteiras, cada uma num ficheiro seu. O
[`reports/city_only_moulds.sqlc`][examples/brick/sqlc/reports/city_only_moulds.sqlc] dá os moldes
que as caixas City usam e que nenhuma caixa Technic usa:

```sql
# The moulds City sets use and no Technic set does: one row per part_num. The order of the sources
# matters: the first is the one the second is taken from.
:except(queries/city_moulds.sqlc, queries/technic_moulds.sqlc)
```

Numa diferença, a ordem conta: `:except(a, b, c)` é `a` menos tudo o que esteja em `b` ou em `c`.
Escritos sem mais nada, o `:union`, o `:intersect` e o `:except` tiram as linhas repetidas, e o
`ALL` mantém-nas (`:except(ALL a, b)`), ao passo que o `DISTINCT` só diz em voz alta o que já se
fazia.

Poupam o corpo de uma consulta colado ao lado do de outra.

Na instrução composta, cada fonte vai embrulhada numa tabela derivada sua,
`SELECT * FROM (…) AS _except_1`, e o todo também, `(…) AS _except`. É que, em SQL, o `INTERSECT`
liga com mais força do que o `UNION` e o `EXCEPT`, e estes dois, entre si, agrupam-se da esquerda
para a direita, de modo que uma fonte sem embrulho se reagruparia com o que tem à volta
(`a EXCEPT b UNION c` quer dizer `(a EXCEPT b) UNION c`) e daria uma tabela plausível, sem erro
nenhum. O embrulho mantém cada fonte inteira, seja ela outra operação de conjuntos ou uma consulta
que acabe em `ORDER BY` e `LIMIT`, pelo que qualquer uma das três pode servir de fonte a outra.

Uma operação de conjuntos compara linhas inteiras, e por isso uma lista de colunas é recusada em vez
de ignorada; o SQLite, esse, só aceita um modificador, o `ALL`, e só depois de `UNION`.

### `:count`: uma relação reduzida a um número

O `:count(source)` conta as linhas de uma consulta, e o `:count(DISTINCT part_num OF source)` os
valores distintos das colunas que nomeia; com várias fontes, conta-se a união delas. Por exemplo,
[`reports/count_theme_parts.sqlc`][examples/brick/sqlc/reports/count_theme_parts.sqlc]:

```sql
# How many distinct moulds (part numbers) the sets of a theme scope use, the theme and every theme
# below it: one row, one count.
:count(DISTINCT part_num OF queries/theme_set_parts.sqlc)
```

Poupa a fonte colada à mão dentro de um `SELECT COUNT(…)`.

Na instrução composta, fica `SELECT COUNT(DISTINCT part_num) FROM (…) AS _count_sub`.

Com várias fontes, o `ALL` da contagem passa também para a união delas, como `UNION ALL`; não o
havendo, a união é um `UNION` escrito sem mais nada, que tira as linhas repetidas e que o SQLite
também aceita.

### Cada relação nomeada uma só vez

O `cargo sqlc compose --with` escreve uma só vez cada relação que uma instrução lê, já com nome, à
cabeça da instrução.

Poupa as cópias, porque uma relação que dois modelos compõem fica escrita duas vezes em qualquer
instrução que componha ambos: a instrução composta leva uma cópia da relação por cada caminho que lá
vai dar, e a base de dados planeia cada uma delas. O
[`reports/shared_moulds.sqlc`][examples/brick/sqlc/reports/shared_moulds.sqlc] lê o fecho dos temas
pelas suas duas fontes, e a sua instrução composta tem por isso esse fecho duas vezes; o
`cargo sqlc tree` desenha cada um dos caminhos (*Referência*).

Com o `--with`, cada instrução composta abre com um único `WITH`, que nomeia cada relação que a
instrução lê, cada uma depois das relações que ela própria lê, e a instrução vem no fim. Uma relação
toma o nome do seu caminho dentro de `--source`, com as pastas unidas por `_`:
`shared/theme_closure.sqlc` passa a chamar-se `shared_theme_closure`, e todos os sítios que a
compõem leem `SELECT * FROM shared_theme_closure`. A instrução dos moldes que a Technic e a City
partilham passa então a ter o fecho uma só vez, e na base de dados não se cria nada.

- Uma instrução que já abra com o seu próprio `WITH` ou `WITH RECURSIVE` recebe os nomes à cabeça
  dessa cláusula, e um `CREATE … AS` recebe-os à cabeça da sua consulta; qualquer outra instrução
  recebe-os à sua frente, e o MySQL não aceita `WITH` antes de um `INSERT`.
- Um modelo com várias instruções leva um `WITH` por instrução, cada um a nomear só o que essa
  instrução lê; uma instrução acaba em cada `;` que não esteja entre aspas, num comentário, entre
  aspas de dólar ou num metacomando do psql, tal como a base de dados do dialeto os lê.
- Um modelo que receba um parâmetro, ele próprio ou através do que compõe, ou que seja composto com
  as vagas preenchidas, fica escrito onde é composto, tal como sem `--with`, e o que ele lê continua
  a ser nomeado.
- Um nome que o dialeto reserve é recusado (o `key` no MySQL, o `left` no PostgreSQL), e o mesmo
  acontece a dois ficheiros que ficariam com o mesmo nome.

Uma instrução pode também escolher os seus nomes. O `:define(path)` escreve o corpo onde o
`:compose` poria o nome, e o nome que está à frente do seu `AS` passa a valer por esse modelo em
tudo o que fica abaixo, a qualquer profundidade:

```sql
WITH theme_closure AS (
    :define(shared/theme_closure.sqlc)
)
:compose(reports/count_theme_parts.sqlc)
```

A fonte da contagem também compõe `shared/theme_closure.sqlc` e, aí, lê-o como
`SELECT * FROM theme_closure`.

Um modelo feito de instruções `CREATE [OR REPLACE] VIEW <name> AS :define(<path>)` é um registo de
vistas. Carregado com `cargo sqlc compose --views <registry>` ou com
`Composer::load_view_registry()`, faz com que todas as composições digam `SELECT * FROM <name>` por
cada modelo que regista; e, composto como modelo, dá as instruções `CREATE VIEW` que há que
executar. Um modelo registado que receba um parâmetro ou deixe uma vaga em aberto é recusado, uma
vez que uma vista não recebe argumentos, e quando uma instrução define um nome para um modelo que
também é vista, vale o nome da instrução.

O `--with` ocupa o lugar de ambos, pelo que não se combina com `--views`, e um `:define` que apareça
sob `--with` é recusado.

### Uma união de braços que leem um mesmo rol: `--respell`

O `cargo sqlc compose --respell` escreve noutra grafia, no ficheiro composto, um certo tipo de
união: aquela cujos braços leem todos um mesmo rol, cortado em cada braço à linha que lhe pertence
e juntado à esquerda, `ON true`, ao que esse braço lê. As verificações do exemplo brick fazem uma
união assim, com o seu rol em [`checks/roster.sqlc`][examples/brick/sqlc/checks/roster.sqlc] e
cada verificação como braço de [`checks/all.sqlc`][examples/brick/sqlc/checks/all.sqlc]. Uma
delas é
[`checks/sets_without_lines.sqlc`][examples/brick/sqlc/checks/sets_without_lines.sqlc]:

```sql
# The sets with no inventory line, under the roster's row for this check: one row per set, keyed
# by set_num, with its name. A set whose inventories hold no line is one, and so is a set with no
# inventory. Where every set has a line, the roster's row comes back alone.
SELECT r.rule, p.set_num, p.detail
FROM (
    :compose(checks/roster.sqlc)
) r
LEFT JOIN (
    SELECT s.set_num, s.name AS detail
    FROM lego_sets s
    WHERE NOT EXISTS (
        SELECT 1
        FROM lego_inventories i
        WHERE i.set_num = s.set_num
          AND EXISTS (SELECT 1 FROM lego_inventory_parts ip WHERE ip.inventory_id = i.id)
    )
) p ON true
WHERE r.slug = 'sets_without_lines'
```

```sql
# Every check's findings, one row each, and a row with no set_num for a check that finds nothing.
:union(ALL checks/lines_uncatalogued.sqlc, checks/sets_without_lines.sqlc)
```

Poupa o rol lido outra vez por cada braço. Na união tal como foi composta, cada braço tem o corpo
do rol e lê-o; na nova grafia, escrita no ficheiro composto, o rol lê-se uma só vez, cada linha
dele emparelhada com os braços cujo corte passa, e as linhas próprias de todos os braços leem-se
numa só união, cada uma marcada com o número do seu braço. Dentro do `SELECT * FROM (` exterior, o
ficheiro composto fica assim:

```sql
SELECT r.rule, p.set_num, p.detail
FROM (SELECT * FROM (VALUES
    ('lines_uncatalogued', 'a line whose part is not in the parts catalogue'),
    ('sets_without_lines', 'a set with no inventory line')
) AS roster (slug, rule)

) r
JOIN (VALUES (1), (2)) AS a(arm)
  ON (a.arm = 1 AND (r.slug = 'lines_uncatalogued'))
  OR (a.arm = 2 AND (r.slug = 'sets_without_lines'))
LEFT JOIN (
SELECT * FROM (
SELECT 1 AS arm, p.set_num AS set_num, p.detail AS detail
```

O `EXPLAIN` mostra o rol varrido uma só vez, ao passo que a instrução tal como foi composta o varre
uma vez por braço, e as duas devolvem as mesmas linhas: o `fit` do exemplo brick confere cada
ficheiro com os varrimentos do seu plano, e o `laws`, as linhas. Cada ficheiro escrito noutra
grafia tem a sua linha, `Respelled sqlc/checks/all.sqlc: 1 union(s), of 2 arms`, a última linha
conta as uniões escritas noutra grafia, e o manifesto regista a opção, pelo que o `--verify` a lê
de lá.

É o único sítio onde compor analisa SQL: o sqlparser lê cada instrução composta, sem nada
conferir, e a que não conseguir ler fica tal como foi composta, com uma linha na saída de erro,
`Unparsed sqlc/script.sqlc: statement(s) 1 of 2, written as composed`; todos os outros bytes do
ficheiro ficam também tal como foram compostos. O `--respell` só escreve noutra grafia para o
PostgreSQL, e para qualquer outro dialeto é recusado. Como assenta apenas no texto composto, sem
catálogo, chave nem colação, só vale onde todos os braços leem o mesmo rol, o juntam `ON true`, o
cortam com um `WHERE` que só lê o rol e devolvem colunas do rol que são as mesmas em todos os
braços, ou colunas simples das linhas do próprio braço. O que assenta nas chaves do catálogo
escreve-se noutra grafia enquanto o PostgreSQL planeia, pelo [speller do warren-surveyor][speller].

### Comentários

Um `#` abre um comentário do modelo em qualquer ponto de uma linha, e o comentário vai até ao fim
dela. É nos comentários que um modelo diz o que devolve: qual é a chave, o que fica e o que fica de
fora.

Poupam as cópias que um comentário `--` faria, uma vez que esse, sendo SQL, passa para todas as
instruções que compõem o modelo, enquanto um `#` fica no modelo.

Os comentários saem na composição e nunca chegam à base de dados, e levam consigo a mudança de
linha, de maneira que a linha seguinte se cola àquela em que estava o comentário.

Dentro de um literal de texto, de um identificador entre aspas, de um comentário `--` ou `/* */` ou
de um metacomando do psql, um `#` é SQL, e uma diretiva também: `'#1'` fica `'#1'`, e
`-- :bind(id)` chega à base de dados como o comentário que é. Os operadores de jsonb `#>`, `#>>` e
`#-` são SQL onde quer que estejam, e `#--` abre comentário; já os operadores `#` e `##` do
PostgreSQL continuam a abri-lo, pelo que se escreve a função que está por trás deles, tal como o
`pg_operator.oprcode` a nomeia: `int4xor(a, b)` em vez de `a # b` entre inteiros.

O compositor lê essas aspas e esses comentários como a base de dados de cada dialeto os lê, e recusa
um modelo que duas bases de dados leriam de maneira diferente num `#`, numa diretiva, num `;` ou no
fim, dizendo as duas leituras. `E'it\'s' # note` é, no PostgreSQL, um literal de texto seguido de
um comentário, ao passo que no SQLite, cujos literais não leem sequências de escape, o `#` fica
dentro de um literal que ainda não fechou.

Um modelo escrito só para alguns dialetos di-lo à cabeça, `:dialect(postgres)` ou
`:dialect(postgres, duckdb)`, e passa então a ser lido apenas pelas bases de dados desses dialetos,
não sendo composto para nenhum outro; o que nada diz fica escrito para todos. Num modelo escrito
para o PostgreSQL, umas aspas de dólar (o corpo de uma função, um bloco `DO`) guardam SQL como um
literal de texto o guarda, `#variable_conflict` incluído, com uma diferença: lá dentro, uma
diretiva compõe-se, e um `:bind`, quer esteja escrito ali, quer venha de um modelo composto ali, é
recusado, porque nenhum valor lá chega.

### Cinco bases de dados

O `cargo sqlc compose --dialect` compõe os mesmos modelos para uma de cinco bases de dados,
`postgres` (por omissão), `mysql`, `mariadb`, `sqlite` ou `duckdb`, cada uma tal como lê o SQL com
a sua configuração por omissão; na biblioteca, quem as nomeia é o `Dialect`.

Poupa uma cópia de cada modelo por cada base de dados. Na instrução composta, o dialeto decide
quatro coisas: os marcadores que o `:bind` escreve; os nomes que o `--with` recusa, que são as
palavras reservadas de cada base de dados, lidas do PostgreSQL 16 e 18, do MySQL 9.7, do
MariaDB 12.3, do SQLite 3.53 e do DuckDB 1.5; onde acabam as aspas, os comentários e as
instruções; e como uma tabela simulada escreve os seus valores. Compostos para cada uma das cinco,
os ficheiros do exemplo brick só diferem nos marcadores.

Um modelo que só algumas das bases de dados leem da mesma maneira di-lo com `:dialect(…)`, e não é
composto para nenhuma outra; já uma configuração que mude o sítio onde as aspas acabam, como o
`standard_conforming_strings` do PostgreSQL desligado ou o `ANSI_QUOTES` do MySQL, não tem dialeto
próprio, e o `--respell`, esse, só escreve para o PostgreSQL.

## Tabelas simuladas: um modelo sobre linhas à escolha

Uma tabela simulada dá a uma tabela linhas à escolha de quem testa, e todas as instruções compostas
depois disso que nomeiem a tabela leem essas linhas em vez das da tabela, de modo que um modelo pode
ser posto à prova exatamente sobre as linhas de que um caso precisa, incluindo as que os dados reais
nunca têm:

```rust,ignore
use sql_composer::MockTable;

composer.add_mock_table(
    MockTable::new("lego_sets")
        .column("set_num", "varchar")
        .column("year", "integer")
        .row(&[Some("100-1"), Some("1990")])
        .row(&[Some("200-1"), Some("1992")]),
);
```

Poupa as tabelas de teste criadas, preenchidas e apagadas à volta de cada caso, e a cópia de uma
consulta escrita outra vez para as ler.

Na instrução composta, a simulação fica à cabeça de cada instrução que nomeie a tabela, no seu
`WITH`: `lego_sets AS (SELECT CAST('100-1' AS varchar) AS set_num, … UNION ALL …)`. Um nome de
`WITH` vale pela tabela com o mesmo nome em todo o sítio onde a instrução a nomeia pelo nome
simples, pelo que o compositor continua sem analisar SQL nenhum. Uma instrução com o seu próprio
`WITH` recebe a simulação como primeira entrada, e uma simulação sem linhas mantém as colunas e não
devolve nada; cada valor é convertido no tipo da sua coluna, escrito como o dialeto o escreve, e
`None` é `NULL`.

A simulação deixa de valer onde o nome simples não chega: um nome com esquema, como
`lego.lego_sets`, e uma vista leem a própria tabela, tal como uma instrução que só nomeie a tabela
entre aspas duplas. Uma instrução que escreva na tabela escreve na própria tabela, e o MySQL não
aceita `WITH` antes de um `INSERT`.

## Grafias: cada modelo escrito da maneira que se escolher

Um modelo pode escrever-se de várias maneiras que devolvem as mesmas linhas: uma condição no `ON` de
uma junção interna ou no `WHERE`, um `NOT` sobre um `AND` ou distribuído por cada operando, uma
relação lida uma só vez escrita no sítio onde se lê ou como consulta WITH. O
`cargo sqlc spellings <template>` expõe as grafias de um modelo, eixo a eixo, contadas antes
de se escrever qualquer uma delas; no `spellings.toml`, na raiz da pasta dos modelos, escolhe-se
uma grafia por eixo, e o `cargo sqlc spell` escreve todos os modelos nas grafias escolhidas, tal
como o `cargo fmt` escreve o código.

Poupa o estilo da casa explicado outra vez a cada pessoa, e a cada agente, que escreva um modelo,
porque passam a ser os próprios modelos a mostrá-lo. O `cargo sqlc spell --check` aponta cada
modelo que não esteja nas grafias escolhidas e não escreve nada, pelo que uma verificação na
integração contínua os mantém nelas.

Para a lista das peças de uma caixa, o `cargo sqlc spellings sets/select_set_parts.sqlc` começa
por contar:

```text
sqlc/sets/select_set_parts.sqlc: 2 spelling(s), counted an axis at a time
  join order: 1, laid out to be read: cargo sqlc spell writes no join order, since an order can change the plan PostgreSQL makes
  how a relation read once is named: 2
    the relation set_part_details (a WITH query): 2
  where a condition sits: 1
  negation: 1
  a semi-join's form: 1
```

Depois, escreve cada grafia por inteiro, a primeira tal como está escrita, e a segunda lê a mesma
relação a partir do `FROM`, como um modelo à parte:

```sql
FROM (
    :compose(shared/set_part_details.sqlc)
) set_part_details
```

Escolhe-se com `naming = "template"` no `spellings.toml`: o `cargo sqlc spell --check` aponta
então cada modelo do exemplo brick que não esteja escrito assim, e o `cargo sqlc spell` escreve-os.
Uma grafia vê-se no diff do modelo e no texto do seu ficheiro composto, e o `fit` do exemplo brick
confere que nenhuma grafia que o `spell` escreva muda o plano que o PostgreSQL faz para cada
ficheiro composto.

O sqlparser lê cada modelo como SQL do PostgreSQL, com cada diretiva representada por SQL do tipo
que lhe corresponde, e não se lê catálogo nenhum. As ordens das junções expõem-se para serem lidas
e nunca se escrevem, porque uma ordem pode mudar o plano; um modelo que outro componha mantém a
cabeça tal como está escrita; e um modelo escrito só para outras bases de dados, tal como uma
instrução que o sqlparser não consiga ler, fica como está, e o mesmo acontece a todos os modelos
onde nada se escolheu.

## O percurso

O [exemplo brick][examples/brick/README.md] percorre a álgebra relacional um modelo de cada vez,
sobre as tabelas de cima, com um subcomando por passo: seleção, projeção e junção, composição, uma
relação passada como argumento, o fecho dos temas, união, interseção e diferença e, daí em diante,
mais do SQL do PostgreSQL, com funções de janela, `DISTINCT ON`, `LATERAL`, `ROLLUP`,
`percentile_cont`, `generate_series`, uma antijunção, divisão, `array_agg`, `jsonb` e `MERGE`.

Pesquisa também com os índices que o sqlc-brickgen constrói: pelas palavras de um nome
(`websearch_to_tsquery`, com a ordem dada pelo `ts_rank` e as palavras marcadas pelo
`ts_headline`), por uma expressão regular (`~*`, e o `regexp_match` para ler o tamanho de um tijolo
no próprio nome) e por lugar (`earth_box` e `earth_distance` para um raio, `<->` para os mais
próximos primeiro). Cada pesquisa preenche uma vaga como qualquer filtro: as palavras e os padrões
entram em `@filter`, e os construtores perto de uma cidade ou de um distrito postal entram em
`@area`.

Examina o próprio catálogo com um rol de verificações, cujas linhas junta numa só união, que o
`--respell` escreve de modo a ler o rol uma só vez. Acaba no `laws`, que são todas as promessas que
o exemplo faz, cada uma conferida com uma segunda grafia da mesma resposta em SQL direto, três delas
sobre tabelas simuladas; já o `fit` confere cada ficheiro composto com o plano que o PostgreSQL faz
para ele, sem executar nenhum. Cada um deles sai com 1 se alguma das suas leis falhar.

Da mesma maneira, o [process-modulus][process-modulus] compõe as consultas que conferem os seus
documentos, numa árvore maior, com um percurso seu e composta com `--with`; e o percurso dele em
português europeu compõe as relações da árvore em inglês, que encontra através do `--search-path`.

## Referência

### `cargo sqlc compose`

```sh
cargo sqlc compose                                  # sqlc/ para .sql/, e depois cargo sqlx prepare
cargo sqlc compose --source assets/sqlc --target assets/.sql
cargo sqlc compose --search-path ../shared/sqlc     # mais modelos, lidos depois do --source
cargo sqlc compose --dialect mysql                  # um de cinco, postgres por omissão
cargo sqlc compose --skip-prepare                   # sem o cargo sqlx prepare
cargo sqlc compose --verify                         # compara, sem escrever nada
cargo sqlc compose --with                           # cada relação nomeada uma vez por instrução
cargo sqlc compose --views sqlc/views.sqlc          # um registo de vistas
cargo sqlc compose --respell                        # os braços de um rol lidos numa só junção
```

- **`--source`** é a pasta dos modelos: `sqlc` por omissão, ou `SQLC_SOURCE_DIR` quando a opção não
  é dada. Compõe-se cada `.sqlc` que lá esteja, a qualquer profundidade, e os caminhos do `:compose`
  leem-se a partir dela.
- **`--target`** é para onde vão os ficheiros compostos: `.sql` por omissão, ou `SQLC_TARGET_DIR`.
  O ficheiro de cada modelo mantém o caminho dele, e `sqlc/sets/select_set_parts.sqlc` dá
  `.sql/sets/select_set_parts.sql`. A composição escreve primeiro numa pasta temporária, que só toma
  o lugar do destino depois de todos os modelos se terem composto; o destino é substituído por
  inteiro, pelo que um modelo apagado ou mudado de sítio não deixa ficheiro para trás, e um ficheiro
  escrito à mão no destino desaparece na composição seguinte. O `--target` deve, portanto,
  apontar para uma pasta que não tenha mais nada. Ao lado dos ficheiros fica o `manifest.json`
  (mais abaixo).
- **`--search-path`** é mais uma pasta a partir da qual se leem os caminhos do `:compose`, depois do
  `--source`, pelo que um modelo pode compor uma relação que está noutra árvore. Os modelos que lá
  estão não dão ficheiro nenhum, e, com `--with`, uma relação encontrada lá toma o nome do seu
  caminho dentro dessa pasta. Para procurar em mais do que uma, repete-se a opção.
- **`--dialect`** escolhe o dialeto, `postgres` (por omissão), `mysql`, `mariadb`, `sqlite` ou
  `duckdb`, e é dele que dependem os marcadores e os nomes reservados.
- **`--skip-prepare`** deixa de fora o `cargo sqlx prepare`, que de outro modo corre assim que os
  ficheiros estão escritos. Se este falhar, o comando sai com 1, e os ficheiros ficam escritos na
  mesma.
- **`--verify`** compõe em memória e compara com o destino, `manifest.json` incluído: dá cada
  ficheiro como `CHANGED`, `MISSING` ou `STALE`, sai com 1 se houver algum e não escreve nada, como
  faz o `cargo fmt --check`. Compõe com as opções que o manifesto regista, de modo que uma opção que
  não seja dada se lê do manifesto, e uma que seja dada com outro valor é recusada, com os dois. Um
  modelo saltado, dá-o tal como o dá uma composição.
- **`--with`** nomeia uma só vez cada relação que uma instrução lê, à cabeça da instrução, e não se
  combina com `--views`.
- **`--views`** carrega um registo de vistas, e cada modelo que o registo nomeia passa a ler-se como
  a sua vista.
- **`--respell`** escreve numa só junção a união dos braços que leem um mesmo rol, para o
  PostgreSQL (*Uma união de braços que leem um mesmo rol*), e é recusado para qualquer outro
  dialeto.

Cada modelo que esteja no `--source` tem uma linha sua: `Composed`; `Shape`, com as vagas que deixa
abertas; ou `Skipped`, na saída de erro, com a vaga por preencher e o modelo onde ela está. A última
linha conta-os a todos: `Composed 51 of 55 template(s) into .sql: 4 shape(s), 0 skipped`. Com o
`--respell`, cada ficheiro escrito noutra grafia tem uma linha `Respelled`, cada instrução que o
sqlparser não consiga ler tem uma linha `Unparsed` na saída de erro, e a última linha acaba com as
uniões escritas noutra grafia: `, 1 union(s) respelled`.

O `manifest.json`, na raiz do destino, regista como foram compostos os ficheiros: as opções
(`dialect`, `with`, `views`, `search_path`, `respell`) e, de cada ficheiro composto, os nomes dos
parâmetros pela ordem dos marcadores (`bind_params`) e onde começam os nomes de cada instrução
(`statement_starts`). O do exemplo brick diz isto de um dos seus ficheiros:

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

Um programa em qualquer linguagem corre, assim, um ficheiro composto pelos nomes: o `$1` é o
`color_name`, e o `$2` o `set_num`. No MySQL e no MariaDB, cada `?` toma o nome que ocupa o seu
lugar no `bind_params`, onde um nome usado duas vezes aparece duas vezes.

### `cargo sqlc tree`

```sh
cargo sqlc tree                                     # os modelos de cada ficheiro composto
cargo sqlc tree reports/shared_moulds.sqlc          # os de um ficheiro composto
cargo sqlc tree --invert shared/theme_closure.sqlc  # os ficheiros que guardam um modelo
```

Mostra os modelos de cada ficheiro composto como o `cargo tree` mostra as dependências de um pacote:
cada modelo que o ficheiro guarda, uma vez por cada caminho que até ele vai dar, com a vaga onde
está cada preenchimento e o nome que fica escrito sempre que um nome está no lugar de um corpo. Um
modelo guardado mais do que uma vez tem ainda uma linha com as suas cópias:

```text
reports/shared_moulds.sqlc
├── queries/technic_moulds.sqlc
│   └── shared/scope_moulds.sqlc
│       └── scopes/technic.sqlc (@scope)
│           └── shared/theme_closure.sqlc
└── queries/city_moulds.sqlc
    └── shared/scope_moulds.sqlc
        └── scopes/city.sqlc (@scope)
            └── shared/theme_closure.sqlc
shared/scope_moulds.sqlc: 2 copies
shared/theme_closure.sqlc: 2 copies
```

Com o `--with`, o fecho fica nomeado uma só vez, à cabeça da instrução,
`shared/theme_closure.sqlc (WITH shared_theme_closure)`, e cada sítio que o compunha lê o nome; se
depois a base de dados lê uma cópia uma vez ou duas, quem o diz é o plano dela, no `EXPLAIN`, e o
`fit` do exemplo brick confere as cópias de cada ficheiro composto com os varrimentos do seu plano,
onde um varrimento só pode faltar por causa de um `LEFT JOIN` que a base de dados tenha retirado.
O `--invert` (`-i`) mostra, para um só modelo, cada ficheiro composto que o guarda, com as suas
cópias, ou seja, cada instrução a que chega uma alteração nesse modelo. O `cargo sqlc tree` aceita o
`--source`, o `--dialect`, o `--with`, o `--views` e o `--search-path`, como o `compose`.

### `cargo sqlc spellings` e `cargo sqlc spell`

```sh
cargo sqlc spellings sets/select_set_parts.sqlc               # todos os eixos contados e escritos
cargo sqlc spellings shared/scope_sets.sqlc --axis conditions # todos os eixos contados, um escrito
cargo sqlc spell --check                                      # aponta, sem escrever nada
cargo sqlc spell                                              # escreve os modelos nas grafias escolhidas
```

Ambos aceitam o `--source`, em cuja raiz está o `spellings.toml`, e o `--search-path`, como o
`compose`. O `spellings` recebe o modelo pelo seu caminho dentro da pasta dos modelos e expõe cada
grafia em cada sítio, incluindo as que nenhuma escolha escreve, como uma condição num `ON` mais
adiante, ao passo que o `spell` escreve todos os modelos dessa pasta. O `spell --check` sai com 1 se
algum modelo não estiver nas grafias escolhidas, e uma lista de junções com mais de 8 relações fica
como está escrita, sem que se exponham as suas ordens.

O `spellings.toml` tem uma chave por eixo, e um eixo que lá não esteja fica escrito como cada modelo
o escreve:

| chave | escolhas | o que cada uma escreve |
|---|---|---|
| `conditions` | `on`, `where` | uma condição no `ON` da primeira junção interna onde já estejam juntadas todas as relações que ela nomeia, ou no `WHERE` |
| `negation` | `pushed`, `over` | cada `NOT` sobre um `AND` ou um `OR` distribuído pelos seus operandos, ou um só `NOT` sobre operandos que são, cada um, um `NOT`; `NOT NOT p` como `p`, num caso e no outro |
| `naming` | `inline`, `with`, `template` | uma relação lida uma só vez escrita onde é lida, como consulta WITH à cabeça da instrução, ou como o `:compose` de um modelo seu |
| `semi_join` | `in`, `exists` | `x IN (SELECT y …)` ou `EXISTS (SELECT … WHERE x = y)` |
| `join_order` | uma tabela: `template`, `order` | nada: a ordem da lista de junções de um modelo, que o `spell` confere com a regra do mais exterior primeiro, dizendo o que encontra |

```toml
conditions = "where"
naming = "template"

[join_order]
template = "shared/scope_sets.sqlc"
order = ["s", "sc", "scope"]
```

Uma chave que nenhum eixo tenha é recusada com os eixos que há, e uma escolha que o seu eixo não
tenha, com as escolhas desse eixo. Uma condição só muda de sítio onde o seu texto diz que relações
lê, com cada coluna nomeada com a sua relação, e só entre junções internas e o `WHERE`; o
`NOT (a = b)` nunca se escreve `a <> b`, nem o `NOT IN` como `NOT EXISTS`, e uma semijunção
debaixo de um `NOT` fica como está escrita.

### Diretivas

| diretiva | compõe-se em |
|---|---|
| `:compose(path)` | a relação que está em `path`, onde a diretiva estiver |
| `:compose(path, @slot = other)` | o mesmo, com a vaga `@slot` preenchida por `other`; várias vagas separam-se por vírgulas |
| `:compose(@slot)` | a relação com que quem compõe preenche `@slot` |
| `:define(path)` | o corpo da relação, e o nome à frente do seu `AS` vale por ela daí para baixo |
| `:bind(name)` | o marcador do dialeto; um por valor, quando o nome recebe uma lista |
| `:bind(name EXPECTING n..m)`, `:bind(name NULL)` | o mesmo; uma contagem fora de `n..m` é recusada, e, exceto no PostgreSQL, também um NULL onde não se escreveu `NULL` |
| `:union(a, b, …)` | as fontes unidas por `UNION`, cada uma embrulhada, e o todo também; `ALL` ou `DISTINCT` à cabeça, dentro dos parênteses |
| `:intersect(a, b, …)` | as fontes unidas por `INTERSECT`, cada uma embrulhada, e o todo também |
| `:except(a, b, …)` | `a` menos cada uma das fontes seguintes, cada uma embrulhada, e o todo também |
| `:count(a)`, `:count(DISTINCT col OF a)` | `SELECT COUNT(…) FROM (…) AS _count_sub` |
| `:dialect(postgres, duckdb)` | nada: os dialetos para que o modelo está escrito, à cabeça; não é composto para nenhum outro |
| `# …` | nada: um comentário do modelo, até ao fim da linha |

Um nome de vaga leva letras, algarismos, `-` e `_`.

### A biblioteca

```rust,ignore
use sql_composer::composer::Composer;
use sql_composer::parser::parse_template;
use sql_composer::types::{Dialect, TemplateSource};

let input = "SELECT set_num, name, year FROM lego_sets WHERE year = :bind(year) AND theme_id = :bind(theme_id)";
let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();

let composer = Composer::new(Dialect::Postgres);
let result = composer.compose(&template).unwrap();

assert_eq!(result.sql, "SELECT set_num, name, year FROM lego_sets WHERE year = $2 AND theme_id = $1");
assert_eq!(result.bind_params, vec!["theme_id", "year"]);
```

O `parse_template_file` lê um modelo de um ficheiro. O `Composer::add_search_path` acrescenta uma
pasta a partir da qual se leem os caminhos do `:compose`, o `compose_with_values` recebe os valores
de cada parâmetro e escreve um marcador por valor, o `load_view_registry` carrega um registo de
vistas, o `add_mock_table` dá a uma tabela linhas à escolha, e com `name_relations = true`
compõe-se como com o `--with`.
O `source::compose_all` compõe todos os modelos de uma pasta como o `cargo sqlc compose` os compõe,
com as opções que um `source::Flags` guarda, e diz o que fez de cada um: `Standing::Written`, com o
seu `ComposedSql`; `Standing::Shape`; ou `Standing::Skipped`. O `Dialect::ALL` lista os dialetos, e
o `"mysql".parse::<Dialect>()` lê um deles pelo nome que o `--dialect` lhe dá.
O `Composer::tree` dá o que a composição de um modelo escreve, como o `cargo sqlc tree` o mostra.

| tipo | o que é |
|---|---|
| `Template` | um modelo analisado: os seus elementos e a sua origem |
| `Element` | SQL literal, um `:bind`, um `:compose` ou `:define`, ou um comando |
| `Binding` | um `:bind`: o nome, os valores que espera e se aceita `NULL` |
| `ComposeRef` | um `:compose()` ou `:define()`, com os argumentos das vagas |
| `ComposeTarget` | um caminho, uma vaga (`@name`) ou um caminho de `:define` |
| `SlotAssignment` | um `@name = path` dentro de `:compose()` |
| `Command` | um `:count()`, `:union()`, `:intersect()` ou `:except()`, com o modificador e as fontes |
| `CommandKind` | `Count`, `Union`, `Intersect` ou `Except` |
| `Composer` | compõe modelos em SQL para um dialeto |
| `ComposedSql` | as instruções compostas e os nomes dos parâmetros delas pela ordem dos marcadores |
| `Held` | um modelo tal como uma instrução composta o guarda: como se chega a ele, o nome que fica escrito por ele e o que ele próprio guarda |
| `Reach` | como se chega a um modelo: `Root`, `Compose`, `Slot`, `Define`, `Source` ou `Head` |
| `source::Flags` | o dialeto, o `--with`, o `--views`, o `--search-path` e, com a funcionalidade `respell`, o `--respell` com que uma pasta se compõe |
| `source::Standing` | o que a composição fez de um modelo: escrito, forma ou saltado |
| `source::Manifest` | o que o `manifest.json` guarda, com a funcionalidade `serde` |
| `respell::Respelled` | um ficheiro escrito noutra grafia: o texto, as instruções, as que ficaram por ler e as uniões escritas noutra grafia, com a funcionalidade `respell` |
| `spellings::Layout` | as grafias de um modelo, eixo a eixo e sítio a sítio, como o `cargo sqlc spellings` as mostra, com a funcionalidade `spellings` |
| `spellings::Picks` | o que o `spellings.toml` guarda, com a funcionalidade `spellings` |
| `Dialect` | `Postgres`, `Mysql`, `Mariadb`, `Sqlite` ou `Duckdb` |
| `MockTable` | linhas à escolha para uma tabela: o nome, as colunas por ordem e as linhas |
| `MockColumn` | uma coluna de uma tabela simulada: o nome e o tipo, escrito como o dialeto o escreve |

A funcionalidade `serde` deriva `Serialize` e `Deserialize` para os tipos principais, e a `std`,
ligada por omissão, compila com a biblioteca padrão. A `respell` traz o sqlparser e o
`respell::respell`, que escreve um ficheiro composto noutra grafia como o `--respell`, e a
`spellings` traz o sqlparser, o `spellings::lay_out` e o `spellings::spell`, que expõem as grafias
de um modelo e o escrevem nas grafias escolhidas, como o `cargo sqlc spellings` e o
`cargo sqlc spell`.

```toml
sql-composer = { version = "0.1.0", features = ["serde"] }
```

### Controladores

Cada controlador envolve a ligação do pacote da sua base de dados num tipo seu, que implementa o
`ComposerConnection` (síncrono) ou o `ComposerConnectionAsync` (assíncrono) e cujo `compose` compõe
um modelo e põe os valores pela ordem dos marcadores, num só passo. Como esse tipo implementa o
`Deref` para a ligação que envolve, os métodos do próprio pacote continuam à mão. Os modelos são os
mesmos para todos os dialetos, e só os marcadores mudam.

#### rusqlite

```toml
[dependencies]
sql-composer = "0.1.0"
sql-composer-rusqlite = "0.1.0"
```

```rust,ignore
use sql_composer::composer::Composer;
use sql_composer::driver::ComposerConnection;
use sql_composer::parser::parse_template;
use sql_composer::types::{Dialect, TemplateSource};
use sql_composer::bind_values;
use sql_composer_rusqlite::SqliteConnection;

let conn = SqliteConnection::open_in_memory().unwrap();
conn.execute("CREATE TABLE lego_sets (set_num TEXT, name TEXT, year INTEGER, theme_id INTEGER, num_parts INTEGER)", []).unwrap();

let input = "SELECT set_num, name FROM lego_sets WHERE year = :bind(year)";
let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();
let composer = Composer::new(Dialect::Sqlite);

let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn rusqlite::types::ToSql>]);
let (sql, params) = conn.compose(&composer, &template, values).unwrap();

let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|v| v.as_ref()).collect();
let mut stmt = conn.prepare(&sql).unwrap();
let _rows = stmt.query(refs.as_slice()).unwrap();
```

#### DuckDB

```toml
[dependencies]
sql-composer = "0.1.0"
sql-composer-duckdb = "0.1.0"
```

```rust,ignore
use sql_composer::composer::Composer;
use sql_composer::driver::ComposerConnection;
use sql_composer::parser::parse_template;
use sql_composer::types::{Dialect, TemplateSource};
use sql_composer::bind_values;
use sql_composer_duckdb::DuckDbConnection;

let conn = DuckDbConnection::open_in_memory().unwrap();
let input = "SELECT set_num, name FROM lego_sets WHERE year = :bind(year)";
let template = parse_template(input, TemplateSource::Literal("example".into())).unwrap();
let composer = Composer::new(Dialect::Duckdb);

let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn duckdb::ToSql>]);
let (sql, params) = conn.compose(&composer, &template, values).unwrap();
```

#### PostgreSQL (síncrono e assíncrono)

```toml
[dependencies]
sql-composer = "0.1.0"
sql-composer-postgres = "0.1.0"  # síncrono e assíncrono, ambos ligados por omissão
# sql-composer-postgres = { version = "0.1.0", default-features = false, features = ["async"] }
```

A funcionalidade `sync` traz o pacote `postgres` e, com ele, o tipo síncrono; a `async` liga o tipo
assíncrono, que assenta no `tokio-postgres`, de que o controlador depende sempre.

```rust,ignore
// Assíncrono (tokio-postgres)
use sql_composer_postgres::{PgClient, boxed_params};
use sql_composer::driver::ComposerConnectionAsync;

let (client, connection) = tokio_postgres::connect("host=localhost dbname=sqlc_brick", tokio_postgres::NoTls).await?;
tokio::spawn(connection);
let client = PgClient::from_client(client);

let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn tokio_postgres::types::ToSql + Sync + Send>]);
let (sql, params) = client.compose(&composer, &template, values).await?;
let refs = boxed_params(&params);
let rows = client.query(&sql as &str, &refs).await?;
```

```rust,ignore
// Síncrono (postgres)
use sql_composer_postgres::{PgConnection, boxed_params_sync};
use sql_composer::driver::ComposerConnection;

let client = postgres::Client::connect("host=localhost dbname=sqlc_brick", postgres::NoTls)?;
let conn = PgConnection::from_client(client);

let values = bind_values!("year" => [Box::new(2017i32) as Box<dyn postgres::types::ToSql + Sync>]);
let (sql, params) = conn.compose(&composer, &template, values)?;
```

#### MySQL (síncrono e assíncrono)

```toml
[dependencies]
sql-composer = "0.1.0"
sql-composer-mysql = "0.1.0"  # síncrono e assíncrono, ambos ligados por omissão
# sql-composer-mysql = { version = "0.1.0", default-features = false, features = ["async"] }
```

A funcionalidade `sync` traz o pacote `mysql` e, com ele, o tipo síncrono; a `async` liga o tipo
assíncrono, que assenta no `mysql_async`, de que o controlador depende sempre.

No MySQL, um nome usado duas vezes são dois marcadores, e cada um recebe de novo os valores do
nome, a partir do primeiro.

```rust,ignore
// Assíncrono (mysql_async)
use sql_composer_mysql::MysqlConn;
use sql_composer::driver::ComposerConnectionAsync;

let pool = mysql_async::Pool::new("mysql://root@localhost/sqlc_brick");
let conn = pool.get_conn().await?;
let conn = MysqlConn::from_conn(conn);

let values = bind_values!("year" => [mysql_async::Value::from(2017i32)]);
let (sql, params) = conn.compose(&composer, &template, values).await?;
```

```rust,ignore
// Síncrono (mysql)
use sql_composer_mysql::MysqlConnection;
use sql_composer::driver::ComposerConnection;

let conn = mysql::Conn::new("mysql://root@localhost/sqlc_brick")?;
let conn = MysqlConnection::from_conn(conn);

let values = bind_values!("year" => [mysql::Value::from(2017i32)]);
let (sql, params) = conn.compose(&composer, &template, values)?;
```

#### sqlx: conferir as instruções compostas

```toml
[dependencies]
sql-composer-sqlx = "0.1.0"                                        # confere no PostgreSQL (por omissão)
# sql-composer-sqlx = { version = "0.1.0", features = ["validate"] } # junta a verificação da sintaxe sem ligação
```

O `verify_postgres`, da funcionalidade `postgres`, ligada por omissão, prepara cada instrução num
PostgreSQL em funcionamento e depois liberta-a, o que confere a sintaxe e confirma que as tabelas e
as colunas existem, e o `verify_mysql`, da funcionalidade `mysql`, faz o mesmo num MySQL em
funcionamento. Já o `validate_syntax`, da funcionalidade `validate`, analisa a instrução sem
ligação nenhuma, com o `sqlparser` e cada marcador lido como `1`, e por isso só confere a sintaxe.

```rust,ignore
use sql_composer_sqlx::verify_postgres;

// Confere as instruções compostas numa base de dados em funcionamento
let stmts = vec![&composed_sql];
verify_postgres("postgres://localhost/sqlc_brick", &stmts).await?;
```

```rust,ignore
use sql_composer_sqlx::validate_syntax;
use sql_composer::Dialect;

// Confere a sintaxe sem base de dados nenhuma (a funcionalidade "validate")
validate_syntax("SELECT set_num, name FROM lego_sets WHERE year = $1", Dialect::Postgres)?;
```

### C e Python

Há dois pacotes que levam o compositor a programas que não estão escritos em Rust: o
`sql-composer-ffi`, que é a sua ABI de C, com o cabeçalho `sql_composer.h`, e o `sql-composer-py`,
que é o módulo de Python `sql_composer`, distribuído como `sql-composer`. Cada um analisa um modelo
e compõe-no para qualquer um dos cinco dialetos, com caminhos de pesquisa, relações nomeadas, um
registo de vistas, tabelas simuladas e o número de valores de cada parâmetro, devolvendo o SQL, os
nomes dos parâmetros pela ordem dos marcadores e onde começam os nomes de cada instrução. Cada um
compõe também uma pasta inteira como o `cargo sqlc compose`, com o `--respell` entre as opções, e
diz o que fez de cada modelo, além de dar o texto do `manifest.json`.

Poupam um segundo compositor escrito para outra linguagem, e um programa que chame o `cargo sqlc`
para compor. Compostos por qualquer um dos dois, com ou sem `--respell`, os modelos do exemplo
brick dão, byte a byte, os ficheiros que o `cargo sqlc compose` escreve, entre eles a união das
verificações escrita noutra grafia.

Os valores, esses, continuam a ser do programa que chama, que os passa com a sua própria biblioteca
de base de dados, pela ordem em que os nomes vêm listados. Nenhum dos dois expõe grafias, o que
fazem o `cargo sqlc` e a funcionalidade `spellings` do núcleo, e as chamadas estão nos README do
[sql-composer-ffi][crates/sql-composer-ffi/README.md] e do
[sql-composer-py][crates/sql-composer-py/README.md].

### O espaço de trabalho

```
crates/
  sql-composer/           # o núcleo: analisador (winnow), tipos, compositor
  sql-composer-rusqlite/  # controlador rusqlite (ComposerConnection)
  sql-composer-duckdb/    # controlador DuckDB (ComposerConnection)
  sql-composer-postgres/  # controlador PostgreSQL (síncrono e assíncrono)
  sql-composer-mysql/     # controlador MySQL (síncrono e assíncrono)
  sql-composer-sqlx/      # integração com o sqlx (verify, validate)
  sql-composer-ffi/       # a ABI de C (sql_composer.h)
  sql-composer-py/        # o módulo de Python sql_composer
  cargo-sqlc/             # o comando cargo sqlc
examples/
  brick/                  # todas as diretivas menos o :define, e mais do SQL do PostgreSQL, na base de dados do sqlc-brickgen
  sqlc-brickgen/          # sqlc-brickgen: um catálogo grande, feito a partir de um real, para o PostgreSQL
```

## Licença

MIT, com direitos de autor de Kenneth Allen Flegal: ver [LICENSE][LICENSE]. Cada pacote publicado
traz o mesmo ficheiro.

As tabelas do exemplo brick são feitas a partir de um catálogo real de caixas de construção, cuja
origem o README do exemplo indica. LEGO® é uma marca do LEGO Group, que não patrocina, não autoriza
nem subscreve este projeto.

[README.md]: https://github.com/soulstompp/sql-composer/blob/HEAD/README.md
[warren-surveyor]: https://github.com/soulstompp/warren-surveyor
[warren-snail]: https://github.com/soulstompp/warren-snail
[speller]: https://github.com/soulstompp/warren-surveyor/blob/HEAD/warren-pg-speller/README.md
[docs/without-rust.md]: https://github.com/soulstompp/sql-composer/blob/HEAD/docs/without-rust.pt.md
[crates/sql-composer-ffi/README.md]: https://github.com/soulstompp/sql-composer/blob/HEAD/crates/sql-composer-ffi/README.pt.md
[crates/sql-composer-py/README.md]: https://github.com/soulstompp/sql-composer/blob/HEAD/crates/sql-composer-py/README.pt.md
[examples/brick/]: https://github.com/soulstompp/sql-composer/tree/HEAD/examples/brick
[examples/sqlc-brickgen/]: https://github.com/soulstompp/sql-composer/tree/HEAD/examples/sqlc-brickgen
[examples/brick/sqlc/shared/set_part_details.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/shared/set_part_details.sqlc
[examples/brick/sqlc/sets/select_set_parts.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/sets/select_set_parts.sqlc
[examples/brick/.sql/sets/select_set_parts.sql]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/.sql/sets/select_set_parts.sql
[examples/brick/sqlc/shared/filtered_set_parts.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/shared/filtered_set_parts.sqlc
[examples/brick/sqlc/filters/by_color.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/filters/by_color.sqlc
[examples/brick/sqlc/sets/select_colored_parts.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/sets/select_colored_parts.sqlc
[examples/brick/sqlc/reports/city_only_moulds.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/reports/city_only_moulds.sqlc
[examples/brick/sqlc/reports/count_theme_parts.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/reports/count_theme_parts.sqlc
[examples/brick/sqlc/reports/shared_moulds.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/reports/shared_moulds.sqlc
[examples/brick/sqlc/checks/roster.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/checks/roster.sqlc
[examples/brick/sqlc/checks/all.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/checks/all.sqlc
[examples/brick/sqlc/checks/sets_without_lines.sqlc]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/sqlc/checks/sets_without_lines.sqlc
[examples/brick/README.md]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/README.pt.md
[process-modulus]: https://github.com/soulstompp/process-modulus
[LICENSE]: https://github.com/soulstompp/sql-composer/blob/HEAD/LICENSE
