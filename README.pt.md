# sql-composer

> **Português europeu, grafia do AO90.** Também disponível em inglês: [`README.md`][README.md].

Uma junção copiada para uma segunda consulta já são duas junções, que nascem iguais e acabam por
divergir.

O sql-composer fica com uma só: cada consulta vive num ficheiro seu, em SQL simples, e as outras
consultas compõem-na pelo caminho desse ficheiro. O `cargo sqlc compose` escreve depois cada
instrução composta tal como chega à base de dados, em SQL simples, sem macros nem nada montado em
tempo de execução, e esse ficheiro, o DBA lê-o, o `EXPLAIN` executa-o e o `query_file!()` do SQLx
confere-o quando o código compila.

A composição funciona porque toda a consulta devolve uma relação, isto é, linhas que partilham as
mesmas colunas, e uma relação pode estar em qualquer sítio onde estaria uma tabela, de modo que uma
consulta composta dentro de outra é lida aí como uma tabela. A isto chama-se fecho, e é no fecho
que assenta toda a linguagem de modelos: uma relação posta dentro de outra, uma relação passada como
argumento, relações combinadas como conjuntos e cada relação nomeada uma só vez.

A instrução composta, quem a planeia continua a ser a base de dados; o sql-composer limita-se a
escrevê-la.

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

O compositor nunca analisa o SQL que insere, e um erro no SQL compõe-se sem queixa: só a base de
dados o recusa. É por isso que os ficheiros compostos ficam no repositório, ao lado dos modelos: o
`cargo sqlc compose --verify` confirma que continuam a ser o que os modelos compõem, e o SQLx, ou
um teste, executa-os.

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
-- SQLite:     ... WHERE year = ?2 AND theme_id = ?1
-- MySQL:      ... WHERE year = ? AND theme_id = ?
```

No PostgreSQL e no SQLite, cada nome é numerado pelo lugar que ocupa, por ordem alfabética, entre os
nomes da instrução, e um nome usado duas vezes fica um só marcador; no MySQL, cada uso é um `?` à
parte, pela ordem em que a instrução os usa. O `bind_params` da instrução composta lista os nomes
pela ordem dos marcadores, que é também a ordem em que o `query_file!()` recebe os argumentos. Num
modelo com várias instruções, cada uma numera os seus marcadores a partir do primeiro, e o
`statement_starts` diz onde começam, no `bind_params`, os nomes de cada uma.

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
compô-lo sozinho (`MissingSlot`), e o `cargo sqlc compose` não escreve ficheiro nenhum para ele. Do
mesmo modo, e sem mensagem nenhuma, salta um modelo que componha outro e lhe deixe uma vaga por
preencher; quem dá por isso é só o `--verify`, e só se o ficheiro composto desse modelo já estiver
no repositório, que é o ficheiro que ele dá como `STALE`.

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
pelas suas duas fontes, e a sua instrução composta tem por isso esse fecho duas vezes.

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
consulta reescrita para as ler.

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

## O percurso

O [exemplo brick][examples/brick/README.md] percorre a álgebra relacional um modelo de cada vez,
sobre as tabelas de cima, com um subcomando por passo: seleção, projeção e junção, composição, uma
relação passada como argumento, o fecho dos temas, união, interseção e diferença e, daí em diante,
mais do SQL do PostgreSQL, com funções de janela, `DISTINCT ON`, `LATERAL`, `ROLLUP`,
`percentile_cont`, `generate_series`, uma antijunção, divisão, `array_agg`, `jsonb` e `MERGE`.

Pesquisa também com os índices que o gerador constrói: pelas palavras de um nome
(`websearch_to_tsquery`, com a ordem dada pelo `ts_rank` e as palavras marcadas pelo
`ts_headline`), por uma expressão regular (`~*`, e o `regexp_match` para ler o tamanho de um tijolo
no próprio nome) e por lugar (`earth_box` e `earth_distance` para um raio, `<->` para os mais
próximos primeiro). Cada pesquisa preenche uma vaga como qualquer filtro: as palavras e os padrões
entram em `@filter`, e os construtores perto de uma cidade ou de um distrito postal entram em
`@area`.

Acaba no `laws`, que são todas as promessas que o exemplo faz, cada uma conferida com uma segunda
escrita da mesma resposta em SQL direto, e duas delas sobre tabelas simuladas; se alguma falhar, o
`laws` sai com 1.

## Referência

### `cargo sqlc compose`

```sh
cargo sqlc compose                                  # sqlc/ para .sql/, e depois cargo sqlx prepare
cargo sqlc compose --source assets/sqlc --target assets/.sql
cargo sqlc compose --dialect mysql                  # postgres (por omissão), mysql ou sqlite
cargo sqlc compose --skip-prepare                   # sem o cargo sqlx prepare
cargo sqlc compose --verify                         # compara, sem escrever nada
cargo sqlc compose --with                           # cada relação nomeada uma vez por instrução
cargo sqlc compose --views sqlc/views.sqlc          # um registo de vistas
```

- **`--source`** é a pasta dos modelos: `sqlc` por omissão, ou `SQLC_SOURCE_DIR` quando a opção não
  é dada. Compõe-se cada `.sqlc` que lá esteja, a qualquer profundidade, e os caminhos do `:compose`
  leem-se a partir dela.
- **`--target`** é para onde vão os ficheiros compostos: `.sql` por omissão, ou `SQLC_TARGET_DIR`.
  O ficheiro de cada modelo mantém o caminho dele, e `sqlc/sets/select_set_parts.sqlc` dá
  `.sql/sets/select_set_parts.sql`. A composição escreve primeiro numa pasta temporária, que só toma
  o lugar do destino depois de todos os modelos se terem composto; o destino é substituído por
  inteiro, pelo que um modelo apagado ou mudado de sítio não deixa ficheiro para trás, e um ficheiro
  escrito à mão no destino desaparece na composição seguinte. O `--target` deve, portanto, apontar
  para uma pasta que não tenha mais nada.
- **`--dialect`** escolhe os marcadores: `postgres`, `mysql` ou `sqlite`.
- **`--skip-prepare`** deixa de fora o `cargo sqlx prepare`, que de outro modo corre assim que os
  ficheiros estão escritos. Se este falhar, o comando sai com 1, e os ficheiros ficam escritos na
  mesma.
- **`--verify`** compõe em memória e compara com o destino: dá cada ficheiro como `CHANGED`,
  `MISSING` ou `STALE`, sai com 1 se houver algum e não escreve nada, como faz o
  `cargo fmt --check`.
  Corre-se com as mesmas opções com que os ficheiros foram compostos, já que opções diferentes
  compõem textos diferentes.
- **`--with`** nomeia uma só vez cada relação que uma instrução lê, à cabeça da instrução, e não se
  combina com `--views`.
- **`--views`** carrega um registo de vistas, e cada modelo que o registo nomeia passa a ler-se como
  a sua vista.

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
| `ComposedSql` | o SQL composto e os nomes dos parâmetros pela ordem dos marcadores |
| `Dialect` | `Postgres`, `Mysql` ou `Sqlite` |
| `MockTable` | linhas à escolha para uma tabela: o nome, as colunas por ordem e as linhas |
| `MockColumn` | uma coluna de uma tabela simulada: o nome e o tipo, escrito como o dialeto o escreve |

A funcionalidade `serde` deriva `Serialize` e `Deserialize` para os tipos principais, e a `std`,
ligada por omissão, compila com a biblioteca padrão.

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
// o DuckDB lê os marcadores $N do PostgreSQL
let composer = Composer::new(Dialect::Postgres);

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

### O espaço de trabalho

```
crates/
  sql-composer/           # o núcleo: analisador (winnow), tipos, compositor
  sql-composer-rusqlite/  # controlador rusqlite (ComposerConnection)
  sql-composer-duckdb/    # controlador DuckDB (ComposerConnection)
  sql-composer-postgres/  # controlador PostgreSQL (síncrono e assíncrono)
  sql-composer-mysql/     # controlador MySQL (síncrono e assíncrono)
  sql-composer-sqlx/      # integração com o sqlx (verify, validate)
  cargo-sqlc/             # o comando cargo sqlc
examples/
  brick/                  # todas as diretivas menos o :define, e mais do SQL do PostgreSQL, na base de dados do sqlc-brickgen
  sqlc-brickgen/          # sqlc-brickgen: um catálogo grande, feito a partir de um real, para o PostgreSQL
```

Os pacotes estão na versão 0.1.0: as versões 0.1 que vierem depois mantêm a API, que só a 0.2 pode mudar.

## Licença

MIT, com direitos de autor de Kenneth Allen Flegal: ver [LICENSE][LICENSE]. Cada pacote publicado
traz o mesmo ficheiro.

As tabelas do exemplo brick são feitas a partir de um catálogo real de caixas de construção, cuja
origem o README do exemplo indica. LEGO® é uma marca do LEGO Group, que não patrocina, não autoriza
nem subscreve este projeto.

[README.md]: https://github.com/soulstompp/sql-composer/blob/HEAD/README.md
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
[examples/brick/README.md]: https://github.com/soulstompp/sql-composer/blob/HEAD/examples/brick/README.md
[LICENSE]: https://github.com/soulstompp/sql-composer/blob/HEAD/LICENSE
