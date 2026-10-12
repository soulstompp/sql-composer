# Exemplo brick

> **Português europeu, grafia do AO90.** Também disponível em inglês: [`README.md`](README.md).

Este exemplo percorre a linguagem de modelos do sql-composer um modelo de cada vez, sobre um
catálogo de caixas de construção: cada passo faz uma pergunta ao catálogo e responde-lhe com um
modelo, e o subcomando do passo compõe esse modelo, imprime a instrução em que ele se compõe e
executa-a.

O percurso parte das entradas de uma caixa e vai construindo sobre elas: primeiro lista-as,
escreve-as em tabelas e estreita-as com um filtro passado de fora; depois junta em âmbitos os temas
a que as caixas pertencem, combina esses âmbitos como conjuntos, conta-os e nomeia cada relação uma
só vez. Daí segue para mais do SQL do PostgreSQL, para pesquisas que leem os índices com que o
catálogo é construído e para um rol de verificações sobre o próprio catálogo, e acaba no `laws`, que
confere o que os passos prometem com uma segunda grafia da mesma resposta, e no `fit`, que confere
cada instrução composta com o plano que o PostgreSQL faz para ela.

O catálogo é a base de dados que o sqlc-brickgen constrói a partir de uma real: as caixas, com os
seus inventários e peças, as cores e os temas, e os construtores que compram as caixas.

## Preparação

O exemplo corre no PostgreSQL 16 ou posterior, onde o `earthdistance` funciona a partir de um
esquema só seu. Com o servidor a correr e o `psql` no `PATH`, na raiz do repositório:

```sh
createdb sqlc_brick
curl -sSfL https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/lego.sql \
    | psql -X -q -v ON_ERROR_STOP=1 -o /dev/null -d sqlc_brick
cargo run --release -p sqlc-brickgen -- --database-url postgres:///sqlc_brick \
    --size small --traps none --indexes keys,composite,search
cargo run -p brick-example -- migrate
```

1. O `createdb` cria a base de dados do exemplo.
2. O `psql` carrega em `public` a base de dados de amostra da Neon, que é o catálogo real que o
   gerador lê, e para na primeira instrução que falhe; de onde vem essa base de dados, e sob que
   licenças, diz-se no fim desta página.
3. O gerador escreve o catálogo e 20 mil caixas sintetizadas, com os seus construtores, coleções e
   compras, no esquema `lego`, que apaga e volta a criar a cada execução. O `--traps none` deixa de
   fora as armadilhas que ele planta, e o `--indexes keys,composite,search` constrói as chaves, os
   índices compostos para as junções e os índices de pesquisa: os nomes das peças pelas suas
   palavras e por trigramas, os nomes das caixas por trigramas, as casas dos construtores sobre a
   Terra e os códigos postais pelo seu prefixo. Antes de escrever o que quer que seja, cria as
   extensões de que esses índices precisam num esquema só delas, `extensions`, à parte de `public`:
   o `cube` e o `earthdistance` para as distâncias sobre a Terra, e o `pg_trgm` para os padrões.
   Como só um superutilizador pode criar o `earthdistance`, com qualquer outro utilizador o gerador
   para e diz o que um superutilizador tem de executar.
4. O `migrate` junta ao mesmo esquema as tabelas próprias do exemplo (`set_category_summary`,
   `inventory_tracking`); pode correr-se outra vez sem perigo, e volta a correr-se depois do
   gerador, que as apaga juntamente com o esquema.

As consultas procuram no esquema `lego`, depois nos esquemas que guardam as três extensões, e nunca
em `public`, onde o catálogo real está com os mesmos nomes de tabela; o `--schema` dá-lhes outro
esquema em vez de `lego`. As extensões que a base de dados já tinha em `public` ficam lá, fora do
alcance das consultas, e as pesquisas por lugar param então na primeira chamada a `ll_to_earth`.

Como chegar a uma base de dados que não seja a `sqlc_brick`, di-lo a secção *Ligação*, no fim desta
página.

## O percurso

Todos os comandos correm na raiz do repositório. Os modelos estão em `examples/brick/sqlc`, e os
ficheiros em que se compõem estão guardados no repositório ao lado deles, em `examples/brick/.sql`,
cada um no caminho do seu modelo; este comando confirma que cada ficheiro composto continua a ser o
que o seu modelo compõe, sem escrever nada:

```sh
cargo sqlc compose --source examples/brick/sqlc --target examples/brick/.sql --skip-prepare --verify
```

Sem o `--verify`, volta a escrevê-los.

Cada subcomando compõe o seu modelo no momento em que corre: imprime a instrução composta e os nomes
dos seus parâmetros, dá-lhes os valores pelo nome e imprime as linhas. Um valor dado com um nome que
a instrução não recebe é recusado, de modo que um valor nunca vai parar a outro parâmetro.

### 1. As entradas de uma caixa

Que peças tem uma caixa? Cada entrada de cada um dos inventários da caixa, com a sua peça, a
categoria dessa peça e a sua cor, e o [`shared/set_part_details.sqlc`][shared/set_part_details.sqlc]
di-lo uma só vez:

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

O seu `WHERE` é uma seleção, que fica com as linhas de uma caixa, a do valor dado a `set_num`, e a
lista do seu `SELECT` é uma projeção, as colunas com que se fica; as junções emparelham cada entrada
com o seu inventário, a sua peça, a categoria dessa peça e a sua cor, e os `LEFT JOIN` mantêm uma
entrada a que o catálogo não sabe dar nome.

Compõe-no uma listagem, o [`sets/select_set_parts.sqlc`][sets/select_set_parts.sqlc]:

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

Compõe-se em [`.sql/sets/select_set_parts.sql`][.sql/sets/select_set_parts.sql], onde o corpo da
junção ficou no `WITH`, no lugar do `:compose`, e o `$1` no do `:bind(set_num)`, e onde as linhas
`#` dos dois modelos desapareceram.

```sh
cargo run -p brick-example -- parts 10182-1
cargo run -p brick-example -- parts 75053-1      # uma caixa vendida em duas versões
```

Cada um imprime as entradas da caixa versão a versão, com o inventário de cada versão, as suas
entradas e as suas peças, deixando de fora as sobresselentes; voltam todas as entradas dos
inventários da caixa, e nenhuma duas vezes, e uma caixa vendida em duas versões é listada e somada
versão a versão. Nas duas caixas, as peças de cada versão são o número que o catálogo regista para a
caixa, o `num_parts`.

O passo seguinte leva a mesma relação, sem lhe mexer, para instruções que escrevem.

### 2. A mesma relação num INSERT, num UPDATE e num MERGE

Quantas peças de cada categoria tem uma caixa, versão a versão, guardadas numa tabela? O
[`shared/set_category_totals.sqlc`][shared/set_category_totals.sqlc] reduz as entradas de uma caixa
a uma linha por versão e categoria:

```sql
# A set's piece totals per version and category: one row per (version, category_name), the pieces
# that are not spares and the spares apart. A category with no spare lines has a spare total of 0,
# and one with only spare lines a part total of 0. A line with no category (its part is not in the
# catalogue) is not totalled here; the parts listing shows it.
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
)
SELECT
    version,
    category_name,
    COALESCE(SUM(quantity) FILTER (WHERE NOT is_spare), 0) AS total_parts,
    COALESCE(SUM(quantity) FILTER (WHERE is_spare), 0) AS total_spare
FROM set_part_details
WHERE category_name IS NOT NULL
GROUP BY version, category_name
```

O [`reports/insert_set_summary.sqlc`][reports/insert_set_summary.sqlc] escreve essas linhas na
`set_category_summary`, uma das tabelas que o `migrate` cria:

```sql
# Populate the per-category part summary for a set: one row per (set, version, category). A set
# sold in several versions gets one summary per version, never a sum across them. The totals are
# the shared ones, so they always agree with the parts listing. Running it again updates each
# category's row rather than adding a second; a category the set no longer has keeps its row.
INSERT INTO set_category_summary (set_num, version, category_name, total_parts, total_spare)
SELECT
    :bind(set_num),
    version,
    category_name,
    total_parts,
    total_spare
FROM (
    :compose(shared/set_category_totals.sqlc)
) t
ON CONFLICT (set_num, version, category_name) DO UPDATE
SET total_parts = EXCLUDED.total_parts,
    total_spare = EXCLUDED.total_spare
```

Compõe-se em [`.sql/reports/insert_set_summary.sql`][.sql/reports/insert_set_summary.sql], com os
totais no seu `FROM` e as entradas da caixa dentro deles, dois níveis abaixo. O `set_num` é recebido
duas vezes, como o valor inserido e como a caixa pela qual as entradas são selecionadas, e das duas
vezes é `$1`.

```sh
cargo run -p brick-example -- summary 10182-1
```

Imprime quantas linhas escreveu e, depois, as categorias, as peças e as sobresselentes de cada
versão, lidas de volta da tabela; corrido duas vezes, deixa uma linha por versão e categoria, cada
uma com os totais das entradas da caixa.

O [`inventory/update_spare_counts.sqlc`][inventory/update_spare_counts.sqlc] compõe as entradas da
caixa num `UPDATE`:

```sql
# Sync the spare counts in inventory_tracking with the set's lines, version by version: a tracked
# part's spare count is the sum of its spare lines across colours in that version, and 0 where it
# has none. A tracked part the set's lines no longer hold keeps its count.
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
)
UPDATE inventory_tracking it
SET
    spare_count = COALESCE(spd.total_spare, 0),
    updated_at = NOW()
FROM (
    SELECT version, part_num, SUM(quantity) FILTER (WHERE is_spare) AS total_spare
    FROM set_part_details
    GROUP BY version, part_num
) spd
WHERE it.set_num = :bind(set_num)
  AND it.version = spd.version
  AND it.part_num = spd.part_num
```

As linhas que ele atualiza, quem as põe lá primeiro é o
[`inventory/track_set_parts.sqlc`][inventory/track_set_parts.sqlc], um `INSERT … SELECT DISTINCT`
sobre as mesmas entradas, que deixa como está uma peça que já se acompanhava, e o `spares` corre os
dois, por essa ordem:

```sh
cargo run -p brick-example -- spares 10182-1
```

Imprime quantas peças passou a acompanhar e, depois, quantas linhas atualizou.

O [`reports/merge_set_summary.sqlc`][reports/merge_set_summary.sqlc] mantém o resumo com um `MERGE`
sobre os mesmos totais: atualiza uma linha que os totais ainda têm, insere a que falte e não apaga
nada:

```sh
cargo run -p brick-example -- merge 75053-1
```

Imprime quantas linhas passaram pelo `MERGE`, e um resumo cujas linhas tenham sido mudadas ou
tiradas à mão volta, depois do `MERGE`, ao que o `INSERT` escreve.

São quatro as instruções que escrevem a partir das entradas de uma caixa, e nenhum dos seus modelos
escreve a junção. Essas instruções, o compositor escreve-as sem executar nenhuma, e o `ON CONFLICT`
precisa da chave única com que cada tabela é criada, coisa que a base de dados confere quando a
instrução corre.

O passo seguinte mantém as entradas da caixa e estreita-as com um filtro, à escolha de quem as
compõe.

### 3. Um filtro passado numa vaga

Que entradas de uma caixa são placas, e quais são pretas? Responde às duas uma só base, com o filtro
deixado em aberto numa vaga: o [`shared/filtered_set_parts.sqlc`][shared/filtered_set_parts.sqlc]
são as entradas de uma caixa que a relação dada em `@filter` deixa ficar, seja ela qual for, à
escolha de quem o compõe:

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

Um filtro é uma relação de padrões, e o [`filters/by_category.sqlc`][filters/by_category.sqlc] dá um
padrão por cada peça de uma categoria, com qualquer cor:

```sql
# A part category, as filter patterns: (this part, any colour) for every part in the category. A
# category belongs to the part, so the patterns come from the parts catalogue and not from any
# inventory. A filter's contract is in shared/filtered_set_parts.sqlc.
SELECT DISTINCT
    p.part_num,
    NULL::integer AS color_id
FROM lego_parts p
JOIN lego_part_categories pc ON pc.id = p.part_cat_id
WHERE pc.name = :bind(category_name)
```

O [`sets/select_category_parts.sqlc`][sets/select_category_parts.sqlc] preenche a vaga com ele:

```sql
# A set's lines whose part is in the category category_name, one row per line, keyed by the line:
# the set's lines with the category's filter in the @filter slot.
:compose(shared/filtered_set_parts.sqlc, @filter = filters/by_category.sqlc)
```

Compõe-se em [`.sql/sets/select_category_parts.sql`][.sql/sets/select_category_parts.sql], com o
corpo do filtro no `WITH`, onde estava o `:compose(@filter)`, e com o `category_name` como `$1` e o
`set_num` como `$2`, pela ordem dos seus nomes. A base, com a vaga em aberto, não é uma consulta,
pelo que a `.sql/shared` não tem ficheiro nenhum para ela.

```sh
cargo run -p brick-example -- by-category 10182-1 "Plates"
cargo run -p brick-example -- by-color 10182-1 "Black"
```

O `by-color` preenche a mesma vaga com o [`filters/by_color.sqlc`][filters/by_color.sqlc], uma cor
com qualquer peça, no [`sets/select_colored_parts.sqlc`][sets/select_colored_parts.sqlc]. Cada um
imprime as entradas da caixa que passam no filtro, cada entrada uma só vez; e, como a categoria
pertence à peça e a cor à entrada, cada filtro só nomeia a coluna de que trata. A base, essa, fica
escrita uma vez, seja qual for o filtro.

A mesma vaga, preenchem-na também as pesquisas dos passos 15 e 16; o passo seguinte deixa as
entradas de uma caixa e passa aos temas a que as caixas pertencem.

### 4. O fecho dos temas

Que temas estão abaixo de um tema? Um tema tem um pai, e esse pai tem por sua vez o seu, de modo que
o âmbito de um tema é esse tema e todos os que estão abaixo dele, a qualquer profundidade. O
[`shared/theme_closure.sqlc`][shared/theme_closure.sqlc] emparelha cada tema consigo próprio e com
cada tema abaixo dele:

```sql
# Every theme paired with every theme at or below it: one row per (ancestor_id, theme_id), each
# theme its own ancestor at depth 0. A theme scope is a theme's whole subtree, so a scope joins this
# relation on theme_id and picks its ancestor_id. It never matches one theme id alone, and never a
# name, which several themes can share.
# The CYCLE clause guarantees the walk ends even if a parent_id ever pointed back into its own
# subtree; such a theme would be marked, and left out.
WITH RECURSIVE closure (ancestor_id, theme_id, depth) AS (
    SELECT t.id, t.id, 0
    FROM lego_themes t
  UNION ALL
    SELECT c.ancestor_id, t.id, c.depth + 1
    FROM lego_themes t
    JOIN closure c ON t.parent_id = c.theme_id
) CYCLE theme_id SET in_cycle USING walked
SELECT ancestor_id, theme_id, depth
FROM closure
WHERE NOT in_cycle
```

Não recebe parâmetro nenhum, pelo que se compõe em si mesmo, já sem as linhas `#`, no
[`.sql/shared/theme_closure.sql`][.sql/shared/theme_closure.sql], e o ficheiro composto corre
sozinho:

```sh
PGOPTIONS='-c search_path=lego' psql -d sqlc_brick -f examples/brick/.sql/shared/theme_closure.sql
```

As linhas de um mesmo `ancestor_id` são um âmbito: esse tema e todos os que estão abaixo dele, cada
um à sua profundidade.

É este fecho que os passos seguintes compõem.

### 5. Uma lista de ids

Que caixas, de 2010 em diante, estão nos temas Technic, City ou Star Wars, ou em qualquer tema
abaixo deles? O [`sets/select_sets_by_themes.sqlc`][sets/select_sets_by_themes.sqlc] recebe os temas
numa só lista, dada a um `IN`:

```sql
# Find sets in any of the given theme scopes: each given theme and every theme below it. One row per
# set, however many of the given scopes contain it. The ORDER BY ends on set_num, so the order is
# total. Within a year, a set with no part count comes before the counted ones; the ORDER BY says
# so, because databases disagree on where a NULL sorts. A set with no year is never at or after
# min_year, so the year needs no such term. The given themes are one list, bound to an IN.
SELECT
    s.set_num,
    s.name,
    s.year,
    t.name AS theme_name,
    s.num_parts
FROM lego_sets s
JOIN lego_themes t ON t.id = s.theme_id
WHERE EXISTS (
    SELECT 1
    FROM (
        :compose(shared/theme_closure.sqlc)
    ) tc
    WHERE tc.theme_id = s.theme_id
      AND tc.ancestor_id IN (:bind(theme_ids EXPECTING 1..20))
)
  AND s.year >= :bind(min_year)
ORDER BY s.year DESC, s.num_parts IS NULL DESC, s.num_parts DESC, s.set_num
```

O `EXISTS` só pergunta se o tema de uma caixa está num dos âmbitos, sem juntar às linhas da caixa
nenhuma das do fecho, e o `EXPECTING 1..20` diz que a lista leva de um a vinte ids.

O ficheiro guardado no repositório, o
[`.sql/sets/select_sets_by_themes.sql`][.sql/sets/select_sets_by_themes.sql], tem um só marcador
para a lista, `IN ($2)`, mas o subcomando compõe o modelo com os seus valores, um marcador por id:
com três ids, o `IN` fica `IN ($2, $3, $4)`, e os parâmetros são o `min_year` e, depois, o
`theme_ids` três vezes. Dados vinte e um, a composição recusa a lista:
`binding 'theme_ids' was given 21 value(s), and expects 1..20`.

```sh
cargo run -p brick-example -- themes 2010 1 52 158
```

O 1 é o tema Technic, o 52 o City e o 158 o Star Wars, e o comando imprime as caixas, das mais
recentes para as mais antigas, cada uma com o nome do seu próprio tema.

O passo seguinte toma um âmbito de cada vez, como relação própria, e combina os âmbitos como
conjuntos.

### 6. Âmbitos, e a sua união

Que caixas, de 2010 em diante, são do Technic e quais são do City, cada uma com o nome do seu
âmbito? Um âmbito é uma relação própria, e o [`scopes/technic.sqlc`][scopes/technic.sqlc] é o do
Technic: cada tema que nele está, com o id do próprio Technic como `scope_id`.

```sql
# The Technic scope: the theme technic_theme_id and every theme below it, one row per theme_id, each
# with the scope's own theme as scope_id.
SELECT tc.ancestor_id AS scope_id, tc.theme_id
FROM (
    :compose(shared/theme_closure.sqlc)
) tc
WHERE tc.ancestor_id = :bind(technic_theme_id)
```

O [`scopes/city.sqlc`][scopes/city.sqlc] é o do City, igual a esse, mas com `city_theme_id`; cada
âmbito fica escrito uma vez, e cada pergunta sobre ele compõe-no. O
[`shared/scope_sets.sqlc`][shared/scope_sets.sqlc] recebe um âmbito na sua vaga `@scope` e dá a cada
caixa, como rótulo, o nome do tema do próprio âmbito:

```sql
# The sets of a theme scope released in min_year or later: one row per set, keyed by set_num, each
# labelled with the name of the scope's own theme. The caller decides the scope (the @scope slot):
# one row per theme_id, with the scope's theme as scope_id. The label is read through scope_id, so
# it always names the scope that chose the rows.
SELECT s.set_num, s.name, s.year, s.num_parts, scope.name AS theme_group
FROM lego_sets s
JOIN (
    :compose(@scope)
) sc ON sc.theme_id = s.theme_id
JOIN lego_themes scope ON scope.id = sc.scope_id
WHERE s.year >= :bind(min_year)
```

O [`queries/technic_sets.sqlc`][queries/technic_sets.sqlc] e o
[`queries/city_sets.sqlc`][queries/city_sets.sqlc] preenchem a vaga, um âmbito cada um, e o
[`reports/combined_theme_sets.sqlc`][reports/combined_theme_sets.sqlc] é a união deles:

```sql
# Technic's sets and City's, each released in min_year or later and labelled with its scope: one row
# per (theme_group, set_num). A set in both scopes comes back once under each label.
:union(queries/technic_sets.sqlc, queries/city_sets.sqlc)
```

Compõe-se em [`.sql/reports/combined_theme_sets.sql`][.sql/reports/combined_theme_sets.sql], com
cada fonte embrulhada numa tabela derivada sua, `_union_1` e `_union_2`, e o todo também, `_union`.
As duas fontes leem o `min_year`, que é um só marcador, `$2`, ao passo que o tema do City é `$1` e o
do Technic `$3`. Um `:union` escrito sem mais nada dá `UNION`, que tira as linhas repetidas; só que
o rótulo faz parte da linha, pelo que uma caixa que esteja nos dois âmbitos dá duas linhas, e ficam
ambas.

```sh
cargo run -p brick-example -- combined 2010
```

Imprime cada caixa com o nome do seu âmbito: estão lá as caixas de cada âmbito desse ano em diante,
cada uma sob o nome do seu âmbito, e nenhuma linha vem duas vezes.

O passo seguinte pede aos mesmos dois âmbitos os seus moldes, e fica com a interseção e a diferença
deles.

### 7. Interseção e diferença

Que moldes partilham as caixas Technic e City, e quais só o City usa? Um molde é um número de peça,
e o [`shared/scope_moulds.sqlc`][shared/scope_moulds.sqlc] são os moldes que alguma caixa de um
âmbito usa, com o âmbito na sua vaga `@scope`:

```sql
# The moulds (part numbers) used by any set in a theme scope: one row per part_num. The caller
# decides the scope (the @scope slot): one row per theme_id, with the scope's own theme as scope_id.
SELECT DISTINCT ip.part_num
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
WHERE s.theme_id IN (
    SELECT sc.theme_id
    FROM (
        :compose(@scope)
    ) sc
)
```

O [`queries/technic_moulds.sqlc`][queries/technic_moulds.sqlc] e o
[`queries/city_moulds.sqlc`][queries/city_moulds.sqlc] preenchem-na, um âmbito cada um, e o
[`reports/shared_moulds.sqlc`][reports/shared_moulds.sqlc] é a interseção deles:

```sql
# The moulds both Technic and City sets use: one row per part_num.
:intersect(queries/technic_moulds.sqlc, queries/city_moulds.sqlc)
```

e o [`reports/city_only_moulds.sqlc`][reports/city_only_moulds.sqlc] a diferença, os do City menos
os do Technic:

```sql
# The moulds City sets use and no Technic set does: one row per part_num. The order of the sources
# matters: the first is the one the second is taken from.
:except(queries/city_moulds.sqlc, queries/technic_moulds.sqlc)
```

Ambos se compõem como a união, cada fonte embrulhada e o todo também, em
[`.sql/reports/shared_moulds.sql`][.sql/reports/shared_moulds.sql] e
[`.sql/reports/city_only_moulds.sql`][.sql/reports/city_only_moulds.sql]; e, como cada fonte compõe
o seu âmbito e cada âmbito o fecho dos temas, cada ficheiro guarda o fecho duas vezes.

```sh
cargo run -p brick-example -- shared-moulds
cargo run -p brick-example -- city-only-moulds
```

Cada um imprime quantos moldes encontrou, e os dois repartem entre si os moldes do City: nenhum está
em ambos, e juntos são todos eles.

O passo seguinte reduz uma relação a um número.

### 8. Contagem

Quantos moldes distintos usam as caixas do Star Wars, contando com as de todos os temas abaixo dele?
O [`queries/theme_set_parts.sqlc`][queries/theme_set_parts.sqlc] é cada entrada de cada caixa de um
âmbito:

```sql
# Every line of every inventory of every set in a theme scope (the theme and every theme below it),
# one row per line, keyed by the line.
SELECT ip.inventory_id, ip.part_num, ip.color_id, ip.is_spare
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
JOIN (
    :compose(shared/theme_closure.sqlc)
) tc ON tc.theme_id = s.theme_id
WHERE tc.ancestor_id = :bind(theme_id)
```

O [`reports/count_theme_parts.sqlc`][reports/count_theme_parts.sqlc] conta os seus números de peça
distintos:

```sql
# How many distinct moulds (part numbers) the sets of a theme scope use, the theme and every theme
# below it: one row, one count.
:count(DISTINCT part_num OF queries/theme_set_parts.sqlc)
```

Compõe-se em [`.sql/reports/count_theme_parts.sql`][.sql/reports/count_theme_parts.sql]:

```sql
SELECT COUNT(DISTINCT part_num) FROM (
SELECT ip.inventory_id, ip.part_num, ip.color_id, ip.is_spare
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
JOIN (
    WITH RECURSIVE closure (ancestor_id, theme_id, depth) AS (
    SELECT t.id, t.id, 0
    FROM lego_themes t
  UNION ALL
    SELECT c.ancestor_id, t.id, c.depth + 1
    FROM lego_themes t
    JOIN closure c ON t.parent_id = c.theme_id
) CYCLE theme_id SET in_cycle USING walked
SELECT ancestor_id, theme_id, depth
FROM closure
WHERE NOT in_cycle

) tc ON tc.theme_id = s.theme_id
WHERE tc.ancestor_id = $1

) AS _count_sub
```

```sh
cargo run -p brick-example -- count 158
```

Imprime um só número, e o seu âmbito são todos os temas abaixo do Star Wars, a qualquer
profundidade, e nenhum outro.

Cada uma das instruções acima guarda o fecho uma vez por cada caminho que lá vai dar, e o passo
seguinte nomeia-o uma só vez.

### 9. Cada relação nomeada uma só vez

Podem os moldes que o Technic e o City partilham compor-se com o fecho escrito uma só vez? O
`--with` nomeia uma só vez cada relação que uma instrução lê, à cabeça da instrução; para compor
numa pasta à parte:

```sh
cargo sqlc compose --source examples/brick/sqlc --target target/brick-with --skip-prepare --with
```

O `target/brick-with/reports/shared_moulds.sql` fica então assim:

```sql
WITH shared_theme_closure AS (
WITH RECURSIVE closure (ancestor_id, theme_id, depth) AS (
    SELECT t.id, t.id, 0
    FROM lego_themes t
  UNION ALL
    SELECT c.ancestor_id, t.id, c.depth + 1
    FROM lego_themes t
    JOIN closure c ON t.parent_id = c.theme_id
) CYCLE theme_id SET in_cycle USING walked
SELECT ancestor_id, theme_id, depth
FROM closure
WHERE NOT in_cycle
)
SELECT * FROM (
SELECT * FROM (
SELECT DISTINCT ip.part_num
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
WHERE s.theme_id IN (
    SELECT sc.theme_id
    FROM (
        SELECT tc.ancestor_id AS scope_id, tc.theme_id
FROM (
    SELECT * FROM shared_theme_closure
) tc
WHERE tc.ancestor_id = $2

    ) sc
)
) AS _intersect_1
INTERSECT
SELECT * FROM (
SELECT DISTINCT ip.part_num
FROM lego_inventory_parts ip
JOIN lego_inventories i ON i.id = ip.inventory_id
JOIN lego_sets s ON s.set_num = i.set_num
WHERE s.theme_id IN (
    SELECT sc.theme_id
    FROM (
        SELECT tc.ancestor_id AS scope_id, tc.theme_id
FROM (
    SELECT * FROM shared_theme_closure
) tc
WHERE tc.ancestor_id = $1

    ) sc
)
) AS _intersect_2
) AS _intersect
```

O fecho não recebe parâmetros, pelo que é nomeado uma só vez, `shared_theme_closure`, e é lido por
cada âmbito como `SELECT * FROM shared_theme_closure`; os âmbitos, esses, recebem os ids dos seus
temas, e por isso cada um continua escrito onde é composto. Na base de dados não se cria nada: é a
instrução do passo 7, com cada relação que ela lê escrita uma só vez.

Os subcomandos compõem sem o `--with`, pelo que o `shared-moulds` corre a grafia do passo 7, e daqui
em diante os passos vão mais fundo no SQL do PostgreSQL.

## Mais fundo no SQL do PostgreSQL

Os modelos daqui em diante compõem-se como os passos de cima mostram, e cada subcomando imprime a
instrução composta antes das suas linhas.

### 10. Compras, lidas através da sua linha da coleção

O que comprou um construtor, e quando? Uma compra nomeia uma linha da coleção do seu construtor, e a
caixa é a dessa linha, pelo que o [`shared/purchase_sets.sqlc`][shared/purchase_sets.sqlc] lê a
caixa através da linha, uma só vez:

```sql
# Every purchase with its builder and the set it bought, one row per purchase, keyed by
# purchase_id. A purchase names a row of its builder's collection, and the set is that row's: the
# purchase never carries the set itself, so the set is read through the collection row.
SELECT
    p.purchase_id,
    p.builder_id,
    c.set_num,
    p.ordered_at,
    p.delivered_at
FROM lego_purchases p
JOIN lego_collection c ON c.builder_id = p.builder_id AND c.row_no = p.row_no
```

O [`builders/purchase_history.sqlc`][builders/purchase_history.sqlc] numera as compras de um
construtor pela ordem em que foram feitas, com as peças compradas até aí, por meio de funções de
janela:

```sql
# A builder's purchases in the order they were placed, each numbered, with the pieces bought so far.
# One row per purchase, keyed by purchase_id. The window's order ends on purchase_id, so it is
# total: two purchases placed at the same instant still take a place each, and the running total
# moves one purchase at a time.
WITH purchase_sets AS (
    :compose(shared/purchase_sets.sqlc)
)
SELECT
    row_number() OVER w AS nth,
    ps.purchase_id,
    ps.ordered_at,
    ps.set_num,
    s.name,
    s.num_parts,
    sum(s.num_parts) OVER w AS pieces_so_far
FROM purchase_sets ps
JOIN lego_sets s ON s.set_num = ps.set_num
WHERE ps.builder_id = :bind(builder_id)
WINDOW w AS (ORDER BY ps.ordered_at, ps.purchase_id)
ORDER BY nth
```

```sh
cargo run -p brick-example -- history 3326
```

Imprime as compras do construtor por ordem, cada uma com o seu número, as suas peças e as peças até
aí, e o último total acumulado são todas as peças que o construtor comprou.

Há mais duas perguntas que compõem as compras: o
[`builders/latest_purchases.sqlc`][builders/latest_purchases.sqlc] fica com a última compra de cada
construtor graças ao `DISTINCT ON`, que mantém a primeira linha de cada construtor no seu
`ORDER BY`, e o [`sets/never_bought.sqlc`][sets/never_bought.sqlc] fica, com um `NOT EXISTS`, com as
caixas a que nenhuma compra chega.

```sh
cargo run -p brick-example -- latest
cargo run -p brick-example -- never-bought
```

O `latest` imprime uma linha por cada construtor que tenha comprado alguma coisa, e o `never-bought`
as caixas nunca compradas, que, juntas com as compradas, são todas as caixas, sem nenhuma que seja
as duas coisas.

O passo seguinte lê, através do fecho, o tema raiz de cada caixa.

### 11. O tema raiz de cada caixa

A que tema raiz pertence uma caixa? Um tema raiz não tem pai, e o
[`shared/set_roots.sqlc`][shared/set_roots.sqlc] junta cada caixa à raiz que está acima do seu tema:

```sql
# Every set with the root theme at the top of its theme's tree, one row per set, keyed by set_num.
# A root theme has no parent, and the theme closure pairs each theme with every theme above it, so
# each set meets exactly one root.
SELECT
    s.set_num,
    s.name,
    s.year,
    s.num_parts,
    r.id AS root_id,
    r.name AS root_name
FROM lego_sets s
JOIN (
    :compose(shared/theme_closure.sqlc)
) tc ON tc.theme_id = s.theme_id
JOIN lego_themes r ON r.id = tc.ancestor_id
WHERE r.parent_id IS NULL
```

Há três perguntas que leem os temas raiz:

- [`reports/theme_set_sizes.sqlc`][reports/theme_set_sizes.sqlc]: o tamanho mediano das caixas de
  cada tema raiz, com o menor e o maior; o `percentile_cont(0.5)` toma o tamanho do meio ou, havendo
  dois no meio, o ponto a meio caminho entre eles.
- [`reports/purchases_by_year_theme.sqlc`][reports/purchases_by_year_theme.sqlc]: as compras por ano
  e por tema raiz, compondo as compras e as raízes; o `ROLLUP` junta o subtotal de cada ano e o
  total geral, e o `GROUPING` dá um rótulo a essas linhas, para que um subtotal nunca seja lido como
  um tema sem nome.
- [`themes/newest_sets.sqlc`][themes/newest_sets.sqlc]: as três caixas mais recentes de cada tema
  raiz, venham de onde vierem abaixo dele, através do fecho; o `LATERAL` deixa a subconsulta ler o
  tema da linha a que está junta, e assim o `LIMIT 3` fica com três por tema.

```sh
cargo run -p brick-example -- sizes
cargo run -p brick-example -- rollup 2016 2017
cargo run -p brick-example -- newest
```

O `sizes` imprime, por cada tema raiz, as suas caixas, quantas delas têm tamanho, e o menor, o
mediano e o maior; o `rollup` imprime quantas linhas encontrou e, depois, o subtotal de cada ano e o
total geral, em que os temas de um ano somam o subtotal desse ano e os anos somam o total; e o
`newest` imprime até três caixas por tema raiz, das mais recentes para as mais antigas.

O passo seguinte conta as caixas de cada ano, incluindo os anos que não têm nenhuma.

### 12. Todos os anos, sem deixar de fora os vazios

Em que anos não saiu nenhuma caixa? O [`reports/release_years.sqlc`][reports/release_years.sqlc]
escreve o calendário com `generate_series` e conta as caixas por ele:

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

```sh
cargo run -p brick-example -- years
```

Imprime o intervalo dos anos e os anos sem lançamentos, e cada ano, do primeiro lançamento ao
último, aparece uma só vez, com 0 quando nesse ano não saiu nada.

O passo seguinte pergunta que construtores têm todas as caixas de um âmbito.

### 13. Os construtores que têm um âmbito inteiro

Quem tem todas as caixas do âmbito de um tema? Esse «todas» é uma divisão, em que um construtor fica
quando as caixas do âmbito, tiradas as que estão na sua coleção, não deixam nada, e é assim que o
[`builders/complete_scope.sqlc`][builders/complete_scope.sqlc] a escreve:

```sql
# The builders who own every set of a theme scope (the theme and every theme below it): one row per
# builder, keyed by builder_id. A builder is kept when the scope's sets, less the sets in the
# builder's collection, leave nothing. A scope with no sets would keep every builder, so the scope
# must hold at least one set.
WITH scope_sets AS (
    SELECT s.set_num
    FROM lego_sets s
    JOIN (
        :compose(shared/theme_closure.sqlc)
    ) tc ON tc.theme_id = s.theme_id
    WHERE tc.ancestor_id = :bind(theme_id)
)
SELECT b.builder_id, b.name
FROM lego_builders b
WHERE EXISTS (SELECT 1 FROM scope_sets)
  AND NOT EXISTS (
    SELECT set_num FROM scope_sets
    EXCEPT
    SELECT c.set_num FROM lego_collection c WHERE c.builder_id = b.builder_id
)
ORDER BY b.builder_id
```

```sh
cargo run -p brick-example -- complete 463      # 463 é o Spybiotics
```

Imprime os construtores que têm todas as caixas do âmbito. Um âmbito sem caixas não deixa ficar
ninguém, graças ao primeiro `EXISTS` do modelo, e as leis dão ao modelo um âmbito assim, sobre
linhas que são delas.

O passo seguinte junta as linhas de uma caixa num único valor.

### 14. Uma caixa em arrays, e num só documento

Que cores usa uma caixa, e como fica uma caixa escrita num só valor? O
[`sets/set_colors.sqlc`][sets/set_colors.sqlc] junta num array as cores de cada versão:

```sql
# A set's colours, version by version: one row per version, keyed by version, with every colour
# once, in name order. A line whose colour is missing from the catalogue has no colour name; it is
# counted in unnamed_lines and kept out of the array, where it would be a NULL.
WITH set_part_details AS (
    :compose(shared/set_part_details.sqlc)
)
SELECT
    version,
    array_agg(DISTINCT color_name ORDER BY color_name)
        FILTER (WHERE color_name IS NOT NULL) AS colors,
    count(*) FILTER (WHERE color_name IS NULL) AS unnamed_lines
FROM set_part_details
GROUP BY version
ORDER BY version
```

O [`sets/set_document.sqlc`][sets/set_document.sqlc] constrói a caixa como um só documento `jsonb`:
o seu número, nome, ano e tema, e cada versão com as suas entradas.

```sh
cargo run -p brick-example -- colors 75053-1
cargo run -p brick-example -- document 10182-1
```

O `colors` imprime as cores de cada versão e quantas entradas não têm nome de cor, e cada versão tem
tantas cores quantas as que as suas entradas nomeiam; o `document` imprime o documento, onde cada
entrada da caixa está uma vez.

Os passos daqui em diante pesquisam.

## Pesquisar com os índices do gerador

As pesquisas leem os índices de pesquisa que a preparação constrói, e cada pesquisa é uma relação
como outra qualquer, que preenche uma vaga tal como os filtros do passo 3.

### 15. Por palavras

Que entradas de uma caixa são janelas? O [`filters/by_words.sqlc`][filters/by_words.sqlc] são as
peças cujo nome corresponde a uma pesquisa, como padrões de filtro:

```sql
# The parts whose names match a search, as filter patterns: (this part, any colour). The search is
# written as a web search engine takes one (websearch_to_tsquery): words that must all be there,
# "a phrase", or, and -a word to leave out, each word taken to its English stem. The parts' words
# index serves it. A filter's contract is in shared/filtered_set_parts.sqlc.
SELECT DISTINCT
    p.part_num,
    NULL::integer AS color_id
FROM lego_parts p
WHERE to_tsvector('english', p.name) @@ websearch_to_tsquery('english', :bind(words))
```

O [`sets/select_parts_by_words.sqlc`][sets/select_parts_by_words.sqlc] preenche com ele o `@filter`
do passo 3, e o [`parts/search_parts.sqlc`][parts/search_parts.sqlc] pesquisa o nome de todas as
peças, com as que melhor correspondem primeiro: o `ts_rank` pontua até que ponto um nome corresponde
à pesquisa, e o `ts_headline` marca as palavras encontradas.

```sh
cargo run -p brick-example -- by-words 10182-1 "window"
cargo run -p brick-example -- search "technic beam"
```

O `by-words` imprime as entradas da caixa cujo nome de peça tem as palavras, cada entrada uma só
vez, e o `search` as peças que têm todas as palavras, as melhores primeiro, cada uma com a sua
pontuação e o seu nome, com as palavras encontradas entre asteriscos.

### 16. Por padrão

Que entradas de uma caixa são placas ou ladrilhos com um pino de largura? O
[`filters/by_pattern.sqlc`][filters/by_pattern.sqlc] são as peças cujo nome corresponde a uma
expressão regular, sem distinguir maiúsculas de minúsculas:

```sql
# The parts whose names match a regular expression, as filter patterns: (this part, any colour). The
# match ignores case (~*), and the parts' trigram index serves it. A filter's contract is in
# shared/filtered_set_parts.sqlc.
SELECT DISTINCT
    p.part_num,
    NULL::integer AS color_id
FROM lego_parts p
WHERE p.name ~* :bind(pattern)
```

O [`sets/select_parts_by_pattern.sqlc`][sets/select_parts_by_pattern.sqlc] preenche com ele a mesma
vaga, e o [`parts/brick_sizes.sqlc`][parts/brick_sizes.sqlc] lê, com o `regexp_match`, o tamanho de
um tijolo no seu próprio nome, como em `Brick 2 x 4`.

```sh
cargo run -p brick-example -- by-pattern 10182-1 '^(plate|tile) 1 x \d+$'
cargo run -p brick-example -- brick-sizes
```

O `by-pattern` imprime as entradas da caixa cujo nome de peça corresponde ao padrão, cada entrada
uma só vez, e o `brick-sizes` cada tijolo cujo nome diz o tamanho, com a sua largura e o seu
comprimento.

### 17. Por lugar

O que compraram os construtores que vivem perto de uma cidade? O
[`shared/area_purchases.sqlc`][shared/area_purchases.sqlc] é o que compraram os construtores de uma
área, por tema raiz, com a área na sua vaga `@area`:

```sql
# What the builders in an area bought, by the root theme of the set: one row per root theme, keyed
# by root_id, with the purchases and the builders who made them, most purchases first. The caller
# decides the area (the @area slot): builders, with one column, builder_id. A builder the area
# names twice still counts once, since the area only decides who is in it.
WITH purchase_sets AS (
    :compose(shared/purchase_sets.sqlc)
),
set_roots AS (
    :compose(shared/set_roots.sqlc)
),
area AS (
    :compose(@area)
)
SELECT
    sr.root_id,
    sr.root_name,
    count(*) AS purchases,
    count(DISTINCT ps.builder_id) AS builders
FROM purchase_sets ps
JOIN set_roots sr ON sr.set_num = ps.set_num
WHERE ps.builder_id IN (SELECT a.builder_id FROM area a)
GROUP BY sr.root_id, sr.root_name
ORDER BY purchases DESC, sr.root_id
```

O [`areas/near_city.sqlc`][areas/near_city.sqlc] são os construtores dentro de um raio à volta de
uma cidade:

```sql
# The builders whose homes lie within km kilometres of a city, on the earth's surface: one row per
# builder, keyed by builder_id, with the distance in metres. The city is chosen by its city_id.
# earth_box is the cube that holds the circle, and the builders' home index serves it;
# earth_distance then keeps the homes inside the circle itself, since the cube's corners lie
# outside it.
SELECT
    b.builder_id,
    earth_distance(ll_to_earth(c.latitude, c.longitude), ll_to_earth(b.latitude, b.longitude))
        AS metres
FROM lego_cities c
JOIN lego_builders b
  ON earth_box(ll_to_earth(c.latitude, c.longitude), :bind(km) * 1000)
     @> ll_to_earth(b.latitude, b.longitude)
WHERE c.city_id = :bind(city_id)
  AND earth_distance(ll_to_earth(c.latitude, c.longitude), ll_to_earth(b.latitude, b.longitude))
      <= :bind(km) * 1000
```

O [`builders/bought_near.sqlc`][builders/bought_near.sqlc] preenche com ele a vaga `@area`; o
[`areas/postcodes.sqlc`][areas/postcodes.sqlc] são os construtores cujo código postal corresponde a
uma expressão regular, e o [`builders/bought_in_postcodes.sqlc`][builders/bought_in_postcodes.sqlc]
preenche a vaga `@area` com eles. Já o [`builders/nearest.sqlc`][builders/nearest.sqlc] são os
construtores mais próximos de uma cidade, do mais próximo para o mais afastado: dentro de um
`LATERAL`, o `<->` ordena as casas pela distância, o índice das casas percorre-as por essa ordem e
só se leem as mais próximas.

```sh
cargo run -p brick-example -- bought-near 67 5      # 67 é Londres; num raio de 5 km
cargo run -p brick-example -- bought-in '^SW'       # o distrito SW de Londres
cargo run -p brick-example -- nearest 67
```

O `bought-near` e o `bought-in` imprimem, por cada tema raiz, as compras e os construtores que as
fizeram, primeiro os temas com mais compras. Os construtores dentro do raio são os que a distância
sobre a superfície da Terra lá põe, e as compras somam todas as que eles fizeram de caixas com tema;
os construtores de um padrão de código postal ancorado num prefixo são os construtores cujo código
postal começa por esse prefixo. O `nearest` imprime os cinco construtores mais próximos da cidade,
com a sua distância em metros, e não há outro mais perto do que eles.

O último passo verifica o próprio catálogo.

## Verificar o catálogo

### 18. Um rol de verificações

Que entradas nomeiam uma peça que o catálogo de peças não tem, e que caixas não têm entrada nenhuma?
Cada pergunta é uma verificação, e a regra de cada verificação fica escrita uma vez, num rol, que o
[`checks/roster.sqlc`][checks/roster.sqlc] dá como `VALUES`, uma linha por verificação:

```sql
# The example's checks, one row per check, keyed by slug: the slug each check is cut to, and the
# rule it finds rows against. Each check reads its rule from here, so a rule is written once.
SELECT * FROM (VALUES
    ('lines_uncatalogued', 'a line whose part is not in the parts catalogue'),
    ('sets_without_lines', 'a set with no inventory line')
) AS roster (slug, rule)
```

Uma verificação lê o rol, cortado à sua própria linha pelo seu `slug`, e junta-lhe à esquerda,
`ON true`, as linhas que encontra. Assim faz o
[`checks/lines_uncatalogued.sqlc`][checks/lines_uncatalogued.sqlc]:

```sql
# The inventory lines whose part is not in the parts catalogue, under the roster's row for this
# check: one row per line, with the set its inventory belongs to, and the line itself in detail.
# Where every line's part is in the catalogue, the roster's row comes back alone.
SELECT r.rule, p.set_num, p.detail
FROM (
    :compose(checks/roster.sqlc)
) r
LEFT JOIN (
    SELECT
        i.set_num,
        format('inventory %s, version %s: part %s in colour %s%s',
            ip.inventory_id, i.version, ip.part_num, ip.color_id,
            CASE WHEN ip.is_spare THEN ', a spare' ELSE '' END) AS detail
    FROM lego_inventory_parts ip
    JOIN lego_inventories i ON i.id = ip.inventory_id
    WHERE NOT EXISTS (SELECT 1 FROM lego_parts pt WHERE pt.part_num = ip.part_num)
) p ON true
WHERE r.slug = 'lines_uncatalogued'
```

As suas linhas são as entradas que o passo 1 marca como `uncatalogued`. O `ON true` junta a linha da
verificação no rol a cada entrada que ela encontra, de modo que cada linha leva consigo a regra da
verificação; e, quando a verificação nada encontra, o `LEFT JOIN` mantém sozinha a linha do rol, sem
`set_num` nem `detail`. Uma verificação que não encontre nada devolve, ainda assim, a sua regra, e
nunca falta entre as linhas. O [`checks/sets_without_lines.sqlc`][checks/sets_without_lines.sqlc]
tem a mesma estrutura, sobre as caixas: uma caixa cujos inventários não têm entrada nenhuma, e uma
caixa sem inventário. O [`checks/all.sqlc`][checks/all.sqlc] é a união das duas:

```sql
# Every check's findings, one row each, and a row with no set_num for a check that finds nothing.
:union(ALL checks/lines_uncatalogued.sqlc, checks/sets_without_lines.sqlc)
```

Compõe-se em [`.sql/checks/all.sql`][.sql/checks/all.sql], com cada verificação embrulhada tal como
a união do passo 6 embrulha as suas fontes; e, como cada uma guarda o corpo do rol, a instrução lê o
rol uma vez por verificação. O `ALL` dá `UNION ALL`, que mantém todas as linhas de todas as
verificações e não procura repetidas para tirar. O `cargo sqlc compose --respell` escreve a união
noutra grafia, no ficheiro composto, de modo que leia o rol uma só vez, como a página do
sql-composer mostra em [*Uma união de braços que leem um mesmo rol*][--respell].

```sh
cargo run -p brick-example -- checks
```

Imprime a regra de cada verificação, quantas linhas encontrou e as dez primeiras, por caixa, e uma
verificação que não encontre nada imprime só a sua regra. As entradas cuja peça o catálogo não tem
são do próprio catálogo real, que o gerador escreve diga o `--traps` o que disser. As linhas da
união são as das duas verificações, cada uma tantas vezes quantas a sua verificação a devolve, e, na
nova grafia, a união devolve as mesmas linhas.

O percurso acaba nas leis.

## As leis

```sh
cargo run -p brick-example -- laws
```

O `laws` confere as promessas do percurso com uma segunda grafia da mesma resposta, escrita em SQL
direto no [`src/main.rs`][src/main.rs]. Cada lei imprime `HOLDS` ou `FAILS`, com os números dos dois
lados, e o `laws` sai com 1 se alguma falhar; uma lei que escreve corre dentro de uma transação que
é revertida, pelo que conferir não deixa nada para trás. O `cargo run -p brick-example -- all` corre
todos os passos com os valores de cima e, no fim, as leis.

| passo | a promessa | conferida com |
|---|---|---|
| 1 | as peças de uma caixa são as suas entradas, versão a versão (10182-1, e 75053-1) | as entradas da caixa contadas nas tabelas; as peças de cada versão comparadas com o `num_parts` |
| 2 | o resumo da 75053-1, corrido duas vezes, é uma linha por versão e categoria | dentro de uma transação: as categorias e as peças contadas nas tabelas |
| 2 | o `MERGE` da 75053-1 repõe o resumo no que o `INSERT` escreve | dentro de uma transação: todos os totais trocados por valores errados e uma linha tirada, e depois o `MERGE` duas vezes |
| 3 | o `by-color` e o `by-category` ficam com as entradas da caixa nessa cor, ou nessa categoria | as entradas da caixa escolhidas por uma condição sobre as tabelas |
| 6 | as caixas do Technic e do City de 2000 em diante são as de cada âmbito, com o rótulo dele | uma descida pelos temas a partir dos temas dos dois âmbitos |
| 7 | os moldes partilhados e os só do City são, juntos, os do City, sem nenhum nos dois | os moldes do City, `queries/city_moulds.sqlc` |
| 8 | o âmbito de um tema é toda a subárvore desse tema | os moldes das caixas encontradas por uma subida, do tema de cada caixa até à sua raiz |
| 10 | o histórico do construtor 3326 são as suas compras por ordem, com os totais acumulados | as compras ordenadas pela hora e depois pelo id; as peças somadas nas tabelas |
| 10 | a última compra de cada construtor é a primeira das suas compras, das mais recentes para as mais antigas | o `row_number()` sobre as compras de cada construtor, das mais recentes para as mais antigas |
| 10 | as caixas nunca compradas e as compradas são todas as caixas, sem nenhuma nas duas | as caixas compradas, lidas através das suas linhas da coleção, e todas as caixas |
| 11 | o tamanho mediano do Star Wars é o do meio dos seus tamanhos | os seus tamanhos listados por uma subida, ordenados, e tomado o do meio |
| 11 | as compras de 2016 a 2017 somam por tema, por ano e no total | os temas de cada ano somados, os anos somados, e as compras desses anos contadas nas tabelas |
| 11 | as caixas mais recentes de cada tema raiz são as suas três primeiras, das mais recentes para as mais antigas | uma subida, e o `row_number()` sobre as caixas de cada raiz, das mais recentes para as mais antigas |
| 12 | cada ano, do primeiro lançamento ao último, aparece uma só vez, os vazios com 0 | o primeiro e o último ano, as caixas com ano, e os anos que elas têm |
| 13 | os construtores que têm o Spybiotics inteiro têm tantas caixas dele quantas ele tem | as caixas distintas do âmbito que cada construtor tem, contadas contra as do âmbito |
| 14 | as cores da 75053-1 são as cores que as suas entradas nomeiam, versão a versão | os nomes de cor distintos contados nas tabelas |
| 14 | o documento da 10182-1 tem cada uma das suas entradas uma vez | as entradas nos arrays do documento, comparadas com as entradas da caixa nas tabelas |
| 15 | o `by-words` fica com as entradas da caixa cujos nomes de peça têm as palavras ('window', 'door frame') | as peças cujo nome tem o radical de cada palavra |
| 15 | a pesquisa de 'technic beam' lista todas as peças que têm as suas palavras, as melhores primeiro | as mesmas peças, ordenadas pelo `ts_rank` e depois pelo `part_num` |
| 16 | o `by-pattern` fica com as entradas da caixa cujos nomes de peça correspondem ao padrão | o `regexp_like`, sem distinguir maiúsculas de minúsculas |
| 16 | os tamanhos dos tijolos são os tijolos cujo nome diz a largura e o comprimento | cada nome cortado em ` x ` com o `split_part`, ambas as partes só com algarismos |
| 17 | os construtores a menos de 5 km de Londres são os que o haversine dá | a distância pelo haversine, com todas as casas medidas |
| 17 | os construtores em `^SW` são aqueles cujo código postal começa pelo seu prefixo | `LIKE 'SW%'` |
| 17 | os 5 construtores mais próximos de Londres estão tão perto como quaisquer outros | todas as casas medidas com o `earth_distance`, as cinco mais próximas |
| 17 | o que compraram os construtores perto de Londres soma as compras deles | as compras dos construtores dentro da distância pelo haversine |
| 18 | as linhas de cada verificação são o que ela encontra, sob a sua regra no rol | as entradas para as quais um `LEFT JOIN` às peças não encontra peça, e todas as caixas menos as que têm entrada (`EXCEPT`), cada uma com quantas examinou |
| 18 | o `checks/all.sqlc` são as linhas de todas as verificações, cada uma tantas vezes quantas a sua verificação a devolve | cada verificação composta sozinha |
| 18 | o `checks/all.sqlc` na nova grafia é uma só união com um braço por verificação, e devolve as linhas tal como composto | as linhas tal como compostas, cada uma tantas vezes quantas aí aparece |
| 12 | sobre linhas simuladas, 1991, entre lançamentos em 1990 e 1992, aparece com 0 | as linhas `1990, 1`, `1991, 0`, `1992, 2` |
| 13 | sobre linhas simuladas, a divisão fica com o dono de todas as caixas, e com ninguém num âmbito vazio | o único dono das duas caixas, e ninguém |
| 18 | sobre linhas simuladas, uma verificação que nada encontra devolve sozinha a sua linha do rol, composta e na nova grafia | a regra sozinha e as duas caixas sem entrada; a nova grafia com os braços em junção interna, que perde a regra |

As três últimas correm o seu modelo sobre tabelas simuladas, com linhas do próprio exemplo no lugar
das do catálogo: o calendário recebe um ano sem lançamentos entre dois que os têm; a divisão, um
tema com outro tema abaixo dele, um construtor que tem as duas caixas desse âmbito, outro que tem só
uma delas e um tema sem caixas; e as verificações, uma caixa com uma entrada cuja peça está no
catálogo, uma caixa cujo inventário não tem entrada nenhuma e uma caixa sem inventário.

## Os planos

```sh
cargo run -p brick-example -- fit
```

O `fit` compõe todos os modelos como o `cargo sqlc compose` os compõe, sem e com `--with`, e planeia
cada ficheiro composto com `EXPLAIN (COSTS OFF)`, como plano genérico quando o ficheiro recebe um
valor. Não executa nenhum deles, faz tudo dentro de uma transação só de leitura que no fim reverte,
e sai com 1 se uma lei falhar, ou se um erro semeado para fazer falhar uma lei não a fizer falhar.

- **Cada tabela, e cada lista de `VALUES`, é percorrida tantas vezes quantas o texto do ficheiro a
  escreve.** O `cargo sqlc tree` diz quantas cópias de cada modelo o ficheiro guarda, e cada cópia
  lê as tabelas que o texto do seu modelo nomeia e as listas de `VALUES` que ele escreve, cada lista
  distinguida pelas suas colunas. Um plano que percorra uma tabela mais ou menos vezes falha, a não
  ser que se diga a razão do planeador: um `LEFT JOIN` que ele tirou, porque nada acima da junção lê
  a tabela e a junção encontra no máximo uma das linhas dela. Cada ficheiro que o `--respell`
  escreve noutra grafia é conferido com o seu plano pelo seu próprio texto: na nova grafia, o
  `checks/all.sql` percorre a lista do rol uma vez, e a lista dos números dos seus braços também uma
  vez, quando, tal como composto, percorre a lista do rol uma vez por verificação.
- **Cada grafia que o `cargo sqlc spell` pode escrever deixa o plano como estava.** Cada uma é
  escrita no lugar do seu modelo, e todos os ficheiros compostos que guardam esse modelo, sem e com
  `--with`, voltam a ser planeados; o plano é o que o ficheiro tem tal como está escrito, ou só
  difere dele na maneira como é impresso, com as mesmas condições por outra ordem ou uma relação
  lida com outro nome. As ordens de junção, essas, são expostas para leitura e nunca escritas.
- **As leis falham onde têm de falhar.** Cada uma é posta perante erros semeados, e falha em todos
  eles: uma cópia de um modelo que o texto guarda e a árvore não conta, uma cópia que a árvore conta
  e o texto não guarda, o texto do `checks/all.sql` na nova grafia conferido com o plano do ficheiro
  tal como composto, e junções escritas por outras ordens.

## Ligação

A base de dados é a que o `--database-url` indicar, ou a de `SQLC_BRICK_DATABASE_URL` quando a opção
não é dada, e, sem nenhum dos dois, é a `postgres:///sqlc_brick`: a base de dados `sqlc_brick` no
servidor local, pelo seu socket Unix, com o utilizador atual.

```sh
cargo run -p brick-example -- --database-url postgres://user@host/dbname migrate
SQLC_BRICK_DATABASE_URL=postgres://user@host/dbname cargo run -p brick-example -- all
```

O exemplo fala TLS (rustls), como um PostgreSQL gerido costuma exigir, e é no URL que se pede: o
`?sslmode=require` cifra sem conferir o certificado do servidor, e o `?sslmode=verify-full` também o
confere, com os certificados de raiz da Mozilla, que vêm incluídos, e com qualquer ficheiro que o
`&sslrootcert=<file>` indique. Sem `sslmode`, a ligação tenta o TLS e, se o servidor não o oferecer,
continua sem cifra.

Os subcomandos leem os modelos de `--sqlc-dir`, por omissão `examples/brick/sqlc`, um caminho
relativo à pasta onde o comando corre, pelo que um subcomando corrido noutro sítio tem de indicar a
pasta.

## Os modelos

```
sqlc/
  shared/
    set_part_details.sqlc          # Resolução canónica: as entradas de uma caixa, por versão
    theme_closure.sqlc             # Cada tema com cada tema abaixo dele
    filtered_set_parts.sqlc        # A consulta de base da vaga @filter
    scope_moulds.sqlc              # Vaga @scope: os moldes que o âmbito de um tema usa
    scope_sets.sqlc                # Vaga @scope: as caixas de um âmbito, rotuladas com o âmbito
    area_purchases.sqlc            # Vaga @area: as compras feitas numa área, por tema raiz
    purchase_sets.sqlc             # Cada compra com construtor e caixa, pela sua linha da coleção
    set_roots.sqlc                 # Cada caixa com o seu tema raiz
    set_category_totals.sqlc       # Os totais de peças de uma caixa por versão e categoria
  scopes/
    technic.sqlc                   # O âmbito do Technic (ids de temas, com o id do próprio âmbito)
    city.sqlc                      # O âmbito do City (ids de temas, com o id do próprio âmbito)
  areas/
    near_city.sqlc                 # earthdistance: os construtores num raio à volta de uma cidade
    postcodes.sqlc                 # ~: os construtores cujo código postal corresponde a um padrão
  sets/
    select_set_parts.sqlc          # SELECT com :compose()
    select_colored_parts.sqlc      # @filter = by_color
    select_category_parts.sqlc     # @filter = by_category
    select_parts_by_words.sqlc     # @filter = by_words
    select_parts_by_pattern.sqlc   # @filter = by_pattern
    select_sets_by_themes.sqlc     # :bind() com vários valores num IN
    never_bought.sqlc              # NOT EXISTS: as caixas que nenhum construtor comprou
    set_colors.sqlc                # array_agg: as cores de uma caixa por versão
    set_document.sqlc              # jsonb: uma caixa num só documento
  parts/
    search_parts.sqlc              # Texto integral, por pontuação: ts_rank e ts_headline
    brick_sizes.sqlc               # regexp_match: o tamanho de um tijolo lido no seu nome
  filters/
    by_color.sqlc                  # Padrões de filtro por cor
    by_category.sqlc               # Padrões de filtro por categoria
    by_words.sqlc                  # Padrões de filtro por texto integral: websearch_to_tsquery
    by_pattern.sqlc                # Padrões de filtro por expressão regular: ~*
  reports/
    insert_set_summary.sqlc        # INSERT SELECT com :compose()
    merge_set_summary.sqlc         # MERGE com :compose()
    purchases_by_year_theme.sqlc   # ROLLUP: as compras por ano e tema raiz
    theme_set_sizes.sqlc           # percentile_cont: o tamanho mediano das caixas por tema raiz
    release_years.sqlc             # generate_series: todos os anos, sem deixar de fora os vazios
    combined_theme_sets.sqlc       # :union() de duas consultas
    count_theme_parts.sqlc         # :count(DISTINCT)
    shared_moulds.sqlc             # :intersect() de duas consultas
    city_only_moulds.sqlc          # :except() de duas consultas
  builders/
    purchase_history.sqlc          # Funções de janela sobre as compras de um construtor
    latest_purchases.sqlc          # DISTINCT ON: a última compra de cada construtor
    complete_scope.sqlc            # Divisão: os construtores que têm um âmbito inteiro
    bought_near.sqlc               # @area = near_city
    bought_in_postcodes.sqlc       # @area = postcodes
    nearest.sqlc                   # <-> num LATERAL: os construtores mais próximos de uma cidade
  themes/
    newest_sets.sqlc               # LATERAL: as três mais recentes de cada tema raiz
  inventory/
    track_set_parts.sqlc           # INSERT … ON CONFLICT com :compose()
    update_spare_counts.sqlc       # UPDATE com :compose()
  queries/
    technic_sets.sqlc              # @scope = technic (para a união)
    city_sets.sqlc                 # @scope = city (para a união)
    theme_set_parts.sqlc           # As entradas do âmbito de um tema (para a contagem)
    technic_moulds.sqlc            # @scope = technic (para a interseção e a diferença)
    city_moulds.sqlc               # @scope = city (para a interseção e a diferença)
  checks/
    roster.sqlc                    # VALUES: as verificações, cada uma com o seu slug e a sua regra
    lines_uncatalogued.sqlc        # As entradas cuja peça não está no catálogo de peças
    sets_without_lines.sqlc        # As caixas sem nenhuma entrada de inventário
    all.sqlc                       # :union(ALL) das verificações, cada uma a ler o rol
```

## Os dados e as suas licenças

O exemplo não traz nada do catálogo: é o `curl` da preparação que o descarrega para a máquina de
quem o corre. Trata-se da base de dados de amostra da Neon, o `lego.sql` de
[neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs), um
repositório sob a licença MIT cujo README dá como origem dos dados a
[LEGO Database](https://www.kaggle.com/datasets/rtatman/lego-database) (rtatman) do Kaggle, sob CC0,
no domínio público. Na origem, os dados vêm do [Rebrickable](https://rebrickable.com), cujos termos
(em vigor desde 2026-07-09, tal como lidos a 2026-10-04) permitem usá-los para qualquer fim,
incluindo o comercial, e pedem que se dê o Rebrickable como fonte, pelo que há que o mencionar onde
quer que os dados se usem; os mesmos termos proíbem usar qualquer conteúdo do Rebrickable para
treinar sistemas de IA.

LEGO® é uma marca do LEGO Group, que não patrocina, não autoriza nem subscreve este projeto.

[shared/set_part_details.sqlc]: sqlc/shared/set_part_details.sqlc
[sets/select_set_parts.sqlc]: sqlc/sets/select_set_parts.sqlc
[.sql/sets/select_set_parts.sql]: .sql/sets/select_set_parts.sql
[shared/set_category_totals.sqlc]: sqlc/shared/set_category_totals.sqlc
[reports/insert_set_summary.sqlc]: sqlc/reports/insert_set_summary.sqlc
[.sql/reports/insert_set_summary.sql]: .sql/reports/insert_set_summary.sql
[inventory/update_spare_counts.sqlc]: sqlc/inventory/update_spare_counts.sqlc
[inventory/track_set_parts.sqlc]: sqlc/inventory/track_set_parts.sqlc
[reports/merge_set_summary.sqlc]: sqlc/reports/merge_set_summary.sqlc
[shared/filtered_set_parts.sqlc]: sqlc/shared/filtered_set_parts.sqlc
[filters/by_category.sqlc]: sqlc/filters/by_category.sqlc
[sets/select_category_parts.sqlc]: sqlc/sets/select_category_parts.sqlc
[.sql/sets/select_category_parts.sql]: .sql/sets/select_category_parts.sql
[filters/by_color.sqlc]: sqlc/filters/by_color.sqlc
[sets/select_colored_parts.sqlc]: sqlc/sets/select_colored_parts.sqlc
[shared/theme_closure.sqlc]: sqlc/shared/theme_closure.sqlc
[.sql/shared/theme_closure.sql]: .sql/shared/theme_closure.sql
[sets/select_sets_by_themes.sqlc]: sqlc/sets/select_sets_by_themes.sqlc
[.sql/sets/select_sets_by_themes.sql]: .sql/sets/select_sets_by_themes.sql
[scopes/technic.sqlc]: sqlc/scopes/technic.sqlc
[scopes/city.sqlc]: sqlc/scopes/city.sqlc
[shared/scope_sets.sqlc]: sqlc/shared/scope_sets.sqlc
[queries/technic_sets.sqlc]: sqlc/queries/technic_sets.sqlc
[queries/city_sets.sqlc]: sqlc/queries/city_sets.sqlc
[reports/combined_theme_sets.sqlc]: sqlc/reports/combined_theme_sets.sqlc
[.sql/reports/combined_theme_sets.sql]: .sql/reports/combined_theme_sets.sql
[shared/scope_moulds.sqlc]: sqlc/shared/scope_moulds.sqlc
[queries/technic_moulds.sqlc]: sqlc/queries/technic_moulds.sqlc
[queries/city_moulds.sqlc]: sqlc/queries/city_moulds.sqlc
[reports/shared_moulds.sqlc]: sqlc/reports/shared_moulds.sqlc
[reports/city_only_moulds.sqlc]: sqlc/reports/city_only_moulds.sqlc
[.sql/reports/shared_moulds.sql]: .sql/reports/shared_moulds.sql
[.sql/reports/city_only_moulds.sql]: .sql/reports/city_only_moulds.sql
[queries/theme_set_parts.sqlc]: sqlc/queries/theme_set_parts.sqlc
[reports/count_theme_parts.sqlc]: sqlc/reports/count_theme_parts.sqlc
[.sql/reports/count_theme_parts.sql]: .sql/reports/count_theme_parts.sql
[shared/purchase_sets.sqlc]: sqlc/shared/purchase_sets.sqlc
[builders/purchase_history.sqlc]: sqlc/builders/purchase_history.sqlc
[builders/latest_purchases.sqlc]: sqlc/builders/latest_purchases.sqlc
[sets/never_bought.sqlc]: sqlc/sets/never_bought.sqlc
[shared/set_roots.sqlc]: sqlc/shared/set_roots.sqlc
[reports/theme_set_sizes.sqlc]: sqlc/reports/theme_set_sizes.sqlc
[reports/purchases_by_year_theme.sqlc]: sqlc/reports/purchases_by_year_theme.sqlc
[themes/newest_sets.sqlc]: sqlc/themes/newest_sets.sqlc
[reports/release_years.sqlc]: sqlc/reports/release_years.sqlc
[builders/complete_scope.sqlc]: sqlc/builders/complete_scope.sqlc
[sets/set_colors.sqlc]: sqlc/sets/set_colors.sqlc
[sets/set_document.sqlc]: sqlc/sets/set_document.sqlc
[filters/by_words.sqlc]: sqlc/filters/by_words.sqlc
[sets/select_parts_by_words.sqlc]: sqlc/sets/select_parts_by_words.sqlc
[parts/search_parts.sqlc]: sqlc/parts/search_parts.sqlc
[filters/by_pattern.sqlc]: sqlc/filters/by_pattern.sqlc
[sets/select_parts_by_pattern.sqlc]: sqlc/sets/select_parts_by_pattern.sqlc
[parts/brick_sizes.sqlc]: sqlc/parts/brick_sizes.sqlc
[shared/area_purchases.sqlc]: sqlc/shared/area_purchases.sqlc
[areas/near_city.sqlc]: sqlc/areas/near_city.sqlc
[builders/bought_near.sqlc]: sqlc/builders/bought_near.sqlc
[areas/postcodes.sqlc]: sqlc/areas/postcodes.sqlc
[builders/bought_in_postcodes.sqlc]: sqlc/builders/bought_in_postcodes.sqlc
[builders/nearest.sqlc]: sqlc/builders/nearest.sqlc
[checks/roster.sqlc]: sqlc/checks/roster.sqlc
[checks/lines_uncatalogued.sqlc]: sqlc/checks/lines_uncatalogued.sqlc
[checks/sets_without_lines.sqlc]: sqlc/checks/sets_without_lines.sqlc
[checks/all.sqlc]: sqlc/checks/all.sqlc
[.sql/checks/all.sql]: .sql/checks/all.sql
[--respell]: ../../README.pt.md#uma-união-de-braços-que-leem-um-mesmo-rol---respell
[src/main.rs]: src/main.rs
