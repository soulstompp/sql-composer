# sqlc-brickgen

> **Português europeu, grafia do AO90.** Também disponível em inglês: [`README.md`](README.md).

O `sqlc-brickgen` constrói, a partir de um catálogo real, um catálogo grande de peças, caixas e
temas, e carrega-o no PostgreSQL juntamente com construtores, as suas coleções e os registos das
suas compras. LEGO® é uma marca do LEGO Group, que não patrocina, não autoriza nem subscreve este
projeto.

O catálogo real são as oito tabelas `lego_*` do `--source-schema` (cores, temas, categorias de
peças, peças, caixas, inventários, entradas dos inventários e caixas dos inventários). O gerador
lê-as uma vez e escreve-as tal como estão, a não ser os anos de lançamento que caem nos anos sem
lançamentos (ver *Anos*) e alguns temas raiz (ver *Armadilhas*), e junta-lhes caixas
sintetizadas que as imitam, cada uma com o tema raiz, o ano e o conteúdo de uma caixa real, mas
com número e nome seus. As tabelas geradas vão para o `--schema`, que se apaga e se cria de novo em
cada execução.

O [exemplo brick](https://github.com/soulstompp/sql-composer/tree/HEAD/examples/brick) do
sql-composer executa as suas consultas compostas na base de dados que o gerador constrói, e é nela
também que o [kit](https://github.com/soulstompp/warren-surveyor/tree/HEAD/kit) do warren-surveyor
cronometra as suas perguntas, com o surveyor a ler os índices do DBA que o gerador constrói (ver
*Carregamento*). Já o warren-snail, no seu
[`bench`](https://github.com/soulstompp/warren-snail/tree/HEAD/bench), escreve as compras e a
coleção numa base de dados que cabe num só ficheiro, declarada pelos índices do DBA que o gerador
constrói sobre elas, e cronometra-a contra o PostgreSQL sobre as mesmas tabelas.

## Executar

O gerador lê o catálogo real do esquema `public` da base de dados que lhe for dada, pelo que é aí
que primeiro se carrega o catálogo, dando-se depois ao gerador o nome dessa mesma base de dados. A
partir da raiz do repositório:

```sh
createdb sqlc_brick
curl -sSfL https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/lego.sql \
    | psql -X -q -v ON_ERROR_STOP=1 -o /dev/null -d sqlc_brick
cargo run --release -p sqlc-brickgen -- --database-url postgres:///sqlc_brick --size small
```

Sem `--size`, o gerador constrói o `huge`, cerca de 2 milhões de caixas sintetizadas com os seus
construtores e as suas compras, o que dá uma carga longa; o `--size small`, com 20 mil caixas, dá
uma primeira carga rápida.

A carga do catálogo para na primeira instrução que falhe, e o gerador, antes de ler o que quer que
seja, confirma que as oito tabelas lá estão e nomeia as que não encontra. O `--schema` não pode
nomear o `--source-schema`, uma vez que o esquema de destino se apaga e se cria de novo.

O `--size small|medium|huge|8m|80m|800m` dá um número de caixas sintetizadas: 20 mil, 200 mil, 2
milhões e, para lá do `huge`, o número que está no nome (8, 80 e 800 milhões). O `--sets N` dá-o
diretamente e prevalece, e por omissão é o `huge`. Tudo o resto cresce com ele: os construtores, as
linhas das coleções e as compras (`PURCHASES_PER_SET` por caixa, real ou sintetizada), e cada
armadilha plantada a uma taxa; já as cidades, os códigos postais, os nomes, as lojas e o calendário
são os mesmos em qualquer tamanho, pelo que, num tamanho maior, ficam mais cheios. O mesmo `--seed`
com as mesmas opções dá as mesmas linhas, e cada construção de uma chave, das estatísticas ou do
`VACUUM` pode levar uma hora por cada dois milhões de caixas, a não ser que o `--build-timeout`
diga outra coisa.

O URL da base de dados também pode vir do `SQLC_BRICKGEN_DATABASE_URL`, mas nunca do `DATABASE_URL`
geral: como o esquema de destino se apaga e se cria de novo, a base de dados tem de ser nomeada de
propósito.

O gerador fala TLS (rustls), como costuma exigir um servidor PostgreSQL gerido por um fornecedor, e
o TLS pede-se no URL: o `?sslmode=require` cifra sem verificar o certificado do servidor, e o
`?sslmode=verify-full` também o verifica, contra os certificados de raiz da Mozilla, que vêm
incluídos, e contra qualquer ficheiro que o `&sslrootcert=<file>` nomeie. Sem `sslmode`, a ligação
tenta o TLS e, se o servidor não o oferecer, passa a ser feita sem ele.

## Dados e licenças

O gerador está sob a licença MIT, com direitos de autor de Kenneth Allen Flegal: ver
[LICENSE](https://github.com/soulstompp/sql-composer/blob/HEAD/examples/sqlc-brickgen/LICENSE).

Do catálogo real, o gerador não traz nada, e é o `curl` de *Executar* que o descarrega para a
máquina de quem o corre. Trata-se do *dump* de exemplo da Neon, o `lego.sql` de
[neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs), um
repositório sob a licença MIT cujo README dá como origem dos dados a
[LEGO Database](https://www.kaggle.com/datasets/rtatman/lego-database) do Kaggle (rtatman), sob
CC0, no domínio público. Na origem, os dados vêm do [Rebrickable](https://rebrickable.com), cujos
termos (em vigor desde 9 de julho de 2026, tal como lidos a 4 de outubro de 2026) permitem usá-los
para qualquer fim, incluindo o comercial, e pedem que se indique o Rebrickable como a sua fonte, o
que se faz onde quer que se usem. Os mesmos termos proíbem usar qualquer conteúdo do Rebrickable
para treinar modelos de IA. O esquema gerado guarda as próprias linhas do catálogo real, e é delas
que as caixas sintetizadas tiram os seus nomes e conteúdos.

As cidades de `data/cities.tsv` são os lugares povoados do Natural Earth, no domínio público, e os
fusos horários são a base de dados de fusos horários da IANA, incluída através do pacote `jiff`,
também no domínio público.

LEGO® é uma marca do LEGO Group, que não patrocina, não autoriza nem subscreve este projeto.

## Carregamento

A carga é hierárquica: cada onda são `--chunk` registos de raiz (caixas ou construtores), escritos
numa só transação, a tabela-mãe primeiro:

1. as caixas, depois os seus inventários, depois todas as entradas e caixas aninhadas desses
   inventários e, por fim, as linhas da onda no `trap_manifest`;
2. os construtores, depois as linhas das suas coleções, depois as compras dessas linhas e, por fim,
   as linhas do manifesto.

Uma onda que falhe é desfeita por inteiro, pelo que as tabelas guardam exatamente as ondas que
foram confirmadas; e escrevem-se `--jobs` ondas ao mesmo tempo, cada uma na sua sessão.

Cada lote segue por `COPY … FROM STDIN`, em formato binário por omissão (o `--copy-format text`
usa o formato de texto, e o `--method unnest` lotes de `INSERT … SELECT FROM UNNEST`). As chaves
primárias constroem-se depois da carga (o `--index-timing before` constrói-as antes), e o mesmo
acontece aos índices compostos que um DBA dá às tabelas para as junções entre elas, cada um
encabeçado pelas colunas que a junção à sua tabela fixa e com as colunas que as consultas leem no
`INCLUDE` (`COMPOSITE_INDEXES` em `src/load.rs`). Dois deles são índices de expressão sobre as
compras, por mês e depois por instante, um dentro de cada linha de coleção e outro sobre todas
elas, que leem o instante através de `<schema>.clock(timestamptz)`, uma função declarada
`IMMUTABLE` que dá um instante como a hora que o relógio marca em UTC e que torna, assim, indexável
o mês de um instante; a carga cria-a antes dos índices, e uma consulta que os queira usar lê o
tempo através da mesma função. Ao lado dos índices compostos, a carga constrói as chaves únicas que
as tabelas declaram além das chaves primárias (`UNIQUE_KEYS` em `src/load.rs`): o `code` dos
códigos postais e as chaves naturais das entradas dos inventários,
`(inventory_id, part_num, color_id, is_spare)`, e das caixas aninhadas, `(inventory_id, set_num)`,
que não têm chave primária. Constrói também o que um DBA acrescenta para as pesquisas que os outros
índices não servem (`SEARCHES`): uma chave `text_pattern_ops` para encontrar um código postal pelo
prefixo, um GiST sobre as casas dos construtores, pela distância (`ll_to_earth`), e GIN sobre os
nomes das peças, pelas suas palavras, e sobre os nomes das peças e das caixas, por trigramas.

As entradas e as caixas aninhadas que o gerador escreve saem já fundidas nessas chaves, e as do
catálogo real tal como estão; por isso, quando se pedem as chaves ou as partições, o gerador
confirma primeiro que o catálogo real não repete nenhuma das duas chaves, e, se repetir, para antes
de escrever o que quer que seja.

O `--indexes` diz quais destes se constroem: `keys` (as chaves primárias e as chaves únicas),
`composite` e `search`, numa lista, ou `none`. Por omissão, constroem-se os três, e a função do
relógio cria-se seja qual for a lista. Os índices de pesquisa precisam das extensões `cube`,
`earthdistance` e `pg_trgm`, e, quando o `search` está na lista, o gerador cria cada uma antes de
escrever o que quer que seja, a não ser que a base de dados já a tenha, num esquema qualquer.
Cria-as num esquema só delas, `extensions`, à parte do catálogo real, que está em `public`, e cria
esse esquema com a primeira; o `--extension-schema` nomeia outro, mas nunca um que uma execução
apague (o `--schema`, o `--oo-schema` ou um esquema de `--partitioning`). Cada índice de pesquisa
nomeia o esquema onde está a sua extensão, e uma consulta que chame as funções das extensões
encontra-as com esse esquema no seu `search_path`. A `earthdistance` não é uma extensão de
confiança, pelo que só um superutilizador a pode criar: um papel que não o seja precisa de que um
superutilizador crie primeiro as três, na base de dados e no seu esquema, ou então deixa o `search`
fora do `--indexes`.

No PostgreSQL 15 e anteriores, as funções da `earthdistance` encontram o `cube` e os seus próprios
tipos no `search_path` de quem quer que as chame, seja a construção do índice, cada escrita nos
construtores ou o autovacuum, e, num esquema só delas, nenhum destes as encontra, pelo que o índice
das casas dos construtores não se consegue construir. Nessas versões, dá-se
`--extension-schema public`. A partir do PostgreSQL 16, a `earthdistance` nomeia o `cube` pelo seu
esquema, e qualquer esquema serve.

Durante a carga, o autovacuum fica desligado nas tabelas e volta a ligar-se no fim (o
`--autovacuum-during-load on` deixa-o ligado), e depois as tabelas passam por um `VACUUM` e um
`ANALYZE`. O `--vacuum off` deixa de fora o `VACUUM`, e o `--analyze off` as estatísticas; com os
dois desligados, as tabelas ficam tal como foram carregadas. As classes orientadas a objetos e as
cópias particionadas passam pelo `VACUUM` e pelo `ANALYZE` seja o que for que estas opções digam. O
`--unlogged` cria tabelas sem registo no WAL (`UNLOGGED`), para execuções descartáveis, e o
`--synchronous-commit` define o modo de confirmação das sessões que carregam (desligado por
omissão).

## Anos

Nos anos de `NO_RELEASE` (de 1994 a 1996 e de 2001 a 2011) não sai caixa nenhuma. Uma caixa real a
que o *dump* dê um desses anos passa para o ano de lançamento mais próximo, o mais tardio de dois
igualmente próximos, e fica listada no `trap_manifest` sob B9, com o ano que o *dump* lhe dá; as
caixas sintetizadas que a imitam ficam com o ano novo.

Os anos que vêm depois do catálogo real (`MODELLED`, de 2018 a 2026) projetam-se a partir dos seus
últimos anos (`MODEL_ON`): cada tema raiz mantém a sua parte das caixas desses anos, e uma caixa
de um ano projetado copia uma delas. Os lançamentos por ano continuam a crescer ao ritmo dos
próprios anos reais (`MODEL_FIT`, que deixa de fora o último ano do *dump*, por estar incompleto),
acelerado `GROWTH_FACTOR` vezes, pelo que os últimos anos têm muitas mais caixas do que os
primeiros.

## Onde cai cada caixa

Uma caixa sintetizada cai num tema raiz e numa década, de 1950 a 2030, a que se chama a sua raiz
e década, e o peso de uma raiz e década são as suas caixas reais mais os lançamentos dos seus anos
projetados. O `--phases <pattern>:<percent>,…` divide as caixas sintetizadas em fases, cada uma a
seguir um padrão:

- `natural`: raízes e décadas sorteadas com os próprios pesos do catálogo real;
- `sorted`: uma só passagem pelas raízes e décadas, por ordem;
- `interleaved`: cada onda percorre todas as raízes e décadas;
- `wavy`: uma onda que viaja pelas raízes e décadas, para a frente e depois para trás
  (`--wavy-period`, `--wavy-amplitude`);
- `hotspot`: uma raiz e década fica com uma parte das caixas (`--hotspot-share`, `--hotspot`);
- `swing`: as ondas alternam entre dois grupos de raízes e décadas (`--swing-a`, `--swing-b`,
  `--swing-period`);
- `paired`: embalagens, ou seja, caixas que trazem outras caixas dentro, escritas aos pares, com as
  caixas de dentro escolhidas pelo ano de lançamento de modo que as duas embalagens de um par
  tenham tantos pares de anos consecutivos uma como a outra, mas um número diferente de sequências
  de três anos consecutivos, pelo que contar os seus anos dois a dois não as distingue. Os anos vêm
  de três pares de padrões (`YEAR_PAIRS` em `src/paired.rs`, que um teste confere contra isto),
  postos a partir de uma janela de anos de lançamento reais que se desloca com a onda, e espelhados
  no caminho de volta. Cada embalagem fica listada no `trap_manifest` sob O5, e o seu pormenor
  nomeia o seu par, se é o primeiro ou o segundo membro, os seus desvios, os desvios da parceira, a
  janela e se está espelhada.

Cada vínculo entre caixas atravessa fronteiras a uma taxa sua: um aninhamento para outro tema
de raiz (`--cross-nesting`), uma versão (`--cross-versions`) ou uma caixa gémea (`--cross-twins`)
para uma raiz e década diferentes das do seu `-1`, e uma linha de coleção para fora da raiz e
década de origem do seu construtor (`--cross-collections`); as três primeiras taxas são, por
omissão, as que se mediram no catálogo real. O `--upto-phase N` escreve só as fases até à N-ésima,
pelo que cada prefixo de uma execução se pode carregar e conferir sozinho.

## Comprar

Toda a caixa lançada até `TODAY` está à venda, e cada uma tem uma procura: a sua popularidade vezes
a parte da sua cronologia que já passou até hoje.
- A popularidade é desigual: o décimo das caixas mais populares fica com cerca de metade dela.
- A cronologia é uma soma de surtos a partir do lançamento da caixa: o próprio lançamento, que se
  vai desvanecendo; regressos e relançamentos em datas posteriores tiradas à sorte; os picos do seu
  tema raiz, janelas que todas as caixas do tema partilham; e um fio fino de vendas em segunda
  mão. Uma caixa lançada no mês passado teve pouco tempo para vender.

Uma linha de coleção sorteia a sua caixa pela procura, da raiz e década de origem do construtor a
não ser que a linha saia delas, e cada exemplar é comprado num dia sorteado da cronologia da caixa,
dentro do horário da loja, num segundo e num milissegundo do seu minuto. O construtor anota o
instante como a hora local do seu fuso de residência, ao milissegundo, segundo o histórico de
desvios desse fuso na base de dados de fusos horários da IANA incluída no binário, de 1950 em
diante.

A linha nomeia a caixa que guarda pelo seu número, `set_num`, e o nome da caixa lê-se nas próprias
caixas. Onde o construtor escreveu o número ou o nome da caixa à sua maneira (as armadilhas K1, K2,
K3 e K5), a linha guarda também o que ele escreveu, em `typed_set_num` ou `typed_name`, e em todas
as outras linhas ambos são NULL. Cada exemplar que a linha guarda é uma compra, que nomeia a linha
por `(builder_id, row_no)`: os exemplares contam-se a partir das compras, e uma compra chega à sua
caixa através da sua linha.

## Onde vivem os construtores

Os construtores vivem em cidades reais, as de `data/cities.tsv`, que são os lugares povoados do
Natural Earth à escala 1:10m (no domínio público) nos oito fusos de residência, nas suas
coordenadas reais e com as suas populações; o cabeçalho do ficheiro diz como se escolheram os
lugares.

- `lego_cities`: cada cidade, com o seu país, região, fuso de residência, coordenadas e população.
- `lego_postcodes`: os distritos postais de uma cidade, uma grelha sobre um quadrado com a área do
  seu núcleo urbano, com um distrito por cada 25 000 pessoas, mais ou menos, pelo que os códigos
  postais são os mesmos em qualquer tamanho. Cada código está no formato do seu país, e os seus
  primeiros caracteres seguem o próprio esquema do país, de modo que um código diz onde fica:
  - o primeiro algarismo de um código ZIP dos Estados Unidos, o de um código PIN indiano e o de um
    código japonês ou português, por estado, prefeitura ou distrito;
  - um código australiano por estado, e um dinamarquês por região;
  - as letras de um código do Reino Unido pela área postal da sua localidade (`AB`, `IV`, `KW`,
    `ZE`, `BT`, …), e as de Londres pelo seu ponto cardeal (`EC`, `WC`, `E`, `N`, `NW`, `W`, `SW`,
    `SE`).

  O resto de cada código é numerado por ordem, e inventado.
- `lego_streets`: as ruas de um distrito, tantas quantas os seus construtores precisem, cada uma de
  norte a sul ou de este a oeste, de uma ponta à outra, com um nome inventado à maneira como o país
  dá nome às ruas.
- `lego_builders`: a rua e o número de porta de cada construtor, ímpar de um lado da rua e par do
  outro, e o ponto da sua casa, ao lado dela.

O fuso de um construtor é o da sua cidade, a que se chega pela sua rua e pelo código postal dela.
Os construtores espalham-se pelas cidades de cada fuso segundo a população, e por cada cidade até
às suas margens; cada ponto de uma rua ou de uma casa é o centro da célula de um milésimo de grau em
que cai, pelo que nomeia um quarteirão e não uma casa, e, como nenhum nome de rua é real, nenhuma
linha é a morada de ninguém.

## Armadilhas

Há linhas plantadas de propósito: chaves escritas em várias grafias, anos e ausências nos limites,
ordens que dependem da colação ou de empates, e datas que atravessam mudanças de hora. Cada linha
plantada fica listada no `trap_manifest`, com a fase, a onda, e a raiz e década de onde veio
(`root_decade`), e o programa, ao arrancar, escreve cada armadilha, a sua taxa e quantas plantou.

O `--traps` diz que armadilhas se plantam: `all` (por omissão), `none`, ou uma lista como
`K1,K3,D5`. Só se podem deixar de fora as armadilhas plantadas a uma taxa e a O5, que a fase
`paired` escreve, e nomear qualquer outra é recusado; as restantes nascem do catálogo real, das
linhas de outras armadilhas ou da maneira como cada onda se escreve, e estão lá diga o `--traps` o
que disser, tal como as compras que, por si sós, caem numa mudança de hora ou atravessam um mês. Sem
a O5, a fase `paired` escreve as suas caixas como a `natural` as escreve. As pré-encomendas da D5
nomeiam uma caixa que ainda não saiu, coisa que só a B2 e a B5 plantam, pelo que a D5 é recusada sem
uma delas.

Cada referência nas tabelas geradas nomeia uma linha que a sua tabela tem, armadilhas incluídas:

- uma caixa da armadilha B3 cujo tema não está na lista de temas do catálogo real leva um tema de
  raiz que o `lego_themes` gerado acrescenta, nomeado pelo seu id;
- um inventário da armadilha K8, arquivado sob o número impresso na caixa, fica arquivado sob um
  registo de caixa sem mais nada com esse número, que está ao lado do registo `-1` da caixa, tal
  como os da armadilha K7;
- uma caixa sintetizada deixa de fora as entradas da caixa real que imita cujo número de peça não
  está na lista de peças. Essas entradas são do próprio catálogo real, escritas tal como estão e
  listadas sob a armadilha B10.

## Saída

As mensagens de registo vão para a saída de erro, onde o `RUST_LOG` as filtra e o
`--log-format json` as escreve em JSON. Cada sessão que carrega dá a si própria o nome
`sqlc-brickgen/<n>` no `application_name`, e a que constrói as chaves e os índices depois da carga o
nome `sqlc-brickgen/build`, pelo que o `pg_stat_activity` mostra quais são as sessões do gerador; e,
a cada `--log-progress-every` segundos, regista-se uma linha de progresso, com as instruções `COPY`
que estão a correr nesse momento.

Na saída padrão, as linhas que começam por `SUMMARY` guardam os números da execução: a
configuração resolvida; as linhas, os bytes, os lotes e o tempo por tabela; as caixas e as entradas
por fase e por raiz e década; as contagens das armadilhas; as compras (as caixas à venda, a parte
comprada pelo menos uma vez e as compras por cada décimo das caixas, das mais compradas para as
menos); o WAL escrito e o tamanho final de cada tabela. A mesma configuração fica escrita na tabela
`generator_run`, onde cada opção que decide as linhas tem por chave o seu próprio nome, com
sublinhados (`--hotspot-share` é `hotspot_share`, `--swing-a` é `swing_a`), e o valor que a execução
usou: o `upto_phase` é o número de fases quando a opção não é dada, e o `hotspot` a raiz e década
mais pesada. O mesmo se passa com as opções da carga `copy_format`, `method`, `index_timing`,
`indexes`, `unlogged`, `synchronous_commit`, `freeze` e `autovacuum_during_load`, e ao lado delas
ficam as definições da função do relógio (`function_clock`) e de cada índice composto, chave única
e índice de pesquisa, cada uma com o seu nome por chave (`composite_<name>`, `unique_<name>`,
`search_<name>`).

## Tabelas orientadas a objetos

O `--oo-schema <name>` constrói também as caixas, as cores e os construtores gerados como
hierarquias de classes, com a herança de tabelas do PostgreSQL, num esquema só delas, que se apaga
e se cria de novo e que não pode ser o do `--schema` nem o do `--source-schema`. A tabela de raiz de
cada hierarquia mantém o nome e as colunas da tabela gerada, e as linhas são as linhas geradas, tal
como estão, cada uma escrita na tabela da sua classe mais específica.

O `--classes` diz que hierarquias se constroem, `none` ou uma lista de `sets`, `colors` e
`builders`, e, quando não é dado, constroem-se as três. Uma lista sem `--oo-schema` é recusada,
porque as classes precisam de um esquema só delas, e o mesmo acontece ao `--classes none` com ele,
porque esse esquema guardaria as classes de uma carga anterior: ou se nomeiam as classes, ou se
deixa de fora o `--oo-schema`. Quando uma das duas é recusada, não se apaga nada.

O CHECK de cada classe lê as próprias colunas da linha:

- **As caixas, pelo tema raiz do seu tema:**
  - `licensed`, com as caixas licenciadas, e `star_wars` por baixo;
  - `in_house`, com todas as outras caixas, e por baixo:
    - `classic_play`, por cima de `town`, `space`, `castle` e `pirates`;
    - `technic`, com `technic_star_wars` por baixo;
    - `star_wars_elsewhere`.
  - `star_wars_all` é um segundo pai das três classes Star Wars, pelo que uma caixa Star Wars é
    também uma caixa licenciada, uma Technic ou uma da casa. Uma caixa sem tema fica em `in_house`.
- **As cores, pelo matiz:**
  - `neutral` guarda as cores cujos canais diferem menos de 32, que não têm propriamente matiz;
  - `primary` (vermelho, verde, azul), `secondary` (amarelo, ciano, magenta) e `tertiary` (laranja,
    verde-lima, verde-primavera, azul-celeste, violeta, rosa) guardam as restantes, uma classe por
    matiz.
  - O matiz é `<schema>.colour_wheel(rgb)`: o lugar numa roda de doze matizes, contado a partir do
    vermelho, a trinta graus cada lugar. A classe de uma cor segue o seu `rgb`, e não o seu nome.
- **Os construtores, pelo país do seu fuso de residência,** segundo o `zone.tab` da base de dados
  da IANA. Um país com dois fusos de residência (os Estados Unidos) tem por baixo uma classe por
  fuso. A classe de um construtor lê-se no seu próprio `street_id`, uma vez que as ruas de um fuso,
  e de um país, são uma só sequência contínua de ids.

O CHECK de uma classe, que as filhas herdam, chama-se `<class>_check`, e uma classe que tem linhas
suas ao lado das das filhas deixa estas de fora com um CHECK `NO INHERIT` chamado
`<class>_no_inherit_check`. Conforme o `--indexes` pedir, cada classe tem a chave primária da
tabela gerada, e cada classe com linhas suas tem todos os índices compostos da tabela gerada; todas
passam por um `VACUUM` e um `ANALYZE`. Uma hierarquia lê-se pela sua raiz, com o esquema gerado
atrás dela para as outras tabelas:

```sql
SET search_path = lego_oo, lego;
SELECT count(*) FROM lego_sets WHERE theme_id = 158;                  -- the Star Wars classes only
SELECT count(*) FROM lego_colors WHERE lego_oo.colour_wheel(rgb) = 0;  -- the red class
```

Uma consulta que nomeie a condição própria de uma classe, como um tema, uma rua ou um lugar da
roda, lê apenas as classes que a podem ter, uma vez que o planeador deixa de fora as outras, desde
que o valor seja uma constante quando a instrução é planeada.

Terminada a construção, uma verificação da colocação volta a ler do catálogo os CHECK de cada
classe e confere quatro coisas, e a construção falha se alguma não se cumprir:

- cada hierarquia tem exatamente as linhas da tabela gerada;
- cada linha que cumpra os CHECK de uma classe está nessa classe ou abaixo dela;
- cada linha de uma classe cumpre os seus CHECK;
- uma classe que não deve ter linhas suas não tem nenhuma.

As linhas `SUMMARY oo` dão as linhas que cada classe tem de seu.

## Cópias particionadas

O `--partitioning` copia também as tabelas geradas, depois da carga, para tabelas particionadas
pela primeira coluna da sua chave natural, que é a chave única sobre os próprios atributos da
tabela. Cada método tem um esquema seu, chamado `<schema>_<method>` (`lego_inheritance`,
`lego_range`, `lego_hash`), e cada execução apaga os três antes da carga, pedidos ou não, para que
nenhuma cópia guarde as linhas de uma carga anterior:

- `inheritance`: tabelas-filhas abaixo de um pai vazio, cada uma com um intervalo da coluna, sob um
  CHECK chamado `<child>_check`, como se particionavam as tabelas antes do particionamento
  declarativo. Uma linha escrita mais tarde, nada a encaminha.
- `range`: `PARTITION BY RANGE`, com uma partição `DEFAULT` para as linhas escritas mais tarde.
- `hash`: `PARTITION BY HASH`.

O `--partitioning` aceita `none` (por omissão) ou uma lista dos três, pelo que uma só carga pode
construí-los todos. O `--partition-tables` nomeia as tabelas a copiar, e o seu valor por omissão,
`all`, são todas as tabelas geradas menos as da própria execução, `generator_run` e
`trap_manifest`, que mesmo assim se podem nomear; o `--partitions` dá o número de partições, 8 por
omissão. Ambos são recusados sem `--partitioning`, e o mesmo acontece ao esquema de qualquer
método que fique com o nome do `--source-schema` ou do `--oo-schema`. As partições chamam-se
`<table>_p0`, `<table>_p1`, …

Uma chave é o que uma tabela declara, e nunca o que as linhas de uma carga calhem a ter. A chave
natural de uma tabela é a sua chave primária, já que os ids do catálogo são os próprios
identificadores do catálogo e um id gerado é a identidade que o gerador dá ao que gera; só uma
tabela que declare duas chaves únicas escolhe uma delas, e as tabelas sem chave primária ficam com
a chave única que declaram:

| tabela | chave natural | coluna de partição |
|---|---|---|
| `lego_inventory_parts` (sem chave primária) | `(inventory_id, part_num, color_id, is_spare)` | `inventory_id` |
| `lego_inventory_sets` (sem chave primária) | `(inventory_id, set_num)` | `inventory_id` |
| `lego_postcodes` | `(code)`, ao lado da chave primária `postcode_id` | `code` |

Os intervalos têm todos a mesma largura, entre o mínimo e o máximo da coluna, lidos depois da
carga. Uma tabela fica de fora quando não tem chave natural (`trap_manifest`) ou quando a sua chave
contém uma expressão ou uma coluna que admite NULL; nos dois métodos por intervalos, fica também de
fora uma tabela cuja chave comece por texto (`lego_sets`, `lego_parts`, `lego_postcodes`,
`generator_run`), uma vez que o texto não tem intervalos de igual largura, mas o `hash` particiona
também essas. O esquema de um método lê-se com o esquema gerado atrás dele, para as tabelas que
deixa de fora:

```sql
SET search_path = lego_range, lego;
EXPLAIN SELECT * FROM lego_purchases WHERE purchase_id = 400000;  -- one partition
```

Cada tabela por herança enche-se numa só passagem pela tabela gerada, sem ficheiros temporários: as
filhas começam por ser as partições de uma tabela declarativa, `<table>_load`, cada uma já sob o
seu CHECK, e é essa tabela que encaminha cada linha para a sua filha. Depois, cada filha é separada
e posta como filha do pai com `INHERIT`, e a `<table>_load` é apagada.

Cada esquema recebe os índices que o `--indexes` pedir. Numa tabela declarativa, criam-se no pai,
que os faz em todas as partições; por herança, criam-se em cada filha, uma vez que um índice cobre
uma só tabela, e não no pai vazio, e o planeador deixa de fora uma filha cujo CHECK a condição de
uma consulta contradiga. Uma chave que contenha a coluna de partição continua única na tabela
inteira: uma tabela declarativa obriga a isso, e, por herança, duas linhas iguais numa dessas
chaves são iguais na coluna de partição, pelo que caem na mesma filha, e o índice único dessa filha
recusa a segunda; uma linha escrita no próprio pai, essa, nada a confere. A chave natural contém
sempre a coluna de partição, e qualquer outra chave não pode ser única na tabela toda, pelo que se
constrói como um índice simples, como acontece à chave primária dos códigos postais, `postcode_id`.

Com o `--unlogged`, as partições e as tabelas por herança ficam sem registo no WAL, mas um pai
declarativo não, uma vez que o PostgreSQL recusa uma tabela particionada `UNLOGGED`, e de qualquer
modo não tem linhas suas. O PostgreSQL recusa também qualquer parâmetro de armazenamento numa
tabela particionada, e o autovacuum nunca analisa nenhuma, pelo que a construção passa um `VACUUM`
e um `ANALYZE` a cada tabela que faz.

Terminada a construção, a execução confere que cada linha está numa só partição e que cada chave é
única: conta as linhas de cada tabela em todas as suas partições contra a tabela gerada e vê que um
pai por herança não tem linhas suas; volta a ler do catálogo cada intervalo, para ver que nenhum se
sobrepõe a outro; e vê que nenhum valor de uma chave que contenha a coluna de partição aparece duas
vezes, o que, onde um índice único válido obriga a chave na cópia, fica a cargo do índice, sem se
contarem as linhas, e, onde nada a obriga, como sem `keys` no `--indexes`, se faz contando-as. A
construção falha se alguma destas coisas não se cumprir. As linhas `SUMMARY partition` dão as
linhas de cada partição, e as linhas `SUMMARY size` o tamanho de cada tabela copiada com todas as
suas partições.

O `generator_run` regista:
- os métodos (`partitioning`), as tabelas (`partition_tables`) e o número (`partitions`);
- a chave natural e a coluna de cada tabela (`partition_key_<table>`, `partition_column_<table>`);
- de cada método, o seu esquema, as tabelas que particiona e as que deixa de fora, com o motivo
  (`partition_<method>_schema`, `partition_<method>_tables`, `partition_<method>_left_out`);
- quando se constroem as chaves, cada chave que deixa de ser única na sua tabela
  (`unique_lost_<method>_<key>`).

## Outras opções

- `--lettered-ppm` (30000): caixas sintetizadas por milhão cujo número começa por um prefixo de
  letras, um dos do catálogo real, sorteado nas suas proporções.
- `--rerelease-ppm` (20000): caixas sintetizadas por milhão sorteadas como relançamento, a versão
  seguinte da caixa anterior com o mesmo número (`-2` depois do seu `-1`).
- `--second-version-ppm` (700): caixas sintetizadas por milhão a que se dá um segundo inventário,
  a versão 2, com a quantidade de uma entrada corrigida.
- `--recolour-ppm` (150000): entradas por milhão do inventário de uma caixa sintetizada cuja cor
  volta a ser sorteada, entre as cores que as entradas do catálogo real dão à sua peça.
- `--wave-timeout` (`300s`): o `statement_timeout` das instruções de cada onda.
- `--maintenance-work-mem` (`1GB`) e `--parallel-maintenance-workers` (4): o `maintenance_work_mem`
  e o `max_parallel_maintenance_workers` da sessão que constrói as chaves e os índices e que faz o
  `VACUUM` depois da carga.
- `--freeze`: carrega as tabelas de referência com `COPY … FREEZE`, que o PostgreSQL permite porque
  cada uma é criada na transação que a carrega. Todas as outras tabelas são escritas por muitas
  transações, pelo que se carregam como de costume, e a execução di-lo.
- `--vacuum on|off` e `--analyze on|off` (ambos `on`): ver *Carregamento*.
- `--dry-run`: gera e codifica todas as ondas e não escreve nada, mas liga-se na mesma, para ler o
  catálogo real.

## Testes

```sh
cargo test -p sqlc-brickgen
```
