# Evidências da ShopBrasil

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Este documento reunirá os comandos de execução, os resultados observados e os links para as evidências da entrega acadêmica. Os resultados serão registrados à medida que os cenários forem executados.

## #1 — Versões e tecnologias

Validação executada em 22 nov. 2026, com Docker 29.9.2 e Docker Compose v5.6.1, sobre a base `main` 049f789. Os contêineres e a rede temporária foram removidos ao final. A decisão está em [decisao-versoes-tecnologias.md](decisao-versoes-tecnologias.md).

| Verificação | Comando resumido | Resultado observado |
|---|---|---|
| Versões publicadas | consulta ao JSON do PyPI | `apache-airflow` 3.4.0 (6 nov. 2026, Python 3.11–3.14); provider postgres 7.1.2 no PyPI, 7.1.1 nas constraints |
| Constraints oficiais | `constraints-3.4.0/constraints-3.12.txt` | provider postgres 7.1.1, common-sql 2.4.0, psycopg2-binary 2.9.13, psycopg 3.3.6, fastapi 0.162.0, uvicorn 0.54.0, Jinja2 3.1.6, pendulum 3.3.0 |
| Suporte a PostgreSQL | README da tag 3.4.0 | PostgreSQL 14, 15, 16, 17 e 18 |
| Minor atual do PostgreSQL | postgresql.org/support/versioning | 18.7 |
| Imagem do Airflow | `docker run --rm apache/airflow:3.4.0-python3.12 airflow version` e `pip list` | `3.4.0`, `Python 3.12.16`, provider postgres 7.1.1, common-sql 2.4.0, psycopg2-binary 2.9.13, pendulum 3.3.0 |
| Imagem do PostgreSQL | `docker run --rm postgres:18.7 postgres --version` | `postgres (PostgreSQL) 18.7`; `PGDATA=/var/lib/postgresql/18/docker`, volume `/var/lib/postgresql` |
| Aplicação de SQL com `psql` | `psql -v ON_ERROR_STOP=1 -f /db/probe.sql` duas vezes, com DDL `IF NOT EXISTS` | 1ª execução: `CREATE TABLE`; 2ª: aviso `already exists, skipping` e código 0; `NUMERIC(12,2)` gravou `10.005` como `10.01` |
| `PostgresHook` | Airflow 3.4.0 com `AIRFLOW_CONN_SHOPBRASIL_POSTGRES` | `select version()` retornou `PostgreSQL 18.7` |
| Pilha web | `python:3.12.16-slim` + `pip install fastapi==0.162.0 uvicorn==0.54.0 psycopg[binary]==3.3.6 jinja2==3.1.6` | importações `3.12.16 0.162.0 0.54.0 3.3.6 3.1.6`; psycopg leu `Decimal('10.01')` |
| Chart.js | registro npm `chart.js/latest` | 4.6.1 |

O arquivo `probe.sql` foi temporário e não faz parte do esquema. O DDL real pertence à #3.


## Lint de arquitetura — #26

Comando versionado, executado na raiz do repositório. O [README de qualidade](../qualidade/README.md) descreve as regras e os códigos de diagnóstico.

```bash
python3 qualidade/lint_arquitetura.py
python3 -m unittest discover -s qualidade/tests
```

| Data | Base | Ambiente | Resultado |
|---|---|---|---|
| 2026-11-22 | `main` em `96fe6c1`, antes do código da #1 | Python 3.14.7 (macOS) | Lint: `OK: 0 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.`, saída 0. Testes: `Ran 38 tests ... OK`. |
| 2026-11-22 | `main` em `96fe6c1` | Python 3.11.16 | Testes: `Ran 38 tests ... OK`. |

Os testes montam repositórios temporários com a configuração versionada. Cobrem um código conforme que passa e exemplos proibidos que falham: imports absolutos, relativos e dinâmicos entre `pipeline/` e `web/` (ARQ001), inversões de camada no Pipeline e na Web/API (ARQ002), pacote externo no domínio, na aplicação e em consultas (ARQ003), drivers, HTTP e hooks vetados no DAG e na apresentação (ARQ004), relativo que sai do pacote (ARQ005), arquivo sem camada (ARQ006), sintaxe inválida (ARQ007), saída 1 com `arquivo:linha:coluna` e saída 2 para configuração que tente liberar imports entre módulos.

Cada PR de código registra o resultado do lint pelo [template de PR](../.github/pull_request_template.md) e pelo workflow `Arquitetura`.

## #2 — Ambiente Docker, Connection e pool

Validação executada em 22 nov. 2026, com Docker 29.9.2 e Docker Compose v5.6.1, sobre a base `main` f942b4e com o patch da #2. O ambiente está descrito em [ambiente-docker.md](ambiente-docker.md). O `db/schema.sql` da #3 ainda não existia. O cenário usou a fixture isolada `e2e/ambiente/fixtures/sql/01-sonda.sql` e o DAG de sonda `e2e_ambiente_sonda`.

Comando: `e2e/ambiente/executar.sh`. Ele sobe o projeto Compose `shopbrasil-e2e-ambiente-<timestamp>` com senhas aleatórias e remove contêineres e volumes ao final. Duração observada: cerca de 1 min. Código de saída 0.

| Cenário | Resultado | Observado |
|---|---|---|
| A1 configuração | PASS | `docker compose config -q` |
| A2 subida limpa | PASS | `up --wait` concluído; api-server, scheduler, dag-processor e postgres `healthy`; `db-init` e `airflow-init` com código 0 |
| A3 versões | PASS | Airflow 3.4.0, PostgreSQL 18.7, provider postgres 7.1.1 |
| A4 `ecommerce_pool` | PASS | 2 slots |
| A5 Connection `shopbrasil_postgres` | PASS | `PostgresHook` conectou como `shopbrasil_writer` ao banco `shopbrasil`, porta 5432 |
| A6 esquema | PASS | aviso `/db/schema.sql não encontrado` (DDL da #3); fixture `e2e_ambiente_sonda` aplicada |
| A7 privilégios por tabela | PASS | 0 violações em 1 tabela de `public`; leitor e escritor sem `CREATE` no esquema e sem `CONNECT` no banco `airflow` |
| A8 leitura da web | PASS | `SELECT` aceito; `INSERT`, `UPDATE` e `DELETE` com `permission denied for table`; `CREATE TABLE` com `permission denied for schema public`; banco `airflow` com `permission denied for database`; senha incorreta rejeitada |
| A9 escrita sem DDL | PASS | `CREATE TABLE` do escritor com `permission denied for schema public` |
| A10 Airflow sem a Web/API | PASS | serviços sem Web/API; 0 erros de importação; DAG run `success`; 4 tasks mapeadas `success` no `ecommerce_pool`; 4 linhas gravadas pelo escritor; concorrência máxima observada 2 |
| A11 reaplicação do `db-init` | PASS | código 0 sobre volume existente; dados preservados e leitura mantida |
| A12 `db-init` com SQL inválido | PASS | código 3, `syntax error` |

Verificação complementar do Compose sem o override E2E, com um `.env` derivado do `.env.example`: subida com os seis serviços nos estados esperados, `POST /auth/token` com o usuário da interface retornou 201, `airflow pools list` mostrou `ecommerce_pool` com 2 slots e o `db-init` registrou o aviso do esquema ausente. Com as senhas vazias do template, `docker compose config` falhou com `defina <VARIÁVEL> no .env`.

Lint de arquitetura sobre o mesmo patch, com Python 3.14.7: `python3 qualidade/lint_arquitetura.py --listar` retornou `OK: 0 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.` e saída 0; `python3 -m unittest discover -s qualidade/tests` retornou `Ran 38 tests ... OK`. O DAG de sonda fica em `e2e/`, fora do escopo do lint, porque usa `PostgresHook` diretamente para testar a Connection.

Na revisão final, `e2e/ambiente/executar.sh` passou as 29 verificações. O A7 conferiu os quatro privilégios do escritor separadamente; A2 e A10 subiram apenas os serviços do Airflow e suas dependências.

## #3 — Esquema analítico

Validação executada em 22 nov. 2026, com Docker 29.9.2 e Docker Compose v5.6.1, sobre a base `main` 9e395db com o patch da #3. O DDL está em [db/schema.sql](../db/schema.sql) e o modelo em [modelo-dados.md](modelo-dados.md).

Correção no `db-init`: a condição `[ --single-transaction -f "$SCHEMA_FILE" ]` de `db/aplicar.sh` terminava com `unexpected operator` e caía no aviso de arquivo ausente, sem aplicar o esquema. A condição passou a ser `[ -f "$SCHEMA_FILE" ]`; o `--single-transaction` continua no `psql`. Com o `db/aplicar.sh` anterior, o cenário do esquema falha em E1 com `AVISO /db/schema.sql não encontrado`.

Comando: `e2e/esquema/executar.sh`. Ele sobe só `postgres` e `db-init` no projeto Compose `shopbrasil-e2e-esquema-<timestamp>`, com senhas aleatórias, e remove contêineres e volumes ao final. Duração observada: cerca de 10 s. Código de saída 0, 36 verificações PASS.

| Cenário | Resultado | Observado |
|---|---|---|
| E1 aplicação em banco vazio | PASS | banco `shopbrasil` inexistente antes do `db-init`; `db-init` com código 0 e `aplicando /db/schema.sql em shopbrasil`; apenas `categoria`, `metricas_categoria` e `metricas_categoria_historico` em `public`, donas da conta administrativa |
| E2 colunas e tipos | PASS | 17 colunas `NOT NULL`; `id` `integer` identity `ALWAYS`; preços `numeric(12,2)`; `quantidade_produtos` `integer`; `data_referencia` e `data_execucao` `date` |
| E3 chaves e FKs | PASS | PK `categoria(id)` e `UNIQUE (codigo)`; PK `metricas_categoria(id)` e `UNIQUE (data_referencia, categoria_id)`; PK `metricas_categoria_historico(run_id, categoria_id)`; duas FKs para `categoria(id)` com `ON DELETE RESTRICT` |
| E4 categoria | PASS | ids 1 e 2 gerados; id manual recusado (`cannot insert a non-DEFAULT value into column "id"`); código duplicado e código vazio recusados; `ON CONFLICT (codigo) DO NOTHING` sem nova linha |
| E5 snapshot diário | PASS | exemplo da #15 lido pela conta de leitura como `10.00`, `10.00`, `10.01`, `3`; limite `9999999999.99` aceito; data e categoria duplicadas, FK inexistente, métrica nula, `numeric field overflow`, mínimo acima da média e quantidade zero recusados; `10.005`, `0.125` e `10.004` arredondados pelo tipo para `10.01`, `0.13` e `10.00`; tipos `numeric`, `integer` e `date` no cliente |
| E6 histórico por run | PASS | run e categoria duplicadas recusadas; `ON CONFLICT DO NOTHING` na mesma run preservou `10.00` e `3`; runs distintas acrescentaram linhas; FK inexistente, `run_id` vazio e máximo abaixo da média recusados; remoção de categoria referenciada recusada por `RESTRICT`; leitor sem `INSERT` |
| E7 reaplicação do `db-init` | PASS | código 0 com 3 avisos `already exists, skipping`; colunas, restrições, md5 dos dados e contagens 2/3/3 iguais; identity continuou (novo id 6, máximo anterior 2) |
| E8 transação | PASS | `BEGIN` + `schema.sql` + `ROLLBACK` não deixou tabelas, então o arquivo não tem `COMMIT` próprio; `db-init` com o esquema real seguido de erro terminou com código 3 e `syntax error` e não criou tabelas; o banco analítico ficou com 3/3/3 linhas |

Cenário de ambiente afetado, `e2e/ambiente/executar.sh`, sobre o mesmo patch: 29 verificações PASS, código de saída 0, cerca de 1 min. Com `db/schema.sql` presente, A6 registrou `aplicado`, A7 conferiu 0 violações em 4 tabelas de `public` (as três da #3 e a sonda) e A11 reaplicou o `db-init` com código 0 e 4 avisos `already exists`.

Lint de arquitetura sobre o mesmo patch, com Python 3.14.7: `python3 qualidade/lint_arquitetura.py --listar` retornou `OK: 0 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.` e saída 0; `python3 -m unittest discover -s qualidade/tests` retornou `Ran 38 tests ... OK`. O patch não tem código Python.

Revisão cruzada do Claude sobre a base `main` 9e395db com o patch da #3 e o ajuste de documentação da revisão, em 22 nov. 2026: `e2e/esquema/executar.sh` terminou com código 0 e todas as 36 verificações PASS; `e2e/ambiente/executar.sh` terminou com código 0 e 29 verificações PASS, com A6 `aplicado`, A7 `violações/tabelas = 0/4` e A11 com 4 avisos `already exists`; lint `OK: 0 arquivo(s) Python verificado(s)` e testes do lint `OK`. Decisões conferidas contra a especificação: `data_execucao` é `date` com a data de referência da run, no sentido de data lógica do Airflow; `descricao` é obrigatória e recebe o código no cadastro automático; as restrições de quantidade positiva e de preços ordenados e não negativos valem para snapshot e histórico, e a #6 rejeita preço negativo antes do cálculo.

## #15 — Contrato e cálculo das métricas

Validação executada em 22 nov. 2026, com Python 3.14.7 (macOS), sobre a base `main` bba4730 com o patch da #15. O contrato está em [modelo-dados.md](modelo-dados.md#contrato-das-métricas--15).

| Verificação | Comando | Resultado observado |
|---|---|---|
| Testes do domínio | `python3 -m unittest discover -s pipeline/tests -t pipeline` | `Ran 22 tests ... OK` |
| Exemplo da issue | `test_exemplo_da_issue` | `10.00`, `10.00`, `10.01` → média `10.00`, mínimo `10.00`, máximo `10.01`, quantidade `3` |
| Empate | `test_empate_arredonda_para_cima` | média exata `10.005` → `10.01` (`ROUND_HALF_UP`) |
| Arredondamento único | `test_arredonda_uma_unica_vez` | `0.004`, `0.004`, `0.007` → `0.01`; arredondar cada preço antes daria `0.00` |
| Sem float | `EntradaInvalidaTest`, `TransporteXcomTest` | `float`, `bool`, `NaN`, infinito, número no lugar de string e texto não decimal recusados; ida e volta por `json.dumps`/`json.loads` preserva os valores |
| Preços inteiros da FakeStore | `FronteiraJsonTest` | `json.loads` com `parse_float=Decimal` e `parse_int=Decimal`: `695` e `22.3` → média `358.65`; só com `parse_float`, o `int` é recusado |
| Saída validada | `SaidaInvalidaTest` | escala diferente de duas casas (`10.005`, `10`, `1E+1`, `10.0`), quantidade 0, mínimo acima da média, mínimo negativo, `float` e `int` recusados |
| Lint de arquitetura | `python3 qualidade/lint_arquitetura.py` | `OK: 5 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.` |

Revisão cruzada do Kiro sobre cf4092d: aprovado com ajustes. Ajustes aplicados: conversão de preços inteiros na fronteira (`parse_int=Decimal`), validação de `MetricasCategoria`, um item por código na saída consolidada, média exata como fração com um único arredondamento e remoção da ressalva sobre valores positivos.

Sem cenário E2E nesta issue: o cálculo ainda não é acionado pelo DAG. A #16 integra o contrato às tasks mapeadas e acrescenta o cenário ao `e2e/pipeline/executar.sh`.

## #4 — DAG TaskFlow, agenda e data de referência

Validação executada em 22 nov. 2026, com Docker 29.9.2 e Docker Compose v5.6.1, sobre a revisão 9a957f0 (base `main` bba4730, com o esquema da #3) e repetida na revisão final 511e988 (base `main` db55d81), com o mesmo resultado. O DAG está descrito no [README do pipeline](../pipeline/README.md#dag-shopbrasil_metricas).

Comando: `e2e/pipeline/executar.sh`. Ele sobe o Airflow e o PostgreSQL no projeto Compose `shopbrasil-e2e-pipeline-<timestamp>`, com senhas aleatórias e os DAGs reais de `pipeline/dags`, e remove contêineres e volumes ao final. Duração observada: cerca de 1 min. Código de saída 0, 19 verificações PASS.

| Cenário | Resultado | Observado |
|---|---|---|
| P0 subida | PASS | `up --wait` dos serviços do Airflow e dependências |
| P1 importação | PASS | DAG `shopbrasil_metricas` registrado; 0 erros de importação |
| P2 estrutura | PASS | TaskGroups `analise`, `ingestao`, `preparacao`, `publicacao`; arestas `preparacao.identificar>ingestao.buscar_produtos`, `ingestao.buscar_produtos>analise.consolidar`, `analise.consolidar>publicacao.publicar`, `preparacao.identificar>publicacao.publicar` |
| P3 agenda | PASS | `timetable_summary` `0 6 * * *`, fuso `America/Sao_Paulo`; `start_date` `2026-11-01T03:00:00+00:00` (00:00 em Brasília); `catchup=False` |
| P3 catchup | PASS | ao despausar, uma única run `scheduled`, a da ocorrência das 06:00 mais recente (`scheduled__2026-11-22T09:00:00+00:00`), sem as retroativas desde 1º nov.; `next_dagrun` às 06:00 de Brasília do dia seguinte |
| P4 run agendada real | PASS | `success`; XCom de `preparacao.identificar` = `data_referencia` `2026-11-22` |
| P4 run da ocorrência de 21/11 | PASS | executada em 22/11 com data lógica `2026-11-21T09:00:00+00:00`; `data_referencia` `2026-11-21`, não a data do relógio; 4 tasks `success` |
| P5 reprocessamento | PASS | `airflow tasks clear` da mesma run: 1 run para a data lógica, 4 tasks com a 2ª tentativa `success`, mesmo `run_id` e `data_referencia` `2026-11-21` |
| P6 run sem data lógica | PASS | run `failed` com `OcorrenciaAusenteError`; nenhuma task seguinte executada |

Observações do Airflow 3.4 usadas pelo cenário: cada tentativa é uma linha de `task_instance`, e o XCom fica em `xcom_v2`, ligado pela `task_instance_id`.

Testes unitários na revisão final, já com os da #15: `python3 -m unittest discover -s pipeline/tests -t pipeline` retornou `Ran 30 tests ... OK`. Lint: `OK: 10 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.`

Revisão cruzada do Kiro sobre 9a957f0: aprovado com ajustes, sem bloqueantes; o Kiro também rodou o E2E com 19 PASS. Ajustes aplicados: `trap` registrado antes de criar o arquivo de senhas, logs removidos quando não há falha, `mktemp` com caminho completo no macOS, porta padrão própria do cenário (18090), P5 lendo o XCom da 2ª tentativa, fuso da agenda vindo do domínio e comentários sobre a API interna do P6 e a janela das 06:00. Mantido como está: runs manuais com data lógica arbitrária publicam para essa data; o README orienta a reprocessar limpando as tasks da run original. Na execução final, nenhum temporário `shopbrasil-e2e-*` ficou em `$TMPDIR`.

## #13 — FakeStore em Buscar Produtos

Validação executada em 22 nov. 2026, com Docker 29.9.2 e Docker Compose v5.6.1, sobre a revisão 768dac1 (base `main` 78ed846). A interface entregue à #6 está no [README do pipeline](../pipeline/README.md#buscar-produtos--13).

Comando: `e2e/pipeline/executar.sh`, com a FakeStore controlada de `e2e/fakestore` (override `e2e/pipeline/compose.e2e.yml`). Código de saída 0 e 32 verificações PASS: as 19 da #4, que foram reexecutadas, e as 13 da #13.

| Cenário | Resultado | Observado |
|---|---|---|
| I1 configuração de produção | PASS | `3|60.0|2.0|15`: `retries`, `retry_delay` em segundos, multiplicador do backoff e timeout HTTP, lidos sem a variável de atraso reduzido do override |
| I2 coleta normal | PASS | 1 tentativa `success`; XCom `tipo_raiz` `array`, 6 produtos, `695` e `109.95` como strings, categoria `men's clothing`; 704 bytes |
| I3 falha transitória | PASS | HTTP 500 na 1ª tentativa; tentativas `1:failed 2:success`; 2 requisições; run `success` e as 3 tasks seguintes executadas |
| I4 falha persistente | PASS | 4 respostas 500; tentativas `1:failed 2:failed 3:failed 4:failed`; run `failed`; `analise.consolidar` e `publicacao.publicar` `upstream_failed`; intervalos de 4, 6 e 16 s entre requisições; exceção registrada nas 4 tentativas |

No Airflow 3.4, a linha da tentativa que leva à retentativa fica com estado `failed`. A espera seguinte é sorteada entre N e 2N.

Verificação contra a FakeStore real (`https://fakestoreapi.com/products`, imagem `apache/airflow:3.4.0-python3.12`):

- O `User-Agent` padrão `Python-urllib/3.12` recebeu `HTTP Error 403: Forbidden`, e `shopbrasil-pipeline/1.0` recebeu 200 com 10.640 bytes.
- `coletar_catalogo(composicao.fonte_catalogo())` devolveu `array` com 20 produtos e XCom de 2.280 bytes.
- Categorias: `men's clothing` 4, `jewelery` 4, `electronics` 6 e `women's clothing` 6.
- Os 20 preços vieram com `tipo_preco` `number`. Os inteiros `695`, `168`, `64`, `109`, `109`, `114` e `599` foram preservados como strings decimais.

Testes unitários: `Ran 42 tests ... OK`. Lint: `OK: 15 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.`

Revisão cruzada do Kiro sobre 0f82aba: **reprovado** por um bloqueante. A FakeStore real responde 403 ao `User-Agent` padrão do `urllib`.

Correções aplicadas em 768dac1:
- `User-Agent` próprio, com teste unitário. A FakeStore controlada passou a responder 403 a `Python-urllib`, como a API real.
- `retry_exponential_backoff=2.0` explícito, conferido no I1.
- Configuração ausente falha com `AirflowFailException`, sem gastar retentativas.
- Remoção de `TIPOS_JSON`, que não era usado.
- O README passou a citar `NaN` e `Infinity` para a #6.

## #17 — Publicação transacional do snapshot diário

Validação executada em 22 nov. 2026, com Docker 29.9.2, Docker Compose v5.6.1 e Python 3.14.7 (macOS), sobre a revisão 01ef1aa (base `main` 78ed846). O desenho está no [README da infraestrutura](../pipeline/shopbrasil_pipeline/infraestrutura/README.md#publicação-17).

Comando: `e2e/publicacao/executar.sh`. Ele sobe Airflow 3.4.0 e PostgreSQL 18.7 no projeto Compose `shopbrasil-e2e-publicacao-<timestamp>` (porta padrão 18100), com o esquema da #3 aplicado pelo `db-init`, e roda o driver [fixtures/publicar.py](../e2e/publicacao/fixtures/publicar.py) dentro do contêiner `airflow-scheduler`. O driver usa `composicao.publicar_snapshot()` com a Connection `shopbrasil_postgres` real; as leituras usam a conta de leitura. Duração observada: cerca de 36 s. Código de saída 0, 26 verificações PASS.

| Cenário | Resultado | Observado |
|---|---|---|
| U0 subida | PASS | `up --wait` dos serviços do Airflow e dependências |
| U1 Connection | PASS | `current_user` pelo `PostgresHook` = `shopbrasil_writer`; banco analítico vazio (0 categorias, 0 métricas) |
| U2 publicação de 2026-11-20 | PASS | `categorias_cadastradas` = `electronics`, `men's clothing`; cadastro com `descricao` igual ao código; snapshot `electronics=20.00/15.00/25.00/2`, `men's clothing=30.00/30.00/30.00/1` |
| U3 publicação inicial de 2026-11-21 | PASS | só `jewelery` cadastrada (`electronics` já existia); snapshot `electronics=10.00/10.00/10.01/3`, `jewelery=10.01/10.00/10.01/2` |
| U4 valores lidos | PASS | conta de leitura: `electronics|10.00|10.00|10.01`, `jewelery|10.01|10.00|10.01`, iguais aos publicados |
| U5 republicação de 2026-11-21 (mesma run) | PASS | snapshot substituído por `electronics=12.00/11.00/13.00/4`, `women's clothing=50.50/40.00/61.00/3`; `jewelery` fora do dia e mantida no cadastro; só `women's clothing` cadastrada; no máximo 1 linha por data+categoria |
| U6 republicação idêntica | PASS | 2 linhas no dia, máximo 1 por chave |
| U7 outra data intacta | PASS | 2026-11-20 com os mesmos valores e o mesmo `id@xmin` (`1@1236 2@1236`) após as publicações de 2026-11-21 |
| U8 falha injetada no ponto de extensão | PASS | etapa adicional viu 2 linhas novas na transação e lançou `FalhaInjetada`; snapshot anterior preservado (mesmos valores e `id@xmin` `7@1256 8@1256`); `toys` não cadastrada |
| U9 falha do banco no meio | PASS | 2º item com `10000000000.00` gerou `NumericValueOutOfRange` (`numeric field overflow`) depois do 1º inserido; snapshot anterior e cadastro preservados |
| U10 recusas antes da transação | PASS | código repetido → `CategoriaRepetidaError`; coleção vazia → `ColecaoVaziaError`; snapshot, cadastro e 2026-11-20 inalterados |

O cenário chama o caso de uso diretamente porque o DAG só passa a acionar a publicação na #16. O `e2e/pipeline/executar.sh` foi repetido na mesma revisão porque o pacote montado no Airflow mudou: código de saída 0, 19 verificações PASS, sem alteração no DAG.

| Verificação | Comando | Resultado observado |
|---|---|---|
| Testes unitários | `python3 -m unittest discover -s pipeline/tests -t pipeline` | `Ran 39 tests ... OK` (9 novos do caso de uso com repositório falso) |
| Lint de arquitetura | `python3 qualidade/lint_arquitetura.py` | `OK: 15 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.` |

Revisão cruzada do Claude sobre 6e8763c: aprovado, sem bloqueantes. A transação é única: o bloco `with conexao` do psycopg2 faz commit ao sair e rollback em qualquer exceção. O conjunto do dia é apagado e reinserido. Coleção vazia e código repetido são recusados antes de abrir a transação. O ponto de extensão `etapas_adicionais` compartilha o cursor com a #18. A aplicação não tem SQL nem Airflow. Na integração com a #13 (b2f180c), as duas fábricas ficaram em `composicao.py`; o adaptador PostgreSQL é importado dentro da função, para que os testes unitários não dependam do provider. Sobre essa revisão: `e2e/publicacao/executar.sh` terminou com código 0 e 26 verificações PASS; testes unitários `OK`; lint `OK: 18 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.`

## #14 — Callbacks, aviso de falha e alerta simulado

Validação executada em 22 nov. 2026, com Docker 29.9.2 e Docker Compose v5.6.1, sobre a revisão 1d297b1 (base `integracao-backlog` c7e5d74).

Comando: `e2e/pipeline/executar.sh`. Código de saída 0 e 39 verificações PASS: as 32 da #4 e da #13 e as 7 da #14. Os callbacks gravam no log de cada tentativa (`/opt/airflow/logs/dag_id=.../run_id=.../task_id=.../attempt=N.log`).

| Cenário | Resultado | Observado |
|---|---|---|
| C1 sucesso | PASS | `[SHOPBRASIL][SUCESSO] ... task=ingestao.buscar_produtos tentativa=1/4` na run normal |
| C2 retry e recuperação | PASS | `[SHOPBRASIL][RETRY] ... tentativa=1/4 erro=HTTPError` na 1ª tentativa e `[SUCESSO]` na 2ª; nenhum alerta |
| C3 falha definitiva | PASS | 3 linhas `[RETRY]` nas tentativas 1 a 3, `[FALHA] ... tentativa=4/4` e um único `[ALERTA SIMULADO]`, só na 4ª tentativa |
| C4 falha de outra task | PASS | `[SHOPBRASIL][FALHA] ... task=preparacao.identificar tentativa=1/1 erro=OcorrenciaAusenteError` na run sem data lógica, sem alerta |

Linhas reais da run `e2e__falha_persistente`:

```text
[SHOPBRASIL][RETRY] dag=shopbrasil_metricas run=e2e__falha_persistente task=ingestao.buscar_produtos tentativa=1/4 erro=HTTPError: HTTP Error 500: Internal Server Error
[SHOPBRASIL][FALHA] dag=shopbrasil_metricas run=e2e__falha_persistente task=ingestao.buscar_produtos tentativa=4/4 erro=HTTPError: HTTP Error 500: Internal Server Error
[SHOPBRASIL][ALERTA SIMULADO] destino=equipe-dados@shopbrasil.example assunto=ShopBrasil: ingestão da FakeStore falhou na run e2e__falha_persistente | Buscar Produtos esgotou 4 tentativa(s); a publicação da run não ocorreu. ...
```

Testes unitários: `Ran 54 tests ... OK`. Lint: `OK: 21 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.`

Revisão cruzada do Kiro sobre 1d297b1: aprovável com recomendações, sem bloqueantes. Ajustes em 30ddff2:
- tentativa contada no ciclo atual, que continua correta depois de `tasks clear`;
- falha sem retentativa (`AirflowFailException`) gera alerta "sem retentativa (erro não recuperável)", e não "esgotou";
- `on_failure_callback` do DAG emite `[SHOPBRASIL][FALHA RUN]`, gravado no log do dag-processor;
- C2 confirma que não há `[FALHA]` na recuperação;
- texto procurado nos logs passado ao grep como argumento.

Sobre 30ddff2, `e2e/pipeline/executar.sh` terminou com código 0 e 40 PASS, incluindo `C5 aviso de falha da run`: 1 linha `[FALHA RUN]` para `e2e__falha_persistente`. Testes unitários: `Ran 56 tests ... OK`.

## #6 — ValidarProdutosOperator

Validação executada em 22 nov. 2026, com Docker 29.9.2 e Docker Compose v5.6.1, sobre a revisão 2221b50 (base `integracao-backlog` 455860c). As regras ficam em `pipeline/shopbrasil_pipeline/aplicacao/validacao.py`, e o operador em `pipeline/dags/operadores/validar_produtos.py`.

Comando: `e2e/pipeline/executar.sh`, com os catálogos controlados `sem_joias`, `vazio`, `preco_invalido` e `categoria_em_branco` em `e2e/fakestore/catalogos/`. Código de saída 0 e 55 verificações PASS, incluindo as 15 da #6.

| Cenário | Resultado | Observado |
|---|---|---|
| P2 estrutura | PASS | aresta linear `ingestao.buscar_produtos>ingestao.validar_produtos>analise.consolidar` |
| V1 coleta normal | PASS | XCom do operador com `electronics,jewelery,men's clothing,women's clothing` e 6 produtos (`codigo_categoria`, `preco`) |
| V1 categoria ausente | PASS | catálogo sem `jewelery` aceito; run `success`; 3 categorias e 3 produtos |
| V2 catálogo vazio | PASS | run `failed`; coleta `1:success`, validação `1:failed` sem retentativa; análise e publicação `upstream_failed`; log `Validação do catálogo: catálogo vazio` e aviso `[SHOPBRASIL][FALHA] ... task=ingestao.validar_produtos` |
| V3 preço inválido | PASS | mesmo comportamento; log `Validação do catálogo: produto 2: preço do tipo boolean` |
| V4 categoria em branco | PASS | mesmo comportamento; log `Validação do catálogo: produto 3: categoria em branco` |

A preservação do snapshot publicado nas rejeições é conferida no banco a partir da #16, quando a publicação passa a ser chamada pelo DAG. Até a #6, a publicação nem chega a ser executada (`upstream_failed`).

Testes unitários: `Ran 62 tests ... OK`. Cobrem catálogo válido sem arredondamento, categoria ausente, vazio, raiz que não é lista, categoria em branco, nula, numérica ou ausente, preço booleano, texto, nulo, negativo, lista ou `NaN`, e a lista de todos os motivos. Lint: `OK: 25 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.`

Ajustes descobertos na verificação:
- Importar `AirflowFailException` de `airflow.sdk.exceptions`, porque o caminho antigo está depreciado no 3.4.
- As consultas ad hoc ao DagBag no E2E usam o `PYTHONPATH` dos processos do Airflow, que inclui a pasta de DAGs.

## #18 — Histórico por run na transação de publicação

Validação executada em 22 nov. 2026, com Docker 29.9.2, Docker Compose v5.6.1 e Python 3.14.7 (macOS), sobre a revisão eebc845 (base `integracao-backlog` c7e5d74). O desenho está no [README da infraestrutura](../pipeline/shopbrasil_pipeline/infraestrutura/README.md#histórico-por-run-18).

Comando: `e2e/publicacao/executar.sh`, o mesmo cenário da #17, estendido com as verificações H1 a H4. `composicao.publicar_snapshot()` agora grava o histórico na mesma transação, por isso U1 a U10 também exercitam a etapa. U1 confere o histórico vazio, e U8 e U9 conferem que ele fica preservado. Duração observada: cerca de 40 s. Código de saída 0, 44 verificações PASS, 0 FAIL.

| Cenário | Resultado | Observado |
|---|---|---|
| H1 primeira publicação grava o histórico | PASS | `scheduled__2026-11-20…`: `electronics=2026-11-20/20.00/15.00/25.00/2`, `men's clothing=2026-11-20/30.00/30.00/30.00/1`; `scheduled__2026-11-21…`: `electronics=2026-11-21/10.00/10.00/10.01/3`, `jewelery=2026-11-21/10.01/10.00/10.01/2`; junção com o snapshot pela data: `2|0;2|0` (linhas, divergentes) |
| H2 reprocessar a mesma run com métricas diferentes | PASS | snapshot de 2026-11-21 substituído (U5, U6), histórico da run com `electronics` e `jewelery` nos valores originais e o mesmo `run:categoria@xmin` de H1; `women's clothing`, chave nova da run, acrescentada; 5 linhas, máximo 1 por run+categoria |
| U8 falha injetada depois do histórico | PASS | a etapa viu 4 linhas da run no histórico dentro da transação (com `toys`); após o rollback, histórico, snapshot e cadastro iguais aos anteriores |
| U9 falha do banco no snapshot | PASS | histórico inalterado (valores e `xmin`) |
| H3 violação de CHECK no histórico | PASS | run_id `" "` → `CheckViolation` em `metricas_categoria_historico_run_id_ck`; snapshot de 2026-11-21 com os mesmos valores e `id@xmin` (`7@1257 8@1257`), `toys` não cadastrada, histórico inalterado |
| H4 run distinta acrescenta | PASS | `manual__2026-11-21T15:00:00+00:00`: snapshot do dia substituído por `electronics=14.00/13.00/15.00/5`, `jewelery=11.00/11.00/11.00/1`; histórico da run com os mesmos valores (`2|0`); linhas das outras runs com o mesmo `xmin`; 7 linhas, máximo 1 por run+categoria, 2 runs para `electronics` em 2026-11-21 |

No reprocessamento da mesma run, `ON CONFLICT (run_id, categoria_id) DO NOTHING` preserva só as chaves já gravadas. Se a coleta reprocessada trouxer uma categoria que a run ainda não tinha, ela é acrescentada, como `women's clothing` em H2. O `e2e/pipeline/executar.sh` foi repetido na mesma revisão porque o pacote montado no Airflow mudou: código de saída 0, 32 verificações PASS, sem alteração no DAG.

| Verificação | Comando | Resultado observado |
|---|---|---|
| Testes unitários | `python3 -m unittest discover -s pipeline/tests -t pipeline` | `Ran 57 tests ... OK` (6 novos: SQL e parâmetros da etapa, mesmos `Decimal` do snapshot, sem controle de transação, ordem antes do único commit, rollback quando o histórico falha, ligação na composição) |
| Lint de arquitetura | `python3 qualidade/lint_arquitetura.py` | `OK: 20 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.` |

## #9 — Consultas Web/API para snapshot e evolução

Validação executada em 22 nov. 2026, com Docker 29.9.2, Docker Compose v5.6.1 e Python 3.14.7 (macOS), sobre a revisão 5f5611e (base `integracao-backlog` c7e5d74). O desenho das camadas e o contrato HTTP para as #19–#21 estão no [README da web](../web/README.md).

Comando: `e2e/web/executar.sh`. Ele constrói a imagem `web/Dockerfile` (`python:3.12.16-slim`, dependências fixadas) e sobe `postgres`, `db-init` e `web` no projeto Compose `shopbrasil-e2e-web-<timestamp>` (porta padrão 18110, só em 127.0.0.1), sem o Airflow. Os dados de teste são gravados pela conta administrativa ([fixtures/snapshot.sql](../e2e/web/fixtures/snapshot.sql)) e as rotas são consultadas pela porta publicada. Duração observada: 14 s. Código de saída 0, 20 verificações PASS.

| Cenário | Resultado | Observado |
|---|---|---|
| W0 subida | PASS | `up --build --wait` de `web` com dependências; serviços criados: `db-init postgres web`; porta `127.0.0.1:18110` |
| W1 credenciais do contêiner | PASS | `SHOPBRASIL_DB_READER_USER=shopbrasil_reader`; nenhuma variável `WRITER`, `POSTGRES_PASSWORD` ou `AIRFLOW` |
| W2 snapshot vazio | PASS | `GET /api/metricas/ultima-data` → `{"data":null}` |
| W3 data com métricas | PASS | fixture `3|4|1` (categorias, snapshot, histórico); `GET /api/metricas?data=2026-11-22` → `electronics` `10.00/10.00/10.01/3` e `jewelery` `10.01/10.00/10.01/2`, em ordem de código, com descrição |
| W4 última data e precisão | PASS | `{"data":"2026-11-24"}`; preços `"1234567890.12"`, `"0.10"`, `"9999999999.99"` como strings, sem float |
| W5 data sem registros | PASS | `{"data":"2026-11-21","vazia":true,"metricas":[]}` |
| W6 série | PASS | `electronics` de 19/11 a 24/11: seis pontos em ordem, `null` em 19, 21, 23 e 24, `"20.00"` em 20 e `"10.00"` em 22; o valor `99.99` do histórico de 21/11 não aparece; intervalo de um dia inclusivo; `men's clothing` (apóstrofo, sem métricas) só com `null` |
| W7 categorias | PASS | `electronics`, `jewelery`, `men's clothing`, em ordem de código |
| W8 erros e documentação | PASS | `422` para `2026-02-30` e para `data` ausente (`loc` `["query","data"]`, `data inválida '2026-02-30'; use AAAA-MM-DD`), `400` para intervalo invertido, `404` para `toys`; `/openapi.json` com as quatro rotas |
| W9 somente leitura | PASS | no contêiner da web, com a configuração da composição: `current_user` = `shopbrasil_reader`; `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` e `CREATE TABLE` → `InsufficientPrivilege`; contagens inalteradas (`3|4|1`); o repositório abre a sessão com `transaction_read_only = on` |

| Verificação | Comando | Resultado observado |
|---|---|---|
| Testes da web no contêiner | `docker compose run --rm --no-deps web python -m unittest discover -s tests -t .` (no E2E, `docker run --rm shopbrasil-web:<tag> ...`) | `Ran 33 tests ... OK` |
| Testes da web no host (só biblioteca padrão) | `python3 -m unittest discover -s web/tests -t web` | `Ran 33 tests ... OK (skipped=21)`: os 12 de `consultas` rodam; apresentação, composição e infraestrutura exigem FastAPI e psycopg |
| Testes do pipeline e do lint | `python3 -m unittest discover -s pipeline/tests -t pipeline`; `python3 -m unittest discover -s qualidade/tests` | `Ran 51 tests ... OK`; `Ran 38 tests ... OK` |
| Lint de arquitetura | `python3 qualidade/lint_arquitetura.py` | `OK: 35 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.` |

## #16 — Mapeamento por categoria, pool e consolidação

Comando: `e2e/pipeline/executar.sh` (parte da suíte de 22 nov. 2026 registrada abaixo, sobre 5f589db). Valores observados:

| Cenário | Critério da #16 | Resultado | Observado |
|---|---|---|---|
| M1 instâncias mapeadas e pool | fan-out por categoria | PASS | `4` instâncias, `map/region_index 0,1,2,3`, pool `ecommerce_pool` |
| M2 concorrência | no máximo duas em paralelo | PASS | máximo de `2` simultâneas, com 3 s por instância |
| M3 consolidação | um resultado por categoria | PASS | `4` itens: `electronics, jewelery, men's clothing, women's clothing`; `men's clothing` = `66.13` |
| M3 XCom pequeno | payloads pequenos | PASS | máximo de `704` bytes (`buscar_produtos`); `calcular_metricas` 139, `consolidar` 555 |
| M4 publicação pelo DAG | saída da #15 entregue à publicação | PASS | snapshot de 21/11 igual ao calculado à parte |
| M5 categoria inédita | instância adicional sem editar o DAG | PASS | `5` instâncias, no máximo `2` simultâneas; `toys` cadastrada e publicada `67.76/15.50/120.01/2` |
| M6 falhas preservam o publicado | — | PASS | runs recusadas sem snapshot; reprocessar com catálogo vazio mantém cadastro, snapshot e histórico (valores e `xmin`) |

Na primeira execução completa, M1 e M3 falharam por consultar `map_index`, que no Airflow 3.4 é `region_index` (corrigido no PR #50).

## Validação final — suíte E2E completa (#23, #11)

Executada em 22 nov. 2026, com Docker 29.9.2, Docker Compose v5.6.1 e Python 3.14.7 (macOS), sobre `main` d7cf1a1 (merge do PR #49) com as correções dos próprios testes descritas abaixo.

Comando: `e2e/executar_todos.sh`. Código de saída 0. **236 verificações PASS, 0 FAIL.**

| Cenário | Resultado | PASS | FAIL | Duração |
|---|---|---|---|---|
| ambiente | PASS | 29 | 0 | 62s |
| esquema | PASS | 36 | 0 | 10s |
| pipeline | PASS | 67 | 0 | 206s |
| publicacao | PASS | 44 | 0 | 39s |
| web | PASS | 34 | 0 | 14s |
| integrado | PASS | 26 | 0 | 158s |

Também passaram, sobre a mesma revisão: lint de arquitetura (`OK: 48 arquivo(s) Python verificado(s), nenhuma violação de arquitetura.`), testes unitários do pipeline e os 47 testes da web no contêiner.

### Achados da primeira execução e correções

A primeira execução, sobre d7cf1a1 sem correções, terminou com ambiente, esquema e publicação aprovados e falhas em pipeline (2), web (5) e integrado (16). Todas estavam nos scripts de teste; o código do pipeline e da web não mudou.

| Achado | Issue | Causa | Correção |
|---|---|---|---|
| M1 e M3 sem resultado | #16 | No Airflow 3.4 a coluna da task mapeada é `region_index`, não `map_index` | Consultas usam `region_index` |
| P1 e P2 com página vazia | #19 | `pagina` sem argumento parava por `set -u` | Parâmetro opcional `${1:-}` |
| P3, P4 e E1 com JSON “diferente” | #19, #21 | O filtro `tojson` do Jinja ordena as chaves; os valores eram iguais | Comparação com JSON canônico de chaves ordenadas |
| N1 a N6 com runs em `queued` | #23 | O DAG entra pausado e o cenário não o despausava | Despausa, aguarda a run agendada automática e só então dispara as runs do cenário |
| N2 a N6 sem publicação (reexecução) | #23 | Datas de outubro, antes do `start_date` (01/11): a run termina `success` sem executar tasks | Datas 10 a 13/11; N1 passa a exigir `publicacao.publicar` executada |
| N4 com JSON “diferente” | #23 | Mesma ordenação do `tojson` | Comparação com chaves ordenadas |

### Conferência visual do painel no navegador

Sobre o ambiente do cenário integrado mantido com `E2E_MANTER=1`, com dados publicados pelo DAG real (10 a 12/11), o painel foi aberto num navegador Chromium:

- Chart.js 4.6.1 carregado de `/estaticos/`, sem erros no console e sem requisições externas.
- Em 11/11, a tabela mostrou 5 categorias (com `toys`), e as barras mostraram os mesmos preços médios: `86.5, 695, 66.13, 67.76, 9.85`.
- Ao trocar a data no campo para 09/11, a página recarregou com “Sem métricas para esta data” na tabela e nas barras, mantendo os filtros da evolução.
- Linha de `jewelery` de 10 a 12/11: `695, 695, null`. Linha de `toys` de 09 a 13/11: `null, null, 67.76, null, null`, com `spanGaps: false`; o único ponto aparece isolado, sem ligação nem zero nos dias sem dados.

## Revisão cruzada — #22

Realizada em 22 nov. 2026 sobre `main` f0fc0b9, conforme a divisão do AGENTS.md. Os revisores trabalharam em modo só leitura.

| Revisor | Frente revisada | Escopo | Veredito |
|---|---|---|---|
| Kiro (Claude Opus 5.5) | DAG e processamento | #6 (PR #42), #13 (PR #37), #14 (PR #39), #16 (PR #43) | Aprovado com recomendações, sem bloqueantes |
| Claude Opus 5.5 | Ambiente e persistência | #18 (PR #40); #17 já revisada no PR #38 | Aprovado com uma observação |
| Codex (GPT-6.2 Astra) | Revisão final integrada e Web/API | #9, #19–#21, #23, conjunto frente ao enunciado | Requisitos atendidos; dois bloqueantes de cobertura na #23 |

Achados e tratamento, todos em `correcoes-revisao-cruzada`:

| Origem | Achado | Severidade | Tratamento |
|---|---|---|---|
| Codex 1 | Suíte não executava os gráficos | Bloqueante (#23) | G1 (web) e N7 (integrado) operam o painel num Chromium real (Playwright 1.49.0): troca de data e filtros pelo formulário, valores entregues ao Chart.js iguais à tabela, lacunas `null`, console sem erros (5f589db) |
| Codex 2 | Preservação conferida só por contagens | Bloqueante (#23) | V5, I4, M6 e N6 comparam cadastro, snapshot e histórico com valores e `xmin` antes e depois (5f589db) |
| Codex 3 | N5 podia aprovar reprocessamento não ocorrido | Recomendação | N5 exige `tasks clear` bem-sucedido e nova tentativa de `publicacao.publicar` (5f589db) |
| Codex 4 | `/?data=0001-01-01` gerava `OverflowError` | Recomendação | Início padrão limitado a `date.min`, com teste (0371ff1) |
| Codex 5 | Guia consultava com a conta administrativa | Recomendação | Exemplos usam `shopbrasil_reader` (6f22bc2) |
| Codex 6 | Datas antigas no README do E2E; PR #49 integrado antes da suíte | Observação | README corrigido (5f589db); desvio de processo registrado abaixo |
| Kiro 1 | DAG chamava o domínio diretamente; lint permitia | Recomendação | Cálculo e tradução de XCom expostos na aplicação; lint proíbe `pipeline_dags → pipeline_dominio`, com teste do caso proibido (7d81c45) |
| Kiro 2 | #16 sem evidências por cenário | Recomendação | Seção da #16 acima |
| Kiro 3 | Tentativa após reprocessar não verificada | Observação | C6 confere `tentativa=1/4` no `attempt=2.log` (5f589db) |
| Kiro 9 | READMEs do pipeline desatualizados | Observação | Atualizados (7d81c45) |
| Kiro 4 | Mínimo e máximo arredondados para duas casas | Observação | Corrigido: a validação recusa preço com mais de duas casas ou acima de `numeric(12,2)` (escala do esquema), e mínimo e máximo não são mais arredondados; preço fora da escala no cálculo é erro (9c59db1, e5e62d5) |
| Kiro 5 | Média não usava `MODO_ARREDONDAMENTO` | Observação | Corrigido: o empate exato é resolvido por `Decimal` com o modo configurado; teste troca o modo e vê a média mudar (e5e62d5) |
| Kiro 6 | Ajustes de teste por variável de ambiente podiam mudar o retry em produção sem aviso | Observação | Corrigido: `[SHOPBRASIL][AVISO] ajuste de teste ativo` no log da coleta; I1 confere nenhum aviso sem as variáveis e C7 confere os avisos no E2E (918cf29) |
| Kiro 7 | Operador sem teste unitário | Observação | Corrigido: `tests/test_operador_validar.py` cobre saída, falha sem retentativa, XCom fora do formato e campo templatizado; roda na imagem do Airflow (4 OK) (9c59db1) |
| Kiro 8 | Código da categoria sem normalização | Observação | Corrigido sem alterar o valor da FakeStore: categoria com espaços nas extremidades é recusada, pois criaria uma chave distinta (9c59db1) |
| Claude | Reprocessar a mesma run com dados novos atualiza o snapshot, mas o histórico da run mantém os valores originais | Observação | Corrigido dentro da especificação, que manda preservar as linhas: a etapa do histórico relê a run e registra `[SHOPBRASIL][AVISO] histórico preservado diverge do snapshot` por categoria; H2 confere um aviso só para a categoria alterada (5d2cfab) |

**Desvio de processo:** o AGENTS.md pedia a suíte completa aprovada antes do merge em `main`. O PR #49 foi integrado com a validação pendente; a primeira execução falhou por defeitos dos testes, corrigidos no PR #50, e a revisão cruzada acima aconteceu depois do merge. O código de produção não teve defeito funcional apontado.

## Validação final após a revisão cruzada

Executada em 22 nov. 2026 sobre 5f589db (`correcoes-revisao-cruzada`), mesmo ambiente da validação anterior. Comando: `e2e/executar_todos.sh`. Código de saída 0. **244 verificações PASS, 0 FAIL.**

| Cenário | Resultado | PASS | FAIL | Duração |
|---|---|---|---|---|
| ambiente | PASS | 29 | 0 | 60s |
| esquema | PASS | 36 | 0 | 11s |
| pipeline | PASS | 71 | 0 | 214s |
| publicacao | PASS | 44 | 0 | 40s |
| web | PASS | 35 | 0 | 16s |
| integrado | PASS | 29 | 0 | 163s |

Destaques: G1 `4.6.1|10.00,10.01|10,10.01@2|…|20,None,10,None@4|spanGaps=False|erros=0`; N7 `4.6.1|86.50,695.00,66.13,67.76,9.85|86.5,695,66.13,67.76,9.85@5|data=2026-11-09;Sem métricas para esta data;…|None,None,67.76,None,None@5|…|erros=0`; N5 `2:success` na nova tentativa da publicação. Lint de arquitetura `OK: 48 arquivo(s)`, testes do lint (39), do pipeline e da web (48) aprovados.

## Validação final após as observações da revisão

Executada em 22 nov. 2026 sobre 5d2cfab (`observacoes-revisao`), com todas as observações da revisão cruzada corrigidas. Comando: `e2e/executar_todos.sh`. Código de saída 0. **246 verificações PASS, 0 FAIL.**

| Cenário | Resultado | PASS | FAIL | Duração |
|---|---|---|---|---|
| ambiente | PASS | 29 | 0 | 61s |
| esquema | PASS | 36 | 0 | 10s |
| pipeline | PASS | 72 | 0 | 206s |
| publicacao | PASS | 45 | 0 | 39s |
| web | PASS | 35 | 0 | 17s |
| integrado | PASS | 29 | 0 | 158s |

Novas verificações: I1 `3|60.0|2.0|15|avisos=0`; C7 `1|1`; H2 aviso de divergência `1|0`. Lint de arquitetura `OK: 49 arquivo(s)`; testes unitários do pipeline, do lint e da web aprovados; testes do operador na imagem do Airflow 3.4.0: 4 OK.

## Conferência do avaliador — #12

Executada em 24 nov. 2026 num clone novo de `main` (a3925eb), seguindo só o README e o [guia de execução](execucao.md), com a FakeStore real. Docker 29.9.2, Compose v5.6.1, macOS.

| Passo | Comando | Resultado observado |
|---|---|---|
| `.env` | bloco do README | 6 segredos gerados em `sh`, `zsh` e `bash` no macOS e em `sh` no Alpine |
| Subida | `docker compose up -d --wait` | `db-init` e `airflow-init` `Exited (0)`; demais serviços `healthy` em 25 s, com imagens já baixadas |
| DAG | `airflow dags list-import-errors`; `dags details` | Sem erros de importação; `timetable_summary 0 6 * * *` |
| Ativação | `airflow dags unpause shopbrasil_metricas` | Run `scheduled__2026-11-24T09:00:00+00:00` `success` em 10 s; 4 instâncias de `calcular_metricas` no `ecommerce_pool` |
| Banco | consulta da seção 4 com `shopbrasil_reader` | 4 categorias em 24/11 (electronics 332.50, jewelery 221.00, men's clothing 51.06, women's clothing 26.29); 4 linhas no histórico da run |
| Painel | `GET /api/metricas/ultima-data`; `GET /` | `{"data":"2026-11-24"}`; HTTP 200 |
| Run de data específica | `airflow dags trigger ... --run-id manual__2026-11-21` | `success`; 4 linhas em 21/11 |
| Reprocessamento | `airflow tasks clear ... -s/-e 2026-11-21T09:00:00+00:00 -y` | Todas as tasks na tentativa 2 com `success`; snapshot de 21/11 e histórico de `manual__2026-11-21` continuam com 4 linhas |
| Queda da API | seção 6 do guia, `FAKESTORE_BASE_URL=http://fakestore.invalid` | `[RETRY]` 1/4, 2/4 e 3/4 com `URLError`; `[FALHA]` e `[ALERTA SIMULADO]` em 4/4; `[FALHA RUN]`; demais tasks `upstream_failed`; snapshot de 21/11 e 24/11 intacto |
| Interface | `POST /auth/token` com o usuário do `.env` | HTTP 201 |
| Sem Docker | `python3 -m unittest discover -s pipeline/tests -t pipeline`; `python3 qualidade/lint_arquitetura.py` | 72 testes OK; `OK: 48 arquivo(s)` (antes do #52) |

Achados de documentação, corrigidos neste registro: o README não trazia pré-requisitos, geração dos segredos, ativação do DAG nem o mapa dos requisitos do enunciado; o guia de execução não mostrava como observar retry, callbacks e alerta fora da suíte E2E. A porta 8000 ocupada por outro projeto local exigiu `SHOPBRASIL_WEB_PORT=8001`, caso agora citado no README e no guia.

Repetida em 24 nov. 2026 sobre 3818018 (`main` com o #52): `docker compose up -d --build --wait`, sem erros de importação, run agendada de 24/11 `success` com os mesmos quatro valores, só `[SHOPBRASIL][SUCESSO]` no log (nenhum aviso de ajuste de teste) e 82 testes do pipeline OK (4 do operador pulados fora da imagem do Airflow). Lint `OK: 49 arquivo(s)`.

## Reexecução do mesmo dia e log da publicação — #12, #17, #18

Em 24 nov. 2026, sobre `main` c4c01fe, foram registradas no log as contagens da publicação e reescrito o texto sobre reexecução no [guia](execucao.md#5-executar-de-novo-o-mesmo-dia) e no [README do pipeline](../pipeline/README.md). O texto anterior dizia "não dispare uma run nova" e "run sem data lógica é recusada", mas o botão **Acionar** da interface preenche a data lógica e funciona. O comportamento não mudou, só a forma de explicá-lo.

Conferência manual com FakeStore real, num ambiente limpo:

| Ação | Snapshot do dia | Histórico | Log de `publicacao.publicar` |
|---|---|---|---|
| Run agendada de 24/11 | 4 linhas | 4 linhas da run | `0 linha(s) anterior(es) do dia substituída(s) por 4`; `4 linha(s) gravada(s), 0 já registrada(s)` |
| `trigger --logical-date` para 21/11 | 4 linhas | 4 linhas da run | idem |
| Clear de `manual__2026-11-21` | 4 linhas | 4 linhas da run | `4 linha(s) anterior(es) do dia substituída(s) por 4`; `0 linha(s) gravada(s), 4 já registrada(s) preservada(s)` |
| Acionar na interface e `trigger --logical-date` no mesmo dia 24/11 (teste anterior, sem o log novo) | continua com 4 linhas | +4 linhas por run nova | — |
| `trigger` sem `--logical-date` | sem mudança | sem mudança | `[SHOPBRASIL][FALHA] ... OcorrenciaAusenteError: run 'sem_data' sem data lógica, então não há dia de referência para publicar; dispare com --logical-date ...` |

Verificações: 85 testes unitários do pipeline OK (4 do operador pulados fora da imagem do Airflow), lint `OK: 49 arquivo(s)`. E2E `e2e/executar_todos.sh pipeline publicacao integrado`: pipeline **72 PASS**, publicacao **45 PASS**, integrado **29 PASS**, 0 FAIL. O integrado falhou duas vezes antes de passar, no build da web, com `additional privileges requested: pass "--allow=fs.read=..."`. A causa era o diretório local do autor, com acento gravado em forma decomposta (NFD), que o Compose 5.6.1 recusa como contexto de build. Executado pelo mesmo diretório em forma composta (NFC), passou. Os nomes versionados do repositório não têm mais acentos nem espaços: o enunciado passou a ser `docs/atividade-01-descricao.pdf`.


## Validação das correções de entrega — #55, #56 e #57

Em 24 nov. 2026, as seis partes da suíte foram executadas em projetos Docker descartáveis, com Airflow 3.4.0, PostgreSQL 18.7 e FakeStore controlada. A revisão combinada `4be24ae` serviu aos cenários ambiente, esquema, pipeline, publicação e web. O integrado foi reexecutado sobre `c2cbb13`, cuja única diferença foi a asserção do motivo da rejeição do instante lógico no próprio cenário integrado.

| Comando | Resultado observado |
|---|---|
| `e2e/executar_todos.sh ambiente esquema publicacao web` | Ambiente 31, esquema 38, publicação 48 e web 35 verificações aprovadas |
| `e2e/executar_todos.sh pipeline` | 77 verificações aprovadas |
| `e2e/executar_todos.sh integrado` | 38 verificações aprovadas |
| Total | 267 verificações aprovadas, 0 falhas |

| Cenário | Resultado observado |
|---|---|
| A10 e N0 — prontidão | DAG registrado quando `up --wait` retornou, DAG ativo após um `unpause` e publicação bem-sucedida |
| Conferência com a FakeStore real | Três subidas com volumes novos na primeira revisão da #55. Em cada uma, um `up --wait` e um `unpause` foram suficientes para publicar quatro linhas. A revisão posterior ampliou o timeout do health check e passou em A10 e N0 |
| N5 e N8 — reprocessamento e disparo manual | `Clear` executou a publicação na tentativa 2. O mesmo instante foi rejeitado por colisão de `dag_id` e `logical_date` e não criou run. Novo horário do mesmo dia mudou o snapshot e preservou o histórico anterior. Uma data anterior ao `start_date` terminou em `success` com zero tasks e zero linhas |
| E7 — atualização de volume | As três linhas anteriores ficaram intactas com `publicado_em` desconhecido. Uma nova inserção recebeu o instante real |
| H1–H3 — publicação | A referência retroativa foi preservada, o banco registrou o instante real e o reprocessamento conservou o instante original. A falha no histórico reverteu a transação |
| M7 — `Clear` com catálogo alterado | Mesma run e segunda publicação bem-sucedida. O snapshot mudou. Valores e instantes das linhas históricas originais foram preservados. `jewelery` e `toys` receberam novas linhas com instante real, e o log registrou a divergência das métricas |

O lint de arquitetura aprovou 49 arquivos. Passaram 81 testes do pipeline no host, 4 do operador na imagem do Airflow, 39 do lint e 48 da Web/API. A revisão cruzada com Claude Opus e Kiro levou ao ajuste do timeout de prontidão, à comprovação da troca de snapshot com catálogo diferente e à cobertura de `Clear` no DAG real. A revisão final também pediu a confirmação do motivo da rejeição do instante lógico, aprovada no N8.
