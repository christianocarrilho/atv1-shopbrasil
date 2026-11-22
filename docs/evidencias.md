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
