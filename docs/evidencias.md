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
