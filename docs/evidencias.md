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
