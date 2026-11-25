# Ambiente Docker — #2

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Este documento descreve o ambiente do [docker-compose.yml](../docker-compose.yml): serviços, contrato de configuração, contas do PostgreSQL, Connection do Airflow, pool e comandos de operação. As versões seguem a [decisão da #1](decisao-versoes-tecnologias.md).

## Serviços

| Serviço | Imagem | Papel |
|---|---|---|
| `postgres` | `postgres:18.7` | Um servidor com dois bancos: `airflow` (metadados) e `shopbrasil` (analítico). Volume `postgres-data` em `/var/lib/postgresql`. Sem porta publicada no host. |
| `db-init` | `postgres:18.7` | Execução única. Roda [db/aplicar.sh](../db/aplicar.sh): cria contas e bancos com [db/acessos.sql](../db/acessos.sql) e aplica [db/schema.sql](../db/schema.sql) numa única transação. |
| `airflow-init` | `apache/airflow:3.4.0-python3.12` | Execução única. Migra os metadados, cria o usuário da interface e define `ecommerce_pool` com 2 slots. |
| `airflow-apiserver` | idem | Interface e API em `http://localhost:${AIRFLOW_API_PORT}` (padrão 27483, só em 127.0.0.1). |
| `airflow-scheduler` | idem | Agenda e executa as tasks com LocalExecutor. |
| `airflow-dag-processor` | idem | Lê os DAGs de `pipeline/dags`. |
| `web` | `shopbrasil-web:local`, construída de [web/Dockerfile](../web/Dockerfile) sobre `python:3.12.16-slim` | Web/API da #9 em `http://localhost:${SHOPBRASIL_WEB_PORT}` (padrão 28617, só em 127.0.0.1), com a conta de leitura. Contrato HTTP em [web/README.md](../web/README.md#contrato-http--para-as-1921). |

Ordem de subida: `postgres` saudável → `db-init` concluído → `airflow-init` concluído → api-server, scheduler e dag-processor. A `web` depende só de `postgres` saudável e `db-init` concluído; o Airflow não depende dela. Um erro de SQL no `db-init` impede a subida do Airflow e da web.

O LocalExecutor dispensa Redis e workers Celery. O triggerer ficou de fora porque nenhuma task usa operadores adiáveis. A imagem do Airflow não recebe pacotes adicionais. Os DAGs ficam em `/opt/airflow/dags` e o pacote `shopbrasil_pipeline` fica em `/opt/airflow/pipeline`, que está no `PYTHONPATH`.

## Contrato de configuração

O Compose usa valores padrão para a demonstração local, incluindo o login `admin` com senha `ShopBrasilDemo2026`. O [.env.example](../.env.example) lista as variáveis que podem ser sobrescritas em um `.env` local. Use senhas alfanuméricas, porque elas entram na URL do banco de metadados e no JSON da Connection sem escape.

| Variável | Uso |
|---|---|
| `POSTGRES_USER`, `POSTGRES_PASSWORD` | Conta administrativa. Dona do banco analítico, aplica o esquema e os GRANTs. A senha vale na criação do volume. |
| `SHOPBRASIL_DB_HOST`, `SHOPBRASIL_DB_PORT`, `SHOPBRASIL_DB_NAME` | Endereço do banco analítico visto pelos contêineres (`postgres`, `5432`, `shopbrasil`). |
| `SHOPBRASIL_DB_WRITER_USER`, `SHOPBRASIL_DB_WRITER_PASSWORD` | Conta de escrita do pipeline, usada pela Connection. |
| `SHOPBRASIL_DB_READER_USER`, `SHOPBRASIL_DB_READER_PASSWORD` | Conta de leitura da Web/API. Não entra nos contêineres do Airflow. |
| `SHOPBRASIL_WEB_PORT` | Porta da Web/API no host, publicada só em 127.0.0.1 (padrão 28617). |
| `AIRFLOW_DB_NAME`, `AIRFLOW_DB_USER`, `AIRFLOW_DB_PASSWORD` | Banco e conta de metadados do Airflow. |
| `AIRFLOW_JWT_SECRET` | Segredo comum aos processos do Airflow 3 para os tokens da Execution API. |
| `AIRFLOW_FERNET_KEY` | Opcional. Cifra Connections e Variables gravadas no banco de metadados. |
| `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_API_PORT` | Login e porta da interface. |
| `AIRFLOW_UID` | Dono dos arquivos criados pelo Airflow. |
| `FAKESTORE_BASE_URL` | URL do catálogo, exposta aos contêineres do Airflow para a #13. |

## Contas e privilégios

O `db-init` reaplica [db/acessos.sql](../db/acessos.sql) a cada execução. O script cria o que falta e sincroniza as senhas das contas da aplicação com o `.env`.

| Conta | Banco `shopbrasil` | Banco `airflow` |
|---|---|---|
| administrativa (`POSTGRES_USER`) | dona do banco e do DDL | superusuário do servidor |
| escrita (`SHOPBRASIL_DB_WRITER_USER`) | `CONNECT`; `SELECT`, `INSERT`, `UPDATE`, `DELETE` nas tabelas; `USAGE`, `SELECT` nas sequências; sem `CREATE` nem `TRUNCATE` | sem `CONNECT` |
| leitura (`SHOPBRASIL_DB_READER_USER`) | `CONNECT`; `SELECT` nas tabelas | sem `CONNECT` |
| metadados (`AIRFLOW_DB_USER`) | sem `CONNECT` | dona do banco |

Os privilégios padrão (`ALTER DEFAULT PRIVILEGES FOR ROLE <administrativa> IN SCHEMA public`) cobrem as tabelas que a conta administrativa criar depois. Por isso o `db/schema.sql` não precisa repetir GRANTs.

## Connection e pool

- **Connection `shopbrasil_postgres`**: definida pela variável `AIRFLOW_CONN_SHOPBRASIL_POSTGRES` em formato JSON, montada no Compose com a conta de escrita e os valores padrão ou sobrescritos pelo `.env`. O `PostgresHook(postgres_conn_id="shopbrasil_postgres")` a resolve em qualquer processo do Airflow. A senha não é gravada no banco de metadados, e a Connection não aparece na lista de Connections da interface.
- **Pool `ecommerce_pool`**: 2 slots, definido por `airflow pools set` no `airflow-init` a cada subida. As tasks mapeadas de cálculo devem declarar `pool="ecommerce_pool"` (#7 e #16).

## Coordenação com a #3 e com a Web/API

- **#3**: o DDL está em [db/schema.sql](../db/schema.sql), idempotente e no esquema `public`, e o [modelo de dados](modelo-dados.md) descreve as tabelas. O `db-init` aplica o arquivo com a conta administrativa, `ON_ERROR_STOP=1` e `--single-transaction`, depois de `db/acessos.sql`. Sem o arquivo, o `db-init` registra `AVISO /db/schema.sql não encontrado` e conclui. O cenário E2E da #2 verifica os privilégios de todas as tabelas de `public` e cobre as tabelas da #3; o cenário `e2e/esquema` cobre o DDL.
- **Web/API (#9)**: o serviço `web` é construído a partir de `web/`, depende do `db-init` concluído e recebe apenas `SHOPBRASIL_DB_HOST`, `SHOPBRASIL_DB_PORT`, `SHOPBRASIL_DB_NAME` e as variáveis `SHOPBRASIL_DB_READER_*`. A conta de escrita e a administrativa não entram no contêiner, e o repositório abre transações somente leitura. O Airflow não depende desse serviço.

## Comandos

```sh
docker compose up -d --build --wait                    # postgres → db-init → Airflow e web
curl -s http://localhost:28617/api/metricas/ultima-data # Web/API com a conta de leitura
docker compose ps -a                                   # db-init e airflow-init com código 0
docker compose run --rm db-init                        # reaplica acessos e db/schema.sql
docker compose exec airflow-scheduler airflow pools get ecommerce_pool
docker compose exec airflow-scheduler python -c \
  'from airflow.providers.postgres.hooks.postgres import PostgresHook as H; print(H(postgres_conn_id="shopbrasil_postgres").get_first("select current_user, current_database()"))'
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d shopbrasil -c "\dp"'
docker compose down                                    # mantém os volumes
docker compose down -v                                 # remove metadados e dados analíticos
```

Os DAGs entram pausados (`DAGS_ARE_PAUSED_AT_CREATION=true`). Despause o DAG do pipeline na interface ou com `airflow dags unpause <dag_id>`.

## Cenário E2E

`e2e/ambiente/executar.sh` sobe um projeto Compose isolado, com senhas aleatórias e volumes próprios, e remove tudo ao final. Os resultados estão em [evidencias.md](evidencias.md#2--ambiente-docker-connection-e-pool). O serviço `web` é coberto por `e2e/web/executar.sh` ([evidências da #9](evidencias.md#9--consultas-webapi-para-snapshot-e-evolução)).
