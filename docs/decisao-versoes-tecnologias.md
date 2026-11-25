# Decisão de versões e tecnologias — #1

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Este documento fixa as versões de Python, Airflow, provider PostgreSQL e PostgreSQL, a tecnologia da Web/API e a forma de aplicar o esquema SQL. As versões são exatas para que o ambiente seja reproduzível. A implementação do Compose, da Connection e do pool pertence à #2. O DDL pertence à #3. A Web/API pertence às #9 e #19–#21.

## Versões

| Componente | Versão | Imagem ou pacote | Fonte de compatibilidade |
|---|---|---|---|
| Apache Airflow | 3.4.0 | `apache/airflow:3.4.0-python3.12` | [README da tag 3.4.0](https://github.com/apache/airflow/blob/3.4.0/README.md#requirements) e [PyPI](https://pypi.org/project/apache-airflow/3.4.0/) |
| Python | 3.12.16 | interpretador da imagem do Airflow e `python:3.12.16-slim` na web | README da tag 3.4.0 (Python 3.11 a 3.14) |
| `apache-airflow-providers-postgres` | 7.1.1 | já instalado na imagem oficial | [constraints-3.4.0/constraints-3.12.txt](https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt) |
| `apache-airflow-providers-common-sql` | 2.4.0 | dependência do provider, já instalada | mesmo arquivo de constraints |
| `psycopg2-binary` | 2.9.13 | driver usado pelo `PostgresHook`, já instalado | mesmo arquivo de constraints |
| `pendulum` | 3.3.0 | já instalado | mesmo arquivo de constraints |
| PostgreSQL | 18.7 | `postgres:18.7` | README da tag 3.4.0 (PostgreSQL 14 a 18) e [política de versões do PostgreSQL](https://www.postgresql.org/support/versioning/) |

Motivos:

- **Airflow 3.4.0** é a versão estável atual, publicada em 6 nov. 2026. A linha 2.x aparece como *deprecated* no README oficial. A versão 3.4.0 oferece TaskFlow, Dynamic Task Mapping, TaskGroups e pools, que o enunciado exige. O laboratório da Aula 2 usa 2.9.3, mas a #2 precisa seguir os comandos do Airflow 3: `airflow api-server` no lugar de `airflow webserver`, e `airflow dag-processor` como processo separado.
- **Python 3.12** é suportado pelo Airflow 3.4.0 e tem imagem oficial publicada. O mesmo patch (3.12.16) na web evita diferenças de comportamento de `Decimal` e de bibliotecas entre os módulos.
- **Provider 7.1.1** é a versão fixada pelas constraints oficiais do Airflow 3.4.0 para Python 3.12 e já vem na imagem. O PyPI tem a 7.1.2, mas ela fica fora das constraints. Por isso a imagem não instala nem atualiza o provider. Se a #2 precisar de outro pacote, deve instalá-lo em um Dockerfile derivado com `--constraint` apontando para o mesmo arquivo, e não por `_PIP_ADDITIONAL_REQUIREMENTS`.
- **PostgreSQL 18.7** é o minor atual da maior versão suportada pelo Airflow 3.4.0. O PostgreSQL 19.0 saiu em 19 nov. 2026, mas ainda não consta da tabela de suporte do Airflow. A mesma versão atende o banco de metadados do Airflow e o banco analítico. Na imagem 18, `PGDATA` é `/var/lib/postgresql/18/docker`. Por isso o volume deve ser montado em `/var/lib/postgresql`, e não em `/var/lib/postgresql/data` como no laboratório.

## Tecnologia Web/API

| Componente | Versão | Papel |
|---|---|---|
| FastAPI | 0.162.0 | rotas da API JSON e da página, com modelos HTTP próprios |
| Uvicorn | 0.54.0 | servidor ASGI do contêiner web |
| Jinja2 | 3.1.6 | renderização da página única no servidor |
| psycopg (`psycopg[binary]`) | 3.3.6 | acesso de leitura ao PostgreSQL com a conta `shopbrasil_reader` |
| Chart.js | 4.6.1 | barras de preço médio e linha de evolução, servido como arquivo estático em `web/` |

O serviço web roda em `python:3.12.16-slim`, em imagem própria e sem importar o pacote do pipeline. A escolha mantém uma única linguagem no repositório. Uma só aplicação FastAPI serve a API e a página, que chamam os mesmos casos de uso. As versões dos pacotes Python coincidem com as constraints do Airflow 3.4.0, embora a web não dependa do Airflow. O Chart.js deixa lacunas nos pontos `null` quando `spanGaps` é `false`, que é o padrão, e isso atende a linha de evolução com dias sem dados. O arquivo do Chart.js fica em `web/` para o painel não depender de CDN durante a avaliação.

Alternativas descartadas: Flask exigiria serialização e validação manuais para a API. Um front-end separado em Node.js acrescentaria uma segunda linguagem e uma etapa de build sem necessidade para uma tela.

## Aplicação do esquema SQL

O esquema fica em `db/schema.sql`, com DDL idempotente (`CREATE ... IF NOT EXISTS`). A #3 escreve esse arquivo. Um serviço Compose de execução única, `db-init`, usa a própria imagem `postgres:18.7` e aplica o arquivo com `psql`:

```sh
psql -v ON_ERROR_STOP=1 -h "$SHOPBRASIL_DB_HOST" -p "$SHOPBRASIL_DB_PORT" \
  -U "$POSTGRES_USER" -d "$SHOPBRASIL_DB_NAME" -f /db/schema.sql
```

O `db-init` depende do PostgreSQL saudável (`condition: service_healthy`). O Airflow e a web dependem do `db-init` concluído (`condition: service_completed_successfully`). Com `ON_ERROR_STOP=1`, um erro de DDL encerra o `db-init` com código diferente de zero e impede a subida dos serviços dependentes. A #2 define a conta administrativa (`POSTGRES_USER`), as contas de escrita e leitura e seus `GRANT`s.

Alternativas descartadas: `/docker-entrypoint-initdb.d` só executa com o volume vazio. Assim, uma alteração no esquema não chegaria a um volume existente. Alembic ou Flyway acrescentariam uma ferramenta de migração para três tabelas. As migrações do Airflow (`airflow db migrate`) cuidam apenas do banco de metadados.

## Comandos previstos

Os nomes de serviço são uma previsão. A #2 pode ajustá-los ao escrever o `docker-compose.yml`.

```sh
docker compose pull
docker compose up -d                      # postgres → db-init → Airflow e web
docker compose run --rm db-init           # reaplica db/schema.sql sob demanda
docker compose exec airflow-scheduler airflow version
docker compose exec airflow-scheduler airflow providers get apache-airflow-providers-postgres
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select version();"'
```

As evidências da validação destas escolhas estão em [evidencias.md](evidencias.md#1--versões-e-tecnologias).
