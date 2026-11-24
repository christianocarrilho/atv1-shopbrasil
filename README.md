# atv1-shopbrasil

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Projeto da Atividade 01 de Orquestração de Workflows. Um DAG do Apache Airflow substitui o script agendado no cron: coleta o catálogo da [FakeStore API](https://fakestoreapi.com/docs), valida os produtos, calcula preço médio, mínimo, máximo e quantidade por categoria e publica o resultado no PostgreSQL sem duplicar dados ao reprocessar. Uma Web/API opcional lê as métricas e as mostra num painel.

O foco da atividade é o pipeline. Esta página leva do clone a uma run concluída em poucos minutos e mostra onde cada requisito do [enunciado](docs/atividade-01-descricao.pdf) está implementado.

## Pré-requisitos

- Docker com Compose v2 (Docker Desktop ou Docker Engine) e cerca de 4 GB de memória livres para os contêineres.
- `openssl`, para gerar as senhas locais.
- Acesso à internet, para baixar as imagens e consultar a FakeStore.
- Portas 8080 (Airflow) e 8000 (painel) livres em 127.0.0.1. Se estiverem ocupadas, troque `AIRFLOW_API_PORT` e `SHOPBRASIL_WEB_PORT` no `.env`.

## Início rápido

**1. Gerar o `.env`.** O repositório não versiona senhas. O bloco abaixo copia o modelo e preenche as senhas em branco com valores aleatórios:

```sh
cp .env.example .env
for v in POSTGRES_PASSWORD SHOPBRASIL_DB_WRITER_PASSWORD SHOPBRASIL_DB_READER_PASSWORD \
         AIRFLOW_DB_PASSWORD AIRFLOW_ADMIN_PASSWORD AIRFLOW_JWT_SECRET; do
  sed -i.bak "s/^$v=\$/$v=$(openssl rand -hex 16)/" .env
done
rm -f .env.bak
grep '^AIRFLOW_ADMIN_' .env   # usuário e senha da interface do Airflow
```

**2. Subir o ambiente.** A primeira subida baixa as imagens e constrói a web, o que leva alguns minutos. As seguintes levam cerca de 30 segundos.

```sh
docker compose up -d --build --wait
```

O `db-init` aplica o esquema e cria as contas do banco. O `airflow-init` cria o usuário da interface e o pool `ecommerce_pool` com 2 slots. A Connection `shopbrasil_postgres` vem do `.env` por variável de ambiente e por isso não aparece em **Admin → Connections** ([detalhes](docs/ambiente-docker.md#connection-e-pool)). `docker compose ps -a` deve mostrar `db-init` e `airflow-init` com `Exited (0)` e os demais serviços `healthy`. O `--wait` aguarda também o registro do DAG `shopbrasil_metricas`. Se a subida falhar no processador, consulte `docker compose logs airflow-dag-processor` e `docker compose exec airflow-dag-processor airflow dags list-import-errors` se o contêiner estiver em execução.

**3. Ativar o DAG.** O DAG `shopbrasil_metricas` entra pausado. Ao despausá-lo, o Airflow cria uma run para a ocorrência das 06:00 de Brasília mais recente, cujo estado pode ser acompanhado com `list-runs`. Aguarde `success` e confirme a execução da task `publicacao.publicar` antes de abrir o painel:

```sh
docker compose exec airflow-scheduler airflow dags unpause shopbrasil_metricas
docker compose exec airflow-scheduler airflow dags list-runs shopbrasil_metricas
```

Na interface, em http://localhost:8080 com o usuário do passo 1, abra o DAG e use a visão **Graph** para ver os TaskGroups `preparacao`, `ingestao`, `analise` e `publicacao`, o fan-out de `calcular_metricas` (uma instância por categoria) e o fan-in em `consolidar`.

**4. Conferir o resultado.** O painel em http://localhost:8000 abre na data mais recente com métricas. As tabelas podem ser consultadas com a conta de leitura, que é a mesma usada pela web:

```sh
set -a; . ./.env; set +a
docker compose exec -e PGPASSWORD="$SHOPBRASIL_DB_READER_PASSWORD" postgres \
  psql -h postgres -U shopbrasil_reader -d shopbrasil -c "
  SELECT m.data_referencia, c.codigo, m.preco_medio, m.preco_minimo, m.preco_maximo, m.quantidade_produtos
    FROM metricas_categoria m JOIN categoria c ON c.id = m.categoria_id ORDER BY 1, 2;"
```

O [guia de execução](docs/execucao.md) continua daqui: run de uma data específica, o que acontece ao executar o mesmo dia de novo, simulação de queda da API com retry e alerta, e a suíte E2E.

## Requisitos da atividade

O DAG fica em [pipeline/dags/shopbrasil_metricas.py](pipeline/dags/shopbrasil_metricas.py). Ele só orquestra: a coleta, o cálculo e o SQL ficam no pacote [shopbrasil_pipeline](pipeline/shopbrasil_pipeline/), separado em domínio, aplicação e infraestrutura (veja [pipeline/README.md](pipeline/README.md)).

### Obrigatórios (seção 3)

| Requisito | Implementação | Como conferir |
|---|---|---|
| Fonte FakeStore | `infraestrutura/fakestore.py`: `GET /products`, timeout de 15 s | Log de `ingestao.buscar_produtos` |
| TaskFlow (`@dag`/`@task`), dependências pelas chamadas | `@dag`, `@task` e `@task_group` no DAG; nenhum `>>` | Visão Graph |
| XCom automático via `return`, só dados pequenos | Cada task devolve um `dict` ou `list` com códigos e preços em texto, nunca o JSON bruto da API | Aba XCom de cada task |
| Topologias linear, fan-out e fan-in | Linear: `identificar` → `buscar_produtos` → `validar_produtos` → `agrupar_por_categoria`. Fan-out: `calcular_metricas.expand(...)`. Fan-in: `consolidar` | Visão Graph |
| 06:00 em America/Sao_Paulo, `pendulum`, `start_date`, `catchup=False` | `CronTriggerTimetable("0 6 * * *", timezone="America/Sao_Paulo")` e `START_DATE = pendulum.datetime(2026, 11, 1, tz=...)` | `airflow dags details shopbrasil_metricas` |
| Buscar Produtos com retry e backoff exponencial | `retries=3`, `retry_delay` de 60 s e `retry_exponential_backoff=2.0` | [Simular queda da API](docs/execucao.md#6-simular-queda-da-api) |
| `try`/`except` + `raise` | Corpo de `buscar_produtos`: registra o erro e relança para acionar o retry | Log da tentativa com falha |
| Callbacks de sucesso, retry e falha na task crítica | `on_success_callback`, `on_retry_callback` e `on_failure_callback` em `buscar_produtos` | Linhas `[SHOPBRASIL][SUCESSO]`, `[RETRY]` e `[FALHA]` no log |
| Dynamic Task Mapping | `calcular_metricas.expand(entrada=grupos)`, sobre as categorias encontradas na coleta, sem lista fixa | Uma instância mapeada por categoria |
| Pool `ecommerce_pool` com 2 slots | `@task(pool="ecommerce_pool")` em `calcular_metricas`; o pool é criado pelo `airflow-init` | `airflow pools get ecommerce_pool` |
| Pelo menos 2 TaskGroups | `preparacao`, `ingestao`, `analise` e `publicacao` | Visão Graph |
| Persistência com `PostgresHook` e Connection | `infraestrutura/publicacao_postgres.py` usa `PostgresHook(postgres_conn_id="shopbrasil_postgres")` | Task `publicacao.publicar` |
| Gravação idempotente | Numa só transação: categoria com `ON CONFLICT DO NOTHING`; snapshot do dia apagado e regravado, com `UNIQUE (data_referencia, categoria_id)`; histórico com `ON CONFLICT (run_id, categoria_id) DO NOTHING` | [Executar de novo o mesmo dia](docs/execucao.md#5-executar-de-novo-o-mesmo-dia): log `[SHOPBRASIL][PUBLICACAO]` e contagem das linhas |

### Opcionais (seção 4), todos implementados

| Requisito | Implementação | Como conferir |
|---|---|---|
| `ValidarProdutosOperator(BaseOperator)` | [pipeline/dags/operadores/validar_produtos.py](pipeline/dags/operadores/validar_produtos.py), task `ingestao.validar_produtos`, entre a coleta e o cálculo. Catálogo vazio ou produto inválido falha sem retry e preserva os dados publicados | Log da task; cenários V2–V5 de `e2e/pipeline` |
| Tabela de histórico em append | `metricas_categoria_historico`: uma linha por run e categoria, com `run_id` e data da execução | Consulta da [seção 4 do guia de execução](docs/execucao.md#4-verificar-o-painel-e-o-banco) |
| Alerta simulado no `on_failure_callback` | Quando a coleta esgota as tentativas, `buscar_produtos` registra `[SHOPBRASIL][ALERTA SIMULADO]`. Toda task com falha registra `[SHOPBRASIL][FALHA]`, e o DAG avisa a falha da run | [Simular queda da API](docs/execucao.md#6-simular-queda-da-api) |

### Entrega (seção 5)

O repositório é público, e o projeto inteiro sobe com `docker compose`: PostgreSQL, Airflow 3.4 (LocalExecutor) e web. As versões estão justificadas na [decisão de versões](docs/decisao-versoes-tecnologias.md).

## Organização

- [pipeline/](pipeline/): o DAG, o operador próprio e o pacote `shopbrasil_pipeline` (domínio, aplicação, infraestrutura e composição), com testes unitários em `pipeline/tests`.
- [db/](db/): o esquema compartilhado ([schema.sql](db/schema.sql)) e as contas do banco. O [modelo de dados](docs/modelo-dados.md) descreve as tabelas.
- [web/](web/): Web/API de leitura com o painel, um extra fora do escopo obrigatório.
- [e2e/](e2e/): a suíte de ponta a ponta com Airflow e PostgreSQL reais e uma FakeStore controlada (`e2e/executar_todos.sh`).
- [qualidade/](qualidade/): o lint de arquitetura (`python3 qualidade/lint_arquitetura.py`).
- [docs/](docs/): a [especificação](docs/especificacao.md), o [guia de execução](docs/execucao.md), o [ambiente Docker](docs/ambiente-docker.md) e as [evidências](docs/evidencias.md) com os comandos e resultados de cada entrega.

## Verificações rápidas sem Docker

```sh
python3 -m unittest discover -s pipeline/tests -t pipeline   # testes unitários do pipeline (Python 3.12 ou superior, só biblioteca padrão)
python3 qualidade/lint_arquitetura.py                        # limites entre as camadas
```

Para encerrar, `docker compose down` mantém os dados e `docker compose down -v` apaga os volumes.
