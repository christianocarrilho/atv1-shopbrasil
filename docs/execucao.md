# Execução e entrega — #12

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Guia para subir os módulos, verificar o DAG e o painel, reprocessar uma run, simular a queda da API e rodar a suíte E2E. O [README](../README.md) traz o início rápido e o mapa dos requisitos da atividade. Detalhes de serviços, contas e Connection estão no [guia do ambiente Docker](ambiente-docker.md).

## 1. Configuração sem segredos versionados

```sh
cp .env.example .env    # só se o .env ainda não existir
```

Preencha no `.env` as senhas em branco (`POSTGRES_PASSWORD`, `SHOPBRASIL_DB_WRITER_PASSWORD`, `SHOPBRASIL_DB_READER_PASSWORD`, `AIRFLOW_DB_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`) e o `AIRFLOW_JWT_SECRET` (`openssl rand -hex 32`). Use valores alfanuméricos; o [README](../README.md#início-rápido) tem um bloco que gera todos de uma vez. O `.env` não é versionado; o [.env.example](../.env.example) é o contrato.

## 2. Subir os módulos

```sh
docker compose up -d --build --wait
```

| Módulo | Endereço | Observação |
|---|---|---|
| PostgreSQL + `db-init` | interno | aplica [db/schema.sql](../db/schema.sql) e as contas de escrita e leitura |
| Airflow 3.4 | http://localhost:8080 | usuário `AIRFLOW_ADMIN_USER` do `.env`; LocalExecutor, Connection `shopbrasil_postgres` e pool `ecommerce_pool` (2 slots) |
| Web/API | http://localhost:8000 | painel em `/`, API em `/api/...`, OpenAPI em `/docs` |

Se as portas 8080 ou 8000 estiverem ocupadas, troque `AIRFLOW_API_PORT` ou `SHOPBRASIL_WEB_PORT` no `.env`. O pipeline não depende da web: `docker compose up -d --wait airflow-apiserver airflow-scheduler airflow-dag-processor` sobe só a orquestração.

## 3. Verificar o DAG

O DAG `shopbrasil_metricas` entra pausado. Despause e confira a agenda (06:00 em America/Sao_Paulo, `catchup=False`):

```sh
docker compose exec airflow-scheduler airflow dags unpause shopbrasil_metricas
docker compose exec airflow-scheduler airflow dags details shopbrasil_metricas
```

Ao despausar, o Airflow cria só a run da ocorrência mais recente. Para uma run manual de uma ocorrência específica:

```sh
docker compose exec airflow-scheduler airflow dags trigger shopbrasil_metricas \
  --logical-date 2026-11-21T09:00:00+00:00 --run-id manual__2026-11-21
```

Na interface (Grid), confira a sequência preparação → ingestão (Buscar Produtos → `ValidarProdutosOperator`) → análise (agrupamento, `calcular_metricas` mapeada por categoria no `ecommerce_pool`, consolidação) → publicação, e os registros `[SHOPBRASIL][...]` dos callbacks nos logs das tasks.

## 4. Verificar o painel e o banco

Abra http://localhost:8000. O painel abre na última data com métricas, mostra a tabela e as barras do preço médio e, abaixo, a linha de evolução com filtros de categoria e intervalo. Data sem registros exibe “Sem métricas para esta data”.

Snapshot e histórico por run, lidos pela conta de leitura (a mesma da web, sem permissão de escrita). Carregue antes as variáveis do `.env` no shell (`set -a; . ./.env; set +a`):

```sh
docker compose exec -e PGPASSWORD="$SHOPBRASIL_DB_READER_PASSWORD" postgres psql -h postgres -U shopbrasil_reader -d shopbrasil -c "
  SELECT m.data_referencia, c.codigo, m.preco_medio, m.preco_minimo, m.preco_maximo, m.quantidade_produtos
    FROM metricas_categoria m JOIN categoria c ON c.id = m.categoria_id ORDER BY 1, 2;"
docker compose exec -e PGPASSWORD="$SHOPBRASIL_DB_READER_PASSWORD" postgres psql -h postgres -U shopbrasil_reader -d shopbrasil -c "
  SELECT h.run_id, h.data_execucao, c.codigo, h.preco_medio, h.quantidade_produtos
    FROM metricas_categoria_historico h JOIN categoria c ON c.id = h.categoria_id ORDER BY 1, 3;"
```

## 5. Reprocessar a própria run

Limpar as tasks da run executa de novo a mesma run, com o mesmo `run_id` e a mesma data de referência (derivada da data lógica, não do relógio). O snapshot do dia é substituído na mesma transação e o histórico da run não duplica (`run_id` + categoria).

```sh
docker compose exec airflow-scheduler airflow tasks clear shopbrasil_metricas \
  -s 2026-11-21T09:00:00+00:00 -e 2026-11-21T09:00:00+00:00 -y
```

Na interface: abra a run, **Clear** → **Clear existing tasks**. Uma run criada sem data lógica é recusada em `preparacao.identificar`, com a orientação de reprocessar por limpeza.

## 6. Simular queda da API

Com o LocalExecutor, as tasks rodam no contêiner `airflow-scheduler`. Para simular a FakeStore fora do ar, recrie só esse serviço apontando para um endereço inexistente e dispare uma run de outra data:

```sh
FAKESTORE_BASE_URL=http://fakestore.invalid docker compose up -d --wait airflow-scheduler
docker compose exec airflow-scheduler airflow dags trigger shopbrasil_metricas \
  --logical-date 2026-11-22T09:00:00+00:00 --run-id manual__2026-11-22
```

`ingestao.buscar_produtos` faz quatro tentativas. As esperas entre elas são sorteadas entre 60 e 119 s, 120 e 239 s e 240 e 479 s, então a run falha de 7 a 14 minutos depois do disparo. Na interface, a task fica em `up_for_retry` entre as tentativas. Para acompanhar os callbacks:

```sh
docker compose exec airflow-scheduler sh -c \
  'grep -rh manual__2026-11-22 /opt/airflow/logs | grep -o "\[SHOPBRASIL\][^\"]*" | sort -u'
```

O resultado esperado é uma linha `[SHOPBRASIL][RETRY]` para cada uma das tentativas 1/4 a 3/4, com o erro de conexão, seguida de `[SHOPBRASIL][FALHA]` e `[SHOPBRASIL][ALERTA SIMULADO]` na tentativa 4/4 e de `[SHOPBRASIL][FALHA RUN]`, o aviso do DAG. As tasks seguintes ficam em `upstream_failed`, nada é publicado para 22/11 e as datas já publicadas permanecem intactas. Para voltar à FakeStore real:

```sh
docker compose up -d --wait airflow-scheduler
```

## 7. Suíte E2E

```sh
e2e/executar_todos.sh
```

Executa todos os cenários em projetos Compose isolados, com dados próprios e FakeStore controlada, e imprime o resultado por cenário. Detalhes e o mapa dos critérios em [e2e/README.md](../e2e/README.md); resultados registrados em [evidencias.md](evidencias.md). O lint de arquitetura roda com `python3 qualidade/lint_arquitetura.py`.

## 8. Entrega acadêmica

Passos finais, registrados aqui só depois de confirmados pelo autor:

- [ ] Acesso de leitura ao repositório conferido para o avaliador.
- [ ] Link do repositório e de [evidencias.md](evidencias.md) postado no canal de entrega da atividade.
