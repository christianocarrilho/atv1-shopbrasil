# Execução e entrega — #12

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Guia para subir os módulos, verificar o DAG e o painel, reprocessar uma run, simular a queda da API e rodar a suíte E2E. O [README](../README.md) traz o início rápido e o mapa dos requisitos da atividade. Detalhes de serviços, contas e Connection estão no [guia do ambiente Docker](ambiente-docker.md).

## 1. Configuração da demonstração

O Compose já inclui os valores padrão. Para personalizar o ambiente, copie o [.env.example](../.env.example) para `.env` e preencha apenas os campos que deseja alterar. Use valores alfanuméricos para as credenciais do banco.

## 2. Subir os módulos

```sh
docker compose up -d --build --wait
```

| Módulo | Endereço | Observação |
|---|---|---|
| PostgreSQL + `db-init` | interno | aplica [db/schema.sql](../db/schema.sql) e as contas de escrita e leitura |
| Airflow 3.4 | http://localhost:27483 | usuário padrão `admin`, senha `ShopBrasilDemo2026`; LocalExecutor, Connection `shopbrasil_postgres` e pool `ecommerce_pool` (2 slots) |
| Web/API | http://localhost:28617 | painel em `/`, API em `/api/...`, OpenAPI em `/docs` |

Se as portas 27483 ou 28617 estiverem ocupadas, troque `AIRFLOW_API_PORT` ou `SHOPBRASIL_WEB_PORT` no `.env`. O pipeline não depende da web: `docker compose up -d --wait airflow-apiserver airflow-scheduler airflow-dag-processor` sobe só a orquestração.

## 3. Verificar o DAG

O DAG `shopbrasil_metricas` entra pausado. Despause e confira a agenda (06:00 em America/Sao_Paulo, `catchup=False`):

```sh
docker compose exec airflow-scheduler airflow dags unpause shopbrasil_metricas
docker compose exec airflow-scheduler airflow dags details shopbrasil_metricas
```

Ao despausar, o Airflow cria só a run da ocorrência mais recente. Na interface, o botão **Acionar** dispara uma run manual com a data lógica preenchida com o horário atual. Pela linha de comando, informe um instante com `--logical-date`. Às 06:00 em Brasília são 09:00 UTC. Use uma data de referência a partir de 01/11/2026, o `start_date` do DAG. Antes desse limite, o Airflow pode marcar a run como `success` sem executar tarefas. O catálogo é consultado no momento da coleta, inclusive para datas de referência passadas.

```sh
docker compose exec airflow-scheduler airflow dags trigger shopbrasil_metricas \
  --logical-date 2026-11-21T09:00:00+00:00 --run-id manual__2026-11-21
```

Na interface (Grid), confirme que `publicacao.publicar` executou e ficou em `success`. Confira as linhas da data no banco com a consulta da seção 4. Confira a sequência preparação → ingestão (Buscar Produtos → `ValidarProdutosOperator`) → análise (agrupamento, `calcular_metricas` mapeada por categoria no `ecommerce_pool`, consolidação) → publicação, e os registros `[SHOPBRASIL][...]` dos callbacks nos logs das tasks.

O Airflow exige um instante lógico diferente para cada run deste DAG. Se o exemplo da seção 3 já rodou às 09:00 UTC, uma run com outro `run_id` e o mesmo instante será rejeitada. Para uma nova execução do mesmo dia, use outro horário que ainda pertença a esse dia em Brasília:

```sh
docker compose exec airflow-scheduler airflow dags trigger shopbrasil_metricas \
  --logical-date 2026-11-21T10:00:00+00:00 --run-id manual__2026-11-21_segunda
```

## 4. Verificar o painel e o banco

Abra http://localhost:28617. O painel abre na última data com métricas, mostra a tabela e as barras do preço médio e, abaixo, a linha de evolução com filtros de categoria e intervalo. Data sem registros exibe “Sem métricas para esta data”.

Snapshot e histórico por run, lidos pela conta de leitura (a mesma da web, sem permissão de escrita):

```sh
docker compose run --rm --no-deps -T --entrypoint sh db-init -c '
  PGPASSWORD="$SHOPBRASIL_DB_READER_PASSWORD" exec psql \
    -h "$SHOPBRASIL_DB_HOST" -p "$SHOPBRASIL_DB_PORT" \
    -U "$SHOPBRASIL_DB_READER_USER" -d "$SHOPBRASIL_DB_NAME" "$@"
' sh -c "
  SELECT m.data_referencia, c.codigo, m.preco_medio, m.preco_minimo, m.preco_maximo, m.quantidade_produtos
    FROM metricas_categoria m JOIN categoria c ON c.id = m.categoria_id ORDER BY 1, 2;"
docker compose run --rm --no-deps -T --entrypoint sh db-init -c '
  PGPASSWORD="$SHOPBRASIL_DB_READER_PASSWORD" exec psql \
    -h "$SHOPBRASIL_DB_HOST" -p "$SHOPBRASIL_DB_PORT" \
    -U "$SHOPBRASIL_DB_READER_USER" -d "$SHOPBRASIL_DB_NAME" "$@"
' sh -c "
  SELECT h.run_id, h.data_execucao, h.publicado_em, c.codigo, h.preco_medio, h.quantidade_produtos
    FROM metricas_categoria_historico h JOIN categoria c ON c.id = h.categoria_id ORDER BY 1, 4;"
```

## 5. Executar de novo o mesmo dia

O comportamento depende da tabela, porque cada uma tem uma regra de unicidade:

- **`metricas_categoria` (snapshot)** guarda uma linha por dia e categoria. Cada publicação bem-sucedida apaga e regrava as linhas daquela data numa única transação. O painel lê essa tabela. Ela atende ao requisito de idempotência: executar de novo nunca duplica linhas, e o dia fica com os valores da coleta mais recente.
- **`metricas_categoria_historico` (histórico)** guarda uma linha por run e categoria. É a tabela do opcional "modo append": uma run nova acrescenta linhas, porque é outra execução a acompanhar. Repetir a mesma run preserva as linhas existentes e só acrescenta linhas para categorias inéditas naquela run. `data_execucao` é o dia de referência; `publicado_em` é o instante real da primeira inserção e fica vazio para linhas legadas cujo instante é desconhecido.

O dia de referência vem sempre da data lógica da run, convertida para Brasília, e nunca do relógio.

| Como o dia é executado de novo | Snapshot do dia | Histórico |
|---|---|---|
| Retry automático de Buscar Produtos | Publicado uma única vez, depois da tentativa bem-sucedida | Uma linha por categoria |
| **Clear** da run (interface ou `airflow tasks clear`): mesmo `run_id` | Regravado com a nova coleta quando o Clear inclui a ingestão, sempre uma linha por categoria | As linhas existentes ficam como estavam. Categorias inéditas nessa run ganham uma linha |
| **Acionar** na interface ou `airflow dags trigger --logical-date ...` para o mesmo dia: `run_id` novo | Regravado com a nova coleta, sempre uma linha por categoria | Mais uma linha por categoria, com o `run_id` novo |
| `airflow dags trigger` **sem** `--logical-date` | Não muda | Não muda: a run falha em `preparacao.identificar` com `OcorrenciaAusenteError`, porque não há dia de referência |

No **Clear**, o histórico conserva os valores e o instante de publicação registrados pela primeira vez para cada categoria. Se a nova coleta trouxer valores diferentes, o snapshot fica com os novos, e o log de `publicacao.publicar` registra `[SHOPBRASIL][AVISO] histórico preservado diverge do snapshot` para cada categoria afetada. Para ter os valores novos também no histórico, dispare uma run nova.

Para repetir a run do passo 3 pela linha de comando:

```sh
docker compose exec airflow-scheduler airflow tasks clear shopbrasil_metricas \
  -s 2026-11-21T09:00:00+00:00 -e 2026-11-21T09:00:00+00:00 -y
```

Na interface, abra a run e use **Clear** → **Clear existing tasks**. O log de `publicacao.publicar` mostra o que aconteceu em cada tabela:

```text
[SHOPBRASIL][HISTORICO] run=manual__2026-11-21: 0 linha(s) gravada(s), 4 já registrada(s) preservada(s) sem alteração
[SHOPBRASIL][PUBLICACAO] run=manual__2026-11-21 data=2026-11-21 snapshot: 4 linha(s) anterior(es) do dia substituída(s) por 4; categoria(s) nova(s) no cadastro: nenhuma
```

Na primeira execução do dia, as linhas são `4 linha(s) gravada(s), 0 já registrada(s)` e `0 linha(s) anterior(es) do dia substituída(s) por 4`. Para conferir as contagens no banco:

```sh
docker compose run --rm --no-deps -T --entrypoint sh db-init -c '
  PGPASSWORD="$SHOPBRASIL_DB_READER_PASSWORD" exec psql \
    -h "$SHOPBRASIL_DB_HOST" -p "$SHOPBRASIL_DB_PORT" \
    -U "$SHOPBRASIL_DB_READER_USER" -d "$SHOPBRASIL_DB_NAME" "$@"
' sh \
  -c "SELECT data_referencia, count(*) FROM metricas_categoria GROUP BY 1 ORDER BY 1;" \
  -c "SELECT run_id, count(*) FROM metricas_categoria_historico GROUP BY 1 ORDER BY 1;"
```

O snapshot mostra sempre uma linha por categoria em cada data. O histórico mostra uma linha por categoria em cada run.

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
