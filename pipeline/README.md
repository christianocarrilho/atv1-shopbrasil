# pipeline

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Orquestra e executa a coleta, a análise e a publicação das métricas.

## DAG `shopbrasil_metricas`

O DAG fica em [dags/shopbrasil_metricas.py](dags/shopbrasil_metricas.py). Roda diariamente às 06:00 em `America/Sao_Paulo` (`CronTriggerTimetable`), com `start_date` em 1º nov. 2026 e `catchup=False`. As etapas ficam nos TaskGroups `preparacao`, `ingestao`, `analise` e `publicacao`, ligadas pelas chamadas TaskFlow.

A data de referência é a data civil, em Brasília, da data lógica da run. No Airflow 3.4 a data lógica de uma run agendada é a própria ocorrência das 06:00, sem intervalo de dados. A regra fica em `shopbrasil_pipeline/dominio/calendario.py` e não usa o relógio. Uma run sem data lógica falha em `preparacao.identificar`.

Para reprocessar, limpe as tasks da própria run (na interface, *Clear*, ou `airflow tasks clear shopbrasil_metricas -s <data lógica> -e <data lógica> -y`). O `run_id` e a data de referência são preservados. Não dispare uma run nova para refazer um dia.

## Buscar Produtos — #13

A task `ingestao.buscar_produtos` chama o caso de uso `coletar_catalogo` (`aplicacao/catalogo.py`) com a fonte obtida em `composicao.py`. O adaptador `infraestrutura/fakestore.py` consulta `GET {FAKESTORE_BASE_URL}/products` com timeout de 15 segundos e `User-Agent: shopbrasil-pipeline/1.0`, porque a FakeStore real responde 403 ao `Python-urllib` padrão, e lê o JSON com `parse_float`, `parse_int` e `parse_constant` em `Decimal`. Erros HTTP, de conexão, de timeout e de JSON são registrados e relançados pela task.

A task tem `retries=3`, `retry_delay` de 60 segundos e `retry_exponential_backoff=2.0`: até quatro tentativas. O Airflow 3.4 sorteia cada espera entre N e 2N, com N = 60 × 2^(n−1) segundos: a primeira fica entre 60 e 119 s, a segunda entre 120 e 239 s e a terceira entre 240 e 479 s. Sem `FAKESTORE_BASE_URL`, a task falha direto com `AirflowFailException`, sem gastar retentativas. A variável `SHOPBRASIL_BUSCA_RETRY_DELAY_SEGUNDOS` reduz o atraso só no override E2E.

Interface entregue à #6 (XCom de `buscar_produtos`), uma projeção pequena que não valida nada:

```json
{"tipo_raiz": "array",
 "produtos": [{"id": "5", "categoria": "jewelery", "tipo_categoria": "string", "preco": "695", "tipo_preco": "number"}]}
```

`categoria` só é preenchida quando o valor é texto, e `preco` só quando é número, como string decimal sem arredondamento. Nos demais casos o campo fica `null`, e `tipo_categoria` ou `tipo_preco` informa o tipo JSON recebido (`boolean`, `string`, `null`, `array`, `object` ou `ausente`). `tipo_raiz` diferente de `array` indica uma resposta que não é lista. Com isso, a #6 rejeita catálogo vazio, categoria em branco, booleano e texto no lugar do preço. `NaN` e `Infinity` chegam com `tipo_preco` `number` e `preco` `"NaN"` ou `"Infinity"`, então a #6 também confere se o preço é finito, e o preço chega à #15 sem passar por float.

## Publicação

O caso de uso `PublicarSnapshot` ([aplicacao/publicacao.py](shopbrasil_pipeline/aplicacao/publicacao.py)) recebe a `Execucao` e a coleção consolidada de `MetricasCategoria`, um item por código, e publica o snapshot da data de referência numa transação do adaptador PostgreSQL ([infraestrutura/publicacao_postgres.py](shopbrasil_pipeline/infraestrutura/publicacao_postgres.py)). A [composição](shopbrasil_pipeline/composicao.py) entrega o caso de uso montado com a Connection `shopbrasil_postgres`: `composicao.publicar_snapshot().executar(execucao, metricas)`. A ligação com a task `publicacao.publicar` do DAG é da #16. O histórico por run da #18 ([infraestrutura/historico_postgres.py](shopbrasil_pipeline/infraestrutura/historico_postgres.py)) já está ligado pela composição como etapa adicional da mesma transação: cada publicação grava as linhas `(run_id, categoria_id)` em append, e o reprocessamento da mesma run preserva as já registradas.

## Testes

```bash
python3 -m unittest discover -s pipeline/tests -t pipeline   # unitários, só biblioteca padrão
e2e/pipeline/executar.sh                                     # E2E do DAG com Airflow e PostgreSQL em Docker
e2e/publicacao/executar.sh                                   # E2E da publicação com a Connection real
```
