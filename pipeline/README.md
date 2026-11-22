# pipeline

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Orquestra e executa a coleta, a análise e a publicação das métricas.

## DAG `shopbrasil_metricas`

O DAG fica em [dags/shopbrasil_metricas.py](dags/shopbrasil_metricas.py). Roda diariamente às 06:00 em `America/Sao_Paulo` (`CronTriggerTimetable`), com `start_date` em 1º nov. 2026 e `catchup=False`. As etapas ficam nos TaskGroups `preparacao`, `ingestao`, `analise` e `publicacao`, ligadas pelas chamadas TaskFlow.

A data de referência é a data civil, em Brasília, da data lógica da run. No Airflow 3.4 a data lógica de uma run agendada é a própria ocorrência das 06:00, sem intervalo de dados. A regra fica em `shopbrasil_pipeline/dominio/calendario.py` e não usa o relógio. Uma run sem data lógica falha em `preparacao.identificar`.

Para reprocessar, limpe as tasks da própria run (na interface, *Clear*, ou `airflow tasks clear shopbrasil_metricas -s <data lógica> -e <data lógica> -y`). O `run_id` e a data de referência são preservados. Não dispare uma run nova para refazer um dia.

## Publicação

O caso de uso `PublicarSnapshot` ([aplicacao/publicacao.py](shopbrasil_pipeline/aplicacao/publicacao.py)) recebe a `Execucao` e a coleção consolidada de `MetricasCategoria`, um item por código, e publica o snapshot da data de referência numa transação do adaptador PostgreSQL ([infraestrutura/publicacao_postgres.py](shopbrasil_pipeline/infraestrutura/publicacao_postgres.py)). A [composição](shopbrasil_pipeline/composicao.py) entrega o caso de uso montado com a Connection `shopbrasil_postgres`: `composicao.publicar_snapshot().executar(execucao, metricas)`. A ligação com a task `publicacao.publicar` do DAG é da #16, e o histórico por run da #18 entra como etapa adicional da mesma transação.

## Testes

```bash
python3 -m unittest discover -s pipeline/tests -t pipeline   # unitários, só biblioteca padrão
e2e/pipeline/executar.sh                                     # E2E do DAG com Airflow e PostgreSQL em Docker
e2e/publicacao/executar.sh                                   # E2E da publicação com a Connection real
```
