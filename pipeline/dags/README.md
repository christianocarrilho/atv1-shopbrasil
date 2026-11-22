# dags

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Define o DAG `shopbrasil_metricas`, suas tasks, callbacks e o operador próprio. Os adaptadores concretos vêm da [composição](../shopbrasil_pipeline/composicao.py); o DAG aciona só a aplicação e a composição, sem importar domínio nem infraestrutura (lint de arquitetura).

| TaskGroup | Tasks | Papel |
|---|---|---|
| `preparacao` | `identificar` | Data de referência pela data lógica da run, no fuso de Brasília |
| `ingestao` | `buscar_produtos` → `validar_produtos` | Coleta da FakeStore com 3 retentativas, backoff e callbacks; [`ValidarProdutosOperator`](operadores/validar_produtos.py) recusa catálogo inválido antes do cálculo |
| `analise` | `agrupar_por_categoria` → `calcular_metricas` (mapeada) → `consolidar` | Fan-out com `.expand` uma instância por categoria observada, no `ecommerce_pool` (2 slots); fan-in exige um resultado por categoria |
| `publicacao` | `publicar` | Snapshot do dia e histórico da run numa transação, pela composição |

Agenda: `CronTriggerTimetable("0 6 * * *")` em `America/Sao_Paulo`, `catchup=False`.
