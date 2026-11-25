# e2e

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Reúne a suíte de ponta a ponta com Airflow e PostgreSQL reais e respostas controladas da FakeStore.

## Comando único (#23)

```sh
e2e/executar_todos.sh              # todos: ambiente esquema pipeline publicacao web integrado
e2e/executar_todos.sh web integrado  # só os cenários indicados
```

Roda os cenários em sequência, cada um isolado, e grava a saída de cada um e um `resumo.md` (cenário, resultado, PASS, FAIL, duração) em `e2e/resultados/<data-hora>/`, pasta fora do versionamento. O código de saída é diferente de zero se algum cenário falhar. O resultado da execução final é registrado em [docs/evidencias.md](../docs/evidencias.md).

## Cenários

| Pasta | Issue | Comando | Cobertura |
|---|---|---|---|
| [ambiente](ambiente/executar.sh) | #2 | `e2e/ambiente/executar.sh` | Subida limpa, versões, Connection, pool de 2 slots, contas de escrita e leitura, Airflow sem a Web/API e `db-init` |
| [esquema](esquema/executar.sh) | #3 | `e2e/esquema/executar.sh` | `db/schema.sql` em banco vazio: colunas e tipos, identity, unicidade, FKs, CHECKs, precisão `NUMERIC(12,2)`, reaplicação com dados e rollback do `db-init` |
| [pipeline](pipeline/executar.sh) | #4, #13 | `e2e/pipeline/executar.sh` | DAG real: importação, TaskGroups e dependências, agenda das 06:00 em Brasília com `catchup=False`, run agendada real, data de referência da data lógica, reprocessamento da mesma run e rejeição de run sem data lógica. #13: FakeStore controlada (`e2e/fakestore`), configuração de produção de Buscar Produtos, XCom pequeno, falha transitória com retry e falha persistente com backoff exponencial |
| [publicacao](publicacao/executar.sh) | #17, #18 | `e2e/publicacao/executar.sh` | Caso de uso e adaptador `PostgresHook` executados no contêiner do Airflow com a Connection real: cadastro de categoria nova, republicação com uma linha por data+categoria, categoria ausente fora do dia com cadastro preservado, outra data intacta, rollback por falha injetada no ponto de extensão e por erro do banco, valores lidos `10.00` e `10.01`. Histórico por run: primeira publicação grava os valores publicados, reprocessar a mesma run preserva as linhas (valores e `xmin`), violação de CHECK no histórico reverte cadastro e snapshot, run distinta acrescenta |
| [web](web/executar.sh) | #9, #19, #20, #21 | `e2e/web/executar.sh` | Serviço `web` construído de `web/` com `postgres` e `db-init`, sem Airflow; dados gravados pela conta administrativa (`fixtures/snapshot.sql`). Rotas JSON pela porta em 127.0.0.1: data com métricas, última data (inclusive `null` sem publicação), data sem registros, série com lacunas e em ordem, histórico ignorado, categorias, erros 422/400/404, preços como strings sem float; contêiner só com a conta de leitura, escrita e DDL negados e transação somente leitura. #19: painel abre na última data, filtro por data, “Sem métricas para esta data”, estado sem publicação, tabela e coleção embutida iguais à API, 422 na página. #20: barras no dia com métricas, estado vazio sem métricas, Chart.js 4.6.1 servido localmente. #21: série da página igual à API com lacunas `null`, filtros padrão, categoria com apóstrofo, avisos de intervalo invertido e categoria desconhecida |
| [integrado](integrado/executar.sh) | #23 | `e2e/integrado/executar.sh` | Airflow, PostgreSQL e web no mesmo projeto. O DAG publica 10/11 (padrão), 11/11 (categoria inédita `toys`) e 12/11 (sem `jewelery`), depois do `start_date` e longe da run agendada ao despausar; para cada dia, snapshot, histórico da run, API e tabela do painel iguais aos valores calculados à parte; painel abre na última data; cadastro com a categoria nova e a ausente; evolução com `null` nos dias sem a categoria, igual na página e na API; reprocessamento de 11/11 com nova tentativa da publicação, data de referência estável e sem duplicatas; falha definitiva da FakeStore preserva dados (valores e xmin) e painel; painel operado num Chromium real (N7) |

Cada cenário usa um projeto Compose próprio, com senhas aleatórias e volumes descartados ao final. Os cenários a partir da #4 usam as funções de [lib/comum.sh](lib/comum.sh), que removem o arquivo de senhas em qualquer saída e mantêm os logs só quando há falha. A porta padrão da API do Airflow difere por cenário (`ambiente` 18080, `pipeline` 18090, `publicacao` 18100, `integrado` 18120, com a web na próxima porta livre). O cenário `web` não sobe o Airflow e usa a porta escolhida (padrão 18110) para a Web/API. Nos cenários com `lib/comum.sh`, se a porta estiver ocupada, o script usa a próxima livre; `E2E_AIRFLOW_API_PORT` fixa a porta. Requer Docker com Compose v2 e `openssl`. Os cenários `web` e `integrado` constroem a imagem de [navegador/Dockerfile](navegador/Dockerfile) (Chromium do Playwright 1.49.0) para operar o painel num navegador real. As fixtures de cada cenário ficam em `<cenário>/fixtures/` e não fazem parte do modelo analítico. O cenário de esquema sobe só `postgres` e `db-init` e leva cerca de 10 s.

## Critérios da #23 por cenário

| Critério da #23 | Onde é conferido |
|---|---|
| Agenda diária às 06:00 em America/Sao_Paulo, sem catchup, run agendada real | `pipeline` P3, P4 |
| Coleta válida: métricas no snapshot, no histórico e na API; tabela e gráficos | `integrado` N1, N2, N7 (Chromium); `web` P2–P4, B1–B3, G1 (Chromium) |
| Categoria inédita e categoria omitida: mapeamento, cadastro, presença por data e lacuna na evolução | `pipeline` V1, M5; `integrado` N3, N4 |
| Falha transitória (retry, callback de sucesso, publicação) e definitiva (retentativas, callback, alerta, preservação) | `pipeline` I3, I4 (estado completo do banco), C1–C5, M6; `integrado` N6 (estado completo do banco) |
| Catálogo vazio e produto inválido separados, falha antes do cálculo, dados preservados | `pipeline` V2–V4, V5 e M6 (cadastro, snapshot e histórico com valores e xmin) |
| Fan-out com mais de duas categorias, no máximo duas simultâneas, fan-in completo, XCom pequeno | `pipeline` M1–M3, M5 |
| Reprocessar a mesma run: data de referência estável, sem duplicatas por data+categoria e run+categoria | `pipeline` P5; `publicacao` H2; `integrado` N5 (exige nova tentativa da publicação) |
| Falha durante a publicação: snapshot e histórico consistentes após rollback | `publicacao` U8, U9, H3 |
| Painel: seleção de data e categoria, barras, linha e mensagem de dia sem registros | `web` P1–P5, B1–B3, E1–E4, G1 (Chromium real: troca de data e filtros pelo formulário, valores entregues ao Chart.js); `integrado` N2, N4, N7 |
| Suíte pelo comando documentado, resultado por cenário | `e2e/executar_todos.sh` e `docs/evidencias.md` |
