# Especificação da ShopBrasil

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Este documento reúne o comportamento aprovado, os limites de arquitetura e o modelo de dados de referência para implementação. O [enunciado](atividade-01-descricao.pdf) define os requisitos acadêmicos. O [AGENTS.md](../AGENTS.md) orienta o trabalho dos agentes. Os números abaixo correspondem às issues do projeto.

## Modelo de dados — #3, #15, #17 e #18

| Tabela | Campos | Restrições e responsabilidade |
|---|---|---|
| `categoria` | `id` inteiro identity, `codigo` texto, `descricao` texto | PK `id`; código obrigatório e único, derivado de `category` da FakeStore. Cadastre categorias novas automaticamente e preserve o cadastro quando uma categoria estiver ausente. |
| `metricas_categoria` | `id` inteiro identity, `categoria_id`, `data_referencia` (`DATE`), `preco_medio`, `preco_minimo`, `preco_maximo`, `quantidade_produtos` | PK `id`; FK obrigatória para `categoria.id`; unicidade por `(data_referencia, categoria_id)`. Resultado diário consumido pelo painel. |
| `metricas_categoria_historico` | `run_id` texto, `categoria_id`, data da execução, `preco_medio`, `preco_minimo`, `preco_maximo`, `quantidade_produtos` | FK obrigatória para `categoria.id`; unicidade por `(run_id, categoria_id)`. Append por run distinta. Preservar linhas existentes no reprocessamento da mesma run. |

Os preços são `NUMERIC` com escala 2 e quantidade inteira. As quatro métricas são obrigatórias em cada linha publicada. A issue #3 materializa o DDL, incluindo precisão total e nomes finais para a data de execução no histórico. A #15 define o formato serializável entre tasks e registra o modo de arredondamento. O código da categoria, antes da resolução da FK, acompanha as quatro métricas na saída consolidada. O Airflow mantém o controle das runs e das tentativas. O modelo analítico tem três tabelas e dispensa o campo `ativo`.

A publicação usa `PostgresHook` e uma Connection do Airflow. Cadastro de categorias, substituição do conjunto de métricas da data de referência e inserção do histórico ocorrem na mesma transação. Uma falha preserva o estado anterior. Uma categoria ausente de uma coleta válida fica fora do snapshot daquela data. Após reprocessamento, continua existindo no máximo uma linha para cada chave de negócio em cada tabela.

## Comportamento do pipeline — #4–#8, #13–#18

- Agendar diariamente às 06:00 em `America/Sao_Paulo`, com `pendulum`, `start_date` e `catchup=False`. A data de referência é a da ocorrência agendada no fuso de Brasília. O reprocessamento executa novamente tarefas da própria run e preserva o `run_id` e a data original. Se a ingestão for repetida, ela lê a resposta disponível naquele momento.
- Buscar produtos na FakeStore. Timeout HTTP de 15 segundos, três retentativas depois da primeira tentativa, atraso inicial de um minuto e backoff exponencial. Relançar exceções da busca. Registrar callbacks de sucesso, retry e falha na task crítica. Avisar falhas do pipeline e simular envio de alerta ao esgotar as tentativas da ingestão.
- Validar com `ValidarProdutosOperator(BaseOperator)` antes dos cálculos. Catálogo vazio ou produto inválido impede publicação. Categorias ausentes são normais quando o catálogo é válido e não vazio.
- Usar TaskFlow, pelo menos dois TaskGroups e dependências pelas chamadas das funções. Identificar trecho linear, fan-out com `.expand(...)` pelas categorias da coleta e fan-in de consolidação. Usar XCom automático via `return` com payload pequeno e pool `ecommerce_pool` com dois slots nas tasks mapeadas.
- Calcular média, mínimo, máximo e quantidade por categoria usando `Decimal`. Arredondar a média uma vez para duas casas. Snapshot, histórico e dashboard exibem os mesmos valores publicados.

## Arquitetura e Web/API — #2, #4, #9, #19–#21

- Dois módulos independentes no mesmo repositório. PostgreSQL e o esquema em `db/` são o contrato entre eles.
- Pipeline: DAG e composição ligam orquestração, aplicação e adaptadores. A aplicação declara os contratos de entrada do catálogo e saída de persistência e usa o domínio. O domínio calcula sem Airflow, HTTP ou SQL. Os adaptadores implementam os contratos.
- Web/API: controller e modelos HTTP chamam o caso de uso. A aplicação define modelos de leitura e o contrato do repositório. A infraestrutura implementa o acesso PostgreSQL de leitura. A composição conecta as camadas. A página e a API compartilham casos de uso, e a web tem tipos próprios.
- Painel em uma tela. Tabela com quatro métricas e barras de preço médio acompanham a data selecionada. A tela abre na última data com métricas e informa “Sem métricas para esta data” quando a consulta estiver vazia. A linha de evolução usa categoria e intervalo, com lacunas nos dias sem observações. A web consulta o snapshot diário. O histórico por run é demonstrado no banco.

## Ambiente, critérios de qualidade e entrega — #1, #2, #11, #12, #22 e #23

- Dockerizar o projeto e manter a execução do pipeline independente da web. Provisionar pool e Connection. Separar credenciais de leitura e escrita. Usar `.env.example` para a configuração inicial e `.env` local para valores privados.
- A #1 fixa as versões compatíveis de Python, Airflow, provider e PostgreSQL, a tecnologia Web/API e a aplicação do esquema. O `start_date` concreto fica na #4. O contrato HTTP fica na #9. Essas são entregas das respectivas issues.
- Claude Opus implementa DAG, ingestão e análise. Kiro com Claude Opus implementa ambiente, esquema e persistência. As duas frentes alinham o contrato da #15 e revisam o código uma da outra. Codex faz a revisão final integrada, incluindo a Web/API.
- Suíte E2E reproduzível em Docker com Airflow e PostgreSQL reais e respostas controladas da FakeStore. A #23 contém o checklist de execução normal, falhas e retentativas, catálogo inválido ou vazio, categorias novas e ausentes, concorrência, XCom, idempotência, rollback e painel.
- Documentar configuração e reprocessamento. Conferir o acesso de leitura do avaliador e registrar a entrega do link no canal acadêmico. Seguir a convenção de cabeçalhos e identificação acadêmica definida no AGENTS.md.
