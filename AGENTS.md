# Convenções da ShopBrasil

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Este arquivo orienta a implementação e a revisão dos artefatos do pipeline e da Web/API.

## Cabeçalhos dos arquivos

- No topo de cada artefato escrito à mão, inclua o nome do autor e uma descrição breve da responsabilidade do arquivo. Quando houver uma decisão técnica relevante naquele ponto, explique em uma frase por que ela foi adotada.
- Em Python, use a docstring do módulo. Em SQL, YAML, Dockerfile, JavaScript, CSS e HTML, use comentários válidos para o formato. Em Markdown, coloque a identificação e a descrição logo após o título. Preserve linhas que precisam vir primeiro, como o shebang de um script.
- Mantenha arquivos gerados e formatos que não aceitam comentários válidos. Se a explicação for útil, coloque-a na documentação mais próxima.
- Use o nome, o código de pessoa e a matrícula indicados no topo deste arquivo nos cabeçalhos dos artefatos escritos à mão. O autor autorizou a publicação desses dados no repositório acadêmico.
- Inclua a conferência desses cabeçalhos na revisão de cada entrega.

## Escopo e arquitetura

Consulte a [especificação da ShopBrasil](docs/especificacao.md) para o modelo de dados e as decisões de implementação consolidadas.

- Consulte o [enunciado da Atividade 01](<docs/Atividade 01 - Descrição.pdf>) como fonte dos requisitos acadêmicos. A seção 3 traz os requisitos obrigatórios, a seção 4 lista os opcionais e a seção 5 define a entrega. Use as issues do repositório como backlog. Os três opcionais aprovados são o operador de validação, o histórico em append e o alerta simulado. A Web/API e a suíte E2E são extras aprovados.
- Mantenha `pipeline/` e `web/` no mesmo repositório, com o esquema compartilhado em `db/`. O esquema PostgreSQL é o contrato entre os módulos. A web usa acesso de leitura e seus próprios tipos, sem importar código do pipeline. O pipeline funciona independentemente da web.
- No pipeline, o DAG define a orquestração e chama a aplicação. A aplicação coordena operações, define os contratos de catálogo e persistência e usa o domínio. O domínio calcula as métricas e depende apenas de Python. Os adaptadores traduzem os dados HTTP e concentram acesso ao banco, SQL e transações. O ponto de composição conecta contratos aos adaptadores concretos. O DAG não contém SQL nem fórmulas.
- Na Web/API, a apresentação recebe filtros e traduz a resposta HTTP. O caso de uso define o modelo de leitura e depende do contrato do repositório. O repositório PostgreSQL implementa esse contrato e contém o SQL. A composição conecta controller, caso de uso e repositório. A página e a API compartilham os casos de uso. Crie entidades de domínio web se surgirem regras de negócio próprias.

## Pipeline e falhas

- Use TaskFlow (`@dag`, `@task`), dependências pelas chamadas, pelo menos dois TaskGroups e trechos linear, fan-out e fan-in identificáveis. Use XCom automático via `return` com dados pequenos e serializáveis. Mapeie com `.expand(...)` sobre as categorias encontradas na coleta, sem lista fixa, e aplique `ecommerce_pool` com dois slots às tasks mapeadas. Consolide os resultados antes de publicar.
- Agende diariamente às 06:00 em `America/Sao_Paulo`, usando `pendulum`, `start_date` e `catchup=False`. A data de referência é a data da ocorrência agendada em Brasília. Para reprocessar, execute novamente as tasks da própria run e preserve `run_id` e data de referência. O Airflow acompanha status e tentativas.
- Em Buscar Produtos, use timeout HTTP de 15 segundos, três retentativas após a tentativa inicial, atraso inicial de um minuto e backoff exponencial. Use `try/except` e relance o erro para acionar o retry. Configure callbacks de retry, sucesso e falha na task. Registre um aviso identificável quando o pipeline falhar e simule o envio do alerta quando a ingestão esgotar as tentativas.
- Execute `ValidarProdutosOperator(BaseOperator)` após a ingestão e antes do cálculo. Catálogo vazio ou qualquer produto inválido interrompe a publicação e preserva os dados existentes. Uma categoria ausente de um catálogo válido fica fora do resultado daquele dia.
- Calcule preço médio, mínimo, máximo e quantidade por categoria. Use `Decimal` no cálculo e arredonde a média uma vez para duas casas ao produzir o resultado. Use `NUMERIC` com duas casas no banco e exiba os mesmos valores na web. Defina na #15 como transportar os valores pelo XCom preservando sua precisão.

## Modelo de dados e publicação

- `categoria`: `id` PK `IDENTITY`, `codigo` único derivado do valor da FakeStore e `descricao`. Cadastre novas categorias automaticamente. O cadastro permanece quando a categoria está ausente da coleta.
- `metricas_categoria`: `id` PK `IDENTITY`, FK `categoria_id`, `data_referencia` e quatro métricas, com unicidade por data e categoria. Uma publicação bem-sucedida substitui o conjunto daquele dia.
- `metricas_categoria_historico`: FK `categoria_id`, `run_id`, data da execução e quatro métricas. Grave em append com unicidade por run e categoria. Reprocessar a mesma run preserva as entradas de histórico já registradas.
- Use `PostgresHook` com uma Connection do Airflow. Cadastro, snapshot e histórico compartilham a transação de publicação. Uma falha reverte a operação e preserva os dados anteriores. O modelo tem três tabelas analíticas, sem campo `ativo` e sem tabela própria de controle de execução.

## Dashboard

- Uma tela com filtro por data, tabela das quatro métricas e barras de preço médio por categoria para o dia selecionado. Abra na data mais recente com métricas, mostre a data selecionada e use “Sem métricas para esta data” quando o dia não tiver registros.
- Na mesma tela, mostre a evolução diária do preço médio em linha com filtros por categoria e intervalo. Mantenha lacunas nos dias sem dados. As visualizações consultam o snapshot diário. Demonstre o histórico por run por consulta ao banco.

## Ambiente, execução do trabalho e entrega

- Dockerize o projeto, provisionando a Connection e o pool. Na #1, registre versões compatíveis de Python, Airflow, provider e PostgreSQL, a tecnologia Web/API e a forma de aplicar o esquema SQL. Defina o valor de `start_date` na configuração do DAG e documente como subir o ambiente.
- Claude Opus implementa DAG, ingestão e análise. Kiro com Claude Opus implementa ambiente, esquema e persistência. Alinhe primeiro o contrato de saída das métricas entre as frentes. Claude revisa a frente do Kiro, Kiro revisa a do Claude e Codex faz a revisão final integrada, incluindo a web.
- Use o padrão de issue `[Tipo] - Área - Verbo no infinitivo + objeto`. Tipos: Task, Feature, Bug, Validação e Documentação. Áreas: Pipeline, Web e Projeto. Inclua label de área, tipo no corpo, escopo, critérios de aceite verificáveis e dependências. Divida entregas extensas em sub-issues com resultados independentes.
- Na #23, use Airflow e PostgreSQL reais em Docker, com dados isolados e respostas controladas no limite HTTP da FakeStore. Cubra execução normal, agendamento, callbacks, retry e falha definitiva, catálogo vazio e inválido, categorias novas e ausentes, limite de concorrência, XCom pequeno, reprocessamento, rollback, API e painel. Documente um comando para rodar a suíte e registre os resultados por cenário.
- Na #12, documente a inicialização dos módulos e o reprocessamento, confira o acesso de leitura para o avaliador e prepare a postagem do link no canal da atividade.

- Siga o fluxo de integração definido em [Fluxo de integração do backlog](#fluxo-de-integração-do-backlog). `main` continua sendo a única branch permanente.
- Reúna comandos, resultados observados, evidências e links em `docs/evidencias.md`. O autor compartilhará com o professor o link do repositório e o do documento.

## Fluxo de integração do backlog

Decisão do autor em 22 nov. 2026, válida a partir de `main` 78ed846: as issues restantes são implementadas primeiro e entram em `main` por um único PR, depois da suíte completa e da fase de correções. As issues #1, #2, #3, #4, #15, #24, #26, #27 e #29 já entraram em `main` pelo fluxo anterior, um PR por issue.

- A branch temporária `integracao-backlog`, criada a partir de `main`, reúne as entregas. Não faça merge individual em `main`.
- Abra uma branch por issue a partir da `integracao-backlog` atualizada, com o número da issue no nome (`issue-<N>-<assunto>`). Abra o PR da issue para `integracao-backlog`, com `Refs #N` no corpo, e integre-o com merge commit para preservar os commits da issue. PRs abertos antes desta decisão para `main` são redirecionados para `integracao-backlog`.
- Durante a implementação, cada PR de issue passa pelo lint de arquitetura, pelos testes unitários e pela revisão cruzada. Acrescente ou atualize no mesmo PR o cenário E2E aplicável e execute verificações rápidas. A execução completa da suíte não bloqueia cada issue.
- Quando todas as issues restantes estiverem integradas, abra um único PR de `integracao-backlog` para `main`. O corpo traz `Closes #N` para cada issue entregue, incluindo as agregadoras cujas filhas estejam validadas, e uma tabela issue → commits/PR de issue que garante a rastreabilidade.
- Sobre a revisão final desse PR, execute a suíte E2E completa e o lint. Corrija os achados em commits que citem a issue afetada e registre comandos e resultados em `docs/evidencias.md`. O merge em `main` só ocorre depois das correções e das validações.
- As issues ficam abertas até o merge do PR final. Confirme o fechamento de cada uma depois dele.

## Critérios obrigatórios de revisão e merge

- Antes de editar, leia este arquivo, a especificação e a issue, incluindo seus critérios de aceite e dependências. Mantenha o escopo da issue. Se encontrar conflito entre instruções ou precisar alterar um contrato aprovado, registre a divergência para o coordenador resolver antes de implementar a parte afetada.
- Atribua toda nova issue a `christianocarrilho`. Use apenas uma referência à issue agregadora até que todas as sub-issues estejam integradas e o conjunto validado.
- Aplique a revisão cruzada definida acima. A revisão final cabe ao coordenador da integração, papel do Codex e, por decisão do autor, assumido pelo Claude nesta fase. Ela confere escopo, cabeçalhos, contratos, arquitetura e evidências. O coordenador integra somente depois de resolver os achados e de passar nas verificações aplicáveis.

## Lint de arquitetura

- No Pipeline, `dominio` usa apenas a biblioteca padrão. `aplicacao` usa seu próprio código e `dominio`, sem acoplar a Airflow, HTTP, PostgreSQL ou adaptadores concretos. `infraestrutura` pode usar os contratos da aplicação e os tipos do domínio. O DAG aciona casos de uso e a composição. Restrinja a ligação com adaptadores concretos ao ponto de composição.
- Na Web/API, `apresentacao` usa os casos de uso e modelos de `consultas`, sem acessar `infraestrutura` diretamente. `consultas` define seus modelos de leitura e contratos sem depender de HTTP ou PostgreSQL. `infraestrutura` implementa esses contratos. A composição é o ponto que conecta apresentação, consultas e adaptadores.
- Proíba imports entre `pipeline/` e `web/` nos dois sentidos. O esquema em `db/` é o contrato entre eles. O lint da #26 deve verificar imports absolutos e relativos e falhar com diagnóstico acionável em caso de violação; cubra o resultado com exemplos permitidos e proibidos.
- Execute o comando versionado do lint de arquitetura em cada PR que altere código ou regras de dependência e registre o resultado no PR. PR de código sujeito às regras só entra em `integracao-backlog` ou em `main` depois que a checagem estiver disponível e passar. Não silencie violações nem amplie permissões para passar no lint. A revisão também verifica responsabilidades que imports não revelam, como SQL no DAG ou no controller.

## E2E por entrega

- Para cada issue funcional ou técnica com comportamento observável de ponta a ponta, acrescente ao mesmo PR um cenário E2E para o comportamento novo ou alterado. Se um cenário existente já cobre o caso, atualize-o quando necessário e identifique-o no PR. Se não houver cenário aplicável, explique por quê no PR.
- Durante a implementação, registre no PR da issue as verificações rápidas executadas. Antes do merge do PR final em `main`, execute a suíte E2E completa sobre a revisão final e registre no PR e em `docs/evidencias.md` o comando, os cenários e o resultado observado. Falha ou ausência de execução aplicável bloqueia esse merge.
- A #23 consolida a execução e a documentação da suíte. Inclua cobertura incremental em cada entrega aplicável; se faltar infraestrutura para executar o cenário, implemente o mínimo necessário ou integre primeiro a dependência que o viabiliza.
