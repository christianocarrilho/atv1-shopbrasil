# atv1-shopbrasil

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Projeto da Atividade 01 de Orquestração de Workflows. O pipeline coleta o catálogo da FakeStore, calcula métricas diárias por categoria e publica os resultados no PostgreSQL. A Web/API consulta essas métricas e as apresenta em um dashboard.

## Organização

- [Pipeline](pipeline/) reúne a orquestração e os módulos de coleta, cálculo e publicação.
- [Web/API](web/) reúne as consultas e o dashboard.
- [Banco](db/) reúne o esquema compartilhado.
- [Documentação](docs/) reúne decisões e guias de operação. Consulte o [enunciado da Atividade 01](<docs/Atividade 01 - Descrição.pdf>) para os requisitos acadêmicos.
- [Testes E2E](e2e/) reúne a validação de ponta a ponta.
- [Qualidade](qualidade/) reúne o lint de arquitetura. Execute `python3 qualidade/lint_arquitetura.py` na raiz do repositório.

## Configuração local

Copie `.env.example` para `.env` se o arquivo local ainda não existir e preencha as senhas. Um `.env` anterior à #2 precisa receber as variáveis novas do template. Depois suba o ambiente:

```sh
docker compose up -d --wait
```

O Airflow fica em http://localhost:8080 e o painel em http://localhost:8000. O [guia de execução](docs/execucao.md) mostra como verificar o DAG e o painel, reprocessar uma run e rodar a suíte E2E completa (`e2e/executar_todos.sh`). O [guia do ambiente Docker](docs/ambiente-docker.md) descreve serviços, contas, Connection, pool e comandos. O [modelo de dados](docs/modelo-dados.md) descreve as tabelas de `db/schema.sql`.
