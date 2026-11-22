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

Copie `.env.example` para `.env` se o arquivo local ainda não existir. Preencha as senhas no `.env` e ajuste o host conforme o ambiente de execução. O template descreve as variáveis iniciais. Sua integração com Docker, a Connection do Airflow e a Web/API será feita nas respectivas issues.
