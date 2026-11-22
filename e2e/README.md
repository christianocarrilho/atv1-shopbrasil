# e2e

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Reúne a suíte de ponta a ponta com Airflow e PostgreSQL reais e respostas controladas da FakeStore.

## Cenários

| Pasta | Issue | Comando | Cobertura |
|---|---|---|---|
| [ambiente](ambiente/) | #2 | `e2e/ambiente/executar.sh` | Subida limpa, versões, Connection, pool de 2 slots, contas de escrita e leitura, Airflow sem a Web/API e `db-init` |

Cada cenário usa um projeto Compose próprio, com senhas aleatórias e volumes descartados ao final. Requer Docker com Compose v2 e `openssl`. As fixtures do cenário de ambiente ficam em `ambiente/fixtures/` e não fazem parte do modelo analítico.
