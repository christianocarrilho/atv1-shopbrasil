# e2e

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Reúne a suíte de ponta a ponta com Airflow e PostgreSQL reais e respostas controladas da FakeStore.

## Cenários

| Pasta | Issue | Comando | Cobertura |
|---|---|---|---|
| [ambiente](ambiente/) | #2 | `e2e/ambiente/executar.sh` | Subida limpa, versões, Connection, pool de 2 slots, contas de escrita e leitura, Airflow sem a Web/API e `db-init` |
| [esquema](esquema/) | #3 | `e2e/esquema/executar.sh` | `db/schema.sql` em banco vazio: colunas e tipos, identity, unicidade, FKs, CHECKs, precisão `NUMERIC(12,2)`, reaplicação com dados e rollback do `db-init` |

Cada cenário usa um projeto Compose próprio, com senhas aleatórias e volumes descartados ao final. Requer Docker com Compose v2 e `openssl`. As fixtures de cada cenário ficam em `<cenário>/fixtures/` e não fazem parte do modelo analítico. O cenário de esquema sobe só `postgres` e `db-init` e leva cerca de 10 s.
