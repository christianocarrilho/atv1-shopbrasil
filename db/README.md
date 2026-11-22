# db

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Reúne o esquema SQL compartilhado entre os módulos e a inicialização do banco.

- [acessos.sql](acessos.sql) cria os bancos `airflow` e `shopbrasil` e as contas de escrita e leitura, com seus privilégios.
- [aplicar.sh](aplicar.sh) é o ponto de entrada do serviço `db-init`. Aplica `acessos.sql` e depois `schema.sql`.
- [schema.sql](schema.sql) é o DDL da #3: `categoria`, `metricas_categoria` e `metricas_categoria_historico` no esquema `public`. É idempotente, não tem GRANTs e roda numa única transação no `db-init`.

O [guia do ambiente](../docs/ambiente-docker.md) descreve as contas e a ordem de aplicação. O [modelo de dados](../docs/modelo-dados.md) descreve colunas, tipos e restrições.
