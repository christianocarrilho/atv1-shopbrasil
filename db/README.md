# db

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Reúne o esquema SQL compartilhado entre os módulos e a inicialização do banco.

- [acessos.sql](acessos.sql) cria os bancos `airflow` e `shopbrasil` e as contas de escrita e leitura, com seus privilégios.
- [aplicar.sh](aplicar.sh) é o ponto de entrada do serviço `db-init`. Aplica `acessos.sql` e depois `schema.sql`.
- `schema.sql` recebe o DDL da #3, idempotente e no esquema `public`.

O [guia do ambiente](../docs/ambiente-docker.md) descreve as contas e a ordem de aplicação.
