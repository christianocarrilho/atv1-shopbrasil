# infraestrutura

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Implementa o acesso HTTP à FakeStore e a publicação transacional no PostgreSQL.

## Publicação (#17)

[publicacao_postgres.py](publicacao_postgres.py) implementa `RepositorioPublicacao` com `PostgresHook` e a Connection `shopbrasil_postgres` (conta de escrita, sem DDL). Uma única transação cadastra os códigos novos (`INSERT ... ON CONFLICT (codigo) DO NOTHING`, `descricao` igual ao código), resolve `categoria_id` pelo código, apaga o snapshot da data de referência e insere o conjunto recebido, com `Decimal` e `date` passados ao driver. O bloco `with conexao` do psycopg2 faz o commit ao final ou o rollback em qualquer exceção.

Ponto de extensão da #18: `PublicadorSnapshotPostgres(etapas_adicionais=[...])` recebe objetos `EtapaTransacional`, chamados como `etapa(cursor, snapshot)` depois do snapshot e antes do commit. `snapshot` (`SnapshotNaTransacao`) traz a `Execucao`, as métricas e o mapa `codigo -> categoria_id` já resolvido. A etapa usa só o cursor recebido, não faz commit nem rollback e lança exceção para desfazer a publicação inteira. A composição registra a etapa.
