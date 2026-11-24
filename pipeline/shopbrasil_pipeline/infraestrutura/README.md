# infraestrutura

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Implementa o acesso HTTP à FakeStore e a publicação transacional no PostgreSQL, com o histórico por run.

## Publicação (#17)

[publicacao_postgres.py](publicacao_postgres.py) implementa `RepositorioPublicacao` com `PostgresHook` e a Connection `shopbrasil_postgres` (conta de escrita, sem DDL). Uma única transação cadastra os códigos novos (`INSERT ... ON CONFLICT (codigo) DO NOTHING`, `descricao` igual ao código), resolve `categoria_id` pelo código, apaga o snapshot da data de referência e insere o conjunto recebido, com `Decimal` e `date` passados ao driver. O bloco `with conexao` do psycopg2 faz o commit ao final ou o rollback em qualquer exceção.

Ponto de extensão: `PublicadorSnapshotPostgres(etapas_adicionais=[...])` recebe objetos `EtapaTransacional`, chamados como `etapa(cursor, snapshot)` depois do snapshot e antes do commit. `snapshot` (`SnapshotNaTransacao`) traz a `Execucao`, as métricas e o mapa `codigo -> categoria_id` já resolvido. A etapa usa só o cursor recebido, não faz commit nem rollback e lança exceção para desfazer a publicação inteira. A composição registra a etapa.

## Histórico por run (#18)

[historico_postgres.py](historico_postgres.py) define `RegistrarHistoricoPostgres`, a etapa adicional que a composição registra no publicador. Com o cursor da publicação, insere em `metricas_categoria_historico` uma linha por categoria publicada: `run_id` da execução, `categoria_id` já resolvido, `data_execucao` igual à `data_referencia` e as quatro métricas, com os mesmos `Decimal` do snapshot. O `INSERT ... ON CONFLICT (run_id, categoria_id) DO NOTHING` faz o append. Uma run distinta acrescenta linhas. No reprocessamento da mesma run, as chaves já gravadas ficam com os valores originais, mesmo que o snapshot do dia seja substituído, e só uma categoria que a run ainda não tinha registrado é acrescentada. Quando os valores preservados diferem do snapshot recém-publicado, a etapa relê as linhas da run e registra `[SHOPBRASIL][AVISO] histórico preservado diverge do snapshot` com os dois conjuntos de valores, por categoria; a diferença fica visível no log da task sem contrariar a regra de preservação. Um erro do banco nessa etapa, como a violação de `metricas_categoria_historico_run_id_ck`, desfaz cadastro e snapshot junto com o histórico. O módulo não importa o provider do Airflow, por isso os testes unitários usam um cursor falso.
