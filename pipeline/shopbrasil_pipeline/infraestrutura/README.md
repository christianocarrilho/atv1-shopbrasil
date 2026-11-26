# infraestrutura

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Implementa o acesso HTTP à FakeStore e a publicação transacional no PostgreSQL, com o histórico por run.

## Publicação e histórico (#17, #18)

[publicacao_postgres.py](publicacao_postgres.py) implementa `RepositorioPublicacao` com `PostgresHook` e a Connection `shopbrasil_postgres` (conta de escrita, sem DDL). Uma única transação faz três passos:

1. Cadastra os códigos novos (`INSERT ... ON CONFLICT (codigo) DO NOTHING`, `descricao` igual ao código) e resolve `categoria_id` pelo código.
2. Apaga o snapshot da data de referência e insere o conjunto recebido, com `Decimal` e `date` passados ao driver.
3. Acrescenta o histórico por run em `metricas_categoria_historico` (`_gravar_historico`): uma linha por categoria com `run_id`, `data_execucao` igual à `data_referencia` e os mesmos `Decimal` do snapshot. O `ON CONFLICT (run_id, categoria_id) DO NOTHING` faz o append: uma run distinta acrescenta linhas, e o reprocessamento da mesma run preserva as já gravadas. Se os valores preservados diferem do snapshot recém-publicado, o log registra `[SHOPBRASIL][AVISO] histórico preservado diverge do snapshot` por categoria.

O bloco `with conexao` do psycopg2 faz o commit ao final ou o rollback em qualquer exceção, desfazendo os três passos juntos. Depois do commit, as linhas `[SHOPBRASIL][PUBLICACAO]` e `[SHOPBRASIL][HISTORICO]` informam quantas linhas foram substituídas, gravadas e preservadas. O provider do Airflow só é importado ao abrir a conexão, por isso os testes unitários usam cursor e conexão falsos com a biblioteca padrão.

## FakeStore (#13)

[fakestore.py](fakestore.py) consulta `GET {FAKESTORE_BASE_URL}/products` com timeout de 15 segundos e lê o JSON com números em `Decimal`. Erros são propagados para o retry da task.
