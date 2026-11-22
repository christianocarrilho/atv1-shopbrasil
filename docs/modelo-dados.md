# Modelo de dados — #3

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Este documento descreve o DDL de [db/schema.sql](../db/schema.sql): nomes finais, tipos, chaves e restrições das três tabelas analíticas, e o que a #15 precisa respeitar ao transportar as métricas até a publicação. O comportamento aprovado está na [especificação](especificacao.md#modelo-de-dados--3-15-17-e-18).

## Aplicação

O `db-init` aplica o arquivo no banco `shopbrasil`, esquema `public`, com a conta administrativa, `ON_ERROR_STOP=1` e `--single-transaction`. Por isso o arquivo:

- usa apenas comandos transacionais e não tem `BEGIN` nem `COMMIT` próprios. Um erro desfaz o arquivo inteiro e encerra o `db-init` com código diferente de zero;
- é idempotente: `CREATE TABLE IF NOT EXISTS` e `COMMENT ON` podem ser reaplicados sobre um volume com dados (`docker compose run --rm db-init`);
- não tem `GRANT`. Os privilégios padrão de [db/acessos.sql](../db/acessos.sql) dão `SELECT`, `INSERT`, `UPDATE` e `DELETE` à conta de escrita e `SELECT` à conta de leitura.

`IF NOT EXISTS` não altera uma tabela que já existe. Uma mudança de coluna ou restrição numa tabela existente exige um `ALTER TABLE` idempotente acrescentado ao arquivo.

## Tabelas

### `categoria`

| Coluna | Tipo | Regras |
|---|---|---|
| `id` | `integer GENERATED ALWAYS AS IDENTITY` | PK `categoria_pk`. O banco gera o valor e recusa id informado. |
| `codigo` | `text NOT NULL` | `UNIQUE` `categoria_codigo_uk`; não pode ser vazio (`categoria_codigo_preenchido_ck`). Valor de `category` da FakeStore. |
| `descricao` | `text NOT NULL` | Texto exibido. A FakeStore só informa `category`, então o cadastro automático da #17 grava o próprio código como descrição. |

O cadastro de categoria nova usa `INSERT ... ON CONFLICT (codigo) DO NOTHING`. Categorias nunca são removidas pelo pipeline, e as FKs usam `ON DELETE RESTRICT`.

### `metricas_categoria`

| Coluna | Tipo | Regras |
|---|---|---|
| `id` | `integer GENERATED ALWAYS AS IDENTITY` | PK `metricas_categoria_pk`. |
| `categoria_id` | `integer NOT NULL` | FK `metricas_categoria_categoria_fk` para `categoria(id)`, `ON DELETE RESTRICT`. |
| `data_referencia` | `date NOT NULL` | Data da ocorrência agendada em `America/Sao_Paulo`. |
| `preco_medio` | `numeric(12,2) NOT NULL` | |
| `preco_minimo` | `numeric(12,2) NOT NULL` | |
| `preco_maximo` | `numeric(12,2) NOT NULL` | |
| `quantidade_produtos` | `integer NOT NULL` | |

Unicidade `metricas_categoria_data_categoria_uk` em `(data_referencia, categoria_id)`. A publicação da #17 substitui o conjunto da data dentro da transação.

### `metricas_categoria_historico`

| Coluna | Tipo | Regras |
|---|---|---|
| `run_id` | `text NOT NULL` | `run_id` do DAG run do Airflow; não pode ser vazio. |
| `categoria_id` | `integer NOT NULL` | FK `metricas_categoria_historico_categoria_fk` para `categoria(id)`, `ON DELETE RESTRICT`. |
| `data_execucao` | `date NOT NULL` | Data de referência da run, a mesma de `metricas_categoria.data_referencia` na publicação. Segue o sentido de data lógica da run no Airflow, e não o instante em que a task rodou, para que o reprocessamento preserve o valor. |
| `preco_medio`, `preco_minimo`, `preco_maximo` | `numeric(12,2) NOT NULL` | |
| `quantidade_produtos` | `integer NOT NULL` | |

A PK `metricas_categoria_historico_pk` em `(run_id, categoria_id)` é a unicidade exigida, por isso a tabela não tem coluna `id`. A gravação usa `INSERT ... ON CONFLICT (run_id, categoria_id) DO NOTHING`: uma run distinta acrescenta linhas e o reprocessamento da mesma run mantém as existentes.

### Restrições das métricas

As duas tabelas de métricas têm as mesmas regras:

- `quantidade_produtos > 0` (`*_quantidade_ck`). Uma categoria entra no resultado do dia só quando tem produtos;
- `0 <= preco_minimo <= preco_medio <= preco_maximo` (`*_precos_ck`).

## Contrato com a #15

A saída consolidada de cada categoria leva o código da categoria e as quatro métricas. A #17 resolve `codigo` para `categoria_id` na publicação.

| Campo do contrato | Coluna | Tipo no banco | Transporte sugerido |
|---|---|---|---|
| código da categoria | `categoria.codigo` | `text` | string |
| preço médio | `preco_medio` | `numeric(12,2)` | string decimal com duas casas, ex. `"10.00"` |
| preço mínimo | `preco_minimo` | `numeric(12,2)` | idem |
| preço máximo | `preco_maximo` | `numeric(12,2)` | idem |
| quantidade | `quantidade_produtos` | `integer` | inteiro |

- **Precisão.** `numeric(12,2)` aceita de `0.00` a `9999999999.99` nas colunas de preço, pela restrição de mínimo não negativo. Um valor com mais de dez dígitos inteiros falha com `numeric field overflow` e desfaz a publicação. A #6 rejeita preço negativo antes do cálculo, e a restrição do banco fica como última barreira.
- **Escala.** O tipo arredonda valores com mais de duas casas, com empate para longe do zero (`10.005` vira `10.01`). Para que o banco grave exatamente o valor calculado, a #15 deve entregar os quatro preços já quantizados em duas casas, e a média arredondada uma única vez no domínio. O modo de arredondamento da média é decisão da #15.
- **Leitura.** O PostgreSQL devolve `numeric`. O `psycopg2` do `PostgresHook` e o `psycopg` da web convertem esse valor em `Decimal`, então snapshot, histórico e painel mostram o mesmo valor publicado.
- **Datas.** `data_referencia` e `data_execucao` são `date`. O transporte pode usar ISO 8601 (`"2026-11-22"`).

## Verificação

O cenário [e2e/esquema](../e2e/esquema/executar.sh) aplica o esquema num PostgreSQL 18.7 real e verifica colunas, chaves, unicidade, FKs, restrições, precisão, reaplicação e rollback. Os resultados estão em [evidencias.md](evidencias.md#3--esquema-analítico).
