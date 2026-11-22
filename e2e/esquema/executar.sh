#!/usr/bin/env bash
# Autor: Christiano Henrique Carrilho Lopes da Silva
# Código de pessoa: 1691353
# Matrícula: 262987
# Cenário E2E do esquema analítico (#3): sobe apenas o PostgreSQL 18.7 e o db-init reais em um
# projeto Compose isolado e verifica, em banco vazio, tabelas, colunas, tipos, chaves identity,
# unicidade, FKs, CHECKs e precisão NUMERIC(12,2) com a conta de escrita do pipeline. Depois
# reaplica o db-init sobre o volume com dados e confirma o rollback de um esquema com erro.
# Dispensa o Airflow porque o esquema é o contrato do banco; o cenário de ambiente cobre o resto.
#
# Uso: e2e/esquema/executar.sh
# Variável opcional: E2E_MANTER=1 (não remove o projeto ao final).

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROJECT="shopbrasil-e2e-esquema-$(date +%s)"
ENV_FILE="$(mktemp -t shopbrasil-e2e-env.XXXXXX)"
LOG_DIR="$(mktemp -d -t shopbrasil-e2e-esquema.XXXXXX)"
FALHAS=0
RESULTADOS=()

segredo() { openssl rand -hex 16; }

WRITER_PASSWORD="$(segredo)"
READER_PASSWORD="$(segredo)"
ADMIN_PASSWORD="$(segredo)"

# O Compose interpola o arquivo inteiro, então as variáveis do Airflow também precisam de valor.
cat >"$ENV_FILE" <<EOF
FAKESTORE_BASE_URL=http://fakestore.invalid
POSTGRES_USER=shopbrasil_admin
POSTGRES_PASSWORD=${ADMIN_PASSWORD}
SHOPBRASIL_DB_HOST=postgres
SHOPBRASIL_DB_PORT=5432
SHOPBRASIL_DB_NAME=shopbrasil
SHOPBRASIL_DB_WRITER_USER=shopbrasil_writer
SHOPBRASIL_DB_WRITER_PASSWORD=${WRITER_PASSWORD}
SHOPBRASIL_DB_READER_USER=shopbrasil_reader
SHOPBRASIL_DB_READER_PASSWORD=${READER_PASSWORD}
AIRFLOW_DB_NAME=airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=$(segredo)
AIRFLOW_JWT_SECRET=$(openssl rand -hex 32)
AIRFLOW_FERNET_KEY=
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=$(segredo)
AIRFLOW_API_PORT=18081
AIRFLOW_UID=50000
EOF

dc() {
  docker compose -p "$PROJECT" --project-directory "$ROOT" --env-file "$ENV_FILE" \
    -f "$ROOT/docker-compose.yml" -f "$ROOT/e2e/esquema/compose.e2e.yml" "$@"
}

# psql pelo endereço de rede do serviço `postgres`, com senha, como os demais contêineres.
psql_como() {
  local usuario="$1" senha="$2" banco="$3"
  shift 3
  dc exec -T -e PGPASSWORD="$senha" postgres \
    psql -X -q -At -v ON_ERROR_STOP=1 -h postgres -U "$usuario" -d "$banco" "$@"
}
admin_sql() { psql_como shopbrasil_admin "$ADMIN_PASSWORD" "${2:-shopbrasil}" -c "$1"; }
escritor_sql() { psql_como shopbrasil_writer "$WRITER_PASSWORD" shopbrasil -c "$1"; }
leitor_sql() { psql_como shopbrasil_reader "$READER_PASSWORD" shopbrasil -c "$1"; }

registrar() {
  local status="$1" cenario="$2" detalhe="$3"
  [[ "$status" == PASS ]] || FALHAS=$((FALHAS + 1))
  RESULTADOS+=("| $cenario | $status | $detalhe |")
  echo "[$status] $cenario: $detalhe"
}

verificar() {
  # verificar <cenário> <esperado> <obtido> [resumo exibido quando passa]
  if [[ "$3" == "$2" ]]; then registrar PASS "$1" "${4:-$(tr '\n' ' ' <<<"$3" | sed 's/ $//')}"; else
    registrar FAIL "$1" "esperado '$(tr '\n' ' ' <<<"$2")', obtido '$(tr '\n' ' ' <<<"$3")'"
  fi
}

recusado() {
  # recusado <cenário> <trecho esperado no erro> <sql>: o escritor executa e o banco deve recusar.
  local saida
  if saida="$(escritor_sql "$3" 2>&1)"; then
    registrar FAIL "$1" "comando aceito: $3"
  elif grep -qF "$2" <<<"$saida"; then
    registrar PASS "$1" "$(grep -o 'ERROR: .*' <<<"$saida" | head -1 | tr -d '\r')"
  else
    registrar FAIL "$1" "$(tr '\n' ' ' <<<"$saida")"
  fi
}

limpar() {
  if [[ "${E2E_MANTER:-0}" == 1 ]]; then
    echo "Projeto mantido: docker compose -p $PROJECT ... down -v (env em $ENV_FILE, logs em $LOG_DIR)"
    return
  fi
  dc down -v --remove-orphans >/dev/null 2>&1
  rm -f "$ENV_FILE"
}
trap limpar EXIT

# Assinaturas usadas antes e depois da reaplicação.
COLUNAS_SQL="
SELECT table_name || '.' || column_name || ':' ||
       CASE WHEN data_type = 'numeric' THEN 'numeric(' || numeric_precision || ',' || numeric_scale || ')'
            ELSE data_type END ||
       ':' || CASE is_nullable WHEN 'NO' THEN 'not null' ELSE 'null' END ||
       CASE WHEN is_identity = 'YES' THEN ':identity ' || identity_generation ELSE '' END
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position"
RESTRICOES_SQL="
SELECT c.conrelid::regclass || ' ' || c.conname || ' ' || pg_get_constraintdef(c.oid)
FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
WHERE n.nspname = 'public' AND c.contype IN ('p', 'u', 'f')
ORDER BY 1"
DADOS_SQL="
SELECT md5(concat_ws('|',
  (SELECT string_agg(concat_ws(',', id, codigo, descricao), ';' ORDER BY id) FROM categoria),
  (SELECT string_agg(concat_ws(',', id, categoria_id, data_referencia, preco_medio, preco_minimo,
                               preco_maximo, quantidade_produtos), ';' ORDER BY id) FROM metricas_categoria),
  (SELECT string_agg(concat_ws(',', run_id, categoria_id, data_execucao, preco_medio, preco_minimo,
                               preco_maximo, quantidade_produtos), ';' ORDER BY run_id, categoria_id)
     FROM metricas_categoria_historico)))
  || ' ' || (SELECT count(*) FROM categoria) || '/' || (SELECT count(*) FROM metricas_categoria)
  || '/' || (SELECT count(*) FROM metricas_categoria_historico)"

echo "Projeto Compose isolado: $PROJECT"

# E1. Aplicação em banco vazio pelo db-init real.
if ! dc config -q; then registrar FAIL "E1 configuração" "config inválida"; exit 1; fi
if ! dc up -d --wait --wait-timeout 120 postgres >"$LOG_DIR/up.log" 2>&1; then
  registrar FAIL "E1 PostgreSQL" "ver $LOG_DIR/up.log"; exit 1
fi
verificar "E1 banco vazio antes do db-init" "0" "$(admin_sql "SELECT count(*) FROM pg_database WHERE datname = 'shopbrasil'" postgres)"
dc run --rm --no-deps db-init >"$LOG_DIR/init.log" 2>&1
codigo=$?
if [[ $codigo -eq 0 ]] && grep -q "aplicando /db/schema.sql em shopbrasil" "$LOG_DIR/init.log"; then
  registrar PASS "E1 db-init aplica db/schema.sql" "código 0; $(grep -c 'already exists' "$LOG_DIR/init.log") aviso(s) 'already exists'"
else
  registrar FAIL "E1 db-init aplica db/schema.sql" "código $codigo; ver $LOG_DIR/init.log"
  cat "$LOG_DIR/init.log"; exit 1
fi
verificar "E1 tabelas em public" "categoria
metricas_categoria
metricas_categoria_historico" "$(admin_sql "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY 1")"
verificar "E1 dono das tabelas e sem tabelas fora de public" "shopbrasil_admin|0" "$(admin_sql "
SELECT string_agg(DISTINCT tableowner, ','),
       (SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('public', 'pg_catalog', 'information_schema'))
FROM pg_tables WHERE schemaname = 'public'")"

# E2. Colunas, tipos, obrigatoriedade e identity. Preços NUMERIC(12,2), quantidade inteira, DATE.
COLUNAS_ESPERADAS="categoria.id:integer:not null:identity ALWAYS
categoria.codigo:text:not null
categoria.descricao:text:not null
metricas_categoria.id:integer:not null:identity ALWAYS
metricas_categoria.categoria_id:integer:not null
metricas_categoria.data_referencia:date:not null
metricas_categoria.preco_medio:numeric(12,2):not null
metricas_categoria.preco_minimo:numeric(12,2):not null
metricas_categoria.preco_maximo:numeric(12,2):not null
metricas_categoria.quantidade_produtos:integer:not null
metricas_categoria_historico.run_id:text:not null
metricas_categoria_historico.categoria_id:integer:not null
metricas_categoria_historico.data_execucao:date:not null
metricas_categoria_historico.preco_medio:numeric(12,2):not null
metricas_categoria_historico.preco_minimo:numeric(12,2):not null
metricas_categoria_historico.preco_maximo:numeric(12,2):not null
metricas_categoria_historico.quantidade_produtos:integer:not null"
colunas_antes="$(admin_sql "$COLUNAS_SQL")"
verificar "E2 colunas e tipos" "$COLUNAS_ESPERADAS" "$colunas_antes" \
  "$(wc -l <<<"$colunas_antes" | tr -d ' ') colunas; ids identity ALWAYS; preços numeric(12,2); quantidade integer; datas date; todas NOT NULL"

# E3. Chaves primárias, unicidade e FKs declaradas.
RESTRICOES_ESPERADAS="categoria categoria_codigo_uk UNIQUE (codigo)
categoria categoria_pk PRIMARY KEY (id)
metricas_categoria metricas_categoria_categoria_fk FOREIGN KEY (categoria_id) REFERENCES categoria(id) ON DELETE RESTRICT
metricas_categoria metricas_categoria_data_categoria_uk UNIQUE (data_referencia, categoria_id)
metricas_categoria metricas_categoria_pk PRIMARY KEY (id)
metricas_categoria_historico metricas_categoria_historico_categoria_fk FOREIGN KEY (categoria_id) REFERENCES categoria(id) ON DELETE RESTRICT
metricas_categoria_historico metricas_categoria_historico_pk PRIMARY KEY (run_id, categoria_id)"
restricoes_antes="$(admin_sql "$RESTRICOES_SQL")"
verificar "E3 PKs, unicidade e FKs" "$RESTRICOES_ESPERADAS" "$restricoes_antes" \
  "PK categoria(id), UNIQUE (codigo); PK metricas_categoria(id), UNIQUE (data_referencia, categoria_id); PK historico(run_id, categoria_id); 2 FKs para categoria(id) ON DELETE RESTRICT"

# E4. Chaves identity e código único em categoria, com a conta de escrita do pipeline.
verificar "E4 identity gera os ids" "1|electronics
2|jewelery" "$(escritor_sql "INSERT INTO categoria (codigo, descricao) VALUES
  ('electronics', 'electronics'), ('jewelery', 'jewelery') RETURNING id, codigo")"
recusado "E4 id manual recusado (ALWAYS)" 'cannot insert a non-DEFAULT value into column "id"' \
  "INSERT INTO categoria (id, codigo, descricao) VALUES (99, 'manual', 'manual')"
recusado "E4 código duplicado" 'violates unique constraint "categoria_codigo_uk"' \
  "INSERT INTO categoria (codigo, descricao) VALUES ('electronics', 'outra')"
recusado "E4 código vazio" 'violates check constraint "categoria_codigo_preenchido_ck"' \
  "INSERT INTO categoria (codigo, descricao) VALUES ('  ', 'vazio')"
verificar "E4 cadastro idempotente por código" "0|2" "$(escritor_sql "
WITH ins AS (INSERT INTO categoria (codigo, descricao) VALUES ('electronics', 'electronics')
             ON CONFLICT (codigo) DO NOTHING RETURNING id)
SELECT (SELECT count(*) FROM ins), (SELECT count(*) FROM categoria)")"

# E5. Snapshot diário: unicidade por data e categoria, FK e precisão.
# Exemplo da #15: preços 10,00, 10,00 e 10,01 publicam média 10,00, mínimo 10,00, máximo 10,01 e 3.
escritor_sql "INSERT INTO metricas_categoria
  (categoria_id, data_referencia, preco_medio, preco_minimo, preco_maximo, quantidade_produtos) VALUES
  (1, '2026-11-22', '10.00', '10.00', '10.01', 3),
  (2, '2026-11-22', '9999999999.99', '0.00', '9999999999.99', 1),
  (1, '2026-11-23', '109.95', '22.30', '999.99', 6)" >/dev/null
verificar "E5 valores lidos pela Web/API com escala 2" "1|2026-11-22|10.00|10.00|10.01|3
2|2026-11-22|9999999999.99|0.00|9999999999.99|1
1|2026-11-23|109.95|22.30|999.99|6" "$(leitor_sql "
SELECT categoria_id, data_referencia, preco_medio, preco_minimo, preco_maximo, quantidade_produtos
FROM metricas_categoria ORDER BY data_referencia, categoria_id")"
recusado "E5 data e categoria duplicadas" 'violates unique constraint "metricas_categoria_data_categoria_uk"' \
  "INSERT INTO metricas_categoria (categoria_id, data_referencia, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
   VALUES (1, '2026-11-22', '1.00', '1.00', '1.00', 1)"
recusado "E5 FK de categoria inexistente" 'violates foreign key constraint "metricas_categoria_categoria_fk"' \
  "INSERT INTO metricas_categoria (categoria_id, data_referencia, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
   VALUES (999, '2026-11-24', '1.00', '1.00', '1.00', 1)"
recusado "E5 métrica obrigatória" 'null value in column "preco_medio"' \
  "INSERT INTO metricas_categoria (categoria_id, data_referencia, preco_minimo, preco_maximo, quantidade_produtos)
   VALUES (1, '2026-11-24', '1.00', '1.00', 1)"
recusado "E5 preço acima de NUMERIC(12,2)" 'numeric field overflow' \
  "INSERT INTO metricas_categoria (categoria_id, data_referencia, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
   VALUES (1, '2026-11-24', '10000000000.00', '1.00', '10000000000.00', 1)"
recusado "E5 mínimo acima da média" 'violates check constraint "metricas_categoria_precos_ck"' \
  "INSERT INTO metricas_categoria (categoria_id, data_referencia, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
   VALUES (1, '2026-11-24', '5.00', '6.00', '7.00', 1)"
recusado "E5 quantidade zero" 'violates check constraint "metricas_categoria_quantidade_ck"' \
  "INSERT INTO metricas_categoria (categoria_id, data_referencia, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
   VALUES (1, '2026-11-24', '1.00', '1.00', '1.00', 0)"
# Valores com mais de duas casas são arredondados pelo tipo (meio para longe do zero); por isso
# o contrato da #15 entrega strings já com duas casas.
verificar "E5 arredondamento do tipo para mais de duas casas" "10.01|0.13|10.00" \
  "$(escritor_sql "SELECT '10.005'::numeric(12,2), '0.125'::numeric(12,2), '10.004'::numeric(12,2)")"
verificar "E5 tipo devolvido ao cliente" "numeric|numeric|numeric|integer|date" "$(leitor_sql "
SELECT pg_typeof(preco_medio) || '|' || pg_typeof(preco_minimo) || '|' || pg_typeof(preco_maximo)
       || '|' || pg_typeof(quantidade_produtos) || '|' || pg_typeof(data_referencia)
FROM metricas_categoria LIMIT 1")"

# E6. Histórico por run: unicidade por run e categoria, append por run distinta e FK.
escritor_sql "INSERT INTO metricas_categoria_historico
  (run_id, categoria_id, data_execucao, preco_medio, preco_minimo, preco_maximo, quantidade_produtos) VALUES
  ('scheduled__2026-11-22T09:00:00+00:00', 1, '2026-11-22', '10.00', '10.00', '10.01', 3),
  ('scheduled__2026-11-22T09:00:00+00:00', 2, '2026-11-22', '15.50', '15.50', '15.50', 1),
  ('manual__2026-11-22T12:00:00+00:00', 1, '2026-11-22', '10.00', '10.00', '10.01', 3)" >/dev/null
recusado "E6 run e categoria duplicadas" 'violates unique constraint "metricas_categoria_historico_pk"' \
  "INSERT INTO metricas_categoria_historico (run_id, categoria_id, data_execucao, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
   VALUES ('scheduled__2026-11-22T09:00:00+00:00', 1, '2026-11-22', '1.00', '1.00', '1.00', 1)"
verificar "E6 reprocessamento da mesma run preserva a linha" "0|10.00|3" "$(escritor_sql "
WITH ins AS (INSERT INTO metricas_categoria_historico
               (run_id, categoria_id, data_execucao, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
             VALUES ('scheduled__2026-11-22T09:00:00+00:00', 1, '2026-11-22', '20.00', '20.00', '20.00', 9)
             ON CONFLICT (run_id, categoria_id) DO NOTHING RETURNING 1)
SELECT (SELECT count(*) FROM ins), preco_medio, quantidade_produtos
FROM metricas_categoria_historico
WHERE run_id = 'scheduled__2026-11-22T09:00:00+00:00' AND categoria_id = 1")"
verificar "E6 append por run distinta" "manual__2026-11-22T12:00:00+00:00|1
scheduled__2026-11-22T09:00:00+00:00|2" "$(leitor_sql "
SELECT run_id, count(*) FROM metricas_categoria_historico GROUP BY run_id ORDER BY run_id")"
recusado "E6 FK de categoria inexistente" 'violates foreign key constraint "metricas_categoria_historico_categoria_fk"' \
  "INSERT INTO metricas_categoria_historico (run_id, categoria_id, data_execucao, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
   VALUES ('manual__x', 999, '2026-11-22', '1.00', '1.00', '1.00', 1)"
recusado "E6 run_id vazio" 'violates check constraint "metricas_categoria_historico_run_id_ck"' \
  "INSERT INTO metricas_categoria_historico (run_id, categoria_id, data_execucao, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
   VALUES ('', 1, '2026-11-22', '1.00', '1.00', '1.00', 1)"
recusado "E6 máximo abaixo da média" 'violates check constraint "metricas_categoria_historico_precos_ck"' \
  "INSERT INTO metricas_categoria_historico (run_id, categoria_id, data_execucao, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
   VALUES ('manual__y', 1, '2026-11-22', '5.00', '4.00', '4.99', 1)"
recusado "E6 categoria referenciada não é removida" 'violates RESTRICT setting of foreign key constraint' \
  "DELETE FROM categoria WHERE id = 1"
negacao_leitor="$(leitor_sql "INSERT INTO categoria (codigo, descricao) VALUES ('leitor', 'leitor')" 2>&1)"
if grep -q "permission denied for table categoria" <<<"$negacao_leitor"; then
  registrar PASS "E6 leitor sem escrita no esquema" "permission denied for table categoria"
else
  registrar FAIL "E6 leitor sem escrita no esquema" "$negacao_leitor"
fi

# E7. Reaplicação do db-init sobre o volume com dados: estrutura e dados intactos, identity contínua.
dados_antes="$(admin_sql "$DADOS_SQL")"
max_id_antes="$(admin_sql "SELECT max(id) FROM categoria")"
dc run --rm --no-deps db-init >"$LOG_DIR/reinit.log" 2>&1
codigo=$?
if [[ $codigo -eq 0 ]] && grep -q "aplicando /db/schema.sql em shopbrasil" "$LOG_DIR/reinit.log"; then
  registrar PASS "E7 db-init reaplicado" "código 0; $(grep -c 'already exists, skipping' "$LOG_DIR/reinit.log") aviso(s) 'already exists, skipping'"
else
  registrar FAIL "E7 db-init reaplicado" "código $codigo; ver $LOG_DIR/reinit.log"
fi
verificar "E7 colunas inalteradas" "$colunas_antes" "$(admin_sql "$COLUNAS_SQL")" "assinatura de E2 idêntica"
verificar "E7 restrições inalteradas" "$restricoes_antes" "$(admin_sql "$RESTRICOES_SQL")" "assinatura de E3 idêntica"
verificar "E7 dados preservados (md5 e contagens)" "$dados_antes" "$(admin_sql "$DADOS_SQL")"
novo_id="$(escritor_sql "INSERT INTO categoria (codigo, descricao) VALUES ('men''s clothing', 'men''s clothing') RETURNING id")"
if [[ "$novo_id" =~ ^[0-9]+$ ]] && (( novo_id > max_id_antes )); then
  registrar PASS "E7 identity continua após reaplicar" "novo id $novo_id > $max_id_antes"
else
  registrar FAIL "E7 identity continua após reaplicar" "novo id '$novo_id', máximo anterior $max_id_antes"
fi

# E8. Transação: o arquivo não tem COMMIT próprio e um erro no db-init desfaz o esquema inteiro.
admin_sql "CREATE DATABASE e2e_esquema_tx" postgres >/dev/null
dc run --rm --no-deps --entrypoint sh db-init -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -X -q -v ON_ERROR_STOP=1 -h "$SHOPBRASIL_DB_HOST" -U "$POSTGRES_USER" \
     -d e2e_esquema_tx -c BEGIN -f /db/schema.sql -c ROLLBACK' >"$LOG_DIR/tx.log" 2>&1
codigo=$?
verificar "E8 BEGIN + schema.sql + ROLLBACK não deixa tabelas" "0|0" \
  "$codigo|$(admin_sql "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'" e2e_esquema_tx)"
dc run --rm --no-deps -e SHOPBRASIL_DB_NAME=e2e_esquema_falho -e SHOPBRASIL_SCHEMA_FILE=/e2e/schema-com-erro.sql \
  db-init >"$LOG_DIR/falho.log" 2>&1
codigo=$?
tabelas_falho="$(admin_sql "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'" e2e_esquema_falho)"
if [[ $codigo -ne 0 ]] && grep -q "syntax error" "$LOG_DIR/falho.log" && [[ "$tabelas_falho" == 0 ]]; then
  registrar PASS "E8 erro no esquema desfaz tudo" "db-init código $codigo, syntax error, 0 tabelas criadas"
else
  registrar FAIL "E8 erro no esquema desfaz tudo" "código $codigo, tabelas $tabelas_falho; ver $LOG_DIR/falho.log"
fi
verificar "E8 banco analítico intacto após a falha" "3|3|3" "$(admin_sql "
SELECT (SELECT count(*) FROM categoria) || '|' || (SELECT count(*) FROM metricas_categoria)
       || '|' || (SELECT count(*) FROM metricas_categoria_historico)")"

echo
echo "| Cenário | Resultado | Observado |"
echo "|---|---|---|"
printf '%s\n' "${RESULTADOS[@]}"
echo
if [[ $FALHAS -eq 0 ]]; then echo "E2E esquema: todos os cenários passaram"; else echo "E2E esquema: $FALHAS falha(s)"; fi
exit $(( FALHAS > 0 ))
