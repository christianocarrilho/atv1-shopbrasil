#!/usr/bin/env bash
# Autor: Christiano Henrique Carrilho Lopes da Silva
# Código de pessoa: 1691353
# Matrícula: 262987
# Cenário E2E do ambiente Docker (#2): sobe um projeto Compose isolado, com volumes e senhas
# próprios, e verifica subida em ambiente limpo, Connection, pool, contas de escrita e leitura,
# execução do Airflow sem a Web/API e o comportamento do db-init. Remove tudo ao final.
# Usa um nome de projeto exclusivo para não tocar nos contêineres e volumes do ambiente local.
#
# Uso: e2e/ambiente/executar.sh
# Variáveis opcionais: E2E_AIRFLOW_API_PORT (padrão 18080) e E2E_MANTER=1 (não remove o projeto).

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROJECT="shopbrasil-e2e-ambiente-$(date +%s)"
ENV_FILE="$(mktemp -t shopbrasil-e2e-env.XXXXXX)"
DAG_ID="e2e_ambiente_sonda"
FALHAS=0
RESULTADOS=()

segredo() { openssl rand -hex 16; }

WRITER_PASSWORD="$(segredo)"
READER_PASSWORD="$(segredo)"
ADMIN_PASSWORD="$(segredo)"

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
AIRFLOW_API_PORT=${E2E_AIRFLOW_API_PORT:-18080}
AIRFLOW_UID=50000
EOF

dc() {
  docker compose -p "$PROJECT" --project-directory "$ROOT" --env-file "$ENV_FILE" \
    -f "$ROOT/docker-compose.yml" -f "$ROOT/e2e/ambiente/compose.e2e.yml" "$@"
}

# psql pelo endereço de rede do serviço `postgres`, o mesmo caminho usado pelos outros contêineres.
# O pg_hba.conf da imagem confia em 127.0.0.1; o endereço da rede exige senha (scram-sha-256).
psql_como() {
  local usuario="$1" senha="$2" banco="$3"
  shift 3
  dc exec -T -e PGPASSWORD="$senha" postgres \
    psql -X -At -v ON_ERROR_STOP=1 -h postgres -U "$usuario" -d "$banco" "$@"
}
admin_sql() { psql_como shopbrasil_admin "$ADMIN_PASSWORD" "$1" -c "$2"; }

registrar() {
  local status="$1" cenario="$2" detalhe="$3"
  [[ "$status" == PASS ]] || FALHAS=$((FALHAS + 1))
  RESULTADOS+=("| $cenario | $status | $detalhe |")
  echo "[$status] $cenario: $detalhe"
}

verificar() {
  # verificar <cenário> <esperado> <obtido>
  if [[ "$3" == "$2" ]]; then registrar PASS "$1" "$3"; else registrar FAIL "$1" "esperado '$2', obtido '$3'"; fi
}

limpar() {
  if [[ "${E2E_MANTER:-0}" == 1 ]]; then
    echo "Projeto mantido: docker compose -p $PROJECT ... down -v (env em $ENV_FILE)"
    return
  fi
  dc down -v --remove-orphans >/dev/null 2>&1
  rm -f "$ENV_FILE"
}
trap limpar EXIT

echo "Projeto Compose isolado: $PROJECT"

# A1. Configuração válida a partir do contrato do .env.example.
if dc config -q; then registrar PASS "A1 configuração" "docker compose config -q"; else registrar FAIL "A1 configuração" "config inválida"; exit 1; fi

# A2. Subida em ambiente limpo: volumes novos, serviços saudáveis e inicializações concluídas.
if dc up -d --wait --wait-timeout 420 airflow-apiserver airflow-scheduler airflow-dag-processor >/tmp/"$PROJECT"-up.log 2>&1; then
  registrar PASS "A2 subida limpa" "up --wait concluído"
else
  registrar FAIL "A2 subida limpa" "ver /tmp/$PROJECT-up.log"
  dc ps -a
  dc logs --no-color db-init airflow-init | tail -50
  exit 1
fi
estados="$(dc ps -a --format '{{.Service}}={{.State}}/{{.Health}}/{{.ExitCode}}' | sort | tr '\n' ' ')"
esperado_estados="airflow-apiserver=running/healthy/0 airflow-dag-processor=running/healthy/0 airflow-init=exited//0 airflow-scheduler=running/healthy/0 db-init=exited//0 postgres=running/healthy/0 "
verificar "A2 estados dos serviços" "$esperado_estados" "$estados"

# A3. Versões fixadas pela #1.
verificar "A3 Airflow" "3.4.0" "$(dc exec -T airflow-scheduler airflow version 2>/dev/null | tail -1)"
verificar "A3 PostgreSQL" "18.7" "$(admin_sql postgres 'SHOW server_version' | awk '{print $1}')"
verificar "A3 provider postgres" "7.1.1" \
  "$(dc exec -T airflow-scheduler python -c 'from importlib.metadata import version; print(version("apache-airflow-providers-postgres"))')"

# A4. Pool com dois slots.
slots="$(dc exec -T airflow-scheduler airflow pools get ecommerce_pool -o json 2>/dev/null \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["slots"])')"
verificar "A4 ecommerce_pool" "2" "$slots"

# A5. Connection analítica resolvida pelo PostgresHook com a conta de escrita.
conexao="$(dc exec -T airflow-scheduler python -c '
from airflow.providers.postgres.hooks.postgres import PostgresHook
print("|".join(map(str, PostgresHook(postgres_conn_id="shopbrasil_postgres").get_first(
    "SELECT current_user, current_database(), inet_server_port()"))))' 2>/dev/null | tail -1)"
verificar "A5 Connection shopbrasil_postgres" "shopbrasil_writer|shopbrasil|5432" "$conexao"

# A6. Esquema: db/schema.sql (#3) aplicado quando existe; a fixture isolada sempre aplicada.
log_init="$(dc logs --no-color db-init 2>/dev/null)"
if [[ -f "$ROOT/db/schema.sql" ]]; then
  if grep -q "aplicando /db/schema.sql" <<<"$log_init"; then registrar PASS "A6 db/schema.sql" "aplicado"; else registrar FAIL "A6 db/schema.sql" "não aplicado"; fi
else
  if grep -q "AVISO /db/schema.sql não encontrado" <<<"$log_init"; then registrar PASS "A6 db/schema.sql" "ausente (DDL da #3), aviso registrado"; else registrar FAIL "A6 db/schema.sql" "aviso ausente"; fi
fi
verificar "A6 fixture de sonda" "e2e_ambiente_sonda" \
  "$(admin_sql shopbrasil "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename='e2e_ambiente_sonda'")"

# A7. Matriz de privilégios sobre todas as tabelas do esquema public (inclui as da #3, se houver).
matriz="$(admin_sql shopbrasil "
SELECT count(*) FILTER (WHERE NOT (has_table_privilege('shopbrasil_reader', c.oid, 'SELECT')
         AND NOT has_table_privilege('shopbrasil_reader', c.oid, 'INSERT')
         AND NOT has_table_privilege('shopbrasil_reader', c.oid, 'UPDATE')
         AND NOT has_table_privilege('shopbrasil_reader', c.oid, 'DELETE')
         AND NOT has_table_privilege('shopbrasil_reader', c.oid, 'TRUNCATE')
         AND has_table_privilege('shopbrasil_writer', c.oid, 'SELECT')
         AND has_table_privilege('shopbrasil_writer', c.oid, 'INSERT')
         AND has_table_privilege('shopbrasil_writer', c.oid, 'UPDATE')
         AND has_table_privilege('shopbrasil_writer', c.oid, 'DELETE')
         AND NOT has_table_privilege('shopbrasil_writer', c.oid, 'TRUNCATE')))
       || '/' || count(*)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')")"
if [[ "$matriz" =~ ^0/[1-9][0-9]*$ ]]; then registrar PASS "A7 privilégios por tabela" "violações/tabelas = $matriz"; else registrar FAIL "A7 privilégios por tabela" "violações/tabelas = $matriz"; fi
verificar "A7 CREATE no esquema e CONNECT no Airflow" "f|f|f|f|t|t" "$(admin_sql shopbrasil "
SELECT has_schema_privilege('shopbrasil_reader', 'public', 'CREATE'),
       has_schema_privilege('shopbrasil_writer', 'public', 'CREATE'),
       has_database_privilege('shopbrasil_reader', 'airflow', 'CONNECT'),
       has_database_privilege('shopbrasil_writer', 'airflow', 'CONNECT'),
       has_database_privilege('shopbrasil_reader', 'shopbrasil', 'CONNECT'),
       has_database_privilege('shopbrasil_writer', 'shopbrasil', 'CONNECT')")"

# A8. Leitura da web: consulta permitida; escrita, DDL e banco do Airflow negados.
leitura="$(psql_como shopbrasil_reader "$READER_PASSWORD" shopbrasil -c 'SELECT count(*) FROM e2e_ambiente_sonda' 2>&1)"
if [[ "$leitura" =~ ^[0-9]+$ ]]; then registrar PASS "A8 leitor consulta" "SELECT count(*) = $leitura"; else registrar FAIL "A8 leitor consulta" "$leitura"; fi
negado() {
  # negado <cenário> <padrão esperado no erro> <usuário> <senha> <banco> <sql>
  local saida
  if saida="$(psql_como "$3" "$4" "$5" -c "$6" 2>&1)"; then
    registrar FAIL "$1" "comando aceito: $6"
  elif grep -q "$2" <<<"$saida"; then
    registrar PASS "$1" "$(grep -o "ERROR: .*\|FATAL: .*" <<<"$saida" | head -1 | tr -d '\r')"
  else
    registrar FAIL "$1" "$saida"
  fi
}
negado "A8 leitor INSERT" "permission denied" shopbrasil_reader "$READER_PASSWORD" shopbrasil \
  "INSERT INTO e2e_ambiente_sonda (run_id, map_index, inicio, fim) VALUES ('x', 0, now(), now())"
negado "A8 leitor UPDATE" "permission denied" shopbrasil_reader "$READER_PASSWORD" shopbrasil \
  "UPDATE e2e_ambiente_sonda SET map_index = 0"
negado "A8 leitor DELETE" "permission denied" shopbrasil_reader "$READER_PASSWORD" shopbrasil \
  "DELETE FROM e2e_ambiente_sonda"
negado "A8 leitor CREATE TABLE" "permission denied" shopbrasil_reader "$READER_PASSWORD" shopbrasil \
  "CREATE TABLE e2e_tentativa (id int)"
negado "A8 leitor banco do Airflow" "permission denied for database" shopbrasil_reader "$READER_PASSWORD" airflow \
  "SELECT 1"
senha_errada="$(psql_como shopbrasil_reader incorreta shopbrasil -c 'SELECT 1' 2>&1)"
if grep -q "password authentication failed" <<<"$senha_errada"; then registrar PASS "A8 autenticação por senha" "senha incorreta rejeitada"; else registrar FAIL "A8 autenticação por senha" "$senha_errada"; fi

# A9. Escrita do pipeline sem DDL.
negado "A9 escritor CREATE TABLE" "permission denied" shopbrasil_writer "$WRITER_PASSWORD" shopbrasil \
  "CREATE TABLE e2e_tentativa (id int)"

# A10. Pipeline no Airflow sem a Web/API: DAG de sonda com Connection e pool reais.
servicos="$(dc ps -a --services | sort | tr '\n' ' ')"
verificar "A10 serviços sem Web/API" "airflow-apiserver airflow-dag-processor airflow-init airflow-scheduler db-init postgres " "$servicos"
verificar "A10 DAG registrado ao concluir up --wait" "t" "$(admin_sql airflow "SELECT EXISTS (SELECT 1 FROM dag WHERE dag_id = '$DAG_ID')")"
erros_import="$(admin_sql airflow "SELECT count(*) FROM import_error")"
verificar "A10 erros de importação" "0" "$erros_import"
dc exec -T airflow-scheduler airflow dags unpause "$DAG_ID" >/dev/null 2>&1
verificar "A10 DAG ativo após um unpause" "f" "$(admin_sql airflow "SELECT is_paused FROM dag WHERE dag_id = '$DAG_ID'")"
dc exec -T airflow-scheduler airflow dags trigger "$DAG_ID" >/dev/null 2>&1
estado_run=""
for _ in $(seq 1 80); do
  estado_run="$(admin_sql airflow "SELECT state FROM dag_run WHERE dag_id = '$DAG_ID' ORDER BY id DESC LIMIT 1")"
  [[ "$estado_run" == success || "$estado_run" == failed ]] && break
  sleep 3
done
verificar "A10 DAG run" "success" "$estado_run"
verificar "A10 tasks mapeadas no pool" "4|4|ecommerce_pool" "$(admin_sql airflow "
SELECT count(*), count(*) FILTER (WHERE state = 'success'), string_agg(DISTINCT pool, ',')
FROM task_instance WHERE dag_id = '$DAG_ID' AND task_id = 'ocupar_slot'")"
verificar "A10 demais tasks" "3|3" "$(admin_sql airflow "
SELECT count(*), count(*) FILTER (WHERE state = 'success')
FROM task_instance WHERE dag_id = '$DAG_ID' AND task_id <> 'ocupar_slot'")"
# Concorrência máxima observada pelos intervalos gravados pelas próprias tasks.
verificar "A10 linhas gravadas e concorrência máxima" "4|2" "$(admin_sql shopbrasil "
SELECT count(*), (SELECT max(n) FROM (
    SELECT (SELECT count(*) FROM e2e_ambiente_sonda b WHERE b.inicio <= a.inicio AND b.fim > a.inicio) AS n
    FROM e2e_ambiente_sonda a) s)
FROM e2e_ambiente_sonda")"

# A11. Reaplicação do db-init sobre volume existente.
if dc run --rm --no-deps db-init >/tmp/"$PROJECT"-reinit.log 2>&1; then
  registrar PASS "A11 db-init reaplicado" "código 0; $(grep -c 'already exists' /tmp/"$PROJECT"-reinit.log) aviso(s) 'already exists'"
else
  registrar FAIL "A11 db-init reaplicado" "ver /tmp/$PROJECT-reinit.log"
fi
verificar "A11 dados preservados e leitura mantida" "4" \
  "$(psql_como shopbrasil_reader "$READER_PASSWORD" shopbrasil -c 'SELECT count(*) FROM e2e_ambiente_sonda' 2>&1)"

# A12. SQL inválido encerra o db-init com erro (ON_ERROR_STOP).
dc run --rm --no-deps -e SHOPBRASIL_SQL_EXTRA_DIR=/e2e/sql-invalido db-init >/tmp/"$PROJECT"-invalido.log 2>&1
codigo=$?
if [[ $codigo -ne 0 ]] && grep -q "syntax error" /tmp/"$PROJECT"-invalido.log; then
  registrar PASS "A12 db-init com SQL inválido" "código $codigo, syntax error"
else
  registrar FAIL "A12 db-init com SQL inválido" "código $codigo"
fi

echo
echo "| Cenário | Resultado | Observado |"
echo "|---|---|---|"
printf '%s\n' "${RESULTADOS[@]}"
echo
if [[ $FALHAS -eq 0 ]]; then echo "E2E ambiente: todos os cenários passaram"; else echo "E2E ambiente: $FALHAS falha(s)"; fi
exit $(( FALHAS > 0 ))
