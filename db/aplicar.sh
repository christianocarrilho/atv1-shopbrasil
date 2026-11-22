#!/bin/sh
# Autor: Christiano Henrique Carrilho Lopes da Silva
# Código de pessoa: 1691353
# Matrícula: 262987
# Ponto de entrada do serviço db-init: provisiona contas e bancos (db/acessos.sql) e aplica
# o esquema compartilhado (db/schema.sql, entregue pela #3) com a conta administrativa.
# Usa psql com ON_ERROR_STOP para que qualquer erro encerre o serviço com código diferente
# de zero e impeça a subida do Airflow, conforme docs/decisao-versoes-tecnologias.md.
set -eu

: "${POSTGRES_USER:?POSTGRES_USER ausente}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD ausente}"
: "${SHOPBRASIL_DB_HOST:?SHOPBRASIL_DB_HOST ausente}"
: "${SHOPBRASIL_DB_PORT:?SHOPBRASIL_DB_PORT ausente}"
: "${SHOPBRASIL_DB_NAME:?SHOPBRASIL_DB_NAME ausente}"

SCHEMA_FILE="${SHOPBRASIL_SCHEMA_FILE:-/db/schema.sql}"
export PGPASSWORD="$POSTGRES_PASSWORD"

psql_admin() {
  psql -X -q -v ON_ERROR_STOP=1 -h "$SHOPBRASIL_DB_HOST" -p "$SHOPBRASIL_DB_PORT" \
    -U "$POSTGRES_USER" "$@"
}

echo "db-init: provisionando bancos e contas"
psql_admin -d postgres \
  -v admin_user="$POSTGRES_USER" \
  -v airflow_db="${AIRFLOW_DB_NAME:?AIRFLOW_DB_NAME ausente}" \
  -v airflow_user="${AIRFLOW_DB_USER:?AIRFLOW_DB_USER ausente}" \
  -v airflow_password="${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD ausente}" \
  -v analytic_db="$SHOPBRASIL_DB_NAME" \
  -v writer_user="${SHOPBRASIL_DB_WRITER_USER:?SHOPBRASIL_DB_WRITER_USER ausente}" \
  -v writer_password="${SHOPBRASIL_DB_WRITER_PASSWORD:?SHOPBRASIL_DB_WRITER_PASSWORD ausente}" \
  -v reader_user="${SHOPBRASIL_DB_READER_USER:?SHOPBRASIL_DB_READER_USER ausente}" \
  -v reader_password="${SHOPBRASIL_DB_READER_PASSWORD:?SHOPBRASIL_DB_READER_PASSWORD ausente}" \
  -f /db/acessos.sql

# O esquema roda em uma única transação: um erro de DDL desfaz o arquivo inteiro e
# interrompe o db-init. Sem o arquivo, o db-init apenas avisa.
if [ -f "$SCHEMA_FILE" ]; then
  echo "db-init: aplicando $SCHEMA_FILE em $SHOPBRASIL_DB_NAME"
  psql_admin -d "$SHOPBRASIL_DB_NAME" --single-transaction -f "$SCHEMA_FILE"
else
  echo "db-init: AVISO $SCHEMA_FILE não encontrado; nenhuma tabela analítica aplicada (DDL da #3)"
fi

# Diretório opcional de SQL adicional, usado apenas pelas fixtures E2E isoladas.
if [ -n "${SHOPBRASIL_SQL_EXTRA_DIR:-}" ]; then
  for extra in "$SHOPBRASIL_SQL_EXTRA_DIR"/*.sql; do
    [ -f "$extra" ] || continue
    echo "db-init: aplicando SQL adicional $extra"
    psql_admin -d "$SHOPBRASIL_DB_NAME" -f "$extra"
  done
fi

echo "db-init: concluído"
