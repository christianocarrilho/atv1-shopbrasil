#!/usr/bin/env bash
# Autor: Christiano Henrique Carrilho Lopes da Silva
# Código de pessoa: 1691353
# Matrícula: 262987
# Funções compartilhadas pelos cenários E2E a partir da #4: projeto Compose isolado com senhas
# aleatórias, psql pelas contas reais, registro de resultados e espera por runs do Airflow.
# Cada cenário define E2E_NOME e, se precisar, E2E_COMPOSE_EXTRA (overrides) e E2E_PORTA_PADRAO
# (porta da API do Airflow, distinta por cenário para permitir execuções em paralelo).

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROJECT="shopbrasil-e2e-${E2E_NOME:?defina E2E_NOME}-$(date +%s)"
FALHAS=0
RESULTADOS=()
ENV_FILE=""
LOG_DIR=""

limpar() {
  if [[ "${E2E_MANTER:-0}" == 1 ]]; then
    echo "Projeto mantido: $PROJECT (env em $ENV_FILE, logs em $LOG_DIR)"
    return
  fi
  [[ -n "$ENV_FILE" ]] && dc down -v --remove-orphans >/dev/null 2>&1
  rm -f "$ENV_FILE"
  # Os logs ficam só quando algum cenário falhou, para diagnóstico.
  if [[ $FALHAS -eq 0 && -n "$LOG_DIR" ]]; then rm -rf "$LOG_DIR"; fi
}
# O trap vem antes de criar os temporários para que uma interrupção não deixe senhas em disco.
trap limpar EXIT

TMP_BASE="${TMPDIR:-/tmp}"
ENV_FILE="$(mktemp "${TMP_BASE%/}/shopbrasil-e2e-env.XXXXXX")"
LOG_DIR="$(mktemp -d "${TMP_BASE%/}/shopbrasil-e2e-logs.XXXXXX")"

segredo() { openssl rand -hex 16; }
WRITER_PASSWORD="$(segredo)"
READER_PASSWORD="$(segredo)"
ADMIN_PASSWORD="$(segredo)"

cat >"$ENV_FILE" <<ENV
FAKESTORE_BASE_URL=${E2E_FAKESTORE_BASE_URL:-http://fakestore.invalid}
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
AIRFLOW_API_PORT=${E2E_AIRFLOW_API_PORT:-${E2E_PORTA_PADRAO:-18090}}
AIRFLOW_UID=50000
ENV

dc() {
  local arquivos=(-f "$ROOT/docker-compose.yml")
  local extra
  for extra in ${E2E_COMPOSE_EXTRA:-}; do arquivos+=(-f "$ROOT/$extra"); done
  docker compose -p "$PROJECT" --project-directory "$ROOT" --env-file "$ENV_FILE" "${arquivos[@]}" "$@"
}

psql_como() {
  local usuario="$1" senha="$2" banco="$3"
  shift 3
  dc exec -T -e PGPASSWORD="$senha" postgres \
    psql -X -At -v ON_ERROR_STOP=1 -h postgres -U "$usuario" -d "$banco" "$@"
}
admin_sql() { psql_como shopbrasil_admin "$ADMIN_PASSWORD" "$1" -c "$2"; }
airflow_cli() { dc exec -T airflow-scheduler airflow "$@"; }

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


subir_airflow() {
  # Sobe apenas o Airflow e suas dependências; a Web/API não participa do pipeline.
  if dc up -d --wait --wait-timeout 420 airflow-apiserver airflow-scheduler airflow-dag-processor >"$LOG_DIR/up.log" 2>&1; then
    registrar PASS "$1" "up --wait concluído"
  else
    registrar FAIL "$1" "ver $LOG_DIR/up.log"
    dc logs --no-color db-init airflow-init | tail -50
    exit 1
  fi
}

aguardar_dag() {
  # aguardar_dag <dag_id>: espera o dag-processor registrar o DAG.
  local _
  for _ in $(seq 1 60); do
    [[ "$(admin_sql airflow "SELECT count(*) FROM dag WHERE dag_id = '$1'")" == 1 ]] && return 0
    sleep 3
  done
  return 1
}

aguardar_run() {
  # aguardar_run <dag_id> <run_id> [tentativas]: imprime o estado final da run.
  local estado="" _
  for _ in $(seq 1 "${3:-100}"); do
    estado="$(admin_sql airflow "SELECT state FROM dag_run WHERE dag_id = '$1' AND run_id = '$2'")"
    [[ "$estado" == success || "$estado" == failed ]] && break
    sleep 3
  done
  echo "$estado"
}

resumo() {
  echo
  echo "| Cenário | Resultado | Observado |"
  echo "|---|---|---|"
  printf '%s\n' "${RESULTADOS[@]}"
  echo
  if [[ $FALHAS -eq 0 ]]; then echo "E2E $E2E_NOME: todos os cenários passaram"; else echo "E2E $E2E_NOME: $FALHAS falha(s)"; fi
  exit $(( FALHAS > 0 ))
}
