#!/usr/bin/env bash
# Autor: Christiano Henrique Carrilho Lopes da Silva
# Código de pessoa: 1691353
# Matrícula: 262987
# Cenário E2E do DAG shopbrasil_metricas sobre Airflow e PostgreSQL reais em Docker.
# Cobertura da #4: importação, TaskGroups e dependências TaskFlow, agenda diária às 06:00 em
# America/Sao_Paulo com catchup=False, data de referência derivada da ocorrência e não do
# relógio, run agendada real, reprocessamento da mesma run e rejeição de run sem data lógica.
# As issues seguintes do pipeline acrescentam seus cenários a este arquivo.
#
# Uso: e2e/pipeline/executar.sh
# Variáveis opcionais: E2E_AIRFLOW_API_PORT (padrão 18090) e E2E_MANTER=1 (não remove o projeto).

E2E_NOME=pipeline
E2E_PORTA_PADRAO=18090
source "$(dirname "${BASH_SOURCE[0]}")/../lib/comum.sh"

DAG_ID="shopbrasil_metricas"
# Ocorrência das 06:00 de 21/11/2026 em Brasília (UTC-3), anterior ao dia da execução do teste.
OCORRENCIA="2026-11-21T09:00:00+00:00"
RUN_ID="e2e__ocorrencia_2026-11-21"

echo "Projeto Compose isolado: $PROJECT"
subir_airflow "P0 subida do Airflow"

# P1. Importação do DAG real montado de pipeline/dags.
if aguardar_dag "$DAG_ID"; then registrar PASS "P1 DAG registrado" "$DAG_ID"; else registrar FAIL "P1 DAG registrado" "ausente"; resumo; fi
verificar "P1 erros de importação" "0" "$(admin_sql airflow "SELECT count(*) FROM import_error")"

# P2. Estrutura: TaskGroups e dependências criadas pelas chamadas TaskFlow.
estrutura="$(dc exec -T airflow-scheduler python - <<'PY' 2>/dev/null | tail -1
from airflow.dag_processing.dagbag import DagBag
dag = DagBag("/opt/airflow/dags").dags["shopbrasil_metricas"]
grupos = sorted(k for k, v in dag.task_group.children.items() if hasattr(v, "children"))
arestas = sorted(f"{u}>{t.task_id}" for t in dag.tasks for u in t.upstream_task_ids)
print(",".join(grupos) + "|" + " ".join(arestas))
PY
)"
verificar "P2 TaskGroups e dependências" \
  "analise,ingestao,preparacao,publicacao|analise.consolidar>publicacao.publicar ingestao.buscar_produtos>analise.consolidar preparacao.identificar>ingestao.buscar_produtos preparacao.identificar>publicacao.publicar" \
  "$estrutura"

# P3. Agenda: cron das 06:00 em Brasília, start_date com pendulum e catchup=False.
verificar "P3 agenda registrada" "0 6 * * *|America/Sao_Paulo" \
  "$(admin_sql airflow "SELECT timetable_summary || '|' || (SELECT data::json->'dag'->'timetable'->'__var'->>'timezone' FROM serialized_dag WHERE dag_id = '$DAG_ID' ORDER BY created_at DESC LIMIT 1) FROM dag WHERE dag_id = '$DAG_ID'" 2>/dev/null)"
config_dag="$(dc exec -T airflow-scheduler python - <<'PY' 2>/dev/null | tail -1
from airflow.dag_processing.dagbag import DagBag
dag = DagBag("/opt/airflow/dags").dags["shopbrasil_metricas"]
print(f"{dag.start_date.isoformat()}|{dag.timezone.name}|catchup={dag.catchup}")
PY
)"
verificar "P3 start_date, fuso e catchup" "2026-11-01T03:00:00+00:00|America/Sao_Paulo|catchup=False" "$config_dag"
airflow_cli dags unpause "$DAG_ID" >/dev/null 2>&1
# Com catchup=False, o Airflow 3.4 cria só a run da ocorrência das 06:00 mais recente, sem as
# retroativas desde o start_date. A ocorrência esperada vem do relógio do PostgreSQL: não rode o
# cenário atravessando 06:00 de Brasília, quando o scheduler criaria uma segunda run agendada.
ULTIMA_SQL="(SELECT (CASE WHEN (now() AT TIME ZONE 'America/Sao_Paulo')::time >= '06:00'
  THEN (now() AT TIME ZONE 'America/Sao_Paulo')::date ELSE (now() AT TIME ZONE 'America/Sao_Paulo')::date - 1 END))"
AGENDADA=""
for _ in $(seq 1 20); do
  AGENDADA="$(admin_sql airflow "SELECT run_id FROM dag_run WHERE dag_id = '$DAG_ID' AND run_type = 'scheduled'")"
  [[ -n "$AGENDADA" ]] && break
  sleep 3
done
verificar "P3 catchup: só a ocorrência mais recente" "1|true" "$(admin_sql airflow "
SELECT count(*) || '|' || bool_and(logical_date = (($ULTIMA_SQL + time '06:00') AT TIME ZONE 'America/Sao_Paulo'))
FROM dag_run WHERE dag_id = '$DAG_ID' AND run_type = 'scheduled'")"
proxima=""
for _ in $(seq 1 20); do
  proxima="$(admin_sql airflow "SELECT to_char(next_dagrun AT TIME ZONE 'America/Sao_Paulo', 'HH24:MI') || '|' || (next_dagrun > now() AND (next_dagrun AT TIME ZONE 'America/Sao_Paulo')::date = $ULTIMA_SQL + 1) FROM dag WHERE dag_id = '$DAG_ID' AND next_dagrun IS NOT NULL")"
  [[ "$proxima" == *"|true" ]] && break
  sleep 3
done
verificar "P3 próxima ocorrência às 06:00 de Brasília, no dia seguinte à agendada" "06:00|true" "$proxima"

# P4. Run da ocorrência de 21/11: a data de referência vem da data lógica, não do relógio.
airflow_cli dags trigger "$DAG_ID" --logical-date "$OCORRENCIA" --run-id "$RUN_ID" >/dev/null 2>&1
verificar "P4 run da ocorrência" "success" "$(aguardar_run "$DAG_ID" "$RUN_ID")"
# No Airflow 3.4 cada tentativa é uma linha de task_instance e o XCom fica em xcom_v2.
# xcom_execucao [run_id] [tentativa]: XCom de preparacao.identificar; sem tentativa, a mais recente.
xcom_execucao() {
  local filtro_tentativa=""
  [[ -n "${2:-}" ]] && filtro_tentativa="AND ti.try_number = $2"
  admin_sql airflow "SELECT x.value->>'run_id' || '|' || (x.value->>'data_referencia')
    FROM xcom_v2 x JOIN task_instance ti ON ti.id = x.task_instance_id
    WHERE ti.dag_id = '$DAG_ID' AND ti.run_id = '${1:-$RUN_ID}' AND ti.task_id = 'preparacao.identificar'
      AND x.key = 'return_value' $filtro_tentativa ORDER BY ti.try_number DESC LIMIT 1"
}
verificar "P4 data de referência da ocorrência" "$RUN_ID|2026-11-21" "$(xcom_execucao)"
verificar "P4 tasks da run" "4|4" "$(admin_sql airflow "
SELECT count(*), count(*) FILTER (WHERE state = 'success') FROM task_instance WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_ID'")"
verificar "P4 run agendada real" "success" "$(aguardar_run "$DAG_ID" "$AGENDADA")"
verificar "P4 data de referência da run agendada" "$AGENDADA|$(admin_sql airflow "SELECT $ULTIMA_SQL")" "$(xcom_execucao "$AGENDADA")"

# P5. Reprocessamento: limpar as tasks da mesma run preserva run_id e data de referência.
airflow_cli tasks clear "$DAG_ID" -s "$OCORRENCIA" -e "$OCORRENCIA" -y >/dev/null 2>&1
sleep 3
verificar "P5 run reprocessada" "success" "$(aguardar_run "$DAG_ID" "$RUN_ID")"
verificar "P5 mesma run e data de referência original na 2ª tentativa" "$RUN_ID|2026-11-21" "$(xcom_execucao "$RUN_ID" 2)"
verificar "P5 uma run, quatro tasks, segunda tentativa com sucesso" "1|4|4" "$(admin_sql airflow "
SELECT (SELECT count(*) FROM dag_run WHERE dag_id = '$DAG_ID' AND logical_date = '$OCORRENCIA'),
       count(DISTINCT task_id), count(*) FILTER (WHERE try_number = 2 AND state = 'success')
FROM task_instance WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_ID'")"

# P6. Run sem data lógica é rejeitada em vez de usar o relógio. A CLI e a UI sempre preenchem a
# data lógica, então o cenário usa trigger_dag do Airflow 3.4.0 (API interna, fixada pela #1).
SEM_DATA="e2e__sem_data_logica"
dc exec -T airflow-scheduler python - "$SEM_DATA" <<'PY' >"$LOG_DIR/sem-data.log" 2>&1
import sys
from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils.types import DagRunTriggeredByType
trigger_dag(dag_id="shopbrasil_metricas", run_id=sys.argv[1], logical_date=None,
            triggered_by=DagRunTriggeredByType.CLI, triggering_user_name="e2e")
PY
verificar "P6 run sem data lógica falha" "failed" "$(aguardar_run "$DAG_ID" "$SEM_DATA")"
log_sem_data="$(dc exec -T airflow-scheduler bash -c "grep -rh 'OcorrenciaAusenteError' /opt/airflow/logs/dag_id=$DAG_ID/run_id=$SEM_DATA/ 2>/dev/null | head -1")"
if [[ -n "$log_sem_data" ]]; then registrar PASS "P6 motivo no log" "OcorrenciaAusenteError"; else registrar FAIL "P6 motivo no log" "mensagem ausente"; fi
verificar "P6 tasks seguintes não executadas" "0" "$(admin_sql airflow "
SELECT count(*) FROM task_instance WHERE dag_id = '$DAG_ID' AND run_id = '$SEM_DATA' AND task_id <> 'preparacao.identificar' AND state = 'success'")"

resumo
