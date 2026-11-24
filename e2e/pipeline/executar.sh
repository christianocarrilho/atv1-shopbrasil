#!/usr/bin/env bash
# Autor: Christiano Henrique Carrilho Lopes da Silva
# Código de pessoa: 1691353
# Matrícula: 262987
# Cenário E2E do DAG shopbrasil_metricas sobre Airflow e PostgreSQL reais em Docker.
# Cobertura da #4: importação, TaskGroups e dependências TaskFlow, agenda diária às 06:00 em
# America/Sao_Paulo com catchup=False, data de referência derivada da ocorrência e não do
# relógio, run agendada real, reprocessamento da mesma run e rejeição de run sem data lógica.
# Cobertura da #13: configuração de produção de Buscar Produtos, coleta pela FakeStore controlada
# (e2e/fakestore) com XCom pequeno e preços decimais, falha transitória com retry e recuperação,
# e falha persistente propagada após as tentativas, com backoff exponencial.
# Cobertura da #14: callbacks de sucesso, retry e falha de Buscar Produtos, alerta simulado só na
# falha definitiva da ingestão e aviso identificável na falha de outra task.
# Cobertura da #6: catálogo válido com categoria ausente aceito; catálogo vazio, preço inválido e
# categoria em branco recusados pelo ValidarProdutosOperator antes do cálculo, sem retentativa.
# Cobertura da #16: fan-out por categoria observada no ecommerce_pool com no máximo duas instâncias
# simultâneas, fan-in com um resultado por categoria, XCom pequeno, categoria inédita sem editar o
# DAG, publicação do snapshot pelo DAG e preservação do snapshot quando a run falha.
# As issues seguintes do pipeline acrescentam seus cenários a este arquivo.
#
# Uso: e2e/pipeline/executar.sh
# Variáveis opcionais: E2E_AIRFLOW_API_PORT (padrão 18090) e E2E_MANTER=1 (não remove o projeto).

E2E_NOME=pipeline
E2E_PORTA_PADRAO=18090
E2E_COMPOSE_EXTRA="e2e/pipeline/compose.e2e.yml"
E2E_FAKESTORE_BASE_URL="http://fakestore:8000"
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

# P2. Estrutura: TaskGroups e dependências criadas pelas chamadas TaskFlow. As consultas ad hoc ao
# DagBag usam o mesmo PYTHONPATH dos processos do Airflow, que incluem a pasta de DAGs (operadores/).
estrutura="$(dc exec -T -e PYTHONPATH=/opt/airflow/pipeline:/opt/airflow/dags airflow-scheduler python - <<'PY' 2>/dev/null | sed -n 's/^RESULTADO=//p' | tail -1
from airflow.dag_processing.dagbag import DagBag
dag = DagBag("/opt/airflow/dags").dags["shopbrasil_metricas"]
grupos = sorted(k for k, v in dag.task_group.children.items() if hasattr(v, "children"))
arestas = sorted(f"{u}>{t.task_id}" for t in dag.tasks for u in t.upstream_task_ids)
print("RESULTADO=" + ",".join(grupos) + "|" + " ".join(arestas))
PY
)"
verificar "P2 TaskGroups e dependências" \
  "analise,ingestao,preparacao,publicacao|analise.agrupar_por_categoria>analise.calcular_metricas analise.agrupar_por_categoria>analise.consolidar analise.calcular_metricas>analise.consolidar analise.consolidar>publicacao.publicar ingestao.buscar_produtos>ingestao.validar_produtos ingestao.validar_produtos>analise.agrupar_por_categoria preparacao.identificar>ingestao.buscar_produtos preparacao.identificar>publicacao.publicar" \
  "$estrutura"

# P3. Agenda: cron das 06:00 em Brasília, start_date com pendulum e catchup=False.
verificar "P3 agenda registrada" "0 6 * * *|America/Sao_Paulo" \
  "$(admin_sql airflow "SELECT timetable_summary || '|' || (SELECT data::json->'dag'->'timetable'->'__var'->>'timezone' FROM serialized_dag WHERE dag_id = '$DAG_ID' ORDER BY created_at DESC LIMIT 1) FROM dag WHERE dag_id = '$DAG_ID'" 2>/dev/null)"
config_dag="$(dc exec -T -e PYTHONPATH=/opt/airflow/pipeline:/opt/airflow/dags airflow-scheduler python - <<'PY' 2>/dev/null | sed -n 's/^RESULTADO=//p' | tail -1
from airflow.dag_processing.dagbag import DagBag
dag = DagBag("/opt/airflow/dags").dags["shopbrasil_metricas"]
print(f"RESULTADO={dag.start_date.isoformat()}|{dag.timezone.name}|catchup={dag.catchup}")
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
verificar "P4 tasks da run (4 categorias mapeadas)" "10|10" "$(admin_sql airflow "
SELECT count(*), count(*) FILTER (WHERE state = 'success') FROM task_instance WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_ID'")"
verificar "P4 run agendada real" "success" "$(aguardar_run "$DAG_ID" "$AGENDADA")"
verificar "P4 data de referência da run agendada" "$AGENDADA|$(admin_sql airflow "SELECT $ULTIMA_SQL")" "$(xcom_execucao "$AGENDADA")"

# P5. Reprocessamento: limpar as tasks da mesma run preserva run_id e data de referência.
airflow_cli tasks clear "$DAG_ID" -s "$OCORRENCIA" -e "$OCORRENCIA" -y >/dev/null 2>&1
sleep 3
verificar "P5 run reprocessada" "success" "$(aguardar_run "$DAG_ID" "$RUN_ID")"
verificar "P5 mesma run e data de referência original na 2ª tentativa" "$RUN_ID|2026-11-21" "$(xcom_execucao "$RUN_ID" 2)"
verificar "P5 uma run, sete tasks, segunda tentativa com sucesso nas 10 instâncias" "1|7|10" "$(admin_sql airflow "
SELECT (SELECT count(*) FROM dag_run WHERE dag_id = '$DAG_ID' AND logical_date = '$OCORRENCIA'),
       count(DISTINCT task_id), count(*) FILTER (WHERE try_number = 2 AND state = 'success')
FROM task_instance WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_ID'")"

# P6. Run sem data lógica é rejeitada em vez de usar o relógio. O botão Acionar da UI preenche a data
# lógica; `airflow dags trigger` sem --logical-date não. O cenário usa trigger_dag do Airflow 3.4.0
# (API interna, fixada pela #1) para criar a run sem data lógica de forma determinística.
SEM_DATA="e2e__sem_data_logica"
dc exec -T -e PYTHONPATH=/opt/airflow/pipeline:/opt/airflow/dags airflow-scheduler python - "$SEM_DATA" <<'PY' >"$LOG_DIR/sem-data.log" 2>&1
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

# ------------------------------------------------------------------------------- #13
BUSCAR="ingestao.buscar_produtos"

fakestore() {
  # fakestore <json de controle>: programa a fila de respostas e zera as requisições.
  dc exec -T fakestore python -c '
import sys, urllib.request
pedido = urllib.request.Request("http://localhost:8000/_controle", data=sys.argv[1].encode(), method="POST")
urllib.request.urlopen(pedido).read()' "$1" >/dev/null
}
intervalos_fakestore() {
  # "<requisições>;<segundos entre requisições consecutivas separados por |>"
  dc exec -T fakestore python -c '
import json, urllib.request
t = json.load(urllib.request.urlopen("http://localhost:8000/_controle"))["requisicoes"]
print(len(t), "|".join(f"{b - a:.0f}" for a, b in zip(t, t[1:])), sep=";")'
}
tentativas() {
  # tentativas <run_id> <task_id>: estados por tentativa, em ordem.
  admin_sql airflow "SELECT string_agg(try_number || ':' || state, ' ' ORDER BY try_number)
    FROM task_instance WHERE dag_id = '$DAG_ID' AND run_id = '$1' AND task_id = '$2'"
}
xcom_catalogo() {
  # xcom_catalogo <run_id> <expressão SQL sobre x.value>
  admin_sql airflow "SELECT $2 FROM xcom_v2 x JOIN task_instance ti ON ti.id = x.task_instance_id
    WHERE ti.dag_id = '$DAG_ID' AND ti.run_id = '$1' AND ti.task_id = '$BUSCAR' AND x.key = 'return_value'
    ORDER BY ti.try_number DESC LIMIT 1"
}

# I1. Configuração de produção, lida sem a variável de atraso reduzido do override E2E.
config_busca="$(dc exec -T -e PYTHONPATH=/opt/airflow/pipeline:/opt/airflow/dags airflow-scheduler env -u SHOPBRASIL_BUSCA_RETRY_DELAY_SEGUNDOS -u SHOPBRASIL_CALCULO_ATRASO_SEGUNDOS python -c '
from airflow.dag_processing.dagbag import DagBag
from shopbrasil_pipeline.composicao import fonte_catalogo
t = DagBag("/opt/airflow/dags").dags["shopbrasil_metricas"].get_task("ingestao.buscar_produtos")
import shopbrasil_metricas as m
print(f"RESULTADO={t.retries}|{t.retry_delay.total_seconds()}|{t.retry_exponential_backoff}|{fonte_catalogo().timeout}|avisos={len(m.avisar_ajustes_de_teste())}")' 2>/dev/null | sed -n 's/^RESULTADO=//p' | tail -1)"
verificar "I1 retries, atraso inicial, backoff, timeout de produção e nenhum ajuste de teste" "3|60.0|2.0|15|avisos=0" "$config_busca"

# I2. Coleta normal (run P4): uma tentativa, projeção com preços como strings decimais e XCom pequeno.
verificar "I2 coleta normal em uma tentativa" "1:success" "$(tentativas "$RUN_ID" "$BUSCAR" | awk '{print $1}')"
verificar "I2 projeção do catálogo" "array|6|695|109.95|men's clothing" "$(xcom_catalogo "$RUN_ID" "
  (x.value->>'tipo_raiz') || '|' || jsonb_array_length(x.value->'produtos') || '|' ||
  (x.value->'produtos'->2->>'preco') || '|' || (x.value->'produtos'->0->>'preco') || '|' || (x.value->'produtos'->0->>'categoria')")"
tamanho="$(xcom_catalogo "$RUN_ID" "octet_length(x.value::text)")"
if [[ "$tamanho" =~ ^[0-9]+$ && "$tamanho" -lt 4096 ]]; then registrar PASS "I2 XCom pequeno" "$tamanho bytes para 6 produtos"; else registrar FAIL "I2 XCom pequeno" "$tamanho"; fi

# I3. Falha transitória: HTTP 500 na primeira tentativa, sucesso na retentativa.
RUN_RETRY="e2e__retry_transitorio"
fakestore '{"respostas": [{"status": 500}]}'
airflow_cli dags trigger "$DAG_ID" --logical-date "2026-11-20T09:00:00+00:00" --run-id "$RUN_RETRY" >/dev/null 2>&1
verificar "I3 run com falha transitória" "success" "$(aguardar_run "$DAG_ID" "$RUN_RETRY")"
# No Airflow 3.4 cada tentativa encerrada fica na própria linha; a que levou à retentativa fica failed.
verificar "I3 tentativas de Buscar Produtos" "1:failed 2:success" "$(tentativas "$RUN_RETRY" "$BUSCAR")"
verificar "I3 requisições à FakeStore" "2" "$(intervalos_fakestore | cut -d';' -f1)"
verificar "I3 tasks seguintes executadas" "6" "$(admin_sql airflow "
SELECT count(DISTINCT task_id) FROM task_instance WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_RETRY' AND task_id <> '$BUSCAR' AND state = 'success'")"

# I4. Falha persistente: quatro respostas 500 esgotam a tentativa inicial e as três retentativas.
RUN_FALHA="e2e__falha_persistente"
ESTADO_ANTES_I4="$(estado_publicado)"
fakestore '{"respostas": [{"status": 500}, {"status": 500}, {"status": 500}, {"status": 500}]}'
airflow_cli dags trigger "$DAG_ID" --logical-date "2026-11-19T09:00:00+00:00" --run-id "$RUN_FALHA" >/dev/null 2>&1
verificar "I4 run com falha persistente" "failed" "$(aguardar_run "$DAG_ID" "$RUN_FALHA" 150)"
verificar "I4 tentativas de Buscar Produtos" "1:failed 2:failed 3:failed 4:failed" "$(tentativas "$RUN_FALHA" "$BUSCAR")"
verificar "I4 análise e publicação não executadas" "upstream_failed,upstream_failed" "$(admin_sql airflow "
SELECT string_agg(state, ',' ORDER BY task_id) FROM task_instance
WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_FALHA' AND task_id IN ('analise.consolidar', 'publicacao.publicar')")"
verificar "I4 snapshot, histórico e cadastro intactos (valores e xmin)" "$ESTADO_ANTES_I4" "$(estado_publicado)"
IFS=';' read -r requisicoes intervalos <<<"$(intervalos_fakestore)"
IFS='|' read -r d1 d2 d3 <<<"$intervalos"
# Com atraso inicial de 2 s e backoff exponencial, as esperas mínimas são 2, 4 e 8 s.
if [[ "$requisicoes" == 4 && "${d1:-0}" -ge 2 && "${d2:-0}" -ge 4 && "${d3:-0}" -ge 8 ]]; then
  registrar PASS "I4 backoff exponencial" "4 requisições; intervalos ${intervalos//|/ s, } s"
else
  registrar FAIL "I4 backoff exponencial" "requisições $requisicoes; intervalos $intervalos"
fi
log_falha="$(dc exec -T airflow-scheduler bash -c "grep -rh 'Falha ao buscar produtos da FakeStore na run $RUN_FALHA' /opt/airflow/logs/dag_id=$DAG_ID/run_id=$RUN_FALHA/ 2>/dev/null | wc -l" | tr -d ' ')"
if [[ "$log_falha" =~ ^[0-9]+$ && "$log_falha" -ge 4 ]]; then
  registrar PASS "I4 exceção registrada e relançada" "$log_falha registro(s) da falha, uma por tentativa"
else
  registrar FAIL "I4 exceção registrada e relançada" "registros: $log_falha"
fi

# ------------------------------------------------------------------------------- #14
contar_log() {
  # contar_log <run_id> <task_id> <tentativa|*> <texto>: linhas do log da task que contêm o texto.
  # O texto vai como argumento posicional para não ser interpretado pelo shell do contêiner.
  dc exec -T airflow-scheduler bash -c 'cat /opt/airflow/logs/dag_id='"$DAG_ID"'/run_id='"$1"'/task_id='"$2"'/attempt='"$3"'.log 2>/dev/null \
    | grep -F -- "$1" | wc -l' _ "$4" | tr -d ' '
}
contar_log_geral() {
  # contar_log_geral <texto>: linhas em todos os logs do Airflow, inclusive os do dag-processor.
  dc exec -T airflow-scheduler bash -c 'grep -rF -- "$1" /opt/airflow/logs 2>/dev/null | wc -l' _ "$1" | tr -d ' '
}

# C1. Sucesso de Buscar Produtos na run normal (P4), na própria tentativa.
verificar "C1 callback de sucesso" "1" "$(contar_log "$RUN_ID" "$BUSCAR" 1 "[SHOPBRASIL][SUCESSO] dag=$DAG_ID run=$RUN_ID task=$BUSCAR tentativa=1/4")"
# C6. Depois do reprocessamento (P5), o callback conta a tentativa do ciclo atual: a 2ª execução
# registrada (attempt=2.log) é a 1ª tentativa do novo ciclo, com 3 retentativas disponíveis.
verificar "C6 tentativa do ciclo atual após reprocessar" "1" "$(contar_log "$RUN_ID" "$BUSCAR" 2 "[SHOPBRASIL][SUCESSO] dag=$DAG_ID run=$RUN_ID task=$BUSCAR tentativa=1/4")"
# C7. Os ajustes de teste do override E2E ficam explícitos no log da coleta, nunca silenciosos.
verificar "C7 aviso dos ajustes de teste ativos" "1|1" \
  "$(contar_log "$RUN_ID" "$BUSCAR" 1 "[SHOPBRASIL][AVISO] ajuste de teste ativo: SHOPBRASIL_BUSCA_RETRY_DELAY_SEGUNDOS=2 (produção: 60)")|$(contar_log "$RUN_ID" "$BUSCAR" 1 "[SHOPBRASIL][AVISO] ajuste de teste ativo: SHOPBRASIL_CALCULO_ATRASO_SEGUNDOS=3 (produção: 0)")"

# C2. Retry na falha transitória (I3): aviso na 1ª tentativa e sucesso na 2ª.
verificar "C2 callback de retry" "1|1" "$(contar_log "$RUN_RETRY" "$BUSCAR" 1 "[SHOPBRASIL][RETRY] dag=$DAG_ID run=$RUN_RETRY task=$BUSCAR tentativa=1/4 erro=HTTPError")|$(contar_log "$RUN_RETRY" "$BUSCAR" 2 "[SHOPBRASIL][SUCESSO]")"
verificar "C2 sem aviso de falha nem alerta na recuperação" "0|0" "$(contar_log "$RUN_RETRY" "$BUSCAR" '*' "[SHOPBRASIL][FALHA]")|$(contar_log "$RUN_RETRY" "$BUSCAR" '*' "[ALERTA SIMULADO]")"

# C3. Falha definitiva (I4): três retries, falha na 4ª tentativa e um único alerta simulado.
retries_c3=0
for tentativa in 1 2 3; do
  retries_c3=$((retries_c3 + $(contar_log "$RUN_FALHA" "$BUSCAR" "$tentativa" "[SHOPBRASIL][RETRY] dag=$DAG_ID run=$RUN_FALHA task=$BUSCAR tentativa=$tentativa/4")))
done
verificar "C3 callbacks de retry nas três primeiras tentativas" "3" "$retries_c3"
verificar "C3 callback de falha na 4ª tentativa" "1" "$(contar_log "$RUN_FALHA" "$BUSCAR" 4 "[SHOPBRASIL][FALHA] dag=$DAG_ID run=$RUN_FALHA task=$BUSCAR tentativa=4/4")"
verificar "C3 alerta simulado na falha definitiva" "1|0" "$(contar_log "$RUN_FALHA" "$BUSCAR" 4 "[SHOPBRASIL][ALERTA SIMULADO] destino=")|$(contar_log "$RUN_FALHA" "$BUSCAR" '[123]' "[ALERTA SIMULADO]")"

# C5. Falha da run inteira: callback do DAG, executado pelo dag-processor.
falha_run=""
for _ in $(seq 1 20); do
  falha_run="$(contar_log_geral "[SHOPBRASIL][FALHA RUN] dag=$DAG_ID run=$RUN_FALHA")"
  [[ "$falha_run" =~ ^[1-9] ]] && break
  sleep 3
done
if [[ "$falha_run" =~ ^[1-9] ]]; then registrar PASS "C5 aviso de falha da run" "$falha_run linha(s) [FALHA RUN] para $RUN_FALHA"; else registrar FAIL "C5 aviso de falha da run" "linhas: $falha_run"; fi

# C4. Outra falha do pipeline (P6, sem data lógica): aviso identificável, sem alerta da ingestão.
verificar "C4 aviso em falha de outra task" "1|0" "$(contar_log "$SEM_DATA" "preparacao.identificar" '*' "[SHOPBRASIL][FALHA] dag=$DAG_ID run=$SEM_DATA task=preparacao.identificar")|$(contar_log "$SEM_DATA" "preparacao.identificar" '*' "[ALERTA SIMULADO]")"

# -------------------------------------------------------------------------------- #6
VALIDAR="ingestao.validar_produtos"

xcom_validados() {
  # xcom_validados <run_id>: categorias distintas e quantidade de produtos aprovados.
  admin_sql airflow "SELECT (SELECT string_agg(DISTINCT p->>'codigo_categoria', ',' ORDER BY p->>'codigo_categoria')
      FROM jsonb_array_elements(x.value->'produtos') p) || '|' || jsonb_array_length(x.value->'produtos')
    FROM xcom_v2 x JOIN task_instance ti ON ti.id = x.task_instance_id
    WHERE ti.dag_id = '$DAG_ID' AND ti.run_id = '$1' AND ti.task_id = '$VALIDAR' AND x.key = 'return_value'
    ORDER BY ti.try_number DESC LIMIT 1"
}

# V1. Catálogo válido sem uma categoria observada antes (jewelery): aceito, só o que veio segue.
verificar "V1 validação da coleta normal" "electronics,jewelery,men's clothing,women's clothing|6" "$(xcom_validados "$RUN_ID")"
RUN_AUSENTE="e2e__categoria_ausente"
fakestore '{"respostas": [{"catalogo": "sem_joias"}]}'
airflow_cli dags trigger "$DAG_ID" --logical-date "2026-11-18T09:00:00+00:00" --run-id "$RUN_AUSENTE" >/dev/null 2>&1
verificar "V1 run com categoria ausente" "success" "$(aguardar_run "$DAG_ID" "$RUN_AUSENTE")"
verificar "V1 categoria ausente fica fora" "electronics,men's clothing,women's clothing|3" "$(xcom_validados "$RUN_AUSENTE")"

# V2–V4. Catálogo vazio, preço inválido e categoria em branco: falha na validação, antes do cálculo.
validacao_rejeita() {
  # validacao_rejeita <cenário> <catálogo> <data lógica> <run_id> <motivo esperado no log>
  fakestore "{\"respostas\": [{\"catalogo\": \"$2\"}]}"
  airflow_cli dags trigger "$DAG_ID" --logical-date "$3" --run-id "$4" >/dev/null 2>&1
  verificar "$1 run" "failed" "$(aguardar_run "$DAG_ID" "$4")"
  verificar "$1 coleta ok, validação falha sem retentativa" "1:success|1:failed" "$(tentativas "$4" "$BUSCAR")|$(tentativas "$4" "$VALIDAR")"
  verificar "$1 cálculo e publicação não executados" "upstream_failed,upstream_failed" "$(admin_sql airflow "
SELECT string_agg(state, ',' ORDER BY task_id) FROM task_instance
WHERE dag_id = '$DAG_ID' AND run_id = '$4' AND task_id IN ('analise.consolidar', 'publicacao.publicar')")"
  verificar "$1 motivo e aviso no log" "1|1" "$(contar_log "$4" "$VALIDAR" 1 "$5")|$(contar_log "$4" "$VALIDAR" 1 "[SHOPBRASIL][FALHA] dag=$DAG_ID run=$4 task=$VALIDAR")"
}
# Retrato do banco antes das rejeições: cadastro, snapshot e histórico com valores e xmin.
ESTADO_ANTES_V="$(estado_publicado)"
validacao_rejeita "V2 catálogo vazio" vazio "2026-11-17T09:00:00+00:00" "e2e__catalogo_vazio" "Validação do catálogo: catálogo vazio"
validacao_rejeita "V3 preço inválido" preco_invalido "2026-11-16T09:00:00+00:00" "e2e__preco_invalido" "Validação do catálogo: produto 2: preço do tipo boolean"
validacao_rejeita "V4 categoria em branco" categoria_em_branco "2026-11-15T09:00:00+00:00" "e2e__categoria_em_branco" "Validação do catálogo: produto 3: categoria em branco"
verificar "V5 snapshot, histórico e cadastro intactos após V2–V4" "$ESTADO_ANTES_V" "$(estado_publicado)"

# ------------------------------------------------------------------------------- #16
CALCULAR="analise.calcular_metricas"
leitor_sql() { psql_como shopbrasil_reader "$READER_PASSWORD" shopbrasil -c "$1"; }
snapshot_do_dia() {
  # snapshot_do_dia <AAAA-MM-DD>: linhas do snapshot publicadas para a data, lidas pela conta da web.
  leitor_sql "SELECT string_agg(c.codigo || '=' || m.preco_medio || '/' || m.preco_minimo || '/' || m.preco_maximo || '/' || m.quantidade_produtos, ';' ORDER BY c.codigo)
    FROM metricas_categoria m JOIN categoria c ON c.id = m.categoria_id WHERE m.data_referencia = '$1'"
}
SNAPSHOT_PADRAO="electronics=86.50/64.00/109.00/2;jewelery=695.00/695.00/695.00/1;men's clothing=66.13/22.30/109.95/2;women's clothing=9.85/9.85/9.85/1"

# M1. Fan-out: uma instância mapeada por categoria observada, todas no ecommerce_pool.
verificar "M1 instâncias mapeadas e pool" "4|0,1,2,3|ecommerce_pool" "$(admin_sql airflow "
SELECT count(*) || '|' || string_agg(region_index::text, ',' ORDER BY region_index) || '|' || string_agg(DISTINCT pool, ',')
FROM task_instance WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_ID' AND task_id = '$CALCULAR' AND try_number = 1")"

# M2. Concorrência: com 3 s por instância, o pool de dois slots limita a duas simultâneas.
concorrencia() {
  admin_sql airflow "SELECT max(n) FROM (
    SELECT (SELECT count(*) FROM task_instance b WHERE b.dag_id = a.dag_id AND b.run_id = a.run_id AND b.task_id = a.task_id
             AND b.try_number = a.try_number AND b.start_date <= a.start_date AND b.end_date > a.start_date) AS n
    FROM task_instance a WHERE a.dag_id = '$DAG_ID' AND a.run_id = '$1' AND a.task_id = '$CALCULAR' AND a.try_number = 1) s"
}
verificar "M2 no máximo duas instâncias simultâneas" "2" "$(concorrencia "$RUN_ID")"

# M3. Fan-in: um resultado por categoria, no contrato da #15, com payloads pequenos no XCom.
xcom_task() {
  # xcom_task <run_id> <task_id> <expressão SQL sobre x.value>: última tentativa, região -1 (task não mapeada; no Airflow 3.4 a coluna é region_index).
  admin_sql airflow "SELECT $3 FROM xcom_v2 x JOIN task_instance ti ON ti.id = x.task_instance_id
    WHERE ti.dag_id = '$DAG_ID' AND ti.run_id = '$1' AND ti.task_id = '$2' AND ti.region_index = -1 AND x.key = 'return_value'
    ORDER BY ti.try_number DESC LIMIT 1"
}
verificar "M3 consolidação com um item por categoria" "4|electronics,jewelery,men's clothing,women's clothing|66.13" "$(xcom_task "$RUN_ID" analise.consolidar "
  jsonb_array_length(x.value) || '|' || (SELECT string_agg(e->>'codigo_categoria', ',' ORDER BY e->>'codigo_categoria') FROM jsonb_array_elements(x.value) e)
  || '|' || (SELECT e->>'preco_medio' FROM jsonb_array_elements(x.value) e WHERE e->>'codigo_categoria' = 'men''s clothing')")"
maiores="$(admin_sql airflow "SELECT ti.task_id || '=' || max(octet_length(x.value::text)) FROM xcom_v2 x JOIN task_instance ti ON ti.id = x.task_instance_id
  WHERE ti.dag_id = '$DAG_ID' AND ti.run_id = '$RUN_ID' GROUP BY ti.task_id ORDER BY 1" | tr '\n' ' ')"
maior="$(admin_sql airflow "SELECT max(octet_length(x.value::text)) FROM xcom_v2 x JOIN task_instance ti ON ti.id = x.task_instance_id
  WHERE ti.dag_id = '$DAG_ID' AND ti.run_id = '$RUN_ID'")"
if [[ "$maior" =~ ^[0-9]+$ && "$maior" -lt 4096 ]]; then registrar PASS "M3 XCom pequeno em todas as tasks" "máximo $maior bytes: $maiores"; else registrar FAIL "M3 XCom pequeno em todas as tasks" "$maiores"; fi

# M4. Publicação pelo DAG: snapshot do dia com os valores calculados, lido pela conta da web.
verificar "M4 snapshot publicado pelo DAG" "$SNAPSHOT_PADRAO" "$(snapshot_do_dia 2026-11-21)"
verificar "M4 categoria ausente fora do dia (V1)" "electronics=64.00/64.00/64.00/1;men's clothing=109.95/109.95/109.95/1;women's clothing=9.85/9.85/9.85/1" "$(snapshot_do_dia 2026-11-18)"

# M5. Categoria inédita: nova instância mapeada sem editar o DAG, cadastrada e publicada.
RUN_NOVA="e2e__categoria_nova"
fakestore '{"respostas": [{"catalogo": "com_nova"}]}'
airflow_cli dags trigger "$DAG_ID" --logical-date "2026-11-14T09:00:00+00:00" --run-id "$RUN_NOVA" >/dev/null 2>&1
verificar "M5 run com categoria nova" "success" "$(aguardar_run "$DAG_ID" "$RUN_NOVA")"
verificar "M5 cinco instâncias mapeadas e no máximo duas simultâneas" "5|2" "$(admin_sql airflow "
SELECT count(*) FROM task_instance WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_NOVA' AND task_id = '$CALCULAR'")|$(concorrencia "$RUN_NOVA")"
verificar "M5 categoria nova cadastrada e publicada" "toys|toys|67.76/15.50/120.01/2" "$(leitor_sql "SELECT codigo || '|' || descricao FROM categoria WHERE codigo = 'toys'")|$(leitor_sql "
SELECT m.preco_medio || '/' || m.preco_minimo || '/' || m.preco_maximo || '/' || m.quantidade_produtos
FROM metricas_categoria m JOIN categoria c ON c.id = m.categoria_id WHERE c.codigo = 'toys' AND m.data_referencia = '2026-11-14'")"

# M6. Falhas preservam o que já foi publicado: as runs recusadas (V2–V4) e a falha da FakeStore (I4)
# não gravaram snapshot, e o reprocessamento da run de 21/11 com catálogo vazio mantém o dia intacto.
verificar "M6 runs com falha sem snapshot" "0" "$(leitor_sql "SELECT count(*) FROM metricas_categoria WHERE data_referencia IN ('2026-11-19', '2026-11-17', '2026-11-16', '2026-11-15')")"
ESTADO_ANTES_M6="$(estado_publicado)"
fakestore '{"respostas": [{"catalogo": "vazio"}]}'
airflow_cli tasks clear "$DAG_ID" -s "$OCORRENCIA" -e "$OCORRENCIA" -y >/dev/null 2>&1
sleep 3
verificar "M6 reprocessamento com catálogo vazio falha" "failed" "$(aguardar_run "$DAG_ID" "$RUN_ID")"
verificar "M6 snapshot de 21/11 preservado" "$SNAPSHOT_PADRAO" "$(snapshot_do_dia 2026-11-21)"
verificar "M6 snapshot, histórico e cadastro intactos (valores e xmin)" "$ESTADO_ANTES_M6" "$(estado_publicado)"

resumo
