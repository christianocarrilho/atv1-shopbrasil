#!/usr/bin/env bash
# Autor: Christiano Henrique Carrilho Lopes da Silva
# Código de pessoa: 1691353
# Matrícula: 262987
# Cenário E2E integrado (#23): Airflow → PostgreSQL → Web/API → painel no mesmo projeto Compose.
# O DAG real coleta da FakeStore controlada e publica três dias: 10/11 com o catálogo padrão,
# 11/11 com uma categoria inédita (toys) e 12/11 sem jewelery. As datas ficam depois do start_date
# do DAG (01/11; antes dele a run não executa tasks) e longe da ocorrência mais recente, que o
# Airflow agenda sozinho ao despausar o DAG. Depois confere, para cada dia, que
# snapshot, histórico da run, API e tabela do painel mostram os mesmos valores; que a categoria
# nova foi cadastrada e a ausente continua no cadastro; que a evolução tem lacunas (null) nos dias
# sem a categoria; e que reprocessar a run de 11/11 mantém a data de referência e não duplica
# data+categoria nem run+categoria. Os cenários isolados por módulo ficam em e2e/<módulo>/.
#
# Uso: e2e/integrado/executar.sh
# Variáveis opcionais: E2E_AIRFLOW_API_PORT (padrão 18120), E2E_WEB_PORT (padrão: próxima livre) e
# E2E_MANTER=1 (não remove o projeto).

E2E_NOME=integrado
E2E_PORTA_PADRAO=18120
E2E_COMPOSE_EXTRA="e2e/pipeline/compose.e2e.yml"
E2E_FAKESTORE_BASE_URL="http://fakestore:8000"
source "$(dirname "${BASH_SOURCE[0]}")/../lib/comum.sh"

WEB_PORT="${E2E_WEB_PORT:-$(porta_livre $((AIRFLOW_API_PORT_E2E + 1)))}"
echo "SHOPBRASIL_WEB_PORT=$WEB_PORT" >>"$ENV_FILE"
BASE="http://127.0.0.1:$WEB_PORT"
DAG_ID="shopbrasil_metricas"

leitor_sql() { psql_como shopbrasil_reader "$READER_PASSWORD" shopbrasil -c "$1"; }
fakestore() {
  dc exec -T fakestore python -c '
import sys, urllib.request
pedido = urllib.request.Request("http://localhost:8000/_controle", data=sys.argv[1].encode(), method="POST")
urllib.request.urlopen(pedido).read()' "$1" >/dev/null
}
json() {
  curl -sS --max-time 10 "$BASE$1" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin), ensure_ascii=False, separators=(",", ":")))'
}
# Formatos comuns "codigo=médio/mínimo/máximo/quantidade;..." em ordem de código.
snapshot_do_dia() {
  leitor_sql "SELECT coalesce(string_agg(c.codigo || '=' || m.preco_medio || '/' || m.preco_minimo || '/' || m.preco_maximo || '/' || m.quantidade_produtos, ';' ORDER BY c.codigo), '')
    FROM metricas_categoria m JOIN categoria c ON c.id = m.categoria_id WHERE m.data_referencia = '$1'"
}
historico_da_run() {
  leitor_sql "SELECT coalesce(string_agg(c.codigo || '=' || h.preco_medio || '/' || h.preco_minimo || '/' || h.preco_maximo || '/' || h.quantidade_produtos, ';' ORDER BY c.codigo), '')
    FROM metricas_categoria_historico h JOIN categoria c ON c.id = h.categoria_id WHERE h.run_id = '$1'"
}
api_do_dia() {
  json "/api/metricas?data=$1" | python3 -c 'import json,sys; print(";".join("%s=%s/%s/%s/%s" % (m["categoria"]["codigo"], m["preco_medio"], m["preco_minimo"], m["preco_maximo"], m["quantidade_produtos"]) for m in json.load(sys.stdin)["metricas"]))'
}
painel_do_dia() {
  # Tabela do painel no mesmo formato; a descrição publicada pelo DAG é o próprio código.
  curl -sS --max-time 10 "$BASE/?data=$1" | python3 "$ROOT/e2e/web/fixtures/pagina.py" | cut -d'|' -f4 \
    | python3 -c 'import sys; l=sys.stdin.read().strip(); print(";".join(r.split("/",1)[0] + "=" + r.split("/",1)[1] for r in l.split(";")) if l else "")'
}
evolucao() {
  # evolucao <código URL> <início> <fim>: "data=preço" por dia, None nos dias sem observação.
  json "/api/evolucao?categoria=$1&inicio=$2&fim=$3" | python3 -c 'import json,sys; print(" ".join("%s=%s" % (p["data"], p["preco_medio"]) for p in json.load(sys.stdin)["pontos"]))'
}
publicar_dia() {
  # publicar_dia <rótulo> <catálogo> <ocorrência UTC> <run_id>
  fakestore "{\"respostas\": [{\"catalogo\": \"$2\"}]}"
  airflow_cli dags trigger "$DAG_ID" --logical-date "$3" --run-id "$4" >/dev/null 2>&1
  verificar "$1 run $4" "success" "$(aguardar_run "$DAG_ID" "$4")"
  # Uma run fora da janela do DAG termina success sem executar tasks; exige a publicação executada.
  verificar "$1 publicação executada em $4" "success" "$(admin_sql airflow "SELECT state FROM task_instance
    WHERE dag_id = '$DAG_ID' AND run_id = '$4' AND task_id = 'publicacao.publicar' ORDER BY try_number DESC LIMIT 1")"
}

PADRAO="electronics=86.50/64.00/109.00/2;jewelery=695.00/695.00/695.00/1;men's clothing=66.13/22.30/109.95/2;women's clothing=9.85/9.85/9.85/1"
COM_NOVA="electronics=86.50/64.00/109.00/2;jewelery=695.00/695.00/695.00/1;men's clothing=66.13/22.30/109.95/2;toys=67.76/15.50/120.01/2;women's clothing=9.85/9.85/9.85/1"
SEM_JOIAS="electronics=64.00/64.00/64.00/1;men's clothing=109.95/109.95/109.95/1;women's clothing=9.85/9.85/9.85/1"

echo "Projeto Compose isolado: $PROJECT (Airflow em 127.0.0.1:$AIRFLOW_API_PORT_E2E, web em $BASE)"
subir_airflow "N0 subida do Airflow"
if dc up -d --build --wait --wait-timeout 300 web >"$LOG_DIR/up-web.log" 2>&1; then
  registrar PASS "N0 subida da web" "up --build --wait concluído"
else
  registrar FAIL "N0 subida da web" "ver $LOG_DIR/up-web.log"; resumo
fi
if aguardar_dag "$DAG_ID"; then registrar PASS "N0 DAG registrado" "$DAG_ID"; else registrar FAIL "N0 DAG registrado" "ausente"; resumo; fi
verificar "N0 painel antes da primeira publicação" "null" "$(json /api/metricas/ultima-data | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["data"]))')"
# DAG entra pausado; ao despausar, o Airflow (catchup=False) cria a run da ocorrência mais recente.
# Ela usa o catálogo padrão e termina antes das runs do cenário, para não concorrer com elas.
airflow_cli dags unpause "$DAG_ID" >/dev/null 2>&1
AGENDADA=""
for _ in $(seq 1 40); do
  AGENDADA="$(admin_sql airflow "SELECT run_id FROM dag_run WHERE dag_id = '$DAG_ID' AND run_type = 'scheduled' LIMIT 1")"
  [[ -n "$AGENDADA" ]] && break
  sleep 3
done
verificar "N0 run agendada ao despausar" "success" "$(aguardar_run "$DAG_ID" "$AGENDADA")"
DATA_AGENDADA="$(json /api/metricas/ultima-data | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"])')"

# N1. Três coletas válidas publicadas pelo DAG (ocorrências das 06:00 de Brasília).
RUN_20="e2e__integrado_2026-11-10"; RUN_21="e2e__integrado_2026-11-11"; RUN_22="e2e__integrado_2026-11-12"
publicar_dia "N1" padrao "2026-11-10T09:00:00+00:00" "$RUN_20"
publicar_dia "N1" com_nova "2026-11-11T09:00:00+00:00" "$RUN_21"
publicar_dia "N1" sem_joias "2026-11-12T09:00:00+00:00" "$RUN_22"

# N2. Mesmos valores no snapshot, no histórico da run, na API e na tabela do painel.
for caso in "2026-11-10|$RUN_20|$PADRAO" "2026-11-11|$RUN_21|$COM_NOVA" "2026-11-12|$RUN_22|$SEM_JOIAS"; do
  IFS='|' read -r dia run esperado <<<"$caso"
  verificar "N2 $dia snapshot|histórico|API|painel" "$esperado|$esperado|$esperado|$esperado" \
    "$(snapshot_do_dia "$dia")|$(historico_da_run "$run")|$(api_do_dia "$dia")|$(painel_do_dia "$dia")"
done
verificar "N2 painel abre na última data publicada" "$DATA_AGENDADA|$DATA_AGENDADA" \
  "$(curl -sS "$BASE/" | python3 "$ROOT/e2e/web/fixtures/pagina.py" | cut -d'|' -f1,2)"

# N3. Categoria inédita cadastrada sem editar o DAG; categoria ausente continua no cadastro.
verificar "N3 cadastro com toys e jewelery" "electronics,jewelery,men's clothing,toys,women's clothing" \
  "$(json /api/categorias | python3 -c 'import json,sys; print(",".join(c["codigo"] for c in json.load(sys.stdin)["categorias"]))')"
verificar "N3 presença por data" "10:4 11:5 12:3" \
  "$(leitor_sql "SELECT string_agg(to_char(data_referencia, 'DD') || ':' || n, ' ' ORDER BY data_referencia) FROM (SELECT data_referencia, count(*) n FROM metricas_categoria WHERE data_referencia BETWEEN '2026-11-10' AND '2026-11-12' GROUP BY 1) t")"

# N4. Evolução com lacunas: null (não zero) nos dias em que a categoria não foi publicada.
verificar "N4 evolução de jewelery" "2026-11-10=695.00 2026-11-11=695.00 2026-11-12=None" "$(evolucao jewelery 2026-11-10 2026-11-12)"
verificar "N4 evolução de toys" "2026-11-10=None 2026-11-11=67.76 2026-11-12=None" "$(evolucao toys 2026-11-10 2026-11-12)"
# Jinja ordena as chaves no tojson; a comparação usa JSON canônico ordenado dos dois lados.
verificar "N4 série do painel igual à API" "$(curl -sS "$BASE/api/evolucao?categoria=toys&inicio=2026-11-10&fim=2026-11-12" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin), ensure_ascii=False, separators=(",", ":"), sort_keys=True))')" \
  "$(curl -sS "$BASE/?categoria=toys&inicio=2026-11-10&fim=2026-11-12" | python3 -c 'import json,re,sys; p=sys.stdin.read(); print(json.dumps(json.loads(re.search(r"id=\"evolucao-dados\">(.*?)</script>", p, re.S).group(1)), ensure_ascii=False, separators=(",", ":"), sort_keys=True))')"

# N5. Reprocessar a run de 11/11: mesma data de referência, sem duplicatas, valores estáveis.
ANTES="$(snapshot_do_dia 2026-11-11)|$(historico_da_run "$RUN_21")"
fakestore '{"respostas": [{"catalogo": "com_nova"}]}'
tentativa_publicar() {
  admin_sql airflow "SELECT coalesce(max(try_number), 0) || ':' || coalesce((SELECT state FROM task_instance
      WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_21' AND task_id = 'publicacao.publicar' ORDER BY try_number DESC LIMIT 1), '')
    FROM task_instance WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_21' AND task_id = 'publicacao.publicar'"
}
TENTATIVA_ANTES="$(tentativa_publicar)"
if airflow_cli tasks clear "$DAG_ID" -s "2026-11-11T09:00:00+00:00" -e "2026-11-11T09:00:00+00:00" -y >"$LOG_DIR/clear.log" 2>&1; then
  registrar PASS "N5 tasks da run limpas" "airflow tasks clear concluído"
else
  registrar FAIL "N5 tasks da run limpas" "ver $LOG_DIR/clear.log"
fi
# A run volta a executar: espera sair de success antes de aguardar o novo estado final.
for _ in $(seq 1 20); do
  [[ "$(admin_sql airflow "SELECT state FROM dag_run WHERE dag_id = '$DAG_ID' AND run_id = '$RUN_21'")" != success ]] && break
  sleep 1
done
verificar "N5 run reprocessada" "success" "$(aguardar_run "$DAG_ID" "$RUN_21")"
# Exige uma nova tentativa bem-sucedida da publicação, para não aprovar um reprocessamento que não ocorreu.
verificar "N5 nova tentativa da publicação" "$(( ${TENTATIVA_ANTES%%:*} + 1 )):success" "$(tentativa_publicar)"
verificar "N5 data de referência estável" "$RUN_21|2026-11-11" "$(admin_sql airflow "SELECT x.value->>'run_id' || '|' || (x.value->>'data_referencia')
  FROM xcom_v2 x JOIN task_instance ti ON ti.id = x.task_instance_id
  WHERE ti.dag_id = '$DAG_ID' AND ti.run_id = '$RUN_21' AND ti.task_id = 'preparacao.identificar' AND x.key = 'return_value'
  ORDER BY ti.try_number DESC LIMIT 1")"
verificar "N5 snapshot e histórico iguais aos de antes" "$ANTES" "$(snapshot_do_dia 2026-11-11)|$(historico_da_run "$RUN_21")"
verificar "N5 sem duplicatas por data+categoria e run+categoria" "1|1" \
  "$(leitor_sql "SELECT max(n) FROM (SELECT count(*) n FROM metricas_categoria GROUP BY data_referencia, categoria_id) t")|$(leitor_sql "SELECT max(n) FROM (SELECT count(*) n FROM metricas_categoria_historico GROUP BY run_id, categoria_id) t")"

# N6. Falha definitiva da FakeStore numa nova ocorrência preserva tudo o que já foi publicado.
ESTADO_ANTES_N6="$(estado_publicado)"
fakestore '{"respostas": [{"status": 500}, {"status": 500}, {"status": 500}, {"status": 500}]}'
airflow_cli dags trigger "$DAG_ID" --logical-date "2026-11-13T09:00:00+00:00" --run-id "e2e__integrado_falha" >/dev/null 2>&1
verificar "N6 run com falha definitiva" "failed" "$(aguardar_run "$DAG_ID" "e2e__integrado_falha" 150)"
verificar "N6 dados (valores e xmin) e painel preservados" "$ESTADO_ANTES_N6|$DATA_AGENDADA" \
  "$(estado_publicado)|$(json /api/metricas/ultima-data | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"])')"

# N7. Painel num Chromium real sobre os dados publicados pelo DAG: barras de 11/11 iguais à tabela
# (com toys), troca de data para um dia sem publicação e linha de toys com lacunas nos outros dias.
verificar "N7 painel no Chromium com dados do DAG" \
  "4.6.1|86.50,695.00,66.13,67.76,9.85|86.5,695,66.13,67.76,9.85@5|data=2026-11-09;Sem métricas para esta data;Sem métricas para esta data;False|None,None,67.76,None,None@5|spanGaps=False|erros=0" \
  "$(navegador_painel 2026-11-11 2026-11-09 toys 2026-11-09 2026-11-13)"

resumo
