#!/usr/bin/env bash
# Autor: Christiano Henrique Carrilho Lopes da Silva
# Código de pessoa: 1691353
# Matrícula: 262987
# Cenário E2E da Web/API (#9). Sobe postgres, db-init e o serviço web construído de web/, grava
# dados de teste pela conta administrativa (fixtures/snapshot.sql) e confere as rotas JSON pela
# porta publicada em 127.0.0.1: data com métricas, última data, data sem registros, série com
# lacunas em ordem de data, categorias, erros 422/400/404, preços como strings decimais e que a
# conta da web só lê. Página do painel (#19): abre na última data, filtro por data, estado vazio
# e mesmos valores da API. Barras (#20): mesma coleção da tabela, estado vazio e Chart.js local.
# Linha (#21): filtros de categoria e intervalo, série igual à API, lacunas como null.
# G (#23): o painel operado num Chromium real, com os valores entregues ao Chart.js. O Airflow não sobe: a web depende só do esquema e da conta de leitura.
#
# Uso: e2e/web/executar.sh
# Variáveis opcionais: E2E_AIRFLOW_API_PORT (porta da web neste cenário; padrão 18110, ou a
# próxima livre) e E2E_MANTER=1 (não remove o projeto).

E2E_NOME=web
E2E_PORTA_PADRAO=18110
source "$(dirname "${BASH_SOURCE[0]}")/../lib/comum.sh"

# O helper escolhe uma porta livre para a API do Airflow; aqui o Airflow não sobe e a porta é da web.
WEB_PORT="$AIRFLOW_API_PORT_E2E"
echo "SHOPBRASIL_WEB_PORT=$WEB_PORT" >>"$ENV_FILE"
BASE="http://127.0.0.1:$WEB_PORT"

# json <rota>: corpo JSON canônico (chaves na ordem da resposta, sem espaços).
json() {
  curl -sS --max-time 10 "$BASE$1" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin), ensure_ascii=False, separators=(",", ":")))'
}
# pagina [query]: resumo da página do painel extraído por fixtures/pagina.py (#19).
pagina() { curl -sS --max-time 10 "$BASE/${1:-}" | python3 "$ROOT/e2e/web/fixtures/pagina.py"; }
# json_ordenado <rota>: JSON canônico com chaves ordenadas, como o filtro tojson do Jinja embute na página.
json_ordenado() {
  curl -sS --max-time 10 "$BASE$1" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin), ensure_ascii=False, separators=(",", ":"), sort_keys=True))'
}
codigo_http() { curl -sS -o /dev/null --max-time 10 -w '%{http_code}' "$BASE$1"; }
reader_sql() { psql_como shopbrasil_reader "$READER_PASSWORD" shopbrasil -c "$1"; }
contagens() {
  reader_sql "SELECT (SELECT count(*) FROM categoria) || '|' || (SELECT count(*) FROM metricas_categoria) || '|' || (SELECT count(*) FROM metricas_categoria_historico)"
}

echo "Projeto Compose isolado: $PROJECT (web em $BASE)"
if dc up -d --build --wait --wait-timeout 300 web >"$LOG_DIR/up.log" 2>&1; then
  registrar PASS "W0 subida de postgres, db-init e web" "up --build --wait concluído"
else
  registrar FAIL "W0 subida de postgres, db-init e web" "ver $LOG_DIR/up.log"
  dc logs --no-color db-init web | tail -50
  resumo
fi

verificar "W0 serviços sem o Airflow" "db-init postgres web " "$(dc ps -a --services | sort | tr '\n' ' ')"
verificar "W0 porta publicada só em 127.0.0.1" "127.0.0.1:$WEB_PORT" "$(dc port web 8000)"
verificar "W1 contêiner recebe só a conta de leitura" "SHOPBRASIL_DB_READER_USER=shopbrasil_reader|0" \
  "$(dc exec -T web env | grep '^SHOPBRASIL_DB_READER_USER=')|$(dc exec -T web env | grep -c -E 'WRITER|POSTGRES_PASSWORD|AIRFLOW')"

# W2. Banco vazio: última data explícita como null.
verificar "W2 última data sem publicação" '{"data":null}' "$(json /api/metricas/ultima-data)"
verificar "P1 painel sem publicação" "||Nenhuma métrica publicada até o momento||null" "$(pagina)"

admin_sql shopbrasil "$(cat "$ROOT/e2e/web/fixtures/snapshot.sql")" >"$LOG_DIR/fixture.log" 2>&1 \
  || { registrar FAIL "W3 fixture" "ver $LOG_DIR/fixture.log"; resumo; }
CONTAGENS="$(contagens)"
verificar "W3 fixture gravada pela conta administrativa" "3|4|1" "$CONTAGENS"

verificar "W3 métricas de 2026-11-22" \
  '{"data":"2026-11-22","vazia":false,"metricas":[{"categoria":{"codigo":"electronics","descricao":"Eletrônicos"},"preco_medio":"10.00","preco_minimo":"10.00","preco_maximo":"10.01","quantidade_produtos":3},{"categoria":{"codigo":"jewelery","descricao":"Joias"},"preco_medio":"10.01","preco_minimo":"10.00","preco_maximo":"10.01","quantidade_produtos":2}]}' \
  "$(json '/api/metricas?data=2026-11-22')"
verificar "W4 última data com métricas" '{"data":"2026-11-24"}' "$(json /api/metricas/ultima-data)"
verificar "W4 preços grandes sem float" \
  '{"data":"2026-11-24","vazia":false,"metricas":[{"categoria":{"codigo":"jewelery","descricao":"Joias"},"preco_medio":"1234567890.12","preco_minimo":"0.10","preco_maximo":"9999999999.99","quantidade_produtos":4}]}' \
  "$(json '/api/metricas?data=2026-11-24')"
verificar "W5 data sem registros" '{"data":"2026-11-21","vazia":true,"metricas":[]}' "$(json '/api/metricas?data=2026-11-21')"

verificar "W6 série com lacunas e em ordem (histórico de 21/11 ignorado)" \
  '{"categoria":{"codigo":"electronics","descricao":"Eletrônicos"},"inicio":"2026-11-19","fim":"2026-11-24","pontos":[{"data":"2026-11-19","preco_medio":null},{"data":"2026-11-20","preco_medio":"20.00"},{"data":"2026-11-21","preco_medio":null},{"data":"2026-11-22","preco_medio":"10.00"},{"data":"2026-11-23","preco_medio":null},{"data":"2026-11-24","preco_medio":null}]}' \
  "$(json '/api/evolucao?categoria=electronics&inicio=2026-11-19&fim=2026-11-24')"
verificar "W6 intervalo inclusivo de um dia" \
  '{"categoria":{"codigo":"jewelery","descricao":"Joias"},"inicio":"2026-11-24","fim":"2026-11-24","pontos":[{"data":"2026-11-24","preco_medio":"1234567890.12"}]}' \
  "$(json '/api/evolucao?categoria=jewelery&inicio=2026-11-24&fim=2026-11-24')"
verificar "W6 código com apóstrofo e sem métricas: só lacunas" \
  '{"categoria":{"codigo":"men'"'"'s clothing","descricao":"Moda masculina"},"inicio":"2026-11-20","fim":"2026-11-21","pontos":[{"data":"2026-11-20","preco_medio":null},{"data":"2026-11-21","preco_medio":null}]}' \
  "$(json "/api/evolucao?categoria=men%27s%20clothing&inicio=2026-11-20&fim=2026-11-21")"
verificar "W7 categorias em ordem de código" \
  '{"categorias":[{"codigo":"electronics","descricao":"Eletrônicos"},{"codigo":"jewelery","descricao":"Joias"},{"codigo":"men'"'"'s clothing","descricao":"Moda masculina"}]}' \
  "$(json /api/categorias)"

verificar "W8 erros: data inválida, sem data, intervalo invertido, categoria desconhecida" "422 422 400 404" \
  "$(codigo_http '/api/metricas?data=2026-02-30') $(codigo_http /api/metricas) $(codigo_http '/api/evolucao?categoria=electronics&inicio=2026-11-24&fim=2026-11-20') $(codigo_http '/api/evolucao?categoria=toys&inicio=2026-11-20&fim=2026-11-24')"
verificar "W8 mensagem do 422" '["query","data"]|data inválida '"'"'2026-02-30'"'"'; use AAAA-MM-DD' \
  "$(curl -sS "$BASE/api/metricas?data=2026-02-30" | python3 -c 'import json,sys; d=json.load(sys.stdin)["detail"][0]; print(json.dumps(d["loc"], separators=(",", ":")) + "|" + d["msg"])')"
verificar "W8 OpenAPI documenta as rotas" "/api/categorias /api/evolucao /api/metricas /api/metricas/ultima-data" \
  "$(curl -sS "$BASE/openapi.json" | python3 -c 'import json,sys; print(" ".join(sorted(json.load(sys.stdin)["paths"])))')"

# W9. A conta da web não escreve nem cria objetos; os dados ficam intactos.
verificar "W9 conta da web só lê" \
  "shopbrasil_reader|InsufficientPrivilege,InsufficientPrivilege,InsufficientPrivilege,InsufficientPrivilege,InsufficientPrivilege" \
  "$(dc exec -T web python - <"$ROOT/e2e/web/fixtures/escrever.py" 2>>"$LOG_DIR/escrever.err" | tail -1)"
verificar "W9 dados intactos após as tentativas" "$CONTAGENS" "$(contagens)"
verificar "W9 repositório abre transação somente leitura" "shopbrasil_reader|on" \
  "$(dc exec -T web python - <<'PY' 2>>"$LOG_DIR/escrever.err" | tail -1
from shopbrasil_web.composicao import configuracao_do_ambiente
from shopbrasil_web.infraestrutura.postgres import RepositorioMetricasPostgres

with RepositorioMetricasPostgres(configuracao_do_ambiente())._cursor() as cursor:
    cursor.execute("SELECT current_user, current_setting('transaction_read_only')")
    print("|".join(cursor.fetchone()))
PY
)"

# P. Página do painel (#19): abre na última data, filtro por data, estado vazio, mesmos valores da API.
verificar "P2 painel abre na data mais recente" \
  "2026-11-24|2026-11-24||Joias/1234567890.12/0.10/9999999999.99/4|$(json_ordenado '/api/metricas?data=2026-11-24')" "$(pagina)"
verificar "P3 selecionar data carrega o dia" \
  "2026-11-22|2026-11-22||Eletrônicos/10.00/10.00/10.01/3;Joias/10.01/10.00/10.01/2|$(json_ordenado '/api/metricas?data=2026-11-22')" \
  "$(pagina '?data=2026-11-22')"
verificar "P4 data sem registros" \
  '2026-11-21|2026-11-21|Sem métricas para esta data||{"data":"2026-11-21","metricas":[],"vazia":true}' \
  "$(pagina '?data=2026-11-21')"
verificar "P5 data inválida na página" "422" "$(codigo_http '/?data=2026-02-30')"

# B. Barras do preço médio (#20): canvas alimentado pela coleção embutida (já conferida em P3), estado
# vazio no dia sem métricas e Chart.js 4.6.1 servido pela própria web, sem CDN.
verificar "B1 barras no dia com métricas" "1|0" \
  "$(curl -sS "$BASE/?data=2026-11-22" | grep -c 'id="grafico-barras"')|$(curl -sS "$BASE/?data=2026-11-22" | grep -c 'id="barras-vazio"')"
verificar "B2 barras em dia sem métricas" "0|1" \
  "$(curl -sS "$BASE/?data=2026-11-21" | grep -c 'id="grafico-barras"')|$(curl -sS "$BASE/?data=2026-11-21" | grep -c 'id="barras-vazio">Sem métricas para esta data<')"
verificar "B3 Chart.js local" "200|1" \
  "$(codigo_http /estaticos/chart.umd.min.js)|$(curl -sS "$BASE/estaticos/chart.umd.min.js" | head -c 200 | grep -c 'Chart.js v4.6.1')"

# E. Linha de evolução (#21): série embutida na página igual a /api/evolucao, lacunas como null.
evolucao_da_pagina() {
  curl -sS "$BASE/$1" | python3 -c 'import html,json,re,sys; p=sys.stdin.read(); m=re.search(r"<script type=\"application/json\" id=\"evolucao-dados\">(.*?)</script>", p, re.S); a=re.search(r"id=\"evolucao-aviso\">(.*?)<", p); print(json.dumps(json.loads(m.group(1)), ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "|" + (html.unescape(a.group(1)) if a else ""))'
}
FILTRO_E1='categoria=electronics&inicio=2026-11-20&fim=2026-11-23'
verificar "E1 série da página igual à API, com lacunas" "$(json_ordenado "/api/evolucao?$FILTRO_E1")|" "$(evolucao_da_pagina "?$FILTRO_E1")"
verificar "E1 pontos e lacunas" "2026-11-20=20.00 2026-11-21=None 2026-11-22=10.00 2026-11-23=None" \
  "$(json "/api/evolucao?$FILTRO_E1" | python3 -c 'import json,sys; print(" ".join("%s=%s" % (p["data"], p["preco_medio"]) for p in json.load(sys.stdin)["pontos"]))')"
verificar "E2 padrão: primeira categoria, 30 dias até a data" "electronics|2026-10-25|2026-11-23|30" \
  "$(evolucao_da_pagina '?data=2026-11-23' | cut -d'|' -f1 | python3 -c 'import json,sys; s=json.load(sys.stdin); print(s["categoria"]["codigo"], s["inicio"], s["fim"], len(s["pontos"]), sep="|")')"
verificar "E3 categoria com espaço e apóstrofo" "men's clothing|2026-11-20|2026-11-24" \
  "$(evolucao_da_pagina "?categoria=men%27s%20clothing&inicio=2026-11-20&fim=2026-11-24" | cut -d'|' -f1 | python3 -c 'import json,sys; s=json.load(sys.stdin); print(s["categoria"]["codigo"], s["inicio"], s["fim"], sep="|")')"
verificar "E4 intervalo invertido vira aviso" "null|Intervalo inválido: início 2026-11-23 depois do fim 2026-11-20" \
  "$(evolucao_da_pagina '?categoria=electronics&inicio=2026-11-23&fim=2026-11-20')"
verificar "E4 categoria desconhecida vira aviso" "null|Categoria 'toys' não cadastrada" "$(evolucao_da_pagina '?categoria=toys')"

# G. Navegador real (#23): Chart.js desenha as barras com os preços da tabela; a troca de data no
# campo leva ao estado vazio; os filtros da evolução desenham a linha com lacunas (null, spanGaps false).
verificar "G1 painel no Chromium: barras, troca de data, linha e console" \
  "4.6.1|10.00,10.01|10,10.01@2|data=2026-11-21;Sem métricas para esta data;Sem métricas para esta data;False|20,None,10,None@4|spanGaps=False|erros=0" \
  "$(navegador_painel 2026-11-22 2026-11-21 electronics 2026-11-20 2026-11-23)"

resumo
