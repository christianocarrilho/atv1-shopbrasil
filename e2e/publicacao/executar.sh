#!/usr/bin/env bash
# Autor: Christiano Henrique Carrilho Lopes da Silva
# Código de pessoa: 1691353
# Matrícula: 262987
# Cenário E2E da publicação transacional do snapshot diário (#17). Executa o caso de uso da
# composição e o adaptador PostgresHook dentro do contêiner do Airflow, com a Connection
# shopbrasil_postgres (conta de escrita), o PostgreSQL 18.7 e o esquema da #3 reais.
# Cobre cadastro de categoria nova, republicação da mesma data com uma linha por data+categoria,
# categoria ausente fora do dia com cadastro preservado, outra data intacta, rollback por falha
# injetada no ponto de extensão da #18 e por erro do banco, e valores lidos iguais aos publicados.
# O DAG só passa a acionar a publicação na #16; por isso o cenário chama o adaptador diretamente.
#
# Uso: e2e/publicacao/executar.sh
# Variáveis opcionais: E2E_AIRFLOW_API_PORT (padrão 18100) e E2E_MANTER=1 (não remove o projeto).

E2E_NOME=publicacao
E2E_PORTA_PADRAO=18100
source "$(dirname "${BASH_SOURCE[0]}")/../lib/comum.sh"

FIXTURE="$ROOT/e2e/publicacao/fixtures/publicar.py"
D0="2026-11-20"
D1="2026-11-21"
R0="scheduled__2026-11-20T09:00:00+00:00"
R1="scheduled__2026-11-21T09:00:00+00:00"

# publicar <modo> <run_id> <data> <métricas JSON>: imprime a última linha (JSON) do driver.
publicar() {
  dc exec -T airflow-scheduler python - "$@" <"$FIXTURE" 2>>"$LOG_DIR/driver.err" | tail -1
}
# m <código> <médio> <mínimo> <máximo> <quantidade>: um item do contrato da #15.
m() {
  printf '{"codigo_categoria":"%s","preco_medio":"%s","preco_minimo":"%s","preco_maximo":"%s","quantidade_produtos":%s}' "$@"
}
lista() { local IFS=,; echo "[$*]"; }
campo() { python3 -c 'import json,sys; v=json.loads(sys.stdin.read() or "{}").get(sys.argv[1], ""); print(",".join(v) if isinstance(v, list) else v)' "$1"; }

writer_sql() { psql_como shopbrasil_writer "$WRITER_PASSWORD" shopbrasil -c "$1"; }
reader_sql() { psql_como shopbrasil_reader "$READER_PASSWORD" shopbrasil -c "$1"; }

# snapshot <data>: categorias e métricas do dia, em ordem de código.
snapshot() {
  reader_sql "SELECT coalesce(string_agg(c.codigo || '=' || m.preco_medio || '/' || m.preco_minimo || '/'
      || m.preco_maximo || '/' || m.quantidade_produtos, ' ' ORDER BY c.codigo), '')
    FROM metricas_categoria m JOIN categoria c ON c.id = m.categoria_id WHERE m.data_referencia = '$1'"
}
# linhas_fisicas <data>: id e xmin de cada linha; muda se a linha for apagada, reinserida ou atualizada.
linhas_fisicas() {
  writer_sql "SELECT coalesce(string_agg(id || '@' || xmin, ' ' ORDER BY id), '') FROM metricas_categoria WHERE data_referencia = '$1'"
}
cadastro() { reader_sql "SELECT string_agg(codigo || '=' || descricao, ' ' ORDER BY id) FROM categoria"; }
max_por_chave() {
  reader_sql "SELECT coalesce(max(n), 0) FROM (SELECT count(*) AS n FROM metricas_categoria GROUP BY data_referencia, categoria_id) t"
}

echo "Projeto Compose isolado: $PROJECT"
subir_airflow "U0 subida do Airflow e do PostgreSQL"

verificar "U1 Connection usa a conta de escrita" "shopbrasil_writer" "$(publicar conta - "$D0" - | campo conta)"
verificar "U1 banco analítico vazio" "0|0" "$(reader_sql "SELECT (SELECT count(*) FROM categoria) || '|' || (SELECT count(*) FROM metricas_categoria)")"

# U2. Primeira publicação, de outra data: cadastra as duas categorias com descricao = código.
M0="$(lista "$(m electronics 20.00 15.00 25.00 2)" "$(m "men's clothing" 30.00 30.00 30.00 1)")"
saida="$(publicar publicar "$R0" "$D0" "$M0")"
verificar "U2 publicação de $D0 cadastra categorias novas" "electronics,men's clothing" "$(campo categorias_cadastradas <<<"$saida")"
verificar "U2 cadastro com descricao igual ao código" "electronics=electronics men's clothing=men's clothing" "$(cadastro)"
SNAP_D0="$(snapshot "$D0")"
FIS_D0="$(linhas_fisicas "$D0")"
verificar "U2 snapshot de $D0" "electronics=20.00/15.00/25.00/2 men's clothing=30.00/30.00/30.00/1" "$SNAP_D0"

# U3. Publicação inicial de D1 com uma categoria nova e outra já cadastrada.
M1="$(lista "$(m electronics 10.00 10.00 10.01 3)" "$(m jewelery 10.01 10.00 10.01 2)")"
saida="$(publicar publicar "$R1" "$D1" "$M1")"
verificar "U3 resultado da publicação de $D1" "$R1|$D1|electronics,jewelery|jewelery" \
  "$(campo run_id <<<"$saida")|$(campo data_referencia <<<"$saida")|$(campo categorias_publicadas <<<"$saida")|$(campo categorias_cadastradas <<<"$saida")"
verificar "U3 snapshot de $D1" "electronics=10.00/10.00/10.01/3 jewelery=10.01/10.00/10.01/2" "$(snapshot "$D1")"

# U4. Valores lidos pela conta de leitura iguais aos publicados, com duas casas.
verificar "U4 valores lidos iguais aos publicados" "electronics|10.00|10.00|10.01;jewelery|10.01|10.00|10.01" \
  "$(reader_sql "SELECT string_agg(c.codigo || '|' || m.preco_medio::text || '|' || m.preco_minimo::text || '|' || m.preco_maximo::text, ';' ORDER BY c.codigo)
    FROM metricas_categoria m JOIN categoria c ON c.id = m.categoria_id WHERE m.data_referencia = '$D1'")"

# U5. Republicação da mesma data (reprocessamento da mesma run): jewelery fica fora do dia,
# women's clothing entra e electronics é substituída.
M1B="$(lista "$(m electronics 12.00 11.00 13.00 4)" "$(m "women's clothing" 50.50 40.00 61.00 3)")"
saida="$(publicar publicar "$R1" "$D1" "$M1B")"
verificar "U5 republicação cadastra só a categoria nova" "women's clothing" "$(campo categorias_cadastradas <<<"$saida")"
verificar "U5 snapshot de $D1 substituído" "electronics=12.00/11.00/13.00/4 women's clothing=50.50/40.00/61.00/3" "$(snapshot "$D1")"
verificar "U5 categoria ausente preservada no cadastro" "electronics men's clothing jewelery women's clothing" \
  "$(reader_sql "SELECT string_agg(codigo, ' ' ORDER BY id) FROM categoria")"
verificar "U5 uma linha por data+categoria" "1" "$(max_por_chave)"

# U6. Repetir a mesma publicação não duplica linhas.
publicar publicar "$R1" "$D1" "$M1B" >/dev/null
verificar "U6 republicação idêntica: 2 linhas no dia, máximo 1 por chave" "2|1" \
  "$(reader_sql "SELECT count(*) FROM metricas_categoria WHERE data_referencia = '$D1'")|$(max_por_chave)"
SNAP_D1="$(snapshot "$D1")"
FIS_D1="$(linhas_fisicas "$D1")"

# U7. A outra data não foi regravada: mesmos valores, ids e xmin.
verificar "U7 $D0 intacta (valores)" "$SNAP_D0" "$(snapshot "$D0")"
verificar "U7 $D0 intacta (id e xmin das linhas)" "$FIS_D0" "$(linhas_fisicas "$D0")"

# U8. Falha injetada no ponto de extensão, depois do snapshot e antes do commit.
M_FALHA="$(lista "$(m electronics 99.00 99.00 99.00 1)" "$(m toys 5.00 5.00 5.00 1)")"
saida="$(publicar falhar_etapa "$R1" "$D1" "$M_FALHA")"
verificar "U8 erro da etapa adicional" "FalhaInjetada|falha injetada após o snapshot (2 linha(s) visíveis na transação)" \
  "$(campo erro <<<"$saida")|$(campo mensagem <<<"$saida")"
verificar "U8 snapshot anterior preservado" "$SNAP_D1|$FIS_D1" "$(snapshot "$D1")|$(linhas_fisicas "$D1")"
verificar "U8 categoria da publicação revertida não cadastrada" "0" "$(reader_sql "SELECT count(*) FROM categoria WHERE codigo = 'toys'")"

# U9. Falha do banco no meio: o 2º item estoura numeric(12,2) depois do 1º já inserido.
M_OVERFLOW="$(lista "$(m electronics 1.00 1.00 1.00 1)" "$(m toys 10000000000.00 10000000000.00 10000000000.00 1)")"
saida="$(publicar publicar "$R1" "$D1" "$M_OVERFLOW")"
verificar "U9 erro do PostgreSQL" "NumericValueOutOfRange|numeric field overflow" "$(campo erro <<<"$saida")|$(campo mensagem <<<"$saida")"
verificar "U9 snapshot anterior preservado" "$SNAP_D1|$FIS_D1" "$(snapshot "$D1")|$(linhas_fisicas "$D1")"
verificar "U9 categoria da publicação revertida não cadastrada" "0" "$(reader_sql "SELECT count(*) FROM categoria WHERE codigo = 'toys'")"

# U10. Código repetido e coleção vazia são recusados antes da transação.
saida="$(publicar publicar "$R1" "$D1" "$(lista "$(m toys 1.00 1.00 1.00 1)" "$(m toys 2.00 2.00 2.00 1)")")"
verificar "U10 código repetido recusado" "CategoriaRepetidaError" "$(campo erro <<<"$saida")"
saida="$(publicar publicar "$R1" "$D1" "[]")"
verificar "U10 coleção vazia recusada" "ColecaoVaziaError" "$(campo erro <<<"$saida")"
verificar "U10 snapshot e cadastro inalterados" "$SNAP_D1|$FIS_D1|0" \
  "$(snapshot "$D1")|$(linhas_fisicas "$D1")|$(reader_sql "SELECT count(*) FROM categoria WHERE codigo = 'toys'")"
verificar "U10 $D0 ainda intacta" "$SNAP_D0|$FIS_D0" "$(snapshot "$D0")|$(linhas_fisicas "$D0")"

resumo
