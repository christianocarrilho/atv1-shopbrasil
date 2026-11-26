"""Driver do cenário E2E da publicação (#17) e do histórico por run (#18), no contêiner do Airflow.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Lido pela entrada padrão (``python - <modo> <run_id> <data> <métricas JSON>``) para não exigir
montagem extra no Compose. Usa o caso de uso da composição com a Connection ``shopbrasil_postgres``
real, que já grava o histórico na mesma transação. O modo ``falhar_etapa`` envolve a gravação do
histórico do adaptador para lançar um erro depois do snapshot e do histórico gravados e antes do
commit. A última linha impressa é um JSON com o resultado.
"""

from __future__ import annotations

import json
import logging
import sys

from airflow.providers.postgres.hooks.postgres import PostgresHook

from shopbrasil_pipeline.aplicacao.execucao import Execucao
from shopbrasil_pipeline.composicao import publicar_snapshot
from shopbrasil_pipeline.dominio.metricas import MetricasCategoria
from shopbrasil_pipeline.infraestrutura import publicacao_postgres
from shopbrasil_pipeline.infraestrutura.publicacao_postgres import CONN_ID_PADRAO

# Avisos do pipeline (como a divergência do histórico, #18) vão ao stderr do driver, que o cenário
# confere; o import do Airflow reconfigura o logging e eles não sairiam por padrão.
_aviso = logging.StreamHandler(sys.stderr)
_aviso.setLevel(logging.WARNING)
logging.getLogger("shopbrasil_pipeline").addHandler(_aviso)


class FalhaInjetada(RuntimeError):
    pass


_gravar_historico_original = publicacao_postgres._gravar_historico


def historico_seguido_de_falha(cursor, execucao, metricas, categoria_ids) -> int:
    # Grava o histórico de verdade e confirma, na mesma transação, que snapshot e histórico novos
    # estão visíveis antes de abortá-la.
    _gravar_historico_original(cursor, execucao, metricas, categoria_ids)
    cursor.execute(
        "SELECT count(*) FROM public.metricas_categoria WHERE data_referencia = %s",
        (execucao.data_referencia,),
    )
    vistas = cursor.fetchone()[0]
    cursor.execute(
        "SELECT count(*) FROM public.metricas_categoria_historico WHERE run_id = %s",
        (execucao.run_id,),
    )
    historico = cursor.fetchone()[0]
    raise FalhaInjetada(
        f"falha injetada após o snapshot ({vistas} linha(s) visíveis na transação, {historico} no histórico da run)"
    )


def main() -> int:
    modo, run_id, data, payload = sys.argv[1:5]
    execucao = Execucao.de_xcom({"run_id": run_id, "data_referencia": data})
    if modo == "conta":
        usuario = PostgresHook(postgres_conn_id=CONN_ID_PADRAO).get_first("SELECT current_user")[0]
        print(json.dumps({"conta": usuario}))
        return 0
    if modo == "falhar_etapa":
        publicacao_postgres._gravar_historico = historico_seguido_de_falha
    elif modo != "publicar":
        raise SystemExit(f"modo desconhecido: {modo}")
    caso_de_uso = publicar_snapshot()
    try:
        metricas = [MetricasCategoria.de_xcom(item) for item in json.loads(payload)]
        resultado = caso_de_uso.executar(execucao, metricas)
    except Exception as erro:  # noqa: BLE001 - o cenário compara o tipo do erro observado
        print(json.dumps({"erro": type(erro).__name__, "mensagem": str(erro).splitlines()[0]}))
        return 1
    print(json.dumps(resultado.para_xcom()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
