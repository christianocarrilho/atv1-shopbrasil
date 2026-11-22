"""Driver do cenário E2E da publicação (#17), executado dentro do contêiner do Airflow.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Lido pela entrada padrão (``python - <modo> <run_id> <data> <métricas JSON>``) para não exigir
montagem extra no Compose. Usa o caso de uso da composição com a Connection ``shopbrasil_postgres``
real. O modo ``falhar_etapa`` injeta, pelo ponto de extensão da #18, uma etapa que lança erro
depois do snapshot gravado e antes do commit. A última linha impressa é um JSON com o resultado.
"""

from __future__ import annotations

import json
import sys

from airflow.providers.postgres.hooks.postgres import PostgresHook

from shopbrasil_pipeline.aplicacao.execucao import Execucao
from shopbrasil_pipeline.aplicacao.publicacao import PublicarSnapshot
from shopbrasil_pipeline.composicao import publicar_snapshot
from shopbrasil_pipeline.dominio.metricas import MetricasCategoria
from shopbrasil_pipeline.infraestrutura.publicacao_postgres import CONN_ID_PADRAO, PublicadorSnapshotPostgres


class FalhaInjetada(RuntimeError):
    pass


def etapa_que_falha(cursor, snapshot) -> None:
    # Confirma que a etapa vê o snapshot novo na mesma transação antes de abortá-la.
    cursor.execute(
        "SELECT count(*) FROM public.metricas_categoria WHERE data_referencia = %s",
        (snapshot.execucao.data_referencia,),
    )
    vistas = cursor.fetchone()[0]
    raise FalhaInjetada(f"falha injetada após o snapshot ({vistas} linha(s) visíveis na transação)")


def main() -> int:
    modo, run_id, data, payload = sys.argv[1:5]
    execucao = Execucao.de_xcom({"run_id": run_id, "data_referencia": data})
    if modo == "conta":
        usuario = PostgresHook(postgres_conn_id=CONN_ID_PADRAO).get_first("SELECT current_user")[0]
        print(json.dumps({"conta": usuario}))
        return 0
    if modo == "publicar":
        caso_de_uso = publicar_snapshot()
    elif modo == "falhar_etapa":
        caso_de_uso = PublicarSnapshot(PublicadorSnapshotPostgres(etapas_adicionais=(etapa_que_falha,)))
    else:
        raise SystemExit(f"modo desconhecido: {modo}")
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
