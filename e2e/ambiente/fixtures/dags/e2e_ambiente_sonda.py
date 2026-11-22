"""DAG de fixture E2E da #2 para validar o ambiente Docker.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Verifica a Connection ``shopbrasil_postgres`` com ``PostgresHook`` e ocupa o pool
``ecommerce_pool`` com quatro tasks mapeadas que gravam seus intervalos na tabela de sonda.
Fica fora de ``pipeline/dags`` e só é montado pelo override E2E, para não se misturar ao DAG
real da #4.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

CONN_ID = "shopbrasil_postgres"
DURACAO_SEGUNDOS = 8


@dag(dag_id="e2e_ambiente_sonda", schedule=None, catchup=False, tags=["e2e", "issue-2"])
def e2e_ambiente_sonda():
    @task
    def verificar_conexao() -> dict:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        usuario, banco, versao = hook.get_first(
            "SELECT current_user, current_database(), current_setting('server_version')"
        )
        return {"usuario": usuario, "banco": banco, "versao": versao}

    @task
    def indices(_conexao: dict) -> list[int]:
        return [0, 1, 2, 3]

    @task(pool="ecommerce_pool")
    def ocupar_slot(map_index: int, run_id: str | None = None) -> int:
        inicio = datetime.now(timezone.utc)
        time.sleep(DURACAO_SEGUNDOS)
        fim = datetime.now(timezone.utc)
        PostgresHook(postgres_conn_id=CONN_ID).run(
            "INSERT INTO e2e_ambiente_sonda (run_id, map_index, inicio, fim) VALUES (%s, %s, %s, %s)",
            parameters=(run_id, map_index, inicio, fim),
        )
        return map_index

    @task
    def consolidar(resultados: list[int]) -> int:
        return len(list(resultados))

    consolidar(ocupar_slot.expand(map_index=indices(verificar_conexao())))


e2e_ambiente_sonda()
