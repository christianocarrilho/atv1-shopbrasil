"""DAG diário das métricas de preço por categoria da ShopBrasil.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Define a orquestração com TaskFlow: o DAG aciona casos de uso da aplicação e não contém SQL,
HTTP nem fórmulas. As dependências vêm das chamadas entre as funções e as etapas ficam em
TaskGroups de preparação, ingestão, análise e publicação.

A agenda usa ``CronTriggerTimetable`` explícito: no Airflow 3.4 a data lógica de cada run é a
própria ocorrência das 06:00 em ``America/Sao_Paulo``, sem intervalo de dados a deslocar a data.
As tasks de ingestão, análise e publicação são o esqueleto que #13, #6, #16 e #17 substituem.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task, task_group

from shopbrasil_pipeline.aplicacao.execucao import Execucao, identificar_execucao

DAG_ID = "shopbrasil_metricas"
FUSO = "America/Sao_Paulo"
AGENDA_CRON = "0 6 * * *"
# Primeira ocorrência considerada. Com catchup=False, o Airflow não cria runs atrasadas
# entre esta data e a ativação do DAG.
START_DATE = pendulum.datetime(2026, 11, 1, tz=FUSO)


@dag(
    dag_id=DAG_ID,
    schedule=CronTriggerTimetable(AGENDA_CRON, timezone=FUSO),
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=["shopbrasil", "fakestore"],
    doc_md=__doc__,
)
def shopbrasil_metricas():
    @task_group(group_id="preparacao")
    def preparacao() -> dict[str, str]:
        @task
        def identificar() -> dict[str, str]:
            contexto = get_current_context()
            execucao = identificar_execucao(contexto["run_id"], contexto.get("logical_date"))
            return execucao.para_xcom()

        return identificar()

    @task_group(group_id="ingestao")
    def ingestao(execucao: dict[str, str]):
        @task
        def buscar_produtos(execucao: dict[str, str]) -> dict[str, str]:
            # Esqueleto da #4: a #13 integra a FakeStore e a #6 acrescenta a validação.
            return {"run_id": Execucao.de_xcom(execucao).run_id, "etapa": "ingestao"}

        return buscar_produtos(execucao)

    @task_group(group_id="analise")
    def analise(produtos: dict[str, str]):
        @task
        def consolidar(produtos: dict[str, str]) -> dict[str, str]:
            # Esqueleto da #4: a #16 agrupa, expande por categoria e consolida pela #15.
            return {"run_id": produtos["run_id"], "etapa": "analise"}

        return consolidar(produtos)

    @task_group(group_id="publicacao")
    def publicacao(execucao: dict[str, str], metricas: dict[str, str]):
        @task
        def publicar(execucao: dict[str, str], metricas: dict[str, str]) -> dict[str, str]:
            # Esqueleto da #4: a #17 e a #18 publicam snapshot e histórico na mesma transação.
            return {**execucao, "etapa": "publicacao"}

        return publicar(execucao, metricas)

    execucao = preparacao()
    publicacao(execucao, analise(ingestao(execucao)))


shopbrasil_metricas()
