"""DAG diário das métricas de preço por categoria da ShopBrasil.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Define a orquestração com TaskFlow: o DAG aciona casos de uso da aplicação e não contém SQL,
HTTP nem fórmulas. As dependências vêm das chamadas entre as funções e as etapas ficam em
TaskGroups de preparação, ingestão, análise e publicação.

A agenda usa ``CronTriggerTimetable`` explícito: no Airflow 3.4 a data lógica de cada run é a
própria ocorrência das 06:00 em ``America/Sao_Paulo``, sem intervalo de dados a deslocar a data.
Buscar Produtos (#13) usa a FakeStore pela composição. As tasks de análise e publicação são o
esqueleto que #16 e #17 substituem; a #6 acrescenta a validação na ingestão.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task, task_group

from shopbrasil_pipeline import composicao
from shopbrasil_pipeline.aplicacao.catalogo import coletar_catalogo
from shopbrasil_pipeline.aplicacao.execucao import identificar_execucao
from shopbrasil_pipeline.dominio.calendario import FUSO_REFERENCIA

DAG_ID = "shopbrasil_metricas"
# O fuso da agenda é o mesmo da regra da data de referência.
FUSO = FUSO_REFERENCIA.key
AGENDA_CRON = "0 6 * * *"
# Primeira ocorrência considerada. Com catchup=False, o Airflow não cria runs atrasadas
# entre esta data e a ativação do DAG.
START_DATE = pendulum.datetime(2026, 11, 1, tz=FUSO)

# Buscar Produtos: três retentativas após a primeira tentativa, atraso inicial de um minuto e
# backoff exponencial. O atraso pode ser reduzido pelo ambiente apenas nos testes E2E, para não
# esperar minutos por retentativa; o valor de produção é 60 segundos.
BUSCA_RETRIES = 3
BUSCA_RETRY_DELAY = timedelta(seconds=int(os.environ.get("SHOPBRASIL_BUSCA_RETRY_DELAY_SEGUNDOS", "60")))

log = logging.getLogger(__name__)


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
        @task(
            retries=BUSCA_RETRIES,
            retry_delay=BUSCA_RETRY_DELAY,
            retry_exponential_backoff=True,
        )
        def buscar_produtos(execucao: dict[str, str]) -> dict[str, object]:
            # A #6 acrescenta a validação depois desta task.
            try:
                catalogo = coletar_catalogo(composicao.fonte_catalogo())
            except Exception:
                log.exception("Falha ao buscar produtos da FakeStore na run %s", execucao["run_id"])
                raise
            log.info("Catálogo coletado: raiz %s com %d produto(s)", catalogo.tipo_raiz, len(catalogo.produtos))
            return catalogo.para_xcom()

        return buscar_produtos(execucao)

    @task_group(group_id="analise")
    def analise(produtos: dict[str, object]):
        @task
        def consolidar(produtos: dict[str, object]) -> dict[str, object]:
            # Esqueleto da #4: a #16 agrupa, expande por categoria e consolida pela #15.
            return {"etapa": "analise", "produtos": len(produtos["produtos"])}

        return consolidar(produtos)

    @task_group(group_id="publicacao")
    def publicacao(execucao: dict[str, str], metricas: dict[str, object]):
        @task
        def publicar(execucao: dict[str, str], metricas: dict[str, object]) -> dict[str, str]:
            # Esqueleto da #4: a #17 e a #18 publicam snapshot e histórico na mesma transação.
            return {**execucao, "etapa": "publicacao"}

        return publicar(execucao, metricas)

    execucao = preparacao()
    publicacao(execucao, analise(ingestao(execucao)))


shopbrasil_metricas()
