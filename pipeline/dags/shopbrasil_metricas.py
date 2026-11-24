"""DAG diário das métricas de preço por categoria da ShopBrasil.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Define a orquestração com TaskFlow: o DAG aciona casos de uso da aplicação e não contém SQL,
HTTP nem fórmulas. As dependências vêm das chamadas entre as funções e as etapas ficam em
TaskGroups de preparação, ingestão, análise e publicação.

Callbacks (#14): Buscar Produtos tem callbacks de sucesso, retry e falha, e a falha definitiva
simula o envio de alerta. Toda outra task herda de ``default_args`` o aviso de falha, gravado no
log da própria tentativa, e o DAG avisa a falha da run inteira, inclusive a marcada pela UI.

A agenda usa ``CronTriggerTimetable`` explícito: no Airflow 3.4 a data lógica de cada run é a
própria ocorrência das 06:00 em ``America/Sao_Paulo``, sem intervalo de dados a deslocar a data.
Trechos: linear (preparação → Buscar Produtos (#13) → ``ValidarProdutosOperator`` (#6) →
agrupamento), fan-out (``calcular_metricas.expand`` por categoria observada, no ``ecommerce_pool``)
e fan-in (``consolidar``), seguido da publicação transacional (#17/#18) pela composição (#16).
"""

from __future__ import annotations

import logging
import os
import time
from datetime import timedelta

import pendulum
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task, task_group

from operadores.validar_produtos import ValidarProdutosOperator
from shopbrasil_pipeline import composicao
from shopbrasil_pipeline.aplicacao.alertas import (
    OcorrenciaTask,
    notificar_falha,
    notificar_falha_ingestao,
    notificar_falha_run,
    notificar_retry,
    notificar_sucesso,
)
from shopbrasil_pipeline.aplicacao.analise import (
    agrupar_por_categoria,
    calcular_categoria,
    consolidar_metricas,
    metricas_de_xcom,
)
from shopbrasil_pipeline.aplicacao.catalogo import coletar_catalogo
from shopbrasil_pipeline.aplicacao.execucao import FUSO_REFERENCIA, Execucao, identificar_execucao

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
# Multiplicador do backoff: no Airflow 3.4 o parâmetro é float, e cada espera é sorteada entre N e
# 2N, com N = atraso inicial × 2^(n−1).
BUSCA_BACKOFF = 2.0
BUSCA_RETRY_DELAY = timedelta(seconds=int(os.environ.get("SHOPBRASIL_BUSCA_RETRY_DELAY_SEGUNDOS", "60")))

# Tasks mapeadas por categoria dividem o pool de dois slots provisionado na #2.
POOL_CALCULO = "ecommerce_pool"
# Atraso artificial por task mapeada, só no override E2E, para tornar observável o limite do pool.
CALCULO_ATRASO_SEGUNDOS = float(os.environ.get("SHOPBRASIL_CALCULO_ATRASO_SEGUNDOS", "0"))

log = logging.getLogger(__name__)

# Valores de produção exigidos pelo enunciado; os ajustes por ambiente existem só para o E2E.
BUSCA_RETRY_DELAY_PRODUCAO = timedelta(seconds=60)
AVISO_AJUSTE_TESTE = "[SHOPBRASIL][AVISO] ajuste de teste ativo: %s; não use fora do E2E"


def avisar_ajustes_de_teste() -> list[str]:
    """Registra no log da task cada ajuste de teste ativo, para que não passe despercebido."""
    ajustes = []
    if BUSCA_RETRY_DELAY != BUSCA_RETRY_DELAY_PRODUCAO:
        ajustes.append(f"SHOPBRASIL_BUSCA_RETRY_DELAY_SEGUNDOS={int(BUSCA_RETRY_DELAY.total_seconds())} (produção: 60)")
    if CALCULO_ATRASO_SEGUNDOS:
        ajustes.append(f"SHOPBRASIL_CALCULO_ATRASO_SEGUNDOS={CALCULO_ATRASO_SEGUNDOS:g} (produção: 0)")
    for ajuste in ajustes:
        log.warning(AVISO_AJUSTE_TESTE, ajuste)
    return ajustes


def _ocorrencia(contexto) -> OcorrenciaTask:
    """Traduz o contexto do callback do Airflow para a ocorrência da aplicação."""
    ti = contexto["ti"]
    retries = contexto["task"].retries
    erro = contexto.get("exception")
    # Depois de um clear, try_number continua crescendo e max_tries = try_number + retries.
    # A tentativa é contada no ciclo atual, como faz o próprio Airflow ao decidir o retry.
    inicio_ciclo = getattr(ti, "max_tries", retries) - retries
    return OcorrenciaTask(
        dag_id=ti.dag_id,
        run_id=ti.run_id,
        task_id=ti.task_id,
        tentativa=ti.try_number - inicio_ciclo,
        max_tentativas=retries + 1,
        erro=f"{type(erro).__name__}: {erro}" if erro else None,
    )


# Callbacks: o Airflow chama estas funções no processo da task, e as mensagens vão para o log
# daquela tentativa. Qualquer task que falhe emite o aviso; a ingestão também simula o alerta.
def avisar_falha(contexto) -> None:
    notificar_falha(composicao.notificador(), _ocorrencia(contexto))


def busca_sucesso(contexto) -> None:
    notificar_sucesso(composicao.notificador(), _ocorrencia(contexto))


def busca_retry(contexto) -> None:
    notificar_retry(composicao.notificador(), _ocorrencia(contexto))


def busca_falha(contexto) -> None:
    notificar_falha_ingestao(composicao.notificador(), _ocorrencia(contexto))


def avisar_falha_run(contexto) -> None:
    # Callback do DAG: no Airflow 3 roda no dag-processor, e a linha vai para o log dele.
    dag_run = contexto["dag_run"]
    notificar_falha_run(composicao.notificador(), dag_run.dag_id, dag_run.run_id, contexto.get("reason"))


@dag(
    dag_id=DAG_ID,
    schedule=CronTriggerTimetable(AGENDA_CRON, timezone=FUSO),
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={"on_failure_callback": avisar_falha},
    on_failure_callback=avisar_falha_run,
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
            retry_exponential_backoff=BUSCA_BACKOFF,
            on_success_callback=busca_sucesso,
            on_retry_callback=busca_retry,
            on_failure_callback=busca_falha,
        )
        def buscar_produtos(execucao: dict[str, str]) -> dict[str, object]:
            avisar_ajustes_de_teste()
            try:
                fonte = composicao.fonte_catalogo()
            except ValueError as erro:
                # Configuração inválida não melhora com retentativas: falha direto.
                raise AirflowFailException(f"configuração da FakeStore inválida: {erro}") from erro
            try:
                catalogo = coletar_catalogo(fonte)
            except Exception:
                log.exception("Falha ao buscar produtos da FakeStore na run %s", execucao["run_id"])
                raise
            log.info("Catálogo coletado: raiz %s com %d produto(s)", catalogo.tipo_raiz, len(catalogo.produtos))
            return catalogo.para_xcom()

        # Linear: a validação recebe a coleta pelo XCom e precede qualquer cálculo.
        validar = ValidarProdutosOperator(task_id="validar_produtos", catalogo=buscar_produtos(execucao))
        return validar.output

    @task_group(group_id="analise")
    def analise(validados: dict[str, object]):
        @task
        def agrupar_por_categoria_task(validados: dict[str, object]) -> list[dict[str, object]]:
            # Uma entrada por código observado na coleta; nenhuma lista fixa de categorias.
            grupos = agrupar_por_categoria(validados["produtos"])
            log.info("Categorias para o fan-out: %s", [g.codigo_categoria for g in grupos])
            return [g.para_xcom() for g in grupos]

        @task(pool=POOL_CALCULO)
        def calcular_metricas(entrada: dict[str, object]) -> dict[str, object]:
            if CALCULO_ATRASO_SEGUNDOS:
                time.sleep(CALCULO_ATRASO_SEGUNDOS)
            return calcular_categoria(entrada)

        @task
        def consolidar(grupos: list[dict[str, object]], resultados) -> list[dict[str, object]]:
            # Fan-in: exige exatamente um resultado para cada categoria agrupada.
            metricas = consolidar_metricas(
                metricas_de_xcom(resultados),
                esperadas=[g["codigo_categoria"] for g in grupos],
            )
            return [m.para_xcom() for m in metricas]

        grupos = agrupar_por_categoria_task.override(task_id="agrupar_por_categoria")(validados)
        # Fan-out: uma instância mapeada por categoria, limitada pelo ecommerce_pool.
        resultados = calcular_metricas.expand(entrada=grupos)
        return consolidar(grupos, resultados)

    @task_group(group_id="publicacao")
    def publicacao(execucao: dict[str, str], metricas: list[dict[str, object]]):
        @task
        def publicar(execucao: dict[str, str], metricas: list[dict[str, object]]) -> dict[str, object]:
            resultado = composicao.publicar_snapshot().executar(
                Execucao.de_xcom(execucao), list(metricas_de_xcom(metricas))
            )
            log.info("Snapshot publicado: %s", resultado.para_xcom())
            return resultado.para_xcom()

        return publicar(execucao, metricas)

    execucao = preparacao()
    publicacao(execucao, analise(ingestao(execucao)))


shopbrasil_metricas()
