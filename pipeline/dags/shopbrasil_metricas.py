"""DAG diário das métricas de preço por categoria da ShopBrasil.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Substitui o script do cron: coleta o catálogo da FakeStore, valida os produtos, calcula as
métricas por categoria em paralelo e publica o resultado no PostgreSQL sem duplicar ao reprocessar.
O DAG só orquestra, com TaskFlow; coleta, cálculo e SQL ficam no pacote ``shopbrasil_pipeline``.

- Agenda: todo dia às 06:00 de ``America/Sao_Paulo``. A data de referência é a data lógica da run.
- Linear: ``identificar`` → ``buscar_produtos`` → ``validar_produtos`` → ``agrupar_por_categoria``.
- Fan-out: ``calcular_metricas.expand`` cria uma task por categoria, limitada pelo ``ecommerce_pool``.
- Fan-in: ``consolidar`` junta os resultados, e ``publicar`` grava tudo numa transação.
- Falhas: ``buscar_produtos`` tem retry com backoff e callbacks de sucesso, retry e falha (com
  alerta simulado). As outras tasks e a run avisam a falha no log.
"""

from __future__ import annotations

import logging
import time
from datetime import timedelta

import pendulum
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task, task_group
from airflow.sdk.exceptions import AirflowFailException

from ajustes_e2e import avisar_ajustes_de_teste, segundos
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

log = logging.getLogger(__name__)

FUSO = FUSO_REFERENCIA.key  # America/Sao_Paulo, o mesmo fuso da data de referência
# Primeira ocorrência considerada; com catchup=False, o Airflow não cria as runs atrasadas.
START_DATE = pendulum.datetime(2026, 11, 1, tz=FUSO)

# Buscar Produtos: 3 retentativas após a tentativa inicial, atraso inicial de 60 s e backoff
# exponencial (cada espera é sorteada entre N e 2N, com N = 60 × 2^(n−1)). Só o E2E encurta o atraso.
BUSCA_RETRIES = 3
BUSCA_RETRY_DELAY = timedelta(seconds=segundos("SHOPBRASIL_BUSCA_RETRY_DELAY_SEGUNDOS", producao=60))
BUSCA_BACKOFF = 2.0
# Pausa por task mapeada, zero em produção; o E2E a usa para observar o limite do pool.
CALCULO_ATRASO_SEGUNDOS = segundos("SHOPBRASIL_CALCULO_ATRASO_SEGUNDOS", producao=0)


# ----------------------------------------------------------------------------- callbacks
# O Airflow chama estas funções no processo da task, e cada aviso vai para o log da tentativa.


def _ocorrencia(contexto) -> OcorrenciaTask:
    """Traduz o contexto do callback para a ocorrência que os avisos descrevem."""
    ti = contexto["ti"]
    retries = contexto["task"].retries
    erro = contexto.get("exception")
    # Depois de um Clear, try_number continua crescendo; a tentativa é contada no ciclo atual.
    inicio_ciclo = getattr(ti, "max_tries", retries) - retries
    return OcorrenciaTask(
        dag_id=ti.dag_id,
        run_id=ti.run_id,
        task_id=ti.task_id,
        tentativa=ti.try_number - inicio_ciclo,
        max_tentativas=retries + 1,
        erro=f"{type(erro).__name__}: {erro}" if erro else None,
    )


def busca_sucesso(contexto) -> None:
    notificar_sucesso(_ocorrencia(contexto))


def busca_retry(contexto) -> None:
    notificar_retry(_ocorrencia(contexto))


def busca_falha(contexto) -> None:
    notificar_falha_ingestao(_ocorrencia(contexto))  # aviso + alerta simulado


def avisar_falha(contexto) -> None:
    notificar_falha(_ocorrencia(contexto))


def avisar_falha_run(contexto) -> None:
    # Callback do DAG: no Airflow 3 roda no dag-processor, e a linha vai para o log dele.
    dag_run = contexto["dag_run"]
    notificar_falha_run(dag_run.dag_id, dag_run.run_id, contexto.get("reason"))


# ----------------------------------------------------------------------------------- DAG


@dag(
    dag_id="shopbrasil_metricas",
    schedule=CronTriggerTimetable("0 6 * * *", timezone=FUSO),
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
    def preparacao():
        @task
        def identificar() -> dict[str, str]:
            # A data de referência vem da data lógica da run, nunca do relógio.
            contexto = get_current_context()
            return identificar_execucao(contexto["run_id"], contexto.get("logical_date")).para_xcom()

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
                raise  # relança para o Airflow aplicar o retry
            log.info("Catálogo coletado: raiz %s com %d produto(s)", catalogo.tipo_raiz, len(catalogo.produtos))
            return catalogo.para_xcom()

        # Operador próprio: catálogo vazio ou produto inválido interrompe a run antes do cálculo.
        return ValidarProdutosOperator(task_id="validar_produtos", catalogo=buscar_produtos(execucao)).output

    @task_group(group_id="analise")
    def analise(validados: dict[str, object]):
        @task(task_id="agrupar_por_categoria")
        def agrupar(validados: dict[str, object]) -> list[dict[str, object]]:
            # Uma entrada por categoria encontrada na coleta; nenhuma lista fixa.
            grupos = agrupar_por_categoria(validados["produtos"])
            log.info("Categorias para o fan-out: %s", [g.codigo_categoria for g in grupos])
            return [g.para_xcom() for g in grupos]

        @task(pool="ecommerce_pool")
        def calcular_metricas(entrada: dict[str, object]) -> dict[str, object]:
            if CALCULO_ATRASO_SEGUNDOS:
                time.sleep(CALCULO_ATRASO_SEGUNDOS)
            return calcular_categoria(entrada)

        @task
        def consolidar(grupos: list[dict[str, object]], resultados) -> list[dict[str, object]]:
            # Fan-in: exige exatamente um resultado para cada categoria agrupada.
            metricas = consolidar_metricas(
                metricas_de_xcom(resultados), esperadas=[g["codigo_categoria"] for g in grupos]
            )
            return [m.para_xcom() for m in metricas]

        grupos = agrupar(validados)
        resultados = calcular_metricas.expand(entrada=grupos)  # fan-out por categoria
        return consolidar(grupos, resultados)

    @task_group(group_id="publicacao")
    def publicacao(execucao: dict[str, str], metricas: list[dict[str, object]]):
        @task
        def publicar(execucao: dict[str, str], metricas: list[dict[str, object]]) -> dict[str, object]:
            # Cadastro, snapshot do dia e histórico da run numa só transação.
            resultado = composicao.publicar_snapshot().executar(
                Execucao.de_xcom(execucao), metricas_de_xcom(metricas)
            )
            return resultado.para_xcom()

        return publicar(execucao, metricas)

    execucao = preparacao()
    publicacao(execucao, analise(ingestao(execucao)))


shopbrasil_metricas()
