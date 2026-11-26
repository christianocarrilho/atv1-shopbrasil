"""Avisos dos callbacks e alerta simulado do pipeline (#14).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Cada aviso é uma linha de log que começa com ``[SHOPBRASIL][<EVENTO>]`` e traz DAG, run, task e
tentativa, para ser encontrada no log da própria tentativa. Na falha definitiva da ingestão, o
alerta à equipe é simulado por uma linha ``[SHOPBRASIL][ALERTA SIMULADO]``: o envio real (e-mail,
chat) ficaria neste ponto. O DAG só traduz o contexto do Airflow para ``OcorrenciaTask``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

PREFIXO = "[SHOPBRASIL]"
DESTINO_ALERTA = "equipe-dados@shopbrasil.example"

log = logging.getLogger("shopbrasil.alertas")


@dataclass(frozen=True)
class OcorrenciaTask:
    """Identifica a task e a tentativa que disparou o callback."""

    dag_id: str
    run_id: str
    task_id: str
    tentativa: int
    max_tentativas: int
    erro: str | None = None

    def descricao(self) -> str:
        texto = (
            f"dag={self.dag_id} run={self.run_id} task={self.task_id} "
            f"tentativa={self.tentativa}/{self.max_tentativas}"
        )
        return f"{texto} erro={self.erro}" if self.erro else texto


def notificar_sucesso(ocorrencia: OcorrenciaTask) -> None:
    log.warning("%s[SUCESSO] %s", PREFIXO, ocorrencia.descricao())


def notificar_retry(ocorrencia: OcorrenciaTask) -> None:
    log.warning("%s[RETRY] %s", PREFIXO, ocorrencia.descricao())


def notificar_falha(ocorrencia: OcorrenciaTask) -> None:
    """Aviso identificável de falha de qualquer task do pipeline."""
    log.warning("%s[FALHA] %s", PREFIXO, ocorrencia.descricao())


def notificar_falha_ingestao(ocorrencia: OcorrenciaTask) -> None:
    """Falha definitiva da ingestão: aviso de falha e alerta simulado.

    A falha é definitiva quando as tentativas se esgotam ou quando o erro não é recuperável (por
    exemplo, configuração inválida), caso em que a task falha antes da última tentativa.
    """
    notificar_falha(ocorrencia)
    if ocorrencia.tentativa >= ocorrencia.max_tentativas:
        causa = f"Buscar Produtos esgotou {ocorrencia.max_tentativas} tentativa(s)"
    else:
        causa = (
            f"Buscar Produtos falhou sem retentativa (erro não recuperável) na tentativa "
            f"{ocorrencia.tentativa}/{ocorrencia.max_tentativas}"
        )
    log.error(
        "%s[ALERTA SIMULADO] destino=%s assunto=ShopBrasil: ingestão da FakeStore falhou na run %s | "
        "%s; a publicação da run não ocorreu. %s",
        PREFIXO,
        DESTINO_ALERTA,
        ocorrencia.run_id,
        causa,
        ocorrencia.descricao(),
    )


def notificar_falha_run(dag_id: str, run_id: str, motivo: str | None) -> None:
    """Aviso de falha da run inteira, inclusive quando nenhuma task chamou o próprio callback."""
    log.warning("%s[FALHA RUN] dag=%s run=%s motivo=%s", PREFIXO, dag_id, run_id, motivo or "não informado")
