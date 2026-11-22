"""Avisos de execução e alerta simulado do pipeline (#14).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Define o contrato do notificador e as mensagens dos callbacks. Cada mensagem começa com
``[SHOPBRASIL][<EVENTO>]`` e traz DAG, run, task e tentativa, para ser encontrada nos logs da
task. Na falha definitiva da ingestão, além do aviso, o alerta é enviado ao notificador, que
nesta entrega apenas simula o envio. O DAG só traduz o contexto do Airflow para
``OcorrenciaTask``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

PREFIXO = "[SHOPBRASIL]"


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


class Notificador(Protocol):
    """Destino dos avisos e alertas do pipeline."""

    def registrar(self, mensagem: str) -> None:
        """Registra um aviso identificável."""
        ...

    def enviar_alerta(self, assunto: str, mensagem: str) -> None:
        """Envia um alerta à equipe responsável."""
        ...


def notificar_sucesso(notificador: Notificador, ocorrencia: OcorrenciaTask) -> None:
    notificador.registrar(f"{PREFIXO}[SUCESSO] {ocorrencia.descricao()}")


def notificar_retry(notificador: Notificador, ocorrencia: OcorrenciaTask) -> None:
    notificador.registrar(f"{PREFIXO}[RETRY] {ocorrencia.descricao()}")


def notificar_falha(notificador: Notificador, ocorrencia: OcorrenciaTask) -> None:
    """Aviso identificável de falha de qualquer task do pipeline."""
    notificador.registrar(f"{PREFIXO}[FALHA] {ocorrencia.descricao()}")


def notificar_falha_ingestao(notificador: Notificador, ocorrencia: OcorrenciaTask) -> None:
    """Falha definitiva da ingestão: aviso e alerta.

    A falha é definitiva quando as tentativas se esgotam ou quando o erro não é recuperável (por
    exemplo, configuração inválida), caso em que a task falha antes da última tentativa.
    """
    notificar_falha(notificador, ocorrencia)
    if ocorrencia.tentativa >= ocorrencia.max_tentativas:
        causa = f"Buscar Produtos esgotou {ocorrencia.max_tentativas} tentativa(s)"
    else:
        causa = (
            f"Buscar Produtos falhou sem retentativa (erro não recuperável) na tentativa "
            f"{ocorrencia.tentativa}/{ocorrencia.max_tentativas}"
        )
    notificador.enviar_alerta(
        assunto=f"ShopBrasil: ingestão da FakeStore falhou na run {ocorrencia.run_id}",
        mensagem=f"{causa}; a publicação da run não ocorreu. {ocorrencia.descricao()}",
    )


def notificar_falha_run(notificador: Notificador, dag_id: str, run_id: str, motivo: str | None) -> None:
    """Aviso de falha da run inteira, inclusive quando nenhuma task chamou o próprio callback."""
    notificador.registrar(f"{PREFIXO}[FALHA RUN] dag={dag_id} run={run_id} motivo={motivo or 'não informado'}")
