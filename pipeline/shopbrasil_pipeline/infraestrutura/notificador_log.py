"""Notificador que registra avisos no log da task e simula o envio de alertas (#14).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

O alerta não sai do contêiner: o envio é simulado por uma linha ``[SHOPBRASIL][ALERTA SIMULADO]``
com destino, assunto e mensagem. Trocar por e-mail ou chat exige só outro adaptador na composição.
"""

from __future__ import annotations

import logging

from shopbrasil_pipeline.aplicacao.alertas import PREFIXO

log = logging.getLogger("shopbrasil.alertas")


class NotificadorLog:
    """Implementa ``Notificador`` com o logging da task do Airflow."""

    def __init__(self, destino: str) -> None:
        self.destino = destino

    def registrar(self, mensagem: str) -> None:
        log.warning(mensagem)

    def enviar_alerta(self, assunto: str, mensagem: str) -> None:
        log.error("%s[ALERTA SIMULADO] destino=%s assunto=%s | %s", PREFIXO, self.destino, assunto, mensagem)
