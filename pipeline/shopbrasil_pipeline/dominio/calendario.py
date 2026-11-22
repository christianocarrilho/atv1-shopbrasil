"""Regra da data de referência das métricas.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

A data de referência é a data civil, no fuso de Brasília, da ocorrência agendada da run. Ela é
derivada da ocorrência e nunca do relógio, para que o reprocessamento da mesma run produza a
mesma data. Usa ``zoneinfo`` da biblioteca padrão para manter o domínio livre do Airflow.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

FUSO_REFERENCIA = ZoneInfo("America/Sao_Paulo")


def data_referencia(ocorrencia: datetime) -> date:
    """Converte a ocorrência agendada para o fuso de Brasília e devolve a data civil.

    Rejeita datas sem fuso, porque o mesmo instante teria datas diferentes conforme o fuso
    assumido.
    """
    if ocorrencia.tzinfo is None or ocorrencia.utcoffset() is None:
        raise ValueError("a ocorrência agendada precisa de fuso horário explícito")
    return ocorrencia.astimezone(FUSO_REFERENCIA).date()
