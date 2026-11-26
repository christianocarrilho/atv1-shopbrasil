"""Ajustes de tempo usados só pela suíte E2E.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

A suíte E2E encurta o atraso do retry (60 s em produção) para não esperar minutos por tentativa e
acrescenta uma pausa ao cálculo para tornar observável o limite de dois slots do pool. Sem as
variáveis de ambiente, valem os valores de produção que o DAG declara. Quando um ajuste está ativo,
``avisar_ajustes_de_teste`` o registra no log da coleta, para que nunca passe despercebido.
"""

from __future__ import annotations

import logging
import os

AVISO_AJUSTE_TESTE = "[SHOPBRASIL][AVISO] ajuste de teste ativo: %s; não use fora do E2E"

log = logging.getLogger(__name__)
# Variáveis consultadas e seus valores de produção; um dict evita repetir a mesma variável quando o
# Airflow analisa o DAG de novo no mesmo processo.
_producao: dict[str, float] = {}


def segundos(variavel: str, producao: float) -> float:
    """Valor de produção, ou o da variável de ambiente do E2E quando definida."""
    _producao[variavel] = producao
    return float(os.environ.get(variavel, producao))


def avisar_ajustes_de_teste() -> list[str]:
    """Registra no log da task cada ajuste de teste ativo e devolve a lista."""
    ajustes = [
        f"{variavel}={segundos(variavel, producao):g} (produção: {producao:g})"
        for variavel, producao in _producao.items()
        if segundos(variavel, producao) != producao
    ]
    for ajuste in ajustes:
        log.warning(AVISO_AJUSTE_TESTE, ajuste)
    return ajustes
