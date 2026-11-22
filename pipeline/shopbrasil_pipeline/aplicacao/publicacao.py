"""Caso de uso e contrato da publicação do snapshot diário (#17).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Recebe a execução e a coleção consolidada de métricas e pede ao repositório que publique o
snapshot da data de referência numa única operação atômica. A coleção é conferida antes de
chegar ao repositório: um código repetido é recusado aqui, com mensagem clara, para que a
transação nem seja aberta. A aplicação não conhece SQL nem Airflow; o adaptador concreto é
injetado pela composição.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Protocol

from shopbrasil_pipeline.aplicacao.execucao import Execucao
from shopbrasil_pipeline.dominio.metricas import MetricasCategoria


class CategoriaRepetidaError(ValueError):
    """A coleção consolidada tem mais de um item para o mesmo código de categoria."""


class ColecaoVaziaError(ValueError):
    """Não há métricas a publicar; publicar o vazio apagaria o snapshot da data."""


@dataclass(frozen=True)
class ResultadoPublicacao:
    """Resumo da publicação confirmada, pequeno o bastante para o XCom."""

    run_id: str
    data_referencia: date
    categorias_publicadas: tuple[str, ...]
    categorias_cadastradas: tuple[str, ...]

    def para_xcom(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "data_referencia": self.data_referencia.isoformat(),
            "categorias_publicadas": list(self.categorias_publicadas),
            "categorias_cadastradas": list(self.categorias_cadastradas),
        }


class RepositorioPublicacao(Protocol):
    """Grava o snapshot de uma data de referência numa única transação.

    A implementação cadastra os códigos novos, resolve ``categoria_id`` pelo código e substitui
    só o conjunto de métricas de ``execucao.data_referencia``. Ou tudo é confirmado, ou nada:
    uma falha desfaz a operação e preserva o snapshot anterior. Gravações que precisem
    compartilhar a transação, como o histórico da #18, entram pelo adaptador antes do commit.
    """

    def publicar_snapshot(
        self, execucao: Execucao, metricas: Sequence[MetricasCategoria]
    ) -> ResultadoPublicacao: ...


def _conferir_colecao(metricas: Iterable[MetricasCategoria]) -> tuple[MetricasCategoria, ...]:
    colecao = tuple(metricas)
    for item in colecao:
        if not isinstance(item, MetricasCategoria):
            raise TypeError(f"item da coleção deve ser MetricasCategoria, recebido {type(item).__name__}")
    if not colecao:
        raise ColecaoVaziaError("coleção de métricas vazia; o snapshot da data não foi alterado")
    repetidos = sorted(codigo for codigo, n in Counter(m.codigo_categoria for m in colecao).items() if n > 1)
    if repetidos:
        raise CategoriaRepetidaError(
            f"coleção consolidada com código de categoria repetido: {', '.join(repetidos)}; "
            "a consolidação deve entregar um item por codigo_categoria"
        )
    return colecao


class PublicarSnapshot:
    """Publica o snapshot diário da execução pelo repositório injetado."""

    def __init__(self, repositorio: RepositorioPublicacao) -> None:
        self._repositorio = repositorio

    def executar(self, execucao: Execucao, metricas: Iterable[MetricasCategoria]) -> ResultadoPublicacao:
        if not isinstance(execucao, Execucao):
            raise TypeError(f"execucao deve ser Execucao, recebido {type(execucao).__name__}")
        colecao = _conferir_colecao(metricas)
        return self._repositorio.publicar_snapshot(execucao, colecao)
