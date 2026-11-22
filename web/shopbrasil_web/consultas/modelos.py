"""Modelos de leitura da Web/API (#9).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Tipos próprios da web para o snapshot diário (`metricas_categoria` + `categoria`) e para a série
de preço médio. São independentes do pipeline, porque o contrato entre os módulos é o esquema em
db/. Os preços são `Decimal`, como o `numeric(12,2)` devolvido pelo psycopg, e a ausência de
observação num dia da série é `None`, distinta de qualquer valor, inclusive zero.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class Categoria:
    """Categoria cadastrada: código vindo da FakeStore e descrição exibida."""

    codigo: str
    descricao: str


@dataclass(frozen=True)
class MetricaCategoria:
    """Linha do snapshot de uma data para uma categoria, com as quatro métricas publicadas."""

    categoria: Categoria
    preco_medio: Decimal
    preco_minimo: Decimal
    preco_maximo: Decimal
    quantidade_produtos: int


@dataclass(frozen=True)
class MetricasDaData:
    """Snapshot de uma data. `metricas` vazia indica, de forma explícita, dia sem registros."""

    data: date
    metricas: tuple[MetricaCategoria, ...]

    @property
    def vazia(self) -> bool:
        return not self.metricas


@dataclass(frozen=True)
class ObservacaoPreco:
    """Preço médio publicado no snapshot de uma data, como o repositório o devolve."""

    data: date
    preco_medio: Decimal


@dataclass(frozen=True)
class PontoEvolucao:
    """Um dia do intervalo. `preco_medio` é `None` quando o snapshot não tem a categoria no dia."""

    data: date
    preco_medio: Decimal | None


@dataclass(frozen=True)
class SerieEvolucao:
    """Série diária do preço médio de uma categoria, um ponto por dia do intervalo inclusivo."""

    categoria: Categoria
    inicio: date
    fim: date
    pontos: tuple[PontoEvolucao, ...]
