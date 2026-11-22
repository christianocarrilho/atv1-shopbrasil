"""Contrato do repositório de leitura das métricas (#9).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Os casos de uso dependem deste Protocol, e não do PostgreSQL: a infraestrutura o implementa e a
composição injeta o adaptador concreto. Todas as leituras vêm do snapshot diário.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Protocol

from shopbrasil_web.consultas.modelos import Categoria, MetricaCategoria, ObservacaoPreco


class RepositorioMetricas(Protocol):
    def metricas_da_data(self, data: date) -> Sequence[MetricaCategoria]:
        """Linhas do snapshot da data, em qualquer ordem; vazia se o dia não tem registros."""
        ...

    def ultima_data(self) -> date | None:
        """Data mais recente com linhas no snapshot, ou `None` se o snapshot está vazio."""
        ...

    def categorias(self) -> Sequence[Categoria]:
        """Categorias cadastradas, em qualquer ordem."""
        ...

    def categoria(self, codigo: str) -> Categoria | None:
        """Categoria pelo código exato, ou `None` se não estiver cadastrada."""
        ...

    def precos_medios(self, codigo: str, inicio: date, fim: date) -> Sequence[ObservacaoPreco]:
        """Preço médio publicado da categoria em cada data com registro em [inicio, fim]."""
        ...
