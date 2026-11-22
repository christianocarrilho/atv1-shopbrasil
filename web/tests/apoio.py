"""Apoio aos testes da Web/API (#9): repositório em memória.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Implementa o contrato `RepositorioMetricas` sem banco, para testar casos de uso e controllers.
Devolve as linhas fora de ordem de propósito, para que os testes provem que a ordenação é do
caso de uso.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from shopbrasil_web.consultas.modelos import Categoria, MetricaCategoria, ObservacaoPreco

ELETRONICOS = Categoria("electronics", "electronics")
JOIAS = Categoria("jewelery", "jewelery")


def metrica(categoria: Categoria, medio: str, minimo: str, maximo: str, n: int) -> MetricaCategoria:
    return MetricaCategoria(categoria, Decimal(medio), Decimal(minimo), Decimal(maximo), n)


class RepositorioEmMemoria:
    def __init__(self, snapshot: dict[date, list[MetricaCategoria]] | None = None,
                 categorias: list[Categoria] | None = None) -> None:
        self.snapshot = snapshot or {}
        self._categorias = categorias if categorias is not None else [JOIAS, ELETRONICOS]
        self.chamadas: list[tuple] = []

    def metricas_da_data(self, data: date) -> list[MetricaCategoria]:
        self.chamadas.append(("metricas_da_data", data))
        return list(reversed(sorted(self.snapshot.get(data, []), key=lambda m: m.categoria.codigo)))

    def ultima_data(self) -> date | None:
        self.chamadas.append(("ultima_data",))
        return max(self.snapshot) if self.snapshot else None

    def categorias(self) -> list[Categoria]:
        self.chamadas.append(("categorias",))
        return list(self._categorias)

    def categoria(self, codigo: str) -> Categoria | None:
        self.chamadas.append(("categoria", codigo))
        return next((c for c in self._categorias if c.codigo == codigo), None)

    def precos_medios(self, codigo: str, inicio: date, fim: date) -> list[ObservacaoPreco]:
        self.chamadas.append(("precos_medios", codigo, inicio, fim))
        pontos = [
            ObservacaoPreco(dia, m.preco_medio)
            for dia, linhas in self.snapshot.items()
            for m in linhas
            if m.categoria.codigo == codigo and inicio <= dia <= fim
        ]
        return sorted(pontos, key=lambda p: p.data, reverse=True)
