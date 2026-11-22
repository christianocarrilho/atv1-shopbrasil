"""Agrupamento por categoria e consolidação das métricas (#16).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Separa os produtos validados pelo código observado na coleta, uma entrada por categoria para o
fan-out do DAG, e junta os resultados do fan-in numa coleção com um item por categoria, no
contrato da #15. As categorias vêm só dos dados da coleta, sem lista fixa, e as duas operações
ordenam pelo código para que a saída não dependa da ordem do catálogo nem da conclusão das tasks.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from decimal import Decimal

from shopbrasil_pipeline.dominio.metricas import MetricasCategoria, PrecosCategoria


class ConsolidacaoInvalidaError(ValueError):
    """Os resultados do fan-in não têm exatamente um item por categoria."""


def agrupar_por_categoria(produtos: Iterable[Mapping[str, str]]) -> tuple[PrecosCategoria, ...]:
    """Agrupa ``{codigo_categoria, preco}`` validados em uma entrada por código observado."""
    precos: dict[str, list[Decimal]] = defaultdict(list)
    for produto in produtos:
        precos[produto["codigo_categoria"]].append(Decimal(produto["preco"]))
    return tuple(PrecosCategoria(codigo, tuple(valores)) for codigo, valores in sorted(precos.items()))


def consolidar_metricas(
    resultados: Iterable[MetricasCategoria], esperadas: Iterable[str] | None = None
) -> tuple[MetricasCategoria, ...]:
    """Junta os resultados mapeados, exigindo um por categoria e, se informado, todas as esperadas."""
    por_codigo: dict[str, MetricasCategoria] = {}
    for resultado in resultados:
        if resultado.codigo_categoria in por_codigo:
            raise ConsolidacaoInvalidaError(f"categoria {resultado.codigo_categoria!r} calculada mais de uma vez")
        por_codigo[resultado.codigo_categoria] = resultado
    if esperadas is not None:
        faltantes = sorted(set(esperadas) - set(por_codigo))
        sobrando = sorted(set(por_codigo) - set(esperadas))
        if faltantes or sobrando:
            raise ConsolidacaoInvalidaError(f"fan-in incompleto: faltam {faltantes}, sobram {sobrando}")
    if not por_codigo:
        raise ConsolidacaoInvalidaError("nenhum resultado para consolidar")
    return tuple(por_codigo[codigo] for codigo in sorted(por_codigo))
