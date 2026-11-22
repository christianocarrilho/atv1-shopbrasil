"""Modelos HTTP da API JSON (#9).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Traduzem os modelos de leitura de `consultas` para o JSON documentado em web/README.md. Preços
viajam como strings decimais com duas casas ("10.00"), e não como número JSON, para que o
navegador não os converta em float e a página exiba o mesmo valor publicado. A ausência de
observação na série é `null`, distinta de "0.00".
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from shopbrasil_web.consultas.modelos import Categoria, MetricaCategoria, MetricasDaData, SerieEvolucao

_DUAS_CASAS = Decimal("0.01")


def decimal_duas_casas(valor: Decimal) -> str:
    """Formata um `numeric(12,2)` como string, sem float e sem arredondar o valor publicado."""
    if valor.as_tuple().exponent != -2:
        # numeric(12,2) sempre chega com escala 2; outra escala indica valor fora do contrato.
        if valor != valor.quantize(_DUAS_CASAS):
            raise ValueError(f"preço {valor} com mais de duas casas")
        valor = valor.quantize(_DUAS_CASAS)
    return f"{valor:f}"


class CategoriaHttp(BaseModel):
    codigo: str = Field(description="Código da categoria, igual ao `category` da FakeStore.")
    descricao: str = Field(description="Texto exibido.")

    @classmethod
    def de(cls, categoria: Categoria) -> CategoriaHttp:
        return cls(codigo=categoria.codigo, descricao=categoria.descricao)


class MetricaHttp(BaseModel):
    categoria: CategoriaHttp
    preco_medio: str = Field(examples=["10.00"], description="Decimal com duas casas, como string.")
    preco_minimo: str = Field(examples=["10.00"])
    preco_maximo: str = Field(examples=["10.01"])
    quantidade_produtos: int = Field(examples=[3])

    @classmethod
    def de(cls, metrica: MetricaCategoria) -> MetricaHttp:
        return cls(
            categoria=CategoriaHttp.de(metrica.categoria),
            preco_medio=decimal_duas_casas(metrica.preco_medio),
            preco_minimo=decimal_duas_casas(metrica.preco_minimo),
            preco_maximo=decimal_duas_casas(metrica.preco_maximo),
            quantidade_produtos=metrica.quantidade_produtos,
        )


class MetricasDaDataHttp(BaseModel):
    data: str = Field(examples=["2026-11-21"], description="Data consultada, ISO 8601.")
    vazia: bool = Field(description="true quando a data não tem registros no snapshot.")
    metricas: list[MetricaHttp] = Field(description="Uma linha por categoria, em ordem de código.")

    @classmethod
    def de(cls, resultado: MetricasDaData) -> MetricasDaDataHttp:
        return cls(
            data=resultado.data.isoformat(),
            vazia=resultado.vazia,
            metricas=[MetricaHttp.de(m) for m in resultado.metricas],
        )


class UltimaDataHttp(BaseModel):
    data: str | None = Field(examples=["2026-11-21"], description="null quando não há métricas publicadas.")


class CategoriasHttp(BaseModel):
    categorias: list[CategoriaHttp]


class PontoHttp(BaseModel):
    data: str = Field(examples=["2026-11-21"])
    preco_medio: str | None = Field(examples=["10.00", None], description="null = dia sem observação (lacuna).")


class EvolucaoHttp(BaseModel):
    categoria: CategoriaHttp
    inicio: str
    fim: str
    pontos: list[PontoHttp] = Field(description="Um ponto por dia de [inicio, fim], em ordem crescente de data.")

    @classmethod
    def de(cls, serie: SerieEvolucao) -> EvolucaoHttp:
        return cls(
            categoria=CategoriaHttp.de(serie.categoria),
            inicio=serie.inicio.isoformat(),
            fim=serie.fim.isoformat(),
            pontos=[
                PontoHttp(
                    data=p.data.isoformat(),
                    preco_medio=None if p.preco_medio is None else decimal_duas_casas(p.preco_medio),
                )
                for p in serie.pontos
            ],
        )


class ErroHttp(BaseModel):
    detail: str
