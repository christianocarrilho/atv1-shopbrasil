"""Controllers da API JSON (#9).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Recebem e validam os filtros HTTP, chamam os casos de uso de `consultas` e traduzem o resultado
para os modelos HTTP. Não têm SQL nem acesso ao banco: os casos de uso chegam prontos pela
composição. As datas são aceitas só no formato AAAA-MM-DD, porque o validador padrão de `date`
também aceitaria timestamps numéricos, o que deixaria o contrato ambíguo para as #19–#21.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from shopbrasil_web.apresentacao.modelos_http import (
    CategoriaHttp,
    CategoriasHttp,
    ErroHttp,
    EvolucaoHttp,
    MetricasDaDataHttp,
    UltimaDataHttp,
)
from shopbrasil_web.consultas.casos_uso import (
    CategoriaNaoEncontradaError,
    ConsultarEvolucao,
    ConsultarMetricasDaData,
    ConsultarUltimaData,
    IntervaloInvalidoError,
    ListarCategorias,
)

_FORMATO_DATA = re.compile(r"\d{4}-\d{2}-\d{2}")
_DESCRICAO_DATA = "Data ISO 8601 no formato AAAA-MM-DD."


@dataclass(frozen=True)
class CasosDeUso:
    """Casos de uso injetados pela composição e compartilhados pela API e pela página."""

    metricas_da_data: ConsultarMetricasDaData
    ultima_data: ConsultarUltimaData
    categorias: ListarCategorias
    evolucao: ConsultarEvolucao


def converter_data(parametro: str, valor: str) -> date:
    """Converte um filtro AAAA-MM-DD; responde 422 com o parâmetro e o valor recebido."""
    try:
        if not _FORMATO_DATA.fullmatch(valor):
            raise ValueError
        return date.fromisoformat(valor)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=[{
                "loc": ["query", parametro],
                "msg": f"data inválida {valor!r}; use AAAA-MM-DD",
                "type": "data_invalida",
            }],
        ) from None


def criar_roteador_api(casos: CasosDeUso) -> APIRouter:
    roteador = APIRouter(prefix="/api", tags=["métricas"])

    @roteador.get(
        "/metricas",
        response_model=MetricasDaDataHttp,
        summary="Métricas do snapshot de uma data",
        description="Uma linha por categoria publicada na data. Sem registros: `vazia=true` e `metricas=[]`.",
    )
    def metricas_da_data(data: Annotated[str, Query(description=_DESCRICAO_DATA)]) -> MetricasDaDataHttp:
        return MetricasDaDataHttp.de(casos.metricas_da_data.executar(converter_data("data", data)))

    @roteador.get(
        "/metricas/ultima-data",
        response_model=UltimaDataHttp,
        summary="Data mais recente com métricas",
    )
    def ultima_data() -> UltimaDataHttp:
        data = casos.ultima_data.executar()
        return UltimaDataHttp(data=None if data is None else data.isoformat())

    @roteador.get("/categorias", response_model=CategoriasHttp, summary="Categorias cadastradas")
    def categorias() -> CategoriasHttp:
        return CategoriasHttp(categorias=[CategoriaHttp.de(c) for c in casos.categorias.executar()])

    @roteador.get(
        "/evolucao",
        response_model=EvolucaoHttp,
        summary="Evolução diária do preço médio de uma categoria",
        description="Um ponto por dia do intervalo inclusivo, em ordem de data; `preco_medio=null` nos dias sem dados.",
        responses={400: {"model": ErroHttp}, 404: {"model": ErroHttp}},
    )
    def evolucao(
        categoria: Annotated[str, Query(min_length=1, description="Código da categoria.")],
        inicio: Annotated[str, Query(description=_DESCRICAO_DATA)],
        fim: Annotated[str, Query(description=_DESCRICAO_DATA)],
    ) -> EvolucaoHttp:
        data_inicio = converter_data("inicio", inicio)
        data_fim = converter_data("fim", fim)
        try:
            serie = casos.evolucao.executar(categoria, data_inicio, data_fim)
        except IntervaloInvalidoError as erro:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"intervalo inválido: {erro}") from None
        except CategoriaNaoEncontradaError as erro:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(erro)) from None
        return EvolucaoHttp.de(serie)

    return roteador
