"""Página do painel de métricas (#19), com barras (#20) e linha de evolução (#21).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Rota ``GET /`` renderizada no servidor com Jinja2. Recebe ``data`` (AAAA-MM-DD) e, sem ela, abre na
data mais recente com métricas. Usa os mesmos casos de uso e modelos HTTP da API JSON, por isso a
tabela mostra exatamente os valores do contrato da #9 (strings decimais, sem float). A coleção
carregada vai também embutida como JSON (``#metricas-dados``) para as barras da #20, e a seção
``#evolucao`` traz a linha da #21: filtros ``categoria``, ``inicio`` e ``fim`` no mesmo formulário,
com padrão na primeira categoria cadastrada e nos 30 dias que terminam na data selecionada. A série
vem do mesmo caso de uso de ``/api/evolucao``; dias sem observação são ``null`` e viram lacunas.
Intervalo inválido ou categoria desconhecida aparecem como aviso na seção, sem derrubar a tela.
Sem SQL: os casos de uso chegam da composição.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from shopbrasil_web.apresentacao.controllers import CasosDeUso, converter_data
from shopbrasil_web.apresentacao.modelos_http import CategoriaHttp, EvolucaoHttp, MetricasDaDataHttp
from shopbrasil_web.consultas.casos_uso import CategoriaNaoEncontradaError, IntervaloInvalidoError

PASTA_TEMPLATES = Path(__file__).parent / "templates"
SEM_METRICAS = "Sem métricas para esta data"
SEM_PUBLICACAO = "Nenhuma métrica publicada até o momento"
DIAS_PADRAO_EVOLUCAO = 30


def criar_roteador_pagina(casos: CasosDeUso) -> APIRouter:
    roteador = APIRouter(include_in_schema=False)
    templates = Jinja2Templates(directory=PASTA_TEMPLATES)

    def evolucao(categorias, categoria: str | None, inicio: str | None, fim: str | None,
                 selecionada: date | None) -> dict[str, object]:
        """Filtros efetivos e série da #21; o aviso substitui a série quando o filtro não vale."""
        data_fim = converter_data("fim", fim) if fim else (selecionada or date.today())
        # Perto de date.min o padrão começa no primeiro dia representável, sem OverflowError.
        dias_antes = min(DIAS_PADRAO_EVOLUCAO - 1, (data_fim - date.min).days)
        data_inicio = converter_data("inicio", inicio) if inicio else data_fim - timedelta(days=dias_antes)
        codigo = categoria or (categorias[0].codigo if categorias else None)
        filtros = {"categoria": codigo, "inicio": data_inicio.isoformat(), "fim": data_fim.isoformat()}
        if codigo is None:
            return {**filtros, "serie": None, "aviso": "Nenhuma categoria cadastrada"}
        try:
            serie = EvolucaoHttp.de(casos.evolucao.executar(codigo, data_inicio, data_fim))
        except IntervaloInvalidoError as erro:
            return {**filtros, "serie": None, "aviso": f"Intervalo inválido: {erro}"}
        except CategoriaNaoEncontradaError as erro:
            mensagem = str(erro)
            return {**filtros, "serie": None, "aviso": mensagem[:1].upper() + mensagem[1:]}
        return {**filtros, "serie": serie.model_dump(mode="json"), "aviso": None}

    @roteador.get("/", response_class=HTMLResponse)
    def painel(
        request: Request,
        data: Annotated[str | None, Query()] = None,
        categoria: Annotated[str | None, Query()] = None,
        inicio: Annotated[str | None, Query()] = None,
        fim: Annotated[str | None, Query()] = None,
    ) -> HTMLResponse:
        if data:
            selecionada = converter_data("data", data)
        else:
            selecionada = casos.ultima_data.executar()
        metricas = None
        if selecionada is not None:
            metricas = MetricasDaDataHttp.de(casos.metricas_da_data.executar(selecionada))
        categorias = [CategoriaHttp.de(c) for c in casos.categorias.executar()]
        return templates.TemplateResponse(request, "painel.html", {
            "data": None if selecionada is None else selecionada.isoformat(),
            "metricas": metricas,
            "metricas_json": None if metricas is None else metricas.model_dump(mode="json"),
            "sem_metricas": SEM_METRICAS,
            "sem_publicacao": SEM_PUBLICACAO,
            "categorias": categorias,
            "evolucao": evolucao(categorias, categoria, inicio, fim, selecionada),
        })

    return roteador
