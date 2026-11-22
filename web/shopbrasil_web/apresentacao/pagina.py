"""Página do painel de métricas (#19).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Rota ``GET /`` renderizada no servidor com Jinja2. Recebe ``data`` (AAAA-MM-DD) e, sem ela, abre na
data mais recente com métricas. Usa os mesmos casos de uso e modelos HTTP da API JSON, por isso a
tabela mostra exatamente os valores do contrato da #9 (strings decimais, sem float). A coleção
carregada vai também embutida como JSON (``#metricas-dados``) para as barras da #20, e a seção
``#evolucao`` é o ponto de inserção da linha da #21. Sem SQL: os casos de uso chegam da composição.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from shopbrasil_web.apresentacao.controllers import CasosDeUso, converter_data
from shopbrasil_web.apresentacao.modelos_http import MetricasDaDataHttp

PASTA_TEMPLATES = Path(__file__).parent / "templates"
SEM_METRICAS = "Sem métricas para esta data"
SEM_PUBLICACAO = "Nenhuma métrica publicada até o momento"


def criar_roteador_pagina(casos: CasosDeUso) -> APIRouter:
    roteador = APIRouter(include_in_schema=False)
    templates = Jinja2Templates(directory=PASTA_TEMPLATES)

    @roteador.get("/", response_class=HTMLResponse)
    def painel(request: Request, data: Annotated[str | None, Query()] = None) -> HTMLResponse:
        if data:
            selecionada = converter_data("data", data)
        else:
            selecionada = casos.ultima_data.executar()
        metricas = None
        if selecionada is not None:
            metricas = MetricasDaDataHttp.de(casos.metricas_da_data.executar(selecionada))
        return templates.TemplateResponse(request, "painel.html", {
            "data": None if selecionada is None else selecionada.isoformat(),
            "metricas": metricas,
            "metricas_json": None if metricas is None else metricas.model_dump(mode="json"),
            "sem_metricas": SEM_METRICAS,
            "sem_publicacao": SEM_PUBLICACAO,
        })

    return roteador
