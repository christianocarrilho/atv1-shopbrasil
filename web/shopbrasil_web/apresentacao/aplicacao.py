"""Aplicação FastAPI da ShopBrasil (#9).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Monta a aplicação a partir dos casos de uso recebidos da composição. A documentação OpenAPI fica
em /docs e /openapi.json. A página do painel (#19) fica em / na mesma aplicação e chama os mesmos
casos de uso. O Chart.js 4.6.1 das barras (#20) é servido de ``estaticos/`` em /estaticos, sem CDN.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from shopbrasil_web.apresentacao.controllers import CasosDeUso, criar_roteador_api
from shopbrasil_web.apresentacao.pagina import criar_roteador_pagina


PASTA_ESTATICOS = Path(__file__).parent / "estaticos"


def criar_aplicacao(casos: CasosDeUso) -> FastAPI:
    aplicacao = FastAPI(
        title="ShopBrasil Web/API",
        version="1.0.0",
        description="Leitura do snapshot diário de métricas por categoria publicado pelo pipeline.",
    )
    aplicacao.include_router(criar_roteador_api(casos))
    aplicacao.include_router(criar_roteador_pagina(casos))
    aplicacao.mount("/estaticos", StaticFiles(directory=PASTA_ESTATICOS), name="estaticos")
    return aplicacao
