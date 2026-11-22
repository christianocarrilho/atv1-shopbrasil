"""Aplicação FastAPI da ShopBrasil (#9).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Monta a aplicação a partir dos casos de uso recebidos da composição. A documentação OpenAPI fica
em /docs e /openapi.json. A página do painel (#19) fica em / na mesma aplicação e chama os mesmos
casos de uso.
"""

from __future__ import annotations

from fastapi import FastAPI

from shopbrasil_web.apresentacao.controllers import CasosDeUso, criar_roteador_api
from shopbrasil_web.apresentacao.pagina import criar_roteador_pagina


def criar_aplicacao(casos: CasosDeUso) -> FastAPI:
    aplicacao = FastAPI(
        title="ShopBrasil Web/API",
        version="1.0.0",
        description="Leitura do snapshot diário de métricas por categoria publicado pelo pipeline.",
    )
    aplicacao.include_router(criar_roteador_api(casos))
    aplicacao.include_router(criar_roteador_pagina(casos))
    return aplicacao
