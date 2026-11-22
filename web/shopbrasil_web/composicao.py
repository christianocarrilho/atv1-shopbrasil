"""Ponto de composição da Web/API (#9).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Lê a configuração do ambiente, cria o repositório PostgreSQL com a conta de leitura, os casos de
uso e a aplicação FastAPI. É o único ponto que conhece as três camadas. O uvicorn usa
`shopbrasil_web.composicao:criar_app --factory`, para que nada se conecte ao banco na importação.
A web recebe só `SHOPBRASIL_DB_READER_*`; a conta de escrita não entra no contêiner.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

from fastapi import FastAPI

from shopbrasil_web.apresentacao.aplicacao import criar_aplicacao
from shopbrasil_web.apresentacao.controllers import CasosDeUso
from shopbrasil_web.consultas.casos_uso import (
    ConsultarEvolucao,
    ConsultarMetricasDaData,
    ConsultarUltimaData,
    ListarCategorias,
)
from shopbrasil_web.consultas.repositorio import RepositorioMetricas
from shopbrasil_web.infraestrutura.postgres import ConfiguracaoBanco, RepositorioMetricasPostgres


class ConfiguracaoAusenteError(RuntimeError):
    """Variável de ambiente obrigatória ausente ou vazia."""


def configuracao_do_ambiente(ambiente: Mapping[str, str] | None = None) -> ConfiguracaoBanco:
    ambiente = os.environ if ambiente is None else ambiente

    def obrigatoria(nome: str) -> str:
        valor = ambiente.get(nome, "").strip()
        if not valor:
            raise ConfiguracaoAusenteError(f"defina {nome} no ambiente da Web/API")
        return valor

    porta = obrigatoria("SHOPBRASIL_DB_PORT")
    if not porta.isdigit():
        raise ConfiguracaoAusenteError(f"SHOPBRASIL_DB_PORT inválida: {porta!r}")
    return ConfiguracaoBanco(
        host=obrigatoria("SHOPBRASIL_DB_HOST"),
        porta=int(porta),
        banco=obrigatoria("SHOPBRASIL_DB_NAME"),
        usuario=obrigatoria("SHOPBRASIL_DB_READER_USER"),
        senha=obrigatoria("SHOPBRASIL_DB_READER_PASSWORD"),
    )


def criar_casos_de_uso(repositorio: RepositorioMetricas) -> CasosDeUso:
    return CasosDeUso(
        metricas_da_data=ConsultarMetricasDaData(repositorio),
        ultima_data=ConsultarUltimaData(repositorio),
        categorias=ListarCategorias(repositorio),
        evolucao=ConsultarEvolucao(repositorio),
    )


def criar_app(ambiente: Mapping[str, str] | None = None) -> FastAPI:
    repositorio = RepositorioMetricasPostgres(configuracao_do_ambiente(ambiente))
    return criar_aplicacao(criar_casos_de_uso(repositorio))
