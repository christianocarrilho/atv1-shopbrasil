"""Testes da API JSON (#9): controllers, modelos HTTP e composição.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Chamam a aplicação ASGI diretamente, com um driver mínimo em asyncio, porque o TestClient do
Starlette exige httpx, que não faz parte da pilha aprovada na #1. Precisam de FastAPI e psycopg
e são pulados sem eles; rodam no contêiner da web:
``docker compose run --rm --no-deps web python -m unittest discover -s tests -t .``.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import unittest
from datetime import date
from urllib.parse import urlencode

from tests.apoio import ELETRONICOS, JOIAS, RepositorioEmMemoria, metrica

TEM_FASTAPI = importlib.util.find_spec("fastapi") is not None
TEM_PSYCOPG = importlib.util.find_spec("psycopg") is not None


def chamar(aplicacao, caminho: str, **query: str) -> tuple[int, object]:
    """Executa um GET na aplicação ASGI e devolve status e corpo JSON."""
    mensagens: list[dict] = []

    async def receive() -> dict:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(mensagem: dict) -> None:
        mensagens.append(mensagem)

    escopo = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "GET",
        "scheme": "http", "path": caminho, "raw_path": caminho.encode(), "root_path": "",
        "query_string": urlencode(query).encode(), "headers": [(b"host", b"teste")],
        "client": ("127.0.0.1", 1), "server": ("teste", 80),
    }
    asyncio.run(aplicacao(escopo, receive, send))
    inicio = next(m for m in mensagens if m["type"] == "http.response.start")
    corpo = b"".join(m.get("body", b"") for m in mensagens if m["type"] == "http.response.body")
    return inicio["status"], json.loads(corpo)


@unittest.skipUnless(TEM_FASTAPI and TEM_PSYCOPG, "FastAPI e psycopg ficam no contêiner da web")
class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        from shopbrasil_web.apresentacao.aplicacao import criar_aplicacao
        from shopbrasil_web.composicao import criar_casos_de_uso

        self.repositorio = RepositorioEmMemoria({
            date(2026, 11, 20): [metrica(ELETRONICOS, "20.00", "15.00", "25.00", 2)],
            date(2026, 11, 22): [
                metrica(JOIAS, "10.01", "10.00", "10.01", 2),
                metrica(ELETRONICOS, "10.00", "10.00", "10.01", 3),
            ],
        })
        self.app = criar_aplicacao(criar_casos_de_uso(self.repositorio))

    def test_metricas_da_data(self) -> None:
        status, corpo = chamar(self.app, "/api/metricas", data="2026-11-22")
        self.assertEqual(200, status)
        self.assertEqual({
            "data": "2026-11-22",
            "vazia": False,
            "metricas": [
                {"categoria": {"codigo": "electronics", "descricao": "electronics"},
                 "preco_medio": "10.00", "preco_minimo": "10.00", "preco_maximo": "10.01",
                 "quantidade_produtos": 3},
                {"categoria": {"codigo": "jewelery", "descricao": "jewelery"},
                 "preco_medio": "10.01", "preco_minimo": "10.00", "preco_maximo": "10.01",
                 "quantidade_produtos": 2},
            ],
        }, corpo)

    def test_data_sem_registros(self) -> None:
        self.assertEqual((200, {"data": "2026-11-21", "vazia": True, "metricas": []}),
                         chamar(self.app, "/api/metricas", data="2026-11-21"))

    def test_data_invalida_ou_ausente_422(self) -> None:
        for valor in ("2026-02-30", "21/11/2026", "1763683200", "2026-11-21T00:00:00"):
            with self.subTest(valor=valor):
                status, corpo = chamar(self.app, "/api/metricas", data=valor)
                self.assertEqual(422, status)
                self.assertEqual(["query", "data"], corpo["detail"][0]["loc"])
                self.assertIn("AAAA-MM-DD", corpo["detail"][0]["msg"])
        status, corpo = chamar(self.app, "/api/metricas")
        self.assertEqual(422, status)
        self.assertEqual(["query", "data"], corpo["detail"][0]["loc"])
        self.assertEqual([], [c for c in self.repositorio.chamadas if c[0] == "metricas_da_data"])

    def test_ultima_data(self) -> None:
        self.assertEqual((200, {"data": "2026-11-22"}), chamar(self.app, "/api/metricas/ultima-data"))

    def test_ultima_data_sem_publicacao(self) -> None:
        from shopbrasil_web.apresentacao.aplicacao import criar_aplicacao
        from shopbrasil_web.composicao import criar_casos_de_uso

        app = criar_aplicacao(criar_casos_de_uso(RepositorioEmMemoria()))
        self.assertEqual((200, {"data": None}), chamar(app, "/api/metricas/ultima-data"))

    def test_categorias(self) -> None:
        self.assertEqual((200, {"categorias": [
            {"codigo": "electronics", "descricao": "electronics"},
            {"codigo": "jewelery", "descricao": "jewelery"},
        ]}), chamar(self.app, "/api/categorias"))

    def test_evolucao_com_lacunas_em_ordem(self) -> None:
        status, corpo = chamar(self.app, "/api/evolucao", categoria="electronics", inicio="2026-11-20", fim="2026-11-23")
        self.assertEqual(200, status)
        self.assertEqual({
            "categoria": {"codigo": "electronics", "descricao": "electronics"},
            "inicio": "2026-11-20",
            "fim": "2026-11-23",
            "pontos": [
                {"data": "2026-11-20", "preco_medio": "20.00"},
                {"data": "2026-11-21", "preco_medio": None},
                {"data": "2026-11-22", "preco_medio": "10.00"},
                {"data": "2026-11-23", "preco_medio": None},
            ],
        }, corpo)

    def test_evolucao_intervalo_invertido_400(self) -> None:
        status, corpo = chamar(self.app, "/api/evolucao", categoria="electronics", inicio="2026-11-22", fim="2026-11-20")
        self.assertEqual(400, status)
        self.assertIn("depois do fim", corpo["detail"])

    def test_evolucao_categoria_desconhecida_404(self) -> None:
        status, corpo = chamar(self.app, "/api/evolucao", categoria="toys", inicio="2026-11-20", fim="2026-11-22")
        self.assertEqual((404, "categoria 'toys' não cadastrada"), (status, corpo["detail"]))

    def test_evolucao_parametros_invalidos_422(self) -> None:
        status, corpo = chamar(self.app, "/api/evolucao", categoria="electronics", inicio="2026-11-20")
        self.assertEqual(422, status)
        self.assertEqual(["query", "fim"], corpo["detail"][0]["loc"])
        status, corpo = chamar(self.app, "/api/evolucao", categoria="electronics", inicio="ontem", fim="2026-11-22")
        self.assertEqual((422, ["query", "inicio"]), (status, corpo["detail"][0]["loc"]))

    def test_openapi_documenta_as_rotas(self) -> None:
        status, corpo = chamar(self.app, "/openapi.json")
        self.assertEqual(200, status)
        self.assertTrue({"/api/metricas", "/api/metricas/ultima-data", "/api/categorias", "/api/evolucao"}
                        <= set(corpo["paths"]))


@unittest.skipUnless(TEM_FASTAPI and TEM_PSYCOPG, "FastAPI e psycopg ficam no contêiner da web")
class ModelosHttpTest(unittest.TestCase):
    def test_decimal_sem_float(self) -> None:
        from decimal import Decimal

        from shopbrasil_web.apresentacao.modelos_http import decimal_duas_casas

        self.assertEqual("10.00", decimal_duas_casas(Decimal("10.00")))
        self.assertEqual("9999999999.99", decimal_duas_casas(Decimal("9999999999.99")))
        self.assertEqual("10.00", decimal_duas_casas(Decimal("10")))
        self.assertEqual("0.10", decimal_duas_casas(Decimal("0.1")))
        with self.assertRaises(ValueError):
            decimal_duas_casas(Decimal("10.005"))


@unittest.skipUnless(TEM_FASTAPI and TEM_PSYCOPG, "FastAPI e psycopg ficam no contêiner da web")
class ComposicaoTest(unittest.TestCase):
    AMBIENTE = {
        "SHOPBRASIL_DB_HOST": "postgres", "SHOPBRASIL_DB_PORT": "5432", "SHOPBRASIL_DB_NAME": "shopbrasil",
        "SHOPBRASIL_DB_READER_USER": "shopbrasil_reader", "SHOPBRASIL_DB_READER_PASSWORD": "segredo",
        "SHOPBRASIL_DB_WRITER_USER": "shopbrasil_writer", "SHOPBRASIL_DB_WRITER_PASSWORD": "outro",
    }

    def test_usa_a_conta_de_leitura(self) -> None:
        from shopbrasil_web.composicao import configuracao_do_ambiente

        configuracao = configuracao_do_ambiente(self.AMBIENTE)
        self.assertEqual(("postgres", 5432, "shopbrasil", "shopbrasil_reader", "segredo"),
                         (configuracao.host, configuracao.porta, configuracao.banco,
                          configuracao.usuario, configuracao.senha))
        self.assertNotIn("segredo", repr(configuracao))

    def test_variavel_ausente(self) -> None:
        from shopbrasil_web.composicao import ConfiguracaoAusenteError, configuracao_do_ambiente

        ambiente = dict(self.AMBIENTE, SHOPBRASIL_DB_READER_PASSWORD="")
        with self.assertRaisesRegex(ConfiguracaoAusenteError, "SHOPBRASIL_DB_READER_PASSWORD"):
            configuracao_do_ambiente(ambiente)

    def test_cria_app_sem_conectar(self) -> None:
        from shopbrasil_web.composicao import criar_app

        app = criar_app(self.AMBIENTE)
        self.assertIn("/api/evolucao", app.openapi()["paths"])


if __name__ == "__main__":
    unittest.main()
