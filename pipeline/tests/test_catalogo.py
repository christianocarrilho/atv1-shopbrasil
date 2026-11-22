"""Testes da projeção do catálogo e do adaptador HTTP da FakeStore (#13).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

O adaptador é testado contra um servidor HTTP local da biblioteca padrão, com respostas
controladas: sucesso, erro HTTP, JSON inválido, conexão recusada e timeout.
"""

from __future__ import annotations

import json
import socket
import threading
import time
import unittest
import urllib.error
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from shopbrasil_pipeline.aplicacao.catalogo import CatalogoColetado, coletar_catalogo, projetar_catalogo
from shopbrasil_pipeline.infraestrutura.fakestore import FakeStoreHttp

CATALOGO = (
    b'[{"id": 1, "title": "Mochila", "price": 109.95, "category": "men\'s clothing", "description": "x"},'
    b' {"id": 5, "title": "Pulseira", "price": 695, "category": "jewelery"}]'
)


class Servidor:
    """Servidor local que responde ``/products`` conforme ``resposta`` (status, corpo, atraso)."""

    def __init__(self) -> None:
        self.resposta = (200, CATALOGO, 0.0)
        self.caminhos: list[str] = []
        servidor = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                servidor.caminhos.append(self.path)
                status, corpo, atraso = servidor.resposta
                time.sleep(atraso)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(corpo)

            def log_message(self, *args):
                pass

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.http.server_address[1]}/"
        threading.Thread(target=self.http.serve_forever, daemon=True).start()

    def fechar(self) -> None:
        self.http.shutdown()
        self.http.server_close()


class FakeStoreHttpTest(unittest.TestCase):
    def setUp(self):
        self.servidor = Servidor()
        self.addCleanup(self.servidor.fechar)

    def test_busca_products_com_decimal(self):
        resposta = FakeStoreHttp(self.servidor.url).buscar_produtos()
        self.assertEqual(self.servidor.caminhos, ["/products"])
        self.assertEqual(resposta[0]["price"], Decimal("109.95"))
        self.assertEqual(resposta[1]["price"], Decimal("695"))
        self.assertIsInstance(resposta[1]["price"], Decimal)

    def test_timeout_padrao_de_15_segundos(self):
        self.assertEqual(FakeStoreHttp(self.servidor.url).timeout, 15)

    def test_propaga_erro_http(self):
        self.servidor.resposta = (503, b"indisponivel", 0.0)
        with self.assertRaises(urllib.error.HTTPError):
            FakeStoreHttp(self.servidor.url).buscar_produtos()

    def test_propaga_json_invalido(self):
        self.servidor.resposta = (200, b"<html>", 0.0)
        with self.assertRaises(json.JSONDecodeError):
            FakeStoreHttp(self.servidor.url).buscar_produtos()

    def test_propaga_timeout(self):
        self.servidor.resposta = (200, CATALOGO, 1.0)
        with self.assertRaises((TimeoutError, socket.timeout, urllib.error.URLError)):
            FakeStoreHttp(self.servidor.url, timeout=0.2).buscar_produtos()

    def test_propaga_conexao_recusada(self):
        with socket.socket() as livre:
            livre.bind(("127.0.0.1", 0))
            porta = livre.getsockname()[1]
        with self.assertRaises(urllib.error.URLError):
            FakeStoreHttp(f"http://127.0.0.1:{porta}").buscar_produtos()

    def test_rejeita_base_vazia(self):
        with self.assertRaises(ValueError):
            FakeStoreHttp("")


class ProjecaoTest(unittest.TestCase):
    def test_projecao_pequena_e_serializavel(self):
        class Fonte:
            def buscar_produtos(self):
                return json.loads(CATALOGO, parse_float=Decimal, parse_int=Decimal)

        catalogo = coletar_catalogo(Fonte())
        xcom = json.loads(json.dumps(catalogo.para_xcom()))
        self.assertEqual(xcom["tipo_raiz"], "array")
        self.assertEqual(
            xcom["produtos"],
            [
                {"id": "1", "categoria": "men's clothing", "tipo_categoria": "string", "preco": "109.95", "tipo_preco": "number"},
                {"id": "5", "categoria": "jewelery", "tipo_categoria": "string", "preco": "695", "tipo_preco": "number"},
            ],
        )
        self.assertEqual(CatalogoColetado.de_xcom(xcom), catalogo)

    def test_preserva_tipos_invalidos_para_a_validacao(self):
        resposta = [
            {"id": 1, "category": "x", "price": True},
            {"id": 2, "category": "x", "price": "10.00"},
            {"id": 3, "category": "x", "price": None},
            {"id": 4, "category": 7, "price": Decimal("1")},
            {"id": 5, "price": Decimal("1")},
            {"id": 6, "category": "x", "price": Decimal("NaN")},
            "texto",
        ]
        produtos = projetar_catalogo(resposta).produtos
        self.assertEqual([p.tipo_preco for p in produtos], ["boolean", "string", "null", "number", "number", "number", "ausente"])
        self.assertEqual([p.preco for p in produtos][:3], [None, None, None])
        self.assertEqual(produtos[3].categoria, None)
        self.assertEqual(produtos[3].tipo_categoria, "number")
        self.assertEqual(produtos[4].tipo_categoria, "ausente")
        self.assertEqual(produtos[5].preco, "NaN")

    def test_raiz_que_nao_e_lista(self):
        catalogo = projetar_catalogo({"products": []})
        self.assertEqual((catalogo.tipo_raiz, catalogo.produtos), ("object", ()))

    def test_catalogo_vazio(self):
        self.assertEqual(projetar_catalogo([]).produtos, ())


if __name__ == "__main__":
    unittest.main()
