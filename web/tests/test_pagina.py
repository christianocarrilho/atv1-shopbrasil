"""Testes da página do painel (#19), das barras (#20) e da linha de evolução (#21).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Chamam a aplicação ASGI diretamente, como os testes da API. Precisam de FastAPI, Jinja2 e psycopg
e são pulados sem eles; rodam no contêiner da web.
"""

from __future__ import annotations

import asyncio
import html
import importlib.util
import json
import re
import unittest
from datetime import date
from urllib.parse import urlencode

from tests.apoio import ELETRONICOS, JOIAS, RepositorioEmMemoria, metrica

TEM_DEPENDENCIAS = all(importlib.util.find_spec(m) is not None for m in ("fastapi", "jinja2", "psycopg"))


def obter(aplicacao, caminho: str, **query: str) -> tuple[int, str, str]:
    """GET na aplicação ASGI; devolve status, content-type e corpo em texto."""
    mensagens: list[dict] = []
    pedido_entregue = False

    async def receive() -> dict:
        # Depois do corpo, fica aguardando como um cliente conectado: respostas de arquivo do
        # Starlette escutam a desconexão, e repetir o pedido criaria um laço sem fim.
        nonlocal pedido_entregue
        if pedido_entregue:
            await asyncio.Event().wait()
        pedido_entregue = True
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
    tipo = dict(inicio["headers"]).get(b"content-type", b"").decode()
    corpo = b"".join(m.get("body", b"") for m in mensagens if m["type"] == "http.response.body")
    return inicio["status"], tipo, corpo.decode()


def linhas_da_tabela(pagina: str) -> list[list[str]]:
    corpo = re.search(r'<table id="tabela-metricas">.*?<tbody>(.*?)</tbody>', pagina, re.S)
    if not corpo:
        return []
    return [
        [html.unescape(c.strip()) for c in re.findall(r"<td[^>]*>(.*?)</td>", linha, re.S)]
        for linha in re.findall(r"<tr[^>]*>(.*?)</tr>", corpo.group(1), re.S)
    ]


def dados_embutidos(pagina: str, ident: str = "metricas-dados"):
    return json.loads(re.search(rf'<script type="application/json" id="{ident}">(.*?)</script>', pagina, re.S).group(1))


@unittest.skipUnless(TEM_DEPENDENCIAS, "FastAPI, Jinja2 e psycopg ficam no contêiner da web")
class PaginaTest(unittest.TestCase):
    def criar(self, snapshot) -> object:
        from shopbrasil_web.apresentacao.aplicacao import criar_aplicacao
        from shopbrasil_web.composicao import criar_casos_de_uso

        return criar_aplicacao(criar_casos_de_uso(RepositorioEmMemoria(snapshot)))

    def setUp(self) -> None:
        self.app = self.criar({
            date(2026, 11, 20): [metrica(ELETRONICOS, "20.00", "15.00", "25.00", 2)],
            date(2026, 11, 22): [
                metrica(JOIAS, "10.01", "10.00", "10.01", 2),
                metrica(ELETRONICOS, "10.00", "10.00", "10.01", 3),
            ],
        })

    def test_abre_na_data_mais_recente(self) -> None:
        status, tipo, pagina = obter(self.app, "/")
        self.assertEqual(200, status)
        self.assertTrue(tipo.startswith("text/html"))
        self.assertIn('value="2026-11-22"', pagina)
        self.assertIn('<span id="data-selecionada">2026-11-22</span>', pagina)
        self.assertEqual([
            ["electronics", "R$ 10,00", "R$ 10,00", "R$ 10,01", "3"],
            ["jewelery", "R$ 10,01", "R$ 10,00", "R$ 10,01", "2"],
        ], linhas_da_tabela(pagina))

    def test_selecionar_data_carrega_o_dia(self) -> None:
        _, _, pagina = obter(self.app, "/", data="2026-11-20")
        self.assertIn('value="2026-11-20"', pagina)
        self.assertEqual([["electronics", "R$ 20,00", "R$ 15,00", "R$ 25,00", "2"]], linhas_da_tabela(pagina))
        self.assertNotIn("Sem métricas para esta data", pagina)

    def test_data_sem_registros(self) -> None:
        _, _, pagina = obter(self.app, "/", data="2026-11-21")
        self.assertIn('value="2026-11-21"', pagina)
        self.assertIn("Sem métricas para esta data", pagina)
        self.assertEqual([], linhas_da_tabela(pagina))
        self.assertEqual({"data": "2026-11-21", "vazia": True, "metricas": []}, dados_embutidos(pagina))

    def test_colecao_embutida_igual_a_api(self) -> None:
        from tests.test_apresentacao import chamar

        _, _, pagina = obter(self.app, "/", data="2026-11-22")
        _, api = chamar(self.app, "/api/metricas", data="2026-11-22")
        self.assertEqual(api, dados_embutidos(pagina))
        self.assertIn('<section id="evolucao">', pagina)

    def test_sem_publicacao(self) -> None:
        _, _, pagina = obter(self.criar({}), "/")
        self.assertIn("Nenhuma métrica publicada até o momento", pagina)
        self.assertIn('value=""', pagina)
        self.assertIsNone(dados_embutidos(pagina))

    def test_data_invalida_422(self) -> None:
        status, _, _ = obter(self.app, "/", data="2026-02-30")
        self.assertEqual(422, status)

    def test_pagina_fora_do_openapi(self) -> None:
        from tests.test_apresentacao import chamar

        _, esquema = chamar(self.app, "/openapi.json")
        self.assertNotIn("/", esquema["paths"])


    def test_barras_usam_a_colecao_da_tabela(self) -> None:
        _, _, pagina = obter(self.app, "/", data="2026-11-22")
        self.assertIn('<canvas id="grafico-barras"', pagina)
        self.assertIn('<script src="/estaticos/chart.umd.min.js"></script>', pagina)
        dados = dados_embutidos(pagina)
        # Cada barra vem de uma linha da coleção; o preço médio é o mesmo exibido na tabela.
        self.assertEqual(
            [linha[1].removeprefix("R$ ").replace(".", "").replace(",", ".")
             for linha in linhas_da_tabela(pagina)],
            [m["preco_medio"] for m in dados["metricas"]],
        )
        self.assertNotIn('id="barras-vazio"', pagina)

    def test_barras_em_dia_sem_metricas_exibem_estado_vazio(self) -> None:
        _, _, pagina = obter(self.app, "/", data="2026-11-21")
        self.assertNotIn('<canvas id="grafico-barras"', pagina)
        self.assertIn('<p class="vazio" id="barras-vazio">Sem métricas para esta data</p>', pagina)

    def test_chartjs_servido_localmente(self) -> None:
        status, tipo, corpo = obter(self.app, "/estaticos/chart.umd.min.js")
        self.assertEqual(200, status)
        self.assertIn("javascript", tipo)
        self.assertIn("Chart.js v4.6.1", corpo[:200])


    def test_evolucao_padrao_primeira_categoria_e_30_dias_ate_a_data(self) -> None:
        _, _, pagina = obter(self.app, "/", data="2026-11-22")
        serie = dados_embutidos(pagina, "evolucao-dados")
        self.assertEqual(("electronics", "2026-10-24", "2026-11-22"),
                         (serie["categoria"]["codigo"], serie["inicio"], serie["fim"]))
        self.assertEqual(30, len(serie["pontos"]))
        self.assertIn('<option value="electronics" selected>', pagina)
        self.assertIn('id="inicio" name="inicio" form="filtro-data" value="2026-10-24"', pagina)
        self.assertIn('<canvas id="grafico-evolucao"', pagina)

    def test_evolucao_com_lacunas_igual_a_api(self) -> None:
        from tests.test_apresentacao import chamar

        filtros = {"categoria": "electronics", "inicio": "2026-11-20", "fim": "2026-11-23"}
        _, _, pagina = obter(self.app, "/", **filtros)
        serie = dados_embutidos(pagina, "evolucao-dados")
        _, api = chamar(self.app, "/api/evolucao", **filtros)
        self.assertEqual(api, serie)
        # Lacuna é null, distinta de zero; cada ponto é o preço médio publicado no snapshot.
        self.assertEqual([("2026-11-20", "20.00"), ("2026-11-21", None), ("2026-11-22", "10.00"), ("2026-11-23", None)],
                         [(p["data"], p["preco_medio"]) for p in serie["pontos"]])
        self.assertIn("spanGaps: false", pagina)

    def test_evolucao_mantem_data_e_filtros_independentes(self) -> None:
        _, _, pagina = obter(self.app, "/", data="2026-11-20", categoria="jewelery",
                             inicio="2026-11-21", fim="2026-11-22")
        self.assertEqual([["electronics", "R$ 20,00", "R$ 15,00", "R$ 25,00", "2"]], linhas_da_tabela(pagina))
        serie = dados_embutidos(pagina, "evolucao-dados")
        self.assertEqual(["2026-11-21", "2026-11-22"], [p["data"] for p in serie["pontos"]])
        self.assertIn('<option value="jewelery" selected>', pagina)

    def test_evolucao_filtro_invalido_vira_aviso(self) -> None:
        status, _, pagina = obter(self.app, "/", categoria="electronics", inicio="2026-11-23", fim="2026-11-20")
        self.assertEqual(200, status)
        self.assertIn('id="evolucao-aviso">Intervalo inválido: início 2026-11-23 depois do fim 2026-11-20<', pagina)
        self.assertIsNone(dados_embutidos(pagina, "evolucao-dados"))
        _, _, pagina = obter(self.app, "/", categoria="toys")
        self.assertIn("Categoria &#39;toys&#39; não cadastrada", pagina)
        status, _, _ = obter(self.app, "/", inicio="2026-13-01")
        self.assertEqual(422, status)


    def test_data_valida_extrema_nao_derruba_a_pagina(self) -> None:
        status, _, pagina = obter(self.app, "/", data="0001-01-01")
        self.assertEqual(200, status)
        self.assertIn("Sem métricas para esta data", pagina)
        serie = dados_embutidos(pagina, "evolucao-dados")
        self.assertEqual(("0001-01-01", "0001-01-01", 1), (serie["inicio"], serie["fim"], len(serie["pontos"])))


if __name__ == "__main__":
    unittest.main()
