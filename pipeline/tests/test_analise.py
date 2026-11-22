"""Testes do agrupamento por categoria e da consolidação (#16).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987
"""

from __future__ import annotations

import unittest
from decimal import Decimal

from shopbrasil_pipeline.aplicacao.analise import (
    ConsolidacaoInvalidaError,
    agrupar_por_categoria,
    consolidar_metricas,
)
from shopbrasil_pipeline.dominio.metricas import calcular_metricas


class AnaliseTest(unittest.TestCase):
    def test_agrupa_pelos_codigos_observados_em_ordem(self):
        grupos = agrupar_por_categoria([
            {"codigo_categoria": "men's clothing", "preco": "109.95"},
            {"codigo_categoria": "electronics", "preco": "64"},
            {"codigo_categoria": "men's clothing", "preco": "22.3"},
            {"codigo_categoria": "toys", "preco": "15.5"},
        ])
        self.assertEqual([g.codigo_categoria for g in grupos], ["electronics", "men's clothing", "toys"])
        self.assertEqual(grupos[1].precos, (Decimal("109.95"), Decimal("22.3")))

    def test_fluxo_completo_um_resultado_por_categoria(self):
        grupos = agrupar_por_categoria([
            {"codigo_categoria": "men's clothing", "preco": "109.95"},
            {"codigo_categoria": "men's clothing", "preco": "22.3"},
            {"codigo_categoria": "jewelery", "preco": "695"},
        ])
        resultados = [calcular_metricas(g) for g in reversed(grupos)]  # ordem de conclusão qualquer
        consolidado = consolidar_metricas(resultados, esperadas=[g.codigo_categoria for g in grupos])
        self.assertEqual([m.para_xcom() for m in consolidado], [
            {"codigo_categoria": "jewelery", "preco_medio": "695.00", "preco_minimo": "695.00",
             "preco_maximo": "695.00", "quantidade_produtos": 1},
            {"codigo_categoria": "men's clothing", "preco_medio": "66.13", "preco_minimo": "22.30",
             "preco_maximo": "109.95", "quantidade_produtos": 2},
        ])

    def test_consolidacao_recusa_repeticao_falta_e_vazio(self):
        a = calcular_metricas(agrupar_por_categoria([{"codigo_categoria": "a", "preco": "1"}])[0])
        with self.assertRaisesRegex(ConsolidacaoInvalidaError, "mais de uma vez"):
            consolidar_metricas([a, a])
        with self.assertRaisesRegex(ConsolidacaoInvalidaError, "faltam \\['b'\\]"):
            consolidar_metricas([a], esperadas=["a", "b"])
        with self.assertRaisesRegex(ConsolidacaoInvalidaError, "nenhum resultado"):
            consolidar_metricas([])


    def test_calcular_categoria_em_xcom(self):
        from shopbrasil_pipeline.aplicacao.analise import calcular_categoria

        self.assertEqual(
            {"codigo_categoria": "x", "preco_medio": "10.01", "preco_minimo": "10.00", "preco_maximo": "10.01",
             "quantidade_produtos": 3},
            calcular_categoria({"codigo_categoria": "x", "precos": ["10.00", "10.01", "10.01"]}),
        )


if __name__ == "__main__":
    unittest.main()
