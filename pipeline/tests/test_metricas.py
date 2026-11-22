"""Testes do contrato e do cálculo das métricas de uma categoria (#15).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Rodam só com a biblioteca padrão: ``python3 -m unittest discover -s pipeline/tests -t pipeline``.
"""

from __future__ import annotations

import json
import unittest
from decimal import Decimal

from shopbrasil_pipeline.dominio.metricas import (
    MetricasCategoria,
    PrecosCategoria,
    calcular_metricas,
)


def precos(codigo: str, *valores: str) -> PrecosCategoria:
    return PrecosCategoria(codigo, tuple(Decimal(v) for v in valores))


class CalcularMetricasTest(unittest.TestCase):
    def test_exemplo_da_issue(self):
        resultado = calcular_metricas(precos("electronics", "10.00", "10.00", "10.01"))
        self.assertEqual(
            resultado,
            MetricasCategoria("electronics", Decimal("10.00"), Decimal("10.00"), Decimal("10.01"), 3),
        )

    def test_empate_arredonda_para_cima(self):
        # Média exata 10.005: ROUND_HALF_UP dá 10.01, como o numeric do PostgreSQL.
        self.assertEqual(calcular_metricas(precos("x", "10.00", "10.01")).preco_medio, Decimal("10.01"))

    def test_arredonda_uma_unica_vez(self):
        # 0.004 + 0.004 + 0.007 = 0.015 / 3 = 0.005 -> 0.01. Arredondar cada preço antes daria 0.00.
        self.assertEqual(calcular_metricas(precos("x", "0.004", "0.004", "0.007")).preco_medio, Decimal("0.01"))

    def test_dizima_periodica(self):
        resultado = calcular_metricas(precos("x", "10", "10", "11"))
        self.assertEqual(resultado.preco_medio, Decimal("10.33"))
        self.assertEqual(str(resultado.preco_minimo), "10.00")

    def test_um_produto(self):
        resultado = calcular_metricas(precos("jewelery", "695"))
        self.assertEqual(resultado.para_xcom()["preco_medio"], "695.00")
        self.assertEqual(resultado.quantidade_produtos, 1)

    def test_media_exata_sem_duplo_arredondamento(self):
        # Média exata 1/3 + 0.005 fica acima do empate: arredonda para cima, nunca no empate.
        self.assertEqual(calcular_metricas(precos("x", "0.01", "0.01", "0.005")).preco_medio, Decimal("0.01"))

    def test_media_entre_minimo_e_maximo(self):
        resultado = calcular_metricas(precos("x", "109.95", "22.3", "55.99", "15.99"))
        self.assertLessEqual(resultado.preco_minimo, resultado.preco_medio)
        self.assertLessEqual(resultado.preco_medio, resultado.preco_maximo)
        self.assertEqual(resultado.preco_medio, Decimal("51.06"))


class FronteiraJsonTest(unittest.TestCase):
    def test_precos_inteiros_e_fracionarios_da_fakestore(self):
        # A FakeStore devolve preços inteiros (695) e fracionários (22.3) no mesmo catálogo.
        produtos = json.loads('[{"price": 695}, {"price": 22.3}]', parse_float=Decimal, parse_int=Decimal)
        entrada = PrecosCategoria("jewelery", tuple(p["price"] for p in produtos))
        self.assertEqual(calcular_metricas(entrada).preco_medio, Decimal("358.65"))

    def test_so_parse_float_deixa_inteiro_como_int_e_e_recusado(self):
        produtos = json.loads('[{"price": 695}]', parse_float=Decimal)
        with self.assertRaises(TypeError):
            PrecosCategoria("jewelery", (produtos[0]["price"],))


class SaidaInvalidaTest(unittest.TestCase):
    def base(self, **troca):
        campos = {"codigo_categoria": "x", "preco_medio": "10.00", "preco_minimo": "9.00",
                  "preco_maximo": "11.00", "quantidade_produtos": 2}
        campos.update(troca)
        return campos

    def test_saida_valida(self):
        self.assertEqual(MetricasCategoria.de_xcom(self.base()).preco_medio, Decimal("10.00"))

    def test_rejeita_escala_diferente_de_duas_casas(self):
        for valor in ("10.005", "10", "1E+1", "10.0"):
            with self.subTest(valor=valor), self.assertRaises(ValueError):
                MetricasCategoria.de_xcom(self.base(preco_medio=valor))

    def test_rejeita_quantidade_zero_e_ordem_invalida(self):
        with self.assertRaises(ValueError):
            MetricasCategoria.de_xcom(self.base(quantidade_produtos=0))
        with self.assertRaises(ValueError):
            MetricasCategoria.de_xcom(self.base(preco_minimo="20.00"))
        with self.assertRaises(ValueError):
            MetricasCategoria.de_xcom(self.base(preco_minimo="-1.00"))

    def test_rejeita_float_e_int_na_construcao_direta(self):
        with self.assertRaises(TypeError):
            MetricasCategoria("x", 10.0, Decimal("1.00"), Decimal("20.00"), 3)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            MetricasCategoria("x", Decimal("10.00"), 1, Decimal("20.00"), 3)  # type: ignore[arg-type]


class EntradaInvalidaTest(unittest.TestCase):
    def test_lista_vira_tupla(self):
        entrada = PrecosCategoria("x", [Decimal("1.00")])  # type: ignore[arg-type]
        self.assertIsInstance(entrada.precos, tuple)

    def test_rejeita_float(self):
        with self.assertRaises(TypeError):
            PrecosCategoria("x", (10.0,))  # type: ignore[arg-type]

    def test_rejeita_bool(self):
        with self.assertRaises(TypeError):
            PrecosCategoria("x", (True,))  # type: ignore[arg-type]

    def test_rejeita_nao_finito(self):
        for valor in ("NaN", "Infinity"):
            with self.subTest(valor=valor), self.assertRaises(ValueError):
                precos("x", valor)

    def test_rejeita_lista_vazia_e_codigo_em_branco(self):
        with self.assertRaises(ValueError):
            PrecosCategoria("x", ())
        with self.assertRaises(ValueError):
            precos("  ", "1.00")


class TransporteXcomTest(unittest.TestCase):
    def test_entrada_ida_e_volta_em_json(self):
        entrada = precos("men's clothing", "109.95", "22.3")
        trafegado = json.loads(json.dumps(entrada.para_xcom()))
        self.assertEqual(trafegado, {"codigo_categoria": "men's clothing", "precos": ["109.95", "22.3"]})
        self.assertEqual(PrecosCategoria.de_xcom(trafegado), entrada)

    def test_saida_ida_e_volta_em_json(self):
        saida = calcular_metricas(precos("electronics", "10.00", "10.00", "10.01"))
        trafegado = json.loads(json.dumps(saida.para_xcom()))
        self.assertEqual(
            trafegado,
            {
                "codigo_categoria": "electronics",
                "preco_medio": "10.00",
                "preco_minimo": "10.00",
                "preco_maximo": "10.01",
                "quantidade_produtos": 3,
            },
        )
        self.assertEqual(MetricasCategoria.de_xcom(trafegado), saida)

    def test_de_xcom_rejeita_numero_no_lugar_de_texto(self):
        with self.assertRaises(TypeError):
            PrecosCategoria.de_xcom({"codigo_categoria": "x", "precos": [10.5]})
        with self.assertRaises(TypeError):
            MetricasCategoria.de_xcom(
                {"codigo_categoria": "x", "preco_medio": 1.0, "preco_minimo": "1.00",
                 "preco_maximo": "1.00", "quantidade_produtos": 1}
            )

    def test_de_xcom_rejeita_texto_nao_decimal_e_quantidade_bool(self):
        with self.assertRaises(ValueError):
            PrecosCategoria.de_xcom({"codigo_categoria": "x", "precos": ["dez"]})
        with self.assertRaises(TypeError):
            MetricasCategoria.de_xcom(
                {"codigo_categoria": "x", "preco_medio": "1.00", "preco_minimo": "1.00",
                 "preco_maximo": "1.00", "quantidade_produtos": True}
            )


if __name__ == "__main__":
    unittest.main()
