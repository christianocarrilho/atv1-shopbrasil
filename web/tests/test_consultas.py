"""Testes dos casos de uso de consultas (#9).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Só biblioteca padrão, com repositório em memória:
``python3 -m unittest discover -s web/tests -t web``.
"""

from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from shopbrasil_web.consultas.casos_uso import (
    MAX_DIAS_INTERVALO,
    CategoriaNaoEncontradaError,
    ConsultarEvolucao,
    ConsultarMetricasDaData,
    ConsultarUltimaData,
    IntervaloInvalidoError,
    ListarCategorias,
)
from shopbrasil_web.consultas.modelos import PontoEvolucao
from tests.apoio import ELETRONICOS, JOIAS, RepositorioEmMemoria, metrica

D20, D21, D22, D23 = (date(2026, 11, d) for d in (20, 21, 22, 23))


def repositorio_padrao() -> RepositorioEmMemoria:
    return RepositorioEmMemoria({
        D20: [metrica(ELETRONICOS, "20.00", "15.00", "25.00", 2)],
        D22: [
            metrica(ELETRONICOS, "10.00", "10.00", "10.01", 3),
            metrica(JOIAS, "10.01", "10.00", "10.01", 2),
        ],
    })


class ConsultarMetricasDaDataTest(unittest.TestCase):
    def test_data_com_metricas_em_ordem_de_codigo(self) -> None:
        resultado = ConsultarMetricasDaData(repositorio_padrao()).executar(D22)
        self.assertEqual(D22, resultado.data)
        self.assertFalse(resultado.vazia)
        self.assertEqual(["electronics", "jewelery"], [m.categoria.codigo for m in resultado.metricas])
        primeira = resultado.metricas[0]
        self.assertEqual((Decimal("10.00"), Decimal("10.00"), Decimal("10.01"), 3),
                         (primeira.preco_medio, primeira.preco_minimo, primeira.preco_maximo,
                          primeira.quantidade_produtos))

    def test_data_sem_registros_e_colecao_vazia_explicita(self) -> None:
        resultado = ConsultarMetricasDaData(repositorio_padrao()).executar(D21)
        self.assertEqual(D21, resultado.data)
        self.assertTrue(resultado.vazia)
        self.assertEqual((), resultado.metricas)


class ConsultarUltimaDataTest(unittest.TestCase):
    def test_ultima_data_com_metricas(self) -> None:
        self.assertEqual(D22, ConsultarUltimaData(repositorio_padrao()).executar())

    def test_sem_publicacao_devolve_none(self) -> None:
        self.assertIsNone(ConsultarUltimaData(RepositorioEmMemoria()).executar())


class ListarCategoriasTest(unittest.TestCase):
    def test_ordenadas_por_codigo(self) -> None:
        self.assertEqual((ELETRONICOS, JOIAS), ListarCategorias(RepositorioEmMemoria()).executar())


class ConsultarEvolucaoTest(unittest.TestCase):
    def test_um_ponto_por_dia_em_ordem_com_lacuna(self) -> None:
        serie = ConsultarEvolucao(repositorio_padrao()).executar("electronics", D20, D23)
        self.assertEqual(ELETRONICOS, serie.categoria)
        self.assertEqual((D20, D23), (serie.inicio, serie.fim))
        self.assertEqual((
            PontoEvolucao(D20, Decimal("20.00")),
            PontoEvolucao(D21, None),
            PontoEvolucao(D22, Decimal("10.00")),
            PontoEvolucao(D23, None),
        ), serie.pontos)

    def test_intervalo_inclusivo_de_um_dia(self) -> None:
        serie = ConsultarEvolucao(repositorio_padrao()).executar("jewelery", D22, D22)
        self.assertEqual((PontoEvolucao(D22, Decimal("10.01")),), serie.pontos)

    def test_categoria_sem_dados_no_intervalo_so_lacunas(self) -> None:
        serie = ConsultarEvolucao(repositorio_padrao()).executar("jewelery", D20, D21)
        self.assertEqual([None, None], [p.preco_medio for p in serie.pontos])

    def test_zero_publicado_nao_e_lacuna(self) -> None:
        repositorio = RepositorioEmMemoria({D20: [metrica(ELETRONICOS, "0.00", "0.00", "0.00", 1)]})
        serie = ConsultarEvolucao(repositorio).executar("electronics", D20, D21)
        self.assertEqual([Decimal("0.00"), None], [p.preco_medio for p in serie.pontos])

    def test_inicio_depois_do_fim(self) -> None:
        repositorio = repositorio_padrao()
        with self.assertRaisesRegex(IntervaloInvalidoError, "depois do fim"):
            ConsultarEvolucao(repositorio).executar("electronics", D22, D20)
        self.assertEqual([], repositorio.chamadas)

    def test_intervalo_longo_demais(self) -> None:
        caso = ConsultarEvolucao(repositorio_padrao())
        self.assertEqual(MAX_DIAS_INTERVALO,
                         len(caso.executar("electronics", date(2024, 1, 1), date(2024, 12, 31)).pontos))
        with self.assertRaisesRegex(IntervaloInvalidoError, "367 dias"):
            caso.executar("electronics", date(2024, 1, 1), date(2025, 1, 1))

    def test_categoria_nao_cadastrada(self) -> None:
        with self.assertRaises(CategoriaNaoEncontradaError) as contexto:
            ConsultarEvolucao(repositorio_padrao()).executar("toys", D20, D22)
        self.assertEqual("toys", contexto.exception.codigo)


if __name__ == "__main__":
    unittest.main()
