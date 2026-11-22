"""Testes do caso de uso da publicação do snapshot diário (#17).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Usam um repositório falso, sem Airflow nem PostgreSQL; o adaptador real é coberto pelo cenário
``e2e/publicacao``. Rodam só com a biblioteca padrão:
``python3 -m unittest discover -s pipeline/tests -t pipeline``.
"""

from __future__ import annotations

import json
import unittest
from collections.abc import Sequence
from datetime import date
from decimal import Decimal

from shopbrasil_pipeline.aplicacao.execucao import Execucao
from shopbrasil_pipeline.aplicacao.publicacao import (
    CategoriaRepetidaError,
    ColecaoVaziaError,
    PublicarSnapshot,
    ResultadoPublicacao,
)
from shopbrasil_pipeline.dominio.metricas import MetricasCategoria

EXECUCAO = Execucao("scheduled__2026-11-21T09:00:00+00:00", date(2026, 11, 21))


def metricas(codigo: str, medio: str = "10.00", minimo: str = "10.00", maximo: str = "10.01", n: int = 3):
    return MetricasCategoria(codigo, Decimal(medio), Decimal(minimo), Decimal(maximo), n)


class RepositorioFalso:
    def __init__(self, erro: Exception | None = None) -> None:
        self.chamadas: list[tuple[Execucao, Sequence[MetricasCategoria]]] = []
        self._erro = erro

    def publicar_snapshot(self, execucao, metricas):
        self.chamadas.append((execucao, metricas))
        if self._erro is not None:
            raise self._erro
        return ResultadoPublicacao(
            execucao.run_id, execucao.data_referencia, tuple(m.codigo_categoria for m in metricas), ()
        )


class PublicarSnapshotTest(unittest.TestCase):
    def test_entrega_execucao_e_colecao_ao_repositorio(self):
        repositorio = RepositorioFalso()
        colecao = [metricas("electronics"), metricas("jewelery", "10.01", n=2)]
        resultado = PublicarSnapshot(repositorio).executar(EXECUCAO, colecao)
        self.assertEqual(len(repositorio.chamadas), 1)
        execucao, enviadas = repositorio.chamadas[0]
        self.assertEqual(execucao, EXECUCAO)
        self.assertEqual(enviadas, tuple(colecao))
        self.assertEqual(resultado.categorias_publicadas, ("electronics", "jewelery"))

    def test_aceita_iteravel_de_uso_unico(self):
        repositorio = RepositorioFalso()
        PublicarSnapshot(repositorio).executar(EXECUCAO, (m for m in [metricas("a"), metricas("b")]))
        self.assertEqual([m.codigo_categoria for m in repositorio.chamadas[0][1]], ["a", "b"])

    def test_codigo_repetido_e_recusado_antes_do_repositorio(self):
        repositorio = RepositorioFalso()
        colecao = [metricas("electronics"), metricas("jewelery"), metricas("electronics", "10.01")]
        with self.assertRaises(CategoriaRepetidaError) as contexto:
            PublicarSnapshot(repositorio).executar(EXECUCAO, colecao)
        self.assertIn("electronics", str(contexto.exception))
        self.assertNotIn("jewelery", str(contexto.exception))
        self.assertEqual(repositorio.chamadas, [])

    def test_codigos_diferentes_so_na_caixa_sao_categorias_distintas(self):
        # O código é gravado exatamente como veio da FakeStore, sem normalização (#15).
        repositorio = RepositorioFalso()
        PublicarSnapshot(repositorio).executar(EXECUCAO, [metricas("Electronics"), metricas("electronics")])
        self.assertEqual(len(repositorio.chamadas), 1)

    def test_colecao_vazia_e_recusada_sem_tocar_no_snapshot(self):
        repositorio = RepositorioFalso()
        with self.assertRaises(ColecaoVaziaError):
            PublicarSnapshot(repositorio).executar(EXECUCAO, [])
        self.assertEqual(repositorio.chamadas, [])

    def test_item_que_nao_e_metrica_e_recusado(self):
        repositorio = RepositorioFalso()
        with self.assertRaises(TypeError):
            PublicarSnapshot(repositorio).executar(EXECUCAO, [metricas("a").para_xcom()])
        self.assertEqual(repositorio.chamadas, [])

    def test_execucao_de_outro_tipo_e_recusada(self):
        repositorio = RepositorioFalso()
        with self.assertRaises(TypeError):
            PublicarSnapshot(repositorio).executar(EXECUCAO.para_xcom(), [metricas("a")])
        self.assertEqual(repositorio.chamadas, [])

    def test_falha_do_repositorio_propaga_sem_ser_engolida(self):
        erro = RuntimeError("falha na transação")
        with self.assertRaises(RuntimeError) as contexto:
            PublicarSnapshot(RepositorioFalso(erro)).executar(EXECUCAO, [metricas("a")])
        self.assertIs(contexto.exception, erro)


class ResultadoPublicacaoTest(unittest.TestCase):
    def test_para_xcom_e_serializavel_em_json(self):
        resultado = ResultadoPublicacao("run-1", date(2026, 11, 21), ("a", "b"), ("b",))
        self.assertEqual(
            json.loads(json.dumps(resultado.para_xcom())),
            {
                "run_id": "run-1",
                "data_referencia": "2026-11-21",
                "categorias_publicadas": ["a", "b"],
                "categorias_cadastradas": ["b"],
            },
        )


if __name__ == "__main__":
    unittest.main()
