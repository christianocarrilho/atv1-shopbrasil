"""Testes do ValidarProdutosOperator (#6).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Precisam do Airflow e são pulados sem ele; rodam na imagem do Airflow:
``docker run --rm -v "$PWD/pipeline:/p" -w /p -e PYTHONPATH=/p:/p/dags apache/airflow:3.4.0-python3.12 python -m unittest tests.test_operador_validar``.
"""

from __future__ import annotations

import importlib.util
import json
import unittest
from decimal import Decimal

from shopbrasil_pipeline.aplicacao.catalogo import projetar_catalogo

TEM_AIRFLOW = importlib.util.find_spec("airflow") is not None


def xcom(*produtos) -> dict:
    return projetar_catalogo(json.loads(json.dumps(list(produtos)), parse_float=Decimal, parse_int=Decimal)).para_xcom()


@unittest.skipUnless(TEM_AIRFLOW, "o operador depende do Airflow")
class ValidarProdutosOperatorTest(unittest.TestCase):
    def operador(self, catalogo):
        from operadores.validar_produtos import ValidarProdutosOperator

        return ValidarProdutosOperator(task_id="validar_produtos", catalogo=catalogo)

    def test_catalogo_valido_devolve_so_os_campos_do_calculo(self) -> None:
        saida = self.operador(xcom({"id": 1, "category": "electronics", "price": 64, "title": "x"})).execute({})
        self.assertEqual({"produtos": [{"codigo_categoria": "electronics", "preco": "64"}]}, saida)

    def test_catalogo_invalido_falha_sem_retentativa(self) -> None:
        from airflow.sdk.exceptions import AirflowFailException

        with self.assertRaisesRegex(AirflowFailException, "catálogo vazio"):
            self.operador(xcom()).execute({})

    def test_xcom_em_formato_inesperado_falha_sem_retentativa(self) -> None:
        from airflow.sdk.exceptions import AirflowFailException

        for valor in (None, [], "texto"):
            with self.subTest(valor=valor), self.assertRaisesRegex(AirflowFailException, "formato inesperado"):
                self.operador(valor).execute({})

    def test_xcom_e_campo_templatizado(self) -> None:
        from operadores.validar_produtos import ValidarProdutosOperator

        self.assertEqual(("catalogo",), ValidarProdutosOperator.template_fields)


if __name__ == "__main__":
    unittest.main()
