"""Testes da data de referência e da identificação da execução (#4).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Rodam só com a biblioteca padrão: ``python3 -m unittest discover -s pipeline/tests -t pipeline``.
"""

from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone

from shopbrasil_pipeline.aplicacao.execucao import (
    Execucao,
    OcorrenciaAusenteError,
    identificar_execucao,
)
from shopbrasil_pipeline.dominio.calendario import FUSO_REFERENCIA, data_referencia


class DataReferenciaTest(unittest.TestCase):
    def test_ocorrencia_das_06h_em_brasilia(self):
        ocorrencia = datetime(2026, 11, 21, 9, 0, tzinfo=timezone.utc)  # 06:00 em Brasília
        self.assertEqual(data_referencia(ocorrencia), date(2026, 11, 21))

    def test_usa_data_civil_de_brasilia_e_nao_a_de_utc(self):
        # 01:30 UTC de 22/11 ainda é 21/11 em Brasília (UTC-3).
        ocorrencia = datetime(2026, 11, 22, 1, 30, tzinfo=timezone.utc)
        self.assertEqual(data_referencia(ocorrencia), date(2026, 11, 21))

    def test_aceita_ocorrencia_em_outro_fuso(self):
        ocorrencia = datetime(2026, 11, 21, 6, 0, tzinfo=FUSO_REFERENCIA)
        self.assertEqual(data_referencia(ocorrencia.astimezone(timezone(timedelta(hours=9)))), date(2026, 11, 21))

    def test_rejeita_ocorrencia_sem_fuso(self):
        with self.assertRaises(ValueError):
            data_referencia(datetime(2026, 11, 21, 6, 0))


class IdentificarExecucaoTest(unittest.TestCase):
    def test_reprocessar_a_mesma_run_preserva_identificacao(self):
        ocorrencia = datetime(2026, 11, 21, 9, 0, tzinfo=timezone.utc)
        primeira = identificar_execucao("scheduled__2026-11-21T09:00:00+00:00", ocorrencia)
        segunda = identificar_execucao("scheduled__2026-11-21T09:00:00+00:00", ocorrencia)
        self.assertEqual(primeira, segunda)
        self.assertEqual(primeira.data_referencia, date(2026, 11, 21))

    def test_xcom_ida_e_volta(self):
        execucao = Execucao("run-1", date(2026, 11, 21))
        self.assertEqual(execucao.para_xcom(), {"run_id": "run-1", "data_referencia": "2026-11-21"})
        self.assertEqual(Execucao.de_xcom(execucao.para_xcom()), execucao)

    def test_rejeita_run_sem_ocorrencia(self):
        with self.assertRaises(OcorrenciaAusenteError):
            identificar_execucao("manual__x", None)

    def test_rejeita_run_id_vazio(self):
        with self.assertRaises(ValueError):
            identificar_execucao("", datetime(2026, 11, 21, 9, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
