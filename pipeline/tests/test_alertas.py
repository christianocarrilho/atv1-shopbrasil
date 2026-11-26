"""Testes das mensagens dos callbacks e do alerta simulado (#14).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987
"""

from __future__ import annotations

import unittest

from shopbrasil_pipeline.aplicacao.alertas import (
    OcorrenciaTask,
    notificar_falha,
    notificar_falha_ingestao,
    notificar_falha_run,
    notificar_retry,
    notificar_sucesso,
)

LOGGER = "shopbrasil.alertas"
OCORRENCIA = OcorrenciaTask("shopbrasil_metricas", "run-1", "ingestao.buscar_produtos", 2, 4, "HTTPError: 500")


def mensagens(logs) -> list[str]:
    return [registro.getMessage() for registro in logs.records]


class AlertasTest(unittest.TestCase):
    def test_mensagens_identificam_evento_run_task_e_tentativa(self):
        with self.assertLogs(LOGGER) as logs:
            notificar_sucesso(OCORRENCIA)
            notificar_retry(OCORRENCIA)
            notificar_falha(OCORRENCIA)
        avisos = mensagens(logs)
        self.assertEqual(
            [m.split(" ")[0] for m in avisos],
            ["[SHOPBRASIL][SUCESSO]", "[SHOPBRASIL][RETRY]", "[SHOPBRASIL][FALHA]"],
        )
        for mensagem in avisos:
            self.assertIn("run=run-1 task=ingestao.buscar_produtos tentativa=2/4", mensagem)
            self.assertNotIn("ALERTA", mensagem)

    def test_falha_da_ingestao_avisa_e_simula_alerta(self):
        with self.assertLogs(LOGGER) as logs:
            notificar_falha_ingestao(OcorrenciaTask("d", "run-9", "ingestao.buscar_produtos", 4, 4, "URLError"))
        aviso, alerta = mensagens(logs)
        self.assertTrue(aviso.startswith("[SHOPBRASIL][FALHA]"))
        self.assertTrue(
            alerta.startswith(
                "[SHOPBRASIL][ALERTA SIMULADO] destino=equipe-dados@shopbrasil.example "
                "assunto=ShopBrasil: ingestão da FakeStore falhou na run run-9 | "
            )
        )
        self.assertIn("esgotou 4 tentativa(s); a publicação da run não ocorreu.", alerta)
        self.assertEqual(logs.records[1].levelname, "ERROR")

    def test_falha_sem_retentativa_nao_diz_que_esgotou(self):
        with self.assertLogs(LOGGER) as logs:
            notificar_falha_ingestao(OcorrenciaTask("d", "run-1", "ingestao.buscar_produtos", 1, 4, "AirflowFailException"))
        alerta = mensagens(logs)[1]
        self.assertIn("sem retentativa (erro não recuperável) na tentativa 1/4", alerta)
        self.assertNotIn("esgotou", alerta)

    def test_falha_da_run(self):
        with self.assertLogs(LOGGER) as logs:
            notificar_falha_run("shopbrasil_metricas", "run-2", None)
        self.assertEqual(mensagens(logs), ["[SHOPBRASIL][FALHA RUN] dag=shopbrasil_metricas run=run-2 motivo=não informado"])


if __name__ == "__main__":
    unittest.main()
