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
    notificar_retry,
    notificar_sucesso,
)
from shopbrasil_pipeline.infraestrutura.notificador_log import NotificadorLog


class Registro:
    def __init__(self):
        self.avisos: list[str] = []
        self.alertas: list[tuple[str, str]] = []

    def registrar(self, mensagem):
        self.avisos.append(mensagem)

    def enviar_alerta(self, assunto, mensagem):
        self.alertas.append((assunto, mensagem))


OCORRENCIA = OcorrenciaTask("shopbrasil_metricas", "run-1", "ingestao.buscar_produtos", 2, 4, "HTTPError: 500")


class AlertasTest(unittest.TestCase):
    def test_mensagens_identificam_evento_run_task_e_tentativa(self):
        registro = Registro()
        notificar_sucesso(registro, OCORRENCIA)
        notificar_retry(registro, OCORRENCIA)
        notificar_falha(registro, OCORRENCIA)
        self.assertEqual(
            [m.split(" ")[0] for m in registro.avisos],
            ["[SHOPBRASIL][SUCESSO]", "[SHOPBRASIL][RETRY]", "[SHOPBRASIL][FALHA]"],
        )
        for mensagem in registro.avisos:
            self.assertIn("run=run-1 task=ingestao.buscar_produtos tentativa=2/4", mensagem)
        self.assertEqual(registro.alertas, [])

    def test_falha_da_ingestao_avisa_e_simula_alerta(self):
        registro = Registro()
        notificar_falha_ingestao(registro, OcorrenciaTask("d", "run-9", "ingestao.buscar_produtos", 4, 4, "URLError"))
        self.assertTrue(registro.avisos[0].startswith("[SHOPBRASIL][FALHA]"))
        self.assertEqual(len(registro.alertas), 1)
        self.assertIn("run-9", registro.alertas[0][0])
        self.assertIn("esgotou 4 tentativa(s)", registro.alertas[0][1])

    def test_notificador_log_simula_envio(self):
        with self.assertLogs("shopbrasil.alertas") as capturado:
            NotificadorLog("equipe@exemplo").enviar_alerta("assunto", "mensagem")
        self.assertIn("[SHOPBRASIL][ALERTA SIMULADO] destino=equipe@exemplo assunto=assunto | mensagem", capturado.output[0])


if __name__ == "__main__":
    unittest.main()
