"""Testes da publicação transacional com o histórico por run (#17, #18).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Usam cursor e conexão falsos, sem PostgreSQL, para conferir a ordem das gravações, o append do
histórico e o desfecho da transação; o comportamento no banco real é coberto por
``e2e/publicacao``. Rodam só com a biblioteca padrão:
``python3 -m unittest discover -s pipeline/tests -t pipeline``.
"""

from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from shopbrasil_pipeline.aplicacao.execucao import Execucao
from shopbrasil_pipeline.dominio.metricas import MetricasCategoria
from shopbrasil_pipeline.infraestrutura.publicacao_postgres import PublicadorSnapshotPostgres, _gravar_historico

LOGGER = "shopbrasil_pipeline.infraestrutura.publicacao_postgres"
EXECUCAO = Execucao("scheduled__2026-11-21T09:00:00+00:00", date(2026, 11, 21))
METRICAS = (
    MetricasCategoria("electronics", Decimal("10.00"), Decimal("10.00"), Decimal("10.01"), 3),
    MetricasCategoria("jewelery", Decimal("10.01"), Decimal("10.00"), Decimal("10.01"), 2),
)
IDS = {"electronics": 7, "jewelery": 9}

class CursorFalso:
    def __init__(
        self, falhar_em: str | None = None, historico_da_run: list[tuple] | None = None, linhas_do_dia: int = 0
    ) -> None:
        self.comandos: list[tuple[str, object]] = []
        self.rowcount = -1
        self._linhas_do_dia = linhas_do_dia
        self.historico_da_run = historico_da_run or []
        self._falhar_em = falhar_em
        self._resultado: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def _registrar(self, sql: str, parametros: object) -> None:
        self.comandos.append((sql, parametros))
        if self._falhar_em and self._falhar_em in sql:
            raise RuntimeError(f"falha simulada em {self._falhar_em}")

    def execute(self, sql, parametros=None):
        self._registrar(sql, parametros)
        if "RETURNING codigo" in sql:
            self._resultado = [(c,) for c in parametros[0]]
        elif "SELECT codigo, id" in sql:
            self._resultado = [(c, i) for i, c in enumerate(parametros[0], start=1)]
        elif "FROM public.metricas_categoria_historico" in sql:
            self._resultado = list(self.historico_da_run)
        elif sql.startswith("DELETE"):
            self.rowcount = self._linhas_do_dia

    def executemany(self, sql, linhas):
        self._registrar(sql, list(linhas))

    def fetchall(self):
        # Como no psycopg2, o resultado se esgota na leitura.
        resultado, self._resultado = self._resultado, []
        return resultado


class ConexaoFalsa:
    def __init__(self, cursor: CursorFalso) -> None:
        self._cursor = cursor
        self.autocommit = True
        self.desfecho: str | None = None
        self.fechada = False

    def __enter__(self):
        return self

    def __exit__(self, tipo, *_):
        # Mesmo contrato do psycopg2: commit ao sair sem erro, rollback em exceção.
        self.desfecho = "rollback" if tipo else "commit"
        return False

    def cursor(self):
        return self._cursor

    def close(self):
        self.fechada = True


class GravarHistoricoTest(unittest.TestCase):
    def test_insere_uma_linha_por_categoria_com_run_e_data_de_referencia(self):
        cursor = CursorFalso()
        _gravar_historico(cursor, EXECUCAO, METRICAS, IDS)
        self.assertEqual(len(cursor.comandos), 2)
        self.assertIn("SELECT", cursor.comandos[0][0])  # leitura do que a run já tem, antes do INSERT
        sql, linhas = cursor.comandos[1]
        self.assertIn("INSERT INTO public.metricas_categoria_historico", sql)
        self.assertIn("ON CONFLICT (run_id, categoria_id) DO NOTHING", sql)
        self.assertNotIn("DO UPDATE", sql)
        self.assertEqual(
            linhas,
            [
                (EXECUCAO.run_id, 7, date(2026, 11, 21), Decimal("10.00"), Decimal("10.00"), Decimal("10.01"), 3),
                (EXECUCAO.run_id, 9, date(2026, 11, 21), Decimal("10.01"), Decimal("10.00"), Decimal("10.01"), 2),
            ],
        )

    def test_valores_sao_os_mesmos_objetos_decimal_publicados(self):
        cursor = CursorFalso()
        _gravar_historico(cursor, EXECUCAO, METRICAS[:1], {"electronics": 1})
        linha = cursor.comandos[1][1][0]
        self.assertIs(linha[3], METRICAS[0].preco_medio)

    def test_nao_controla_a_transacao(self):
        cursor = CursorFalso()
        _gravar_historico(cursor, EXECUCAO, METRICAS, IDS)
        texto = " ".join(sql for sql, _ in cursor.comandos).upper()
        for palavra in ("COMMIT", "ROLLBACK", "BEGIN"):
            self.assertNotIn(palavra, texto)

    def test_reprocessamento_sem_mudanca_nao_avisa(self):
        iguais = [(7, Decimal("10.00"), Decimal("10.00"), Decimal("10.01"), 3),
                  (9, Decimal("10.01"), Decimal("10.00"), Decimal("10.01"), 2)]
        cursor = CursorFalso(historico_da_run=iguais)
        with self.assertNoLogs(LOGGER, "WARNING"):
            preservadas = _gravar_historico(cursor, EXECUCAO, METRICAS, IDS)
        self.assertEqual(2, preservadas)

    def test_reprocessamento_com_valores_novos_preserva_e_avisa(self):
        antigos = [(7, Decimal("20.00"), Decimal("15.00"), Decimal("25.00"), 2),
                   (9, Decimal("10.01"), Decimal("10.00"), Decimal("10.01"), 2)]
        cursor = CursorFalso(historico_da_run=antigos)
        with self.assertLogs(LOGGER, "WARNING") as logs:
            _gravar_historico(cursor, EXECUCAO, METRICAS, IDS)
        self.assertEqual(1, len(logs.output))
        self.assertIn("categoria=electronics histórico=20.00/15.00/25.00/2 snapshot=10.00/10.00/10.01/3", logs.output[0])
        # A linha original não é atualizada: o INSERT segue com DO NOTHING.
        self.assertNotIn("DO UPDATE", cursor.comandos[1][0])


class PublicacaoComHistoricoTest(unittest.TestCase):
    def _publicar(self, cursor: CursorFalso) -> ConexaoFalsa:
        conexao = ConexaoFalsa(cursor)
        try:
            PublicadorSnapshotPostgres(abrir_conexao=lambda: conexao).publicar_snapshot(EXECUCAO, METRICAS)
        finally:
            self.assertTrue(conexao.fechada)
        return conexao

    def test_historico_grava_depois_do_snapshot_e_antes_do_unico_commit(self):
        cursor = CursorFalso()
        conexao = self._publicar(cursor)
        alvos = [sql.split()[0] + " " + sql.split()[2] for sql, _ in cursor.comandos]
        self.assertEqual(
            alvos,
            [
                "INSERT public.categoria",
                "SELECT id",
                "DELETE public.metricas_categoria",
                "INSERT public.metricas_categoria",
                "SELECT preco_medio,",  # o que a run já tem no histórico, antes do INSERT
                "INSERT public.metricas_categoria_historico",
            ],
        )
        self.assertFalse(conexao.autocommit)
        self.assertEqual(conexao.desfecho, "commit")

    def test_log_da_publicacao_informa_linhas_substituidas_depois_do_commit(self):
        cursor = CursorFalso(linhas_do_dia=4)
        with self.assertLogs(LOGGER, "INFO") as logs:
            conexao = self._publicar(cursor)
        self.assertEqual(conexao.desfecho, "commit")
        self.assertIn(
            f"[SHOPBRASIL][PUBLICACAO] run={EXECUCAO.run_id} data=2026-11-21 snapshot: 4 linha(s) anterior(es) "
            "do dia substituída(s) por 2; categoria(s) nova(s) no cadastro: electronics, jewelery",
            logs.output[0],
        )
        self.assertIn(f"[SHOPBRASIL][HISTORICO] run={EXECUCAO.run_id}: 2 linha(s) gravada(s), 0 já registrada(s)", logs.output[1])

    def test_log_conta_linhas_preservadas_no_reprocessamento(self):
        iguais = [(1, Decimal("10.00"), Decimal("10.00"), Decimal("10.01"), 3),
                  (2, Decimal("10.01"), Decimal("10.00"), Decimal("10.01"), 2)]
        with self.assertLogs(LOGGER, "INFO") as logs:
            self._publicar(CursorFalso(historico_da_run=iguais))
        self.assertIn("0 linha(s) gravada(s), 2 já registrada(s) preservada(s)", logs.output[1])

    def test_falha_no_historico_reverte_a_publicacao(self):
        cursor = CursorFalso(falhar_em="metricas_categoria_historico")
        conexao = ConexaoFalsa(cursor)
        with self.assertNoLogs(LOGGER, "INFO"):
            with self.assertRaisesRegex(RuntimeError, "falha simulada"):
                PublicadorSnapshotPostgres(abrir_conexao=lambda: conexao).publicar_snapshot(EXECUCAO, METRICAS)
        self.assertEqual(conexao.desfecho, "rollback")
        self.assertTrue(conexao.fechada)


if __name__ == "__main__":
    unittest.main()
