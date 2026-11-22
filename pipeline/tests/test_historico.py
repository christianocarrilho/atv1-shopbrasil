"""Testes da etapa de histórico por run e da sua ligação na composição (#18).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Usam cursor e conexão falsos, sem PostgreSQL. O provider do Airflow é substituído por um módulo
vazio em ``sys.modules`` só para importar o adaptador da #17 e conferir a ordem das gravações e o
desfecho da transação; o comportamento no banco real é coberto por ``e2e/publicacao``. Rodam só
com a biblioteca padrão: ``python3 -m unittest discover -s pipeline/tests -t pipeline``.
"""

from __future__ import annotations

import sys
import types
import unittest
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from unittest import mock

from shopbrasil_pipeline.aplicacao.execucao import Execucao
from shopbrasil_pipeline.dominio.metricas import MetricasCategoria
from shopbrasil_pipeline.infraestrutura.historico_postgres import RegistrarHistoricoPostgres

EXECUCAO = Execucao("scheduled__2026-11-21T09:00:00+00:00", date(2026, 11, 21))
METRICAS = (
    MetricasCategoria("electronics", Decimal("10.00"), Decimal("10.00"), Decimal("10.01"), 3),
    MetricasCategoria("jewelery", Decimal("10.01"), Decimal("10.00"), Decimal("10.01"), 2),
)


def _provider_falso() -> dict[str, types.ModuleType]:
    """Módulos mínimos para importar ``publicacao_postgres`` sem o Airflow instalado."""
    nomes = ["airflow", "airflow.providers", "airflow.providers.postgres", "airflow.providers.postgres.hooks"]
    modulos = {nome: types.ModuleType(nome) for nome in nomes}
    hooks = types.ModuleType("airflow.providers.postgres.hooks.postgres")
    hooks.PostgresHook = object  # nunca instanciado: os testes injetam ``abrir_conexao``
    modulos[hooks.__name__] = hooks
    return modulos


@dataclass(frozen=True)
class SnapshotFalso:
    execucao: Execucao
    metricas: tuple[MetricasCategoria, ...]
    categoria_ids: dict[str, int]


class CursorFalso:
    def __init__(self, falhar_em: str | None = None) -> None:
        self.comandos: list[tuple[str, object]] = []
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

    def executemany(self, sql, linhas):
        self._registrar(sql, list(linhas))

    def fetchall(self):
        return self._resultado


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


class RegistrarHistoricoTest(unittest.TestCase):
    def test_insere_uma_linha_por_categoria_com_run_e_data_de_referencia(self):
        cursor = CursorFalso()
        RegistrarHistoricoPostgres()(cursor, SnapshotFalso(EXECUCAO, METRICAS, {"electronics": 7, "jewelery": 9}))
        self.assertEqual(len(cursor.comandos), 1)
        sql, linhas = cursor.comandos[0]
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
        RegistrarHistoricoPostgres()(cursor, SnapshotFalso(EXECUCAO, METRICAS[:1], {"electronics": 1}))
        linha = cursor.comandos[0][1][0]
        self.assertIs(linha[3], METRICAS[0].preco_medio)
        self.assertIsInstance(linha[3], Decimal)

    def test_nao_controla_a_transacao(self):
        cursor = CursorFalso()
        RegistrarHistoricoPostgres()(cursor, SnapshotFalso(EXECUCAO, METRICAS, {"electronics": 1, "jewelery": 2}))
        texto = " ".join(sql for sql, _ in cursor.comandos).upper()
        for palavra in ("COMMIT", "ROLLBACK", "BEGIN"):
            self.assertNotIn(palavra, texto)


class PublicacaoComHistoricoTest(unittest.TestCase):
    def setUp(self):
        # patch.dict restaura sys.modules ao final, descartando também o adaptador importado aqui.
        patcher = mock.patch.dict(sys.modules, _provider_falso())
        patcher.start()
        self.addCleanup(patcher.stop)
        sys.modules.pop("shopbrasil_pipeline.infraestrutura.publicacao_postgres", None)
        from shopbrasil_pipeline.infraestrutura import publicacao_postgres

        self.modulo = publicacao_postgres

    def _publicar(self, cursor: CursorFalso) -> ConexaoFalsa:
        conexao = ConexaoFalsa(cursor)
        publicador = self.modulo.PublicadorSnapshotPostgres(
            etapas_adicionais=(RegistrarHistoricoPostgres(),), abrir_conexao=lambda: conexao
        )
        try:
            publicador.publicar_snapshot(EXECUCAO, METRICAS)
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
                "INSERT public.metricas_categoria_historico",
            ],
        )
        self.assertFalse(conexao.autocommit)
        self.assertEqual(conexao.desfecho, "commit")

    def test_falha_no_historico_reverte_a_publicacao(self):
        cursor = CursorFalso(falhar_em="metricas_categoria_historico")
        conexao = ConexaoFalsa(cursor)
        publicador = self.modulo.PublicadorSnapshotPostgres(
            etapas_adicionais=(RegistrarHistoricoPostgres(),), abrir_conexao=lambda: conexao
        )
        with self.assertRaisesRegex(RuntimeError, "falha simulada"):
            publicador.publicar_snapshot(EXECUCAO, METRICAS)
        self.assertEqual(conexao.desfecho, "rollback")
        self.assertTrue(conexao.fechada)

    def test_composicao_liga_o_historico_na_publicacao(self):
        from shopbrasil_pipeline import composicao

        caso_de_uso = composicao.publicar_snapshot()
        etapas = caso_de_uso._repositorio._etapas
        self.assertEqual([type(e) for e in etapas], [RegistrarHistoricoPostgres])


if __name__ == "__main__":
    unittest.main()
