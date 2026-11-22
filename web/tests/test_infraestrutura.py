"""Testes do repositório PostgreSQL de leitura (#9) com conexão falsa.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Conferem o mapeamento das linhas para os modelos de leitura, os parâmetros enviados, a transação
somente leitura e que o SQL toca só `metricas_categoria` e `categoria`. O SQL contra o PostgreSQL
real é coberto por ``e2e/web/executar.sh``. Precisam de psycopg e são pulados sem ele.
"""

from __future__ import annotations

import importlib.util
import re
import unittest
from datetime import date
from decimal import Decimal

TEM_PSYCOPG = importlib.util.find_spec("psycopg") is not None


class CursorFalso:
    def __init__(self, conexao: ConexaoFalsa) -> None:
        self._conexao = conexao

    def __enter__(self) -> CursorFalso:
        return self

    def __exit__(self, *_: object) -> None:
        pass

    def execute(self, sql: str, parametros: dict | None = None) -> None:
        self._conexao.executados.append((sql, parametros, self._conexao.read_only))

    def fetchall(self) -> list[tuple]:
        return self._conexao.linhas


class ConexaoFalsa:
    def __init__(self, linhas: list[tuple]) -> None:
        self.linhas = linhas
        self.read_only = False
        self.executados: list[tuple] = []
        self.fechada = False

    def __enter__(self) -> ConexaoFalsa:
        return self

    def __exit__(self, *_: object) -> None:
        self.fechada = True

    def cursor(self) -> CursorFalso:
        return CursorFalso(self)


@unittest.skipUnless(TEM_PSYCOPG, "psycopg fica no contêiner da web")
class RepositorioMetricasPostgresTest(unittest.TestCase):
    def repositorio(self, linhas: list[tuple]):
        from shopbrasil_web.infraestrutura.postgres import ConfiguracaoBanco, RepositorioMetricasPostgres

        self.configuracao = ConfiguracaoBanco("postgres", 5432, "shopbrasil", "shopbrasil_reader", "segredo")
        self.conexao = ConexaoFalsa(linhas)
        self.recebida = None

        def conectar(configuracao):
            self.recebida = configuracao
            return self.conexao

        return RepositorioMetricasPostgres(self.configuracao, conectar)

    def test_metricas_da_data(self) -> None:
        from shopbrasil_web.consultas.modelos import Categoria, MetricaCategoria

        repositorio = self.repositorio([
            ("electronics", "Eletrônicos", Decimal("10.00"), Decimal("10.00"), Decimal("10.01"), 3),
        ])
        self.assertEqual(
            [MetricaCategoria(Categoria("electronics", "Eletrônicos"),
                              Decimal("10.00"), Decimal("10.00"), Decimal("10.01"), 3)],
            repositorio.metricas_da_data(date(2026, 11, 21)),
        )
        sql, parametros, read_only = self.conexao.executados[0]
        self.assertEqual({"data": date(2026, 11, 21)}, parametros)
        self.assertTrue(read_only)
        self.assertTrue(self.conexao.fechada)
        self.assertIs(self.configuracao, self.recebida)

    def test_ultima_data_vazia_e_preenchida(self) -> None:
        self.assertIsNone(self.repositorio([(None,)]).ultima_data())
        self.assertEqual(date(2026, 11, 22), self.repositorio([(date(2026, 11, 22),)]).ultima_data())

    def test_categoria_e_categorias(self) -> None:
        from shopbrasil_web.consultas.modelos import Categoria

        self.assertIsNone(self.repositorio([]).categoria("toys"))
        self.assertEqual({"codigo": "toys"}, self.conexao.executados[0][1])
        self.assertEqual(Categoria("toys", "toys"), self.repositorio([("toys", "toys")]).categoria("toys"))
        self.assertEqual([Categoria("a", "A")], self.repositorio([("a", "A")]).categorias())

    def test_precos_medios(self) -> None:
        from shopbrasil_web.consultas.modelos import ObservacaoPreco

        repositorio = self.repositorio([(date(2026, 11, 20), Decimal("20.00"))])
        self.assertEqual([ObservacaoPreco(date(2026, 11, 20), Decimal("20.00"))],
                         repositorio.precos_medios("electronics", date(2026, 11, 20), date(2026, 11, 23)))
        self.assertEqual({"codigo": "electronics", "inicio": date(2026, 11, 20), "fim": date(2026, 11, 23)},
                         self.conexao.executados[0][1])

    def test_sql_le_so_snapshot_e_categoria(self) -> None:
        from shopbrasil_web.infraestrutura import postgres

        for nome in ("SQL_METRICAS_DA_DATA", "SQL_ULTIMA_DATA", "SQL_CATEGORIAS", "SQL_CATEGORIA", "SQL_PRECOS_MEDIOS"):
            sql = getattr(postgres, nome)
            with self.subTest(nome=nome):
                self.assertTrue(sql.lstrip().upper().startswith("SELECT"))
                tabelas = set(re.findall(r"(?:FROM|JOIN)\s+public\.(\w+)", sql))
                self.assertTrue(tabelas <= {"metricas_categoria", "categoria"}, tabelas)
                self.assertNotIn("historico", sql)

    def test_parametros_de_conexao(self) -> None:
        from shopbrasil_web.infraestrutura.postgres import ConfiguracaoBanco

        parametros = ConfiguracaoBanco("h", 5432, "shopbrasil", "shopbrasil_reader", "s").parametros()
        self.assertEqual(("h", 5432, "shopbrasil", "shopbrasil_reader", "s", "shopbrasil_web"),
                         (parametros["host"], parametros["port"], parametros["dbname"], parametros["user"],
                          parametros["password"], parametros["application_name"]))


if __name__ == "__main__":
    unittest.main()
