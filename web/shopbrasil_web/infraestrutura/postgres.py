"""Repositório PostgreSQL de leitura das métricas (#9).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Implementa `RepositorioMetricas` com psycopg 3 e a conta de leitura (`SHOPBRASIL_DB_READER_*`),
consultando só o snapshot `metricas_categoria` e o cadastro `categoria`. Cada consulta abre uma
conexão curta em transação somente leitura: o volume do painel é pequeno e isso dispensa pool,
que não faz parte da pilha aprovada na #1. O `read_only` é uma segunda barreira; a primeira é a
conta sem privilégio de escrita. Os `numeric(12,2)` chegam como `Decimal` e seguem assim até a
apresentação, sem passar por float.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from shopbrasil_web.consultas.modelos import Categoria, MetricaCategoria, ObservacaoPreco

SQL_METRICAS_DA_DATA = """
SELECT c.codigo, c.descricao, m.preco_medio, m.preco_minimo, m.preco_maximo, m.quantidade_produtos
  FROM public.metricas_categoria m
  JOIN public.categoria c ON c.id = m.categoria_id
 WHERE m.data_referencia = %(data)s
 ORDER BY c.codigo
"""

SQL_ULTIMA_DATA = "SELECT max(data_referencia) FROM public.metricas_categoria"

SQL_CATEGORIAS = "SELECT codigo, descricao FROM public.categoria ORDER BY codigo"

SQL_CATEGORIA = "SELECT codigo, descricao FROM public.categoria WHERE codigo = %(codigo)s"

SQL_PRECOS_MEDIOS = """
SELECT m.data_referencia, m.preco_medio
  FROM public.metricas_categoria m
  JOIN public.categoria c ON c.id = m.categoria_id
 WHERE c.codigo = %(codigo)s
   AND m.data_referencia BETWEEN %(inicio)s AND %(fim)s
 ORDER BY m.data_referencia
"""


@dataclass(frozen=True)
class ConfiguracaoBanco:
    """Endereço do banco analítico e credenciais da conta de leitura."""

    host: str
    porta: int
    banco: str
    usuario: str
    senha: str = ""
    timeout_conexao: int = 5

    def parametros(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.porta,
            "dbname": self.banco,
            "user": self.usuario,
            "password": self.senha,
            "connect_timeout": self.timeout_conexao,
            "application_name": "shopbrasil_web",
        }

    def __repr__(self) -> str:  # não expõe a senha em logs
        return (f"ConfiguracaoBanco(host={self.host!r}, porta={self.porta}, banco={self.banco!r}, "
                f"usuario={self.usuario!r})")


def conectar_psycopg(configuracao: ConfiguracaoBanco) -> psycopg.Connection:
    return psycopg.connect(**configuracao.parametros())


class RepositorioMetricasPostgres:
    """Leitura do snapshot diário com a conta de leitura."""

    def __init__(
        self,
        configuracao: ConfiguracaoBanco,
        conectar: Callable[[ConfiguracaoBanco], Any] = conectar_psycopg,
    ) -> None:
        self._configuracao = configuracao
        self._conectar = conectar

    @contextmanager
    def _cursor(self) -> Iterator[Any]:
        with self._conectar(self._configuracao) as conexao:
            conexao.read_only = True
            with conexao.cursor() as cursor:
                yield cursor

    def _consultar(self, sql: str, parametros: dict[str, Any] | None = None) -> list[tuple]:
        with self._cursor() as cursor:
            cursor.execute(sql, parametros)
            return list(cursor.fetchall())

    def metricas_da_data(self, data: date) -> Sequence[MetricaCategoria]:
        return [
            MetricaCategoria(Categoria(codigo, descricao), medio, minimo, maximo, quantidade)
            for codigo, descricao, medio, minimo, maximo, quantidade
            in self._consultar(SQL_METRICAS_DA_DATA, {"data": data})
        ]

    def ultima_data(self) -> date | None:
        linhas = self._consultar(SQL_ULTIMA_DATA)
        return linhas[0][0] if linhas else None

    def categorias(self) -> Sequence[Categoria]:
        return [Categoria(codigo, descricao) for codigo, descricao in self._consultar(SQL_CATEGORIAS)]

    def categoria(self, codigo: str) -> Categoria | None:
        linhas = self._consultar(SQL_CATEGORIA, {"codigo": codigo})
        return Categoria(*linhas[0]) if linhas else None

    def precos_medios(self, codigo: str, inicio: date, fim: date) -> Sequence[ObservacaoPreco]:
        return [
            ObservacaoPreco(data, medio)
            for data, medio in self._consultar(SQL_PRECOS_MEDIOS, {"codigo": codigo, "inicio": inicio, "fim": fim})
        ]
