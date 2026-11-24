"""Adaptador PostgreSQL da publicação transacional do snapshot diário (#17).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Implementa ``RepositorioPublicacao`` com ``PostgresHook`` e a Connection ``shopbrasil_postgres``,
que usa a conta de escrita (DML sem DDL). Numa única transação: cadastra os códigos novos,
resolve ``categoria_id`` pelo código, apaga o snapshot da data de referência e insere o conjunto
recebido. Apagar e reinserir o conjunto do dia, em vez de fazer upsert linha a linha, é o que tira
do snapshot a categoria ausente da coleta sem tocar nas outras datas nem no cadastro.

Ponto de extensão da #18: ``etapas_adicionais`` recebe objetos ``EtapaTransacional`` que rodam
com o mesmo cursor, depois do snapshot e antes do único commit. Uma exceção numa etapa desfaz
cadastro e snapshot juntos.

Depois do commit, uma linha ``[SHOPBRASIL][PUBLICACAO]`` informa quantas linhas do dia foram
substituídas, para que o log da task mostre a idempotência sem consulta ao banco.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from airflow.providers.postgres.hooks.postgres import PostgresHook

from shopbrasil_pipeline.aplicacao.execucao import Execucao
from shopbrasil_pipeline.aplicacao.publicacao import ResultadoPublicacao
from shopbrasil_pipeline.dominio.metricas import MetricasCategoria

CONN_ID_PADRAO = "shopbrasil_postgres"

log = logging.getLogger(__name__)

# WITH ORDINALITY mantém a ordem da coleção na geração dos ids das categorias novas.
_SQL_CADASTRAR = """
INSERT INTO public.categoria (codigo, descricao)
SELECT c.codigo, c.codigo
FROM unnest(%s::text[]) WITH ORDINALITY AS c(codigo, ordem)
ORDER BY c.ordem
ON CONFLICT (codigo) DO NOTHING
RETURNING codigo
"""

_SQL_RESOLVER = "SELECT codigo, id FROM public.categoria WHERE codigo = ANY(%s::text[])"

_SQL_APAGAR_DIA = "DELETE FROM public.metricas_categoria WHERE data_referencia = %s"

_SQL_INSERIR = """
INSERT INTO public.metricas_categoria
    (categoria_id, data_referencia, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
VALUES (%s, %s, %s, %s, %s, %s)
"""


@dataclass(frozen=True)
class SnapshotNaTransacao:
    """O que uma etapa adicional recebe: a publicação em curso e os ids já resolvidos."""

    execucao: Execucao
    metricas: tuple[MetricasCategoria, ...]
    categoria_ids: Mapping[str, int]


class EtapaTransacional(Protocol):
    """Gravação extra que compartilha a transação da publicação (ponto de extensão da #18).

    Recebe o cursor da transação aberta. Deve usar só esse cursor, sem ``commit``, ``rollback``
    nem nova conexão. Para abortar a publicação inteira, basta lançar uma exceção.
    """

    def __call__(self, cursor: Any, snapshot: SnapshotNaTransacao) -> None: ...


def _conexao_do_hook(conn_id: str) -> Callable[[], Any]:
    def abrir() -> Any:
        return PostgresHook(postgres_conn_id=conn_id).get_conn()

    return abrir


class PublicadorSnapshotPostgres:
    """Repositório de publicação sobre a Connection do Airflow."""

    def __init__(
        self,
        conn_id: str = CONN_ID_PADRAO,
        etapas_adicionais: Sequence[EtapaTransacional] = (),
        abrir_conexao: Callable[[], Any] | None = None,
    ) -> None:
        self._abrir_conexao = abrir_conexao or _conexao_do_hook(conn_id)
        self._etapas = tuple(etapas_adicionais)

    def publicar_snapshot(
        self, execucao: Execucao, metricas: Sequence[MetricasCategoria]
    ) -> ResultadoPublicacao:
        codigos = [m.codigo_categoria for m in metricas]
        conexao = self._abrir_conexao()
        try:
            # O psycopg2 abre a transação no primeiro comando; o bloco ``with conexao`` faz o
            # commit ao sair sem erro e o rollback em qualquer exceção. Não há commit intermediário.
            conexao.autocommit = False
            with conexao:
                with conexao.cursor() as cursor:
                    cursor.execute(_SQL_CADASTRAR, (codigos,))
                    cadastrados = {linha[0] for linha in cursor.fetchall()}

                    cursor.execute(_SQL_RESOLVER, (codigos,))
                    categoria_ids = {codigo: categoria_id for codigo, categoria_id in cursor.fetchall()}
                    faltantes = [c for c in codigos if c not in categoria_ids]
                    if faltantes:
                        raise RuntimeError(f"categorias sem id após o cadastro: {faltantes}")

                    cursor.execute(_SQL_APAGAR_DIA, (execucao.data_referencia,))
                    substituidas = cursor.rowcount
                    # Decimal e date vão direto ao driver, que os envia como numeric e date.
                    cursor.executemany(
                        _SQL_INSERIR,
                        [
                            (
                                categoria_ids[m.codigo_categoria],
                                execucao.data_referencia,
                                m.preco_medio,
                                m.preco_minimo,
                                m.preco_maximo,
                                m.quantidade_produtos,
                            )
                            for m in metricas
                        ],
                    )

                    snapshot = SnapshotNaTransacao(execucao, tuple(metricas), categoria_ids)
                    for etapa in self._etapas:
                        etapa(cursor, snapshot)
        finally:
            conexao.close()

        log.info(
            "[SHOPBRASIL][PUBLICACAO] run=%s data=%s snapshot: %d linha(s) anterior(es) do dia "
            "substituída(s) por %d; categoria(s) nova(s) no cadastro: %s",
            execucao.run_id,
            execucao.data_referencia.isoformat(),
            substituidas,
            len(codigos),
            ", ".join(c for c in codigos if c in cadastrados) or "nenhuma",
        )
        return ResultadoPublicacao(
            run_id=execucao.run_id,
            data_referencia=execucao.data_referencia,
            categorias_publicadas=tuple(codigos),
            categorias_cadastradas=tuple(c for c in codigos if c in cadastrados),
        )
