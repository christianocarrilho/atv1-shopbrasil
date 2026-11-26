"""Adaptador PostgreSQL da publicação transacional do snapshot diário e do histórico (#17, #18).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Implementa ``RepositorioPublicacao`` com ``PostgresHook`` e a Connection ``shopbrasil_postgres``,
que usa a conta de escrita (DML sem DDL). Numa única transação:

1. cadastra os códigos de categoria novos (``ON CONFLICT DO NOTHING``) e resolve ``categoria_id``;
2. apaga o snapshot da data de referência e insere o conjunto recebido. Apagar e reinserir o dia,
   em vez de fazer upsert linha a linha, é o que tira do snapshot a categoria ausente da coleta sem
   tocar nas outras datas nem no cadastro;
3. acrescenta o histórico por run (``ON CONFLICT (run_id, categoria_id) DO NOTHING``): uma run nova
   acrescenta linhas e o reprocessamento da mesma run preserva as já registradas.

Uma falha em qualquer passo desfaz os três. Depois do commit, as linhas ``[SHOPBRASIL][PUBLICACAO]``
e ``[SHOPBRASIL][HISTORICO]`` mostram no log quantas linhas foram substituídas, gravadas e
preservadas, sem consulta ao banco. O provider do Airflow é importado só ao abrir a conexão, para
que os testes unitários rodem apenas com a biblioteca padrão.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

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

_SQL_LER_HISTORICO_DA_RUN = """
SELECT categoria_id, preco_medio, preco_minimo, preco_maximo, quantidade_produtos
  FROM public.metricas_categoria_historico
 WHERE run_id = %s AND categoria_id = ANY(%s)
"""

_SQL_INSERIR_HISTORICO = """
INSERT INTO public.metricas_categoria_historico
    (run_id, categoria_id, data_execucao, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id, categoria_id) DO NOTHING
"""

AVISO_DIVERGENCIA = (
    "[SHOPBRASIL][AVISO] histórico preservado diverge do snapshot: run=%s categoria=%s "
    "histórico=%s snapshot=%s (reprocessamento da mesma run mantém a linha original)"
)


def _abrir_conexao_do_hook(conn_id: str) -> Any:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=conn_id).get_conn()


def _valores(m: MetricasCategoria) -> tuple:
    return (m.preco_medio, m.preco_minimo, m.preco_maximo, m.quantidade_produtos)


def _gravar_historico(
    cursor: Any, execucao: Execucao, metricas: Sequence[MetricasCategoria], categoria_ids: Mapping[str, int]
) -> int:
    """Acrescenta o histórico da run no cursor da transação aberta e devolve quantas linhas já existiam.

    Antes do INSERT, lê as linhas que a run já tem (as que o ``ON CONFLICT`` vai preservar) e avisa
    cada categoria cujo valor publicado agora difere do registrado, para que a diferença entre
    snapshot e histórico nunca passe em silêncio.
    """
    publicados = {categoria_ids[m.codigo_categoria]: m for m in metricas}
    cursor.execute(_SQL_LER_HISTORICO_DA_RUN, (execucao.run_id, list(publicados)))
    registradas = cursor.fetchall()
    for categoria_id, *registrado in registradas:
        metrica = publicados[categoria_id]
        if tuple(registrado) != _valores(metrica):
            log.warning(
                AVISO_DIVERGENCIA,
                execucao.run_id,
                metrica.codigo_categoria,
                "/".join(map(str, registrado)),
                "/".join(map(str, _valores(metrica))),
            )
    # Os mesmos objetos Decimal do snapshot vão ao driver, por isso os valores coincidem.
    cursor.executemany(
        _SQL_INSERIR_HISTORICO,
        [
            (execucao.run_id, categoria_ids[m.codigo_categoria], execucao.data_referencia, *_valores(m))
            for m in metricas
        ],
    )
    return len(registradas)


class PublicadorSnapshotPostgres:
    """Repositório de publicação sobre a Connection do Airflow."""

    def __init__(self, conn_id: str = CONN_ID_PADRAO, abrir_conexao: Callable[[], Any] | None = None) -> None:
        self._abrir_conexao = abrir_conexao or (lambda: _abrir_conexao_do_hook(conn_id))

    def publicar_snapshot(self, execucao: Execucao, metricas: Sequence[MetricasCategoria]) -> ResultadoPublicacao:
        codigos = [m.codigo_categoria for m in metricas]
        conexao = self._abrir_conexao()
        try:
            # O psycopg2 abre a transação no primeiro comando; o bloco ``with conexao`` faz o
            # commit ao sair sem erro e o rollback em qualquer exceção. Não há commit intermediário.
            conexao.autocommit = False
            with conexao:
                with conexao.cursor() as cursor:
                    cursor.execute(_SQL_CADASTRAR, (codigos,))
                    novos = {linha[0] for linha in cursor.fetchall()}
                    cadastrados = [c for c in codigos if c in novos]

                    cursor.execute(_SQL_RESOLVER, (codigos,))
                    categoria_ids = dict(cursor.fetchall())
                    faltantes = [c for c in codigos if c not in categoria_ids]
                    if faltantes:
                        raise RuntimeError(f"categorias sem id após o cadastro: {faltantes}")

                    cursor.execute(_SQL_APAGAR_DIA, (execucao.data_referencia,))
                    substituidas = cursor.rowcount
                    # Decimal e date vão direto ao driver, que os envia como numeric e date.
                    cursor.executemany(
                        _SQL_INSERIR,
                        [(categoria_ids[m.codigo_categoria], execucao.data_referencia, *_valores(m)) for m in metricas],
                    )

                    preservadas = _gravar_historico(cursor, execucao, metricas, categoria_ids)
        finally:
            conexao.close()

        log.info(
            "[SHOPBRASIL][PUBLICACAO] run=%s data=%s snapshot: %d linha(s) anterior(es) do dia "
            "substituída(s) por %d; categoria(s) nova(s) no cadastro: %s",
            execucao.run_id,
            execucao.data_referencia.isoformat(),
            substituidas,
            len(codigos),
            ", ".join(cadastrados) or "nenhuma",
        )
        log.info(
            "[SHOPBRASIL][HISTORICO] run=%s: %d linha(s) gravada(s), %d já registrada(s) "
            "preservada(s) sem alteração",
            execucao.run_id,
            len(codigos) - preservadas,
            preservadas,
        )
        return ResultadoPublicacao(
            run_id=execucao.run_id,
            data_referencia=execucao.data_referencia,
            categorias_publicadas=tuple(codigos),
            categorias_cadastradas=tuple(cadastrados),
        )
