"""Etapa transacional que grava o histórico por run da publicação (#18).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Implementa ``EtapaTransacional`` do adaptador da #17: com o cursor da transação aberta, insere em
``metricas_categoria_historico`` as quatro métricas de cada categoria publicada, com o ``run_id``
da execução e ``data_execucao`` igual à data de referência. O PostgreSQL preenche ``publicado_em``
na primeira inserção. ``ON CONFLICT (run_id, categoria_id)
DO NOTHING`` faz o append: uma run distinta acrescenta linhas e o reprocessamento da mesma run
mantém as já registradas, sem atualizá-las, como manda a especificação. Antes do INSERT, a etapa lê as
linhas que a run já tem: a contagem vai para a linha ``[SHOPBRASIL][HISTORICO]`` (gravadas e
preservadas) e, se o reprocessamento publicar valores diferentes dos já registrados, a etapa emite um
aviso ``[SHOPBRASIL][AVISO] histórico preservado diverge do snapshot`` por categoria, para que a
diferença entre snapshot e histórico nunca passe em silêncio. A etapa não faz commit nem rollback;
uma exceção do banco desfaz cadastro e snapshot junto com o histórico.
O módulo não importa o provider do Airflow para que os testes unitários rodem só com a
biblioteca padrão; o tipo do snapshot vem do adaptador apenas na checagem estática.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shopbrasil_pipeline.infraestrutura.publicacao_postgres import SnapshotNaTransacao

_SQL_INSERIR_HISTORICO = """
INSERT INTO public.metricas_categoria_historico
    (run_id, categoria_id, data_execucao, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id, categoria_id) DO NOTHING
"""

_SQL_LER_HISTORICO_DA_RUN = """
SELECT categoria_id, preco_medio, preco_minimo, preco_maximo, quantidade_produtos
  FROM public.metricas_categoria_historico
 WHERE run_id = %s AND categoria_id = ANY(%s)
"""

AVISO_DIVERGENCIA = (
    "[SHOPBRASIL][AVISO] histórico preservado diverge do snapshot: run=%s categoria=%s "
    "histórico=%s snapshot=%s (reprocessamento da mesma run mantém a linha original)"
)

log = logging.getLogger(__name__)


def _metricas(linha: tuple) -> str:
    return "/".join(str(v) for v in linha)


class RegistrarHistoricoPostgres:
    """Etapa adicional da publicação: append do histórico por ``(run_id, categoria_id)``."""

    def __call__(self, cursor: Any, snapshot: SnapshotNaTransacao) -> list[str]:
        execucao = snapshot.execucao
        # Lido antes do INSERT: as linhas que já existem são as que o ON CONFLICT vai preservar.
        divergentes, preservadas = self._avisar_divergencias(cursor, snapshot)
        # Os mesmos objetos Decimal do snapshot vão ao driver, por isso os valores coincidem.
        cursor.executemany(
            _SQL_INSERIR_HISTORICO,
            [
                (
                    execucao.run_id,
                    snapshot.categoria_ids[m.codigo_categoria],
                    execucao.data_referencia,
                    m.preco_medio,
                    m.preco_minimo,
                    m.preco_maximo,
                    m.quantidade_produtos,
                )
                for m in snapshot.metricas
            ],
        )
        log.info(
            "[SHOPBRASIL][HISTORICO] run=%s: %d linha(s) gravada(s), %d já registrada(s) "
            "preservada(s) sem alteração",
            execucao.run_id,
            len(snapshot.metricas) - preservadas,
            preservadas,
        )
        return divergentes

    @staticmethod
    def _avisar_divergencias(cursor: Any, snapshot: SnapshotNaTransacao) -> tuple[list[str], int]:
        """Compara o histórico já registrado da run com o snapshot e avisa cada categoria diferente.

        Devolve as categorias divergentes e quantas linhas a run já tinha.
        """
        execucao = snapshot.execucao
        publicados = {
            snapshot.categoria_ids[m.codigo_categoria]: (m.codigo_categoria, (m.preco_medio, m.preco_minimo, m.preco_maximo, m.quantidade_produtos))
            for m in snapshot.metricas
        }
        cursor.execute(_SQL_LER_HISTORICO_DA_RUN, (execucao.run_id, list(publicados)))
        divergentes = []
        registradas = cursor.fetchall()
        for categoria_id, *registrado in registradas:
            codigo, publicado = publicados[categoria_id]
            if tuple(registrado) != publicado:
                log.warning(AVISO_DIVERGENCIA, execucao.run_id, codigo, _metricas(tuple(registrado)), _metricas(publicado))
                divergentes.append(codigo)
        return sorted(divergentes), len(registradas)
