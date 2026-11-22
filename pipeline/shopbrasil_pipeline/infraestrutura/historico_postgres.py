"""Etapa transacional que grava o histórico por run da publicação (#18).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Implementa ``EtapaTransacional`` do adaptador da #17: com o cursor da transação aberta, insere em
``metricas_categoria_historico`` as quatro métricas de cada categoria publicada, com o ``run_id``
da execução e ``data_execucao`` igual à data de referência. ``ON CONFLICT (run_id, categoria_id)
DO NOTHING`` faz o append: uma run distinta acrescenta linhas e o reprocessamento da mesma run
mantém as já registradas, sem atualizá-las. A etapa não faz commit nem rollback; uma exceção do
banco desfaz cadastro e snapshot junto com o histórico.
O módulo não importa o provider do Airflow para que os testes unitários rodem só com a
biblioteca padrão; o tipo do snapshot vem do adaptador apenas na checagem estática.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shopbrasil_pipeline.infraestrutura.publicacao_postgres import SnapshotNaTransacao

_SQL_INSERIR_HISTORICO = """
INSERT INTO public.metricas_categoria_historico
    (run_id, categoria_id, data_execucao, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (run_id, categoria_id) DO NOTHING
"""


class RegistrarHistoricoPostgres:
    """Etapa adicional da publicação: append do histórico por ``(run_id, categoria_id)``."""

    def __call__(self, cursor: Any, snapshot: SnapshotNaTransacao) -> None:
        execucao = snapshot.execucao
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
