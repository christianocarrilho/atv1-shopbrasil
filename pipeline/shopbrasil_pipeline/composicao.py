"""Ponto de composição do pipeline da ShopBrasil.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Conecta os contratos da aplicação aos adaptadores concretos. O DAG obtém daqui os casos de uso
já montados e não importa a infraestrutura. A #18 acrescenta o histórico em
``etapas_adicionais`` para que ele grave na mesma transação do snapshot.
"""

from __future__ import annotations

from shopbrasil_pipeline.aplicacao.publicacao import PublicarSnapshot
from shopbrasil_pipeline.infraestrutura.publicacao_postgres import (
    CONN_ID_PADRAO,
    PublicadorSnapshotPostgres,
)


def publicar_snapshot(conn_id: str = CONN_ID_PADRAO) -> PublicarSnapshot:
    """Caso de uso da publicação ligado ao PostgreSQL pela Connection do Airflow."""
    return PublicarSnapshot(PublicadorSnapshotPostgres(conn_id=conn_id, etapas_adicionais=()))
