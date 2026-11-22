"""Ponto de composição do pipeline da ShopBrasil.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Conecta os contratos da aplicação aos adaptadores concretos: FakeStore (#13) e publicação no
PostgreSQL (#17), com o histórico por run (#18) na mesma transação. O DAG obtém daqui fontes e
casos de uso já montados e não importa a infraestrutura. A configuração vem do ambiente do
contêiner. Os adaptadores PostgreSQL são importados dentro da função para que os testes
unitários, só com a biblioteca padrão, importem este módulo sem o provider do Airflow. O histórico entra em ``etapas_adicionais`` para gravar com o mesmo
cursor, depois do snapshot e antes do único commit.
"""

from __future__ import annotations

import os

from shopbrasil_pipeline.aplicacao.catalogo import FonteCatalogo
from shopbrasil_pipeline.aplicacao.publicacao import PublicarSnapshot
from shopbrasil_pipeline.infraestrutura.fakestore import FakeStoreHttp

CONN_ID_PADRAO = "shopbrasil_postgres"


def fonte_catalogo() -> FonteCatalogo:
    """FakeStore configurada por ``FAKESTORE_BASE_URL``."""
    return FakeStoreHttp(os.environ.get("FAKESTORE_BASE_URL", ""))


def publicar_snapshot(conn_id: str = CONN_ID_PADRAO) -> PublicarSnapshot:
    """Caso de uso da publicação ligado ao PostgreSQL pela Connection do Airflow.

    Cadastro, snapshot e histórico por run são confirmados juntos ou revertidos juntos.
    """
    from shopbrasil_pipeline.infraestrutura.historico_postgres import RegistrarHistoricoPostgres
    from shopbrasil_pipeline.infraestrutura.publicacao_postgres import PublicadorSnapshotPostgres

    return PublicarSnapshot(
        PublicadorSnapshotPostgres(conn_id=conn_id, etapas_adicionais=(RegistrarHistoricoPostgres(),))
    )
