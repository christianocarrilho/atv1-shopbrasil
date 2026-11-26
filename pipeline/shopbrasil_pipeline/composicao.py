"""Ponto de composição do pipeline da ShopBrasil.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Conecta os contratos da aplicação aos adaptadores concretos: a FakeStore (#13) e a publicação no
PostgreSQL (#17), que grava cadastro, snapshot e histórico por run (#18) numa só transação. O DAG
obtém daqui fonte e caso de uso já montados e não importa a infraestrutura. A configuração vem do
ambiente do contêiner.
"""

from __future__ import annotations

import os

from shopbrasil_pipeline.aplicacao.catalogo import FonteCatalogo
from shopbrasil_pipeline.aplicacao.publicacao import PublicarSnapshot
from shopbrasil_pipeline.infraestrutura.fakestore import FakeStoreHttp
from shopbrasil_pipeline.infraestrutura.publicacao_postgres import CONN_ID_PADRAO, PublicadorSnapshotPostgres


def fonte_catalogo() -> FonteCatalogo:
    """FakeStore configurada por ``FAKESTORE_BASE_URL``."""
    return FakeStoreHttp(os.environ.get("FAKESTORE_BASE_URL", ""))


def publicar_snapshot(conn_id: str = CONN_ID_PADRAO) -> PublicarSnapshot:
    """Caso de uso da publicação ligado ao PostgreSQL pela Connection do Airflow."""
    return PublicarSnapshot(PublicadorSnapshotPostgres(conn_id=conn_id))
