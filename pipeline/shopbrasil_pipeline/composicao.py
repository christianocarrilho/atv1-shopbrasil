"""Ponto de composição do pipeline da ShopBrasil.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Conecta os contratos da aplicação aos adaptadores concretos: FakeStore (#13), publicação no
PostgreSQL (#17) e notificador dos callbacks (#14). O DAG obtém daqui fontes e casos de uso já
montados e não importa a infraestrutura. A configuração vem do ambiente do contêiner. O adaptador PostgreSQL é importado
dentro da função para que os testes unitários, só com a biblioteca padrão, importem este módulo
sem o provider do Airflow. A #18 acrescenta o histórico em ``etapas_adicionais`` para que ele
grave na mesma transação do snapshot.
"""

from __future__ import annotations

import os

from shopbrasil_pipeline.aplicacao.alertas import Notificador
from shopbrasil_pipeline.aplicacao.catalogo import FonteCatalogo
from shopbrasil_pipeline.aplicacao.publicacao import PublicarSnapshot
from shopbrasil_pipeline.infraestrutura.fakestore import FakeStoreHttp
from shopbrasil_pipeline.infraestrutura.notificador_log import NotificadorLog

CONN_ID_PADRAO = "shopbrasil_postgres"


def fonte_catalogo() -> FonteCatalogo:
    """FakeStore configurada por ``FAKESTORE_BASE_URL``."""
    return FakeStoreHttp(os.environ.get("FAKESTORE_BASE_URL", ""))


def publicar_snapshot(conn_id: str = CONN_ID_PADRAO) -> PublicarSnapshot:
    """Caso de uso da publicação ligado ao PostgreSQL pela Connection do Airflow."""
    from shopbrasil_pipeline.infraestrutura.publicacao_postgres import PublicadorSnapshotPostgres

    return PublicarSnapshot(PublicadorSnapshotPostgres(conn_id=conn_id, etapas_adicionais=()))


def notificador() -> Notificador:
    """Notificador por log; o destino do alerta simulado vem de ``SHOPBRASIL_ALERTA_DESTINO``."""
    return NotificadorLog(os.environ.get("SHOPBRASIL_ALERTA_DESTINO", "equipe-dados@shopbrasil.example"))
