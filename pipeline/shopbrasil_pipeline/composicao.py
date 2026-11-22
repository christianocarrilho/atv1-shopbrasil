"""Ponto de composição do pipeline da ShopBrasil.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Conecta os contratos da aplicação aos adaptadores concretos. É o único lugar em que o DAG obtém
adaptadores, e lê a configuração do ambiente do contêiner.
"""

from __future__ import annotations

import os

from shopbrasil_pipeline.aplicacao.catalogo import FonteCatalogo
from shopbrasil_pipeline.infraestrutura.fakestore import FakeStoreHttp


def fonte_catalogo() -> FonteCatalogo:
    """FakeStore configurada por ``FAKESTORE_BASE_URL``."""
    return FakeStoreHttp(os.environ.get("FAKESTORE_BASE_URL", ""))
