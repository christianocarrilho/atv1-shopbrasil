"""Adaptador HTTP da FakeStore (#13).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Consulta ``GET {FAKESTORE_BASE_URL}/products`` com timeout de 15 segundos e decodifica o JSON
com ``parse_float`` e ``parse_int`` em ``Decimal``, porque o catálogo mistura preços inteiros e
fracionários e nenhum deles pode passar por float. Usa ``urllib`` da biblioteca padrão para não
acrescentar dependência ao Airflow. Não trata erros: HTTP, conexão, timeout e JSON inválido são
propagados para que a retentativa da task os trate.
"""

from __future__ import annotations

import json
import urllib.request
from decimal import Decimal

TIMEOUT_SEGUNDOS = 15
ROTA_PRODUTOS = "/products"


class FakeStoreHttp:
    """Implementa ``FonteCatalogo`` sobre a API HTTP da FakeStore."""

    def __init__(self, base_url: str, timeout: float = TIMEOUT_SEGUNDOS) -> None:
        if not base_url:
            raise ValueError("FAKESTORE_BASE_URL não definida")
        self.url = base_url.rstrip("/") + ROTA_PRODUTOS
        self.timeout = timeout

    def buscar_produtos(self) -> object:
        requisicao = urllib.request.Request(self.url, headers={"Accept": "application/json"})
        # urlopen levanta HTTPError para status 4xx/5xx e URLError/TimeoutError para rede.
        with urllib.request.urlopen(requisicao, timeout=self.timeout) as resposta:
            corpo = resposta.read()
        return json.loads(
            corpo,
            parse_float=Decimal,
            parse_int=Decimal,
            parse_constant=Decimal,  # NaN e Infinity viram Decimal não finito, recusado pela #6.
        )
