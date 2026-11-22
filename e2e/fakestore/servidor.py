"""FakeStore controlada para os cenários E2E do pipeline.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Responde ``GET /products`` no limite HTTP da FakeStore real, consumindo uma fila de respostas
programada pelo cenário em ``POST /_controle``. Com a fila vazia, devolve o catálogo ``padrao``.
Como a API real, responde 403 ao User-Agent padrão ``Python-urllib``.
``GET /_controle`` informa os instantes das requisições recebidas, usados para conferir o backoff.
Usa só a biblioteca padrão e roda na imagem do Airflow já baixada pelo ambiente.

Itens da fila: ``{"status": 500}``, ``{"catalogo": "<nome em catalogos/>"}`` ou
``{"corpo": "<texto bruto>"}``, com ``"atraso"`` opcional em segundos.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

CATALOGOS = Path(__file__).parent / "catalogos"
estado = {"fila": [], "requisicoes": []}
trava = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def _responder(self, status: int, corpo: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(corpo)))
        self.end_headers()
        self.wfile.write(corpo)

    def do_GET(self):  # noqa: N802
        if self.path == "/_controle":
            with trava:
                self._responder(200, json.dumps(estado).encode())
            return
        if self.path == "/_saude":
            self._responder(200, b"{}")
            return
        if self.path.rstrip("/") != "/products":
            self._responder(404, b'{"erro": "rota desconhecida"}')
            return
        if self.headers.get("User-Agent", "").startswith("Python-urllib"):
            self._responder(403, b'{"erro": "user-agent recusado"}')
            return
        with trava:
            estado["requisicoes"].append(time.time())
            item = estado["fila"].pop(0) if estado["fila"] else {"catalogo": "padrao"}
        time.sleep(float(item.get("atraso", 0)))
        if "corpo" in item:
            self._responder(int(item.get("status", 200)), item["corpo"].encode())
        elif "catalogo" in item:
            self._responder(int(item.get("status", 200)), (CATALOGOS / f"{item['catalogo']}.json").read_bytes())
        else:
            self._responder(int(item["status"]), b'{"erro": "falha simulada"}')

    def do_POST(self):  # noqa: N802
        if self.path != "/_controle":
            self._responder(404, b"{}")
            return
        pedido = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        with trava:
            estado["fila"] = list(pedido.get("respostas", []))
            if pedido.get("zerar_requisicoes", True):
                estado["requisicoes"] = []
        self._responder(200, json.dumps(estado).encode())

    def log_message(self, formato, *args):
        print("fakestore:", formato % args, flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
