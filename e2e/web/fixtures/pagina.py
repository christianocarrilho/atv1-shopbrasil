"""Extrai da página do painel o que o E2E da web confere (#19).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Lê o HTML em stdin e imprime: data do filtro | data exibida | estado vazio | linhas da tabela
(células separadas por "/" e linhas por ";") | coleção embutida (JSON canônico).
"""

import html
import json
import re
import sys

pagina = sys.stdin.read()


def achar(padrao):
    m = re.search(padrao, pagina, re.S)
    return html.unescape(m.group(1).strip()) if m else ""


tbody = achar(r'<table id="tabela-metricas">.*?<tbody>(.*?)</tbody>')
linhas = [
    "/".join(html.unescape(c.strip()) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S))
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", tbody, re.S)
]
dados = json.loads(achar(r'<script type="application/json" id="metricas-dados">(.*?)</script>') or "null")
print("|".join([
    achar(r'<input type="date" id="data" name="data" value="([^"]*)"'),
    achar(r'<span id="data-selecionada">(.*?)</span>'),
    achar(r'<p class="vazio" id="estado-vazio">(.*?)</p>'),
    ";".join(linhas),
    json.dumps(dados, ensure_ascii=False, separators=(",", ":")),
]))
