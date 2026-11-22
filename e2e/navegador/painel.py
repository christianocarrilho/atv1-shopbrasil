"""Exercita o painel num Chromium real e imprime o que foi desenhado (#23).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Uso: python painel.py <base> <data> <data_vazia> <categoria> <inicio> <fim>

Abre o painel na data, lê a tabela e os dados efetivamente entregues ao Chart.js nas barras;
troca a data no campo, como o usuário, para um dia sem métricas; depois escolhe categoria e
intervalo no formulário e clica em Atualizar, lendo a linha desenhada. Imprime um JSON com o
observado e os erros de console ou de página, para o cenário comparar.
"""

import json
import sys

from playwright.sync_api import sync_playwright

base, data, data_vazia, categoria, inicio, fim = sys.argv[1:7]

LER_GRAFICO = """id => {
  const c = document.getElementById(id);
  const g = c && window.Chart && Chart.getChart(c);
  if (!g) return null;
  const d = g.data.datasets[0];
  return {tipo: g.config.type, rotulos: g.data.labels, valores: d.data, spanGaps: d.spanGaps === true,
          desenhado: g.getDatasetMeta(0).data.length};
}"""

resultado = {"erros": []}
with sync_playwright() as p:
    navegador = p.chromium.launch()
    pagina = navegador.new_page()
    pagina.on("console", lambda m: m.type == "error" and resultado["erros"].append(m.text))
    pagina.on("pageerror", lambda e: resultado["erros"].append(str(e)))

    pagina.goto(f"{base}/?data={data}")
    pagina.wait_for_function("() => window.Chart && Chart.getChart(document.getElementById('grafico-barras'))")
    resultado["chartjs"] = pagina.evaluate("() => Chart.version")
    resultado["tabela_preco_medio"] = pagina.eval_on_selector_all(
        "#tabela-metricas tbody tr", "rs => rs.map(r => r.cells[1].textContent.trim())")
    resultado["barras"] = pagina.evaluate(LER_GRAFICO, "grafico-barras")

    # Troca de data pelo campo: o evento change envia o formulário.
    with pagina.expect_navigation():
        pagina.fill("#data", data_vazia)
        pagina.dispatch_event("#data", "change")
    resultado["vazio_url"] = pagina.url.split("?", 1)[1].split("&")[0]
    resultado["vazio_texto"] = pagina.inner_text("#estado-vazio")
    resultado["vazio_barras"] = pagina.inner_text("#barras-vazio")
    resultado["vazio_tem_grafico_barras"] = pagina.query_selector("#grafico-barras") is not None

    # Filtros da evolução: categoria e intervalo, enviados pelo botão Atualizar.
    pagina.select_option("#categoria", categoria)
    pagina.fill("#inicio", inicio)
    pagina.fill("#fim", fim)
    with pagina.expect_navigation():
        pagina.click("#evolucao button[type=submit]")
    pagina.wait_for_function("() => window.Chart && Chart.getChart(document.getElementById('grafico-evolucao'))")
    resultado["linha"] = pagina.evaluate(LER_GRAFICO, "grafico-evolucao")
    navegador.close()

print(json.dumps(resultado, ensure_ascii=False, separators=(",", ":")))
