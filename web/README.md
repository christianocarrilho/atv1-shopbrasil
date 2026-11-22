# web

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Organiza o serviço de leitura das métricas e o dashboard. A #9 entrega a API JSON e as camadas; a página das #19–#21 entra na mesma aplicação FastAPI e chama os mesmos casos de uso.

## Camadas

| Camada | Arquivos | Responsabilidade |
|---|---|---|
| `consultas` | [modelos.py](shopbrasil_web/consultas/modelos.py), [repositorio.py](shopbrasil_web/consultas/repositorio.py), [casos_uso.py](shopbrasil_web/consultas/casos_uso.py) | Modelos de leitura, contrato `RepositorioMetricas` e casos de uso. Só biblioteca padrão, sem HTTP nem PostgreSQL. |
| `infraestrutura` | [postgres.py](shopbrasil_web/infraestrutura/postgres.py) | `RepositorioMetricasPostgres`: psycopg 3, conta de leitura, transação somente leitura, SQL só sobre `metricas_categoria` e `categoria`. |
| `apresentacao` | [aplicacao.py](shopbrasil_web/apresentacao/aplicacao.py), [controllers.py](shopbrasil_web/apresentacao/controllers.py), [modelos_http.py](shopbrasil_web/apresentacao/modelos_http.py), [pagina.py](shopbrasil_web/apresentacao/pagina.py), [templates/](shopbrasil_web/apresentacao/templates/) | FastAPI, validação dos filtros, tradução para JSON e página do painel (Jinja2). Sem SQL. |
| composição | [composicao.py](shopbrasil_web/composicao.py) | Lê o ambiente, cria repositório, casos de uso e aplicação (`criar_app`, usada pelo uvicorn com `--factory`). |

O pacote não importa `pipeline/`: o contrato entre os módulos é o esquema em [db/schema.sql](../db/schema.sql). As leituras usam só o snapshot diário; o histórico por run é demonstrado por consulta ao banco.

## Contrato HTTP — para as #19–#21

Base: `http://localhost:${SHOPBRASIL_WEB_PORT}` (padrão 8000, só em 127.0.0.1). A documentação OpenAPI gerada fica em `/docs` e `/openapi.json`.

Convenções:

- Datas em ISO 8601, sempre `AAAA-MM-DD`, na entrada e na saída.
- Preços são **strings decimais com duas casas** (`"10.00"`), iguais ao `numeric(12,2)` publicado. Não converta para número antes de exibir na tabela; para o Chart.js, `Number(valor)` só no gráfico.
- `quantidade_produtos` é inteiro.
- Ausência de dados é explícita: `vazia: true` com `metricas: []` para dia sem registros, `data: null` sem nenhuma publicação e `preco_medio: null` para dia sem observação na série. `null` nunca é zero.

### `GET /api/metricas?data=AAAA-MM-DD`

Métricas do snapshot da data, uma linha por categoria, em ordem de código.

```json
{
  "data": "2026-11-22",
  "vazia": false,
  "metricas": [
    {"categoria": {"codigo": "electronics", "descricao": "Eletrônicos"},
     "preco_medio": "10.00", "preco_minimo": "10.00", "preco_maximo": "10.01", "quantidade_produtos": 3}
  ]
}
```

Data sem registros: `200` com `{"data": "2026-11-21", "vazia": true, "metricas": []}`. A #19 exibe “Sem métricas para esta data” quando `vazia` é `true`; a #20 usa a mesma coleção para as barras.

### `GET /api/metricas/ultima-data`

Data mais recente com métricas, para a página abrir nela: `{"data": "2026-11-24"}`, ou `{"data": null}` quando o snapshot está vazio.

### `GET /api/categorias`

Categorias cadastradas, em ordem de código, para o filtro da evolução: `{"categorias": [{"codigo": "electronics", "descricao": "Eletrônicos"}]}`. Inclui categorias ausentes da coleta mais recente, porque o cadastro permanece.

### `GET /api/evolucao?categoria=<código>&inicio=AAAA-MM-DD&fim=AAAA-MM-DD`

Série do preço médio da categoria no intervalo **inclusivo** `[inicio, fim]`, com **um ponto por dia**, em ordem crescente de data. Dias sem a categoria no snapshot têm `preco_medio: null`, sem interpolação nem zero; com `spanGaps: false` (padrão do Chart.js) eles viram lacunas na linha. O intervalo aceita até 366 dias. O `categoria` é o código exato, com escape de URL (`men%27s%20clothing`).

```json
{
  "categoria": {"codigo": "electronics", "descricao": "Eletrônicos"},
  "inicio": "2026-11-20",
  "fim": "2026-11-23",
  "pontos": [
    {"data": "2026-11-20", "preco_medio": "20.00"},
    {"data": "2026-11-21", "preco_medio": null},
    {"data": "2026-11-22", "preco_medio": "10.00"},
    {"data": "2026-11-23", "preco_medio": null}
  ]
}
```

### Erros

| Status | Quando | Corpo |
|---|---|---|
| `422` | Parâmetro obrigatório ausente, data fora de `AAAA-MM-DD` ou inexistente (`2026-02-30`) | `{"detail": [{"loc": ["query", "data"], "msg": "data inválida '2026-02-30'; use AAAA-MM-DD", "type": "data_invalida"}]}` (formato de validação do FastAPI) |
| `400` | `inicio` depois de `fim` ou intervalo maior que 366 dias | `{"detail": "intervalo inválido: início 2026-11-24 depois do fim 2026-11-20"}` |
| `404` | Categoria não cadastrada em `/api/evolucao` | `{"detail": "categoria 'toys' não cadastrada"}` |

## Painel — #19

`GET /` renderiza o painel no servidor com Jinja2, usando os mesmos casos de uso e modelos HTTP da API.

- Sem `data`, abre na data mais recente com métricas (`ultima-data`). Selecionar outra data no campo recarrega a página com `?data=AAAA-MM-DD`.
- A tabela mostra categoria, preço médio, mínimo, máximo e quantidade de produtos, com os mesmos valores do JSON. Data sem registros exibe “Sem métricas para esta data”; sem nenhuma publicação, “Nenhuma métrica publicada até o momento”.
- A coleção carregada fica embutida como JSON em `<script type="application/json" id="metricas-dados">`, igual a `/api/metricas?data=…`, para as barras da #20. A seção `#evolucao` é o ponto de inserção da linha da #21.
- Data inválida responde `422`, como na API. A página fica fora do OpenAPI.

## Configuração e execução

O serviço `web` do [docker-compose.yml](../docker-compose.yml) é construído de [Dockerfile](Dockerfile) (`python:3.12.16-slim`, dependências fixadas em [requirements.txt](requirements.txt)) e recebe só `SHOPBRASIL_DB_HOST`, `SHOPBRASIL_DB_PORT`, `SHOPBRASIL_DB_NAME`, `SHOPBRASIL_DB_READER_USER` e `SHOPBRASIL_DB_READER_PASSWORD`. Variável ausente impede a subida com `defina <VARIÁVEL> no ambiente da Web/API`.

```sh
docker compose up -d --build --wait web                     # postgres → db-init → web
curl -s http://localhost:8000/api/metricas/ultima-data
docker compose run --rm --no-deps web python -m unittest discover -s tests -t .
```

## Testes

- `python3 -m unittest discover -s web/tests -t web`: roda em qualquer Python 3.12+. Os testes de `consultas` usam só a biblioteca padrão; os de apresentação, composição e infraestrutura precisam de FastAPI e psycopg e são pulados fora do contêiner.
- No contêiner (comando acima), todos rodam. A API é chamada como aplicação ASGI por um driver mínimo, porque o `TestClient` exige httpx, fora da pilha da #1.
- [e2e/web/executar.sh](../e2e/web/executar.sh) cobre o SQL contra o PostgreSQL 18.7 real, as rotas pela porta publicada e a conta somente leitura.
