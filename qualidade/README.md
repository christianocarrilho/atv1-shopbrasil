# qualidade

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Reúne o lint de arquitetura da #26. Ele verifica os imports do Pipeline e da Web/API contra as regras do [AGENTS.md](../AGENTS.md) e da [especificação](../docs/especificacao.md). Usa apenas a biblioteca padrão do Python 3.11 ou superior, para não depender das versões e da tecnologia Web/API que a #1 define.

## Comandos

Na raiz do repositório:

```bash
python3 qualidade/lint_arquitetura.py            # verifica pipeline/ e web/
python3 qualidade/lint_arquitetura.py --listar   # mostra também a camada de cada arquivo
python3 -m unittest discover -s qualidade/tests  # exemplos permitidos e proibidos
```

O lint retorna `0` sem violações, `1` quando encontra violações e `2` para erro de uso ou de configuração. Cada violação informa `arquivo:linha:coluna`, o código, a regra, o trecho do import e a correção sugerida:

```text
pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py:1:1: ARQ002 camada pipeline_aplicacao não pode importar pipeline_infraestrutura ('..infraestrutura.postgres'); permitidas: pipeline_dominio [from ..infraestrutura.postgres import Publicador]
    correção: A aplicação usa o domínio e declara contratos. Defina um Protocol na aplicação e implemente-o em infraestrutura; a composição injeta o adaptador.
```

## Regras

As camadas e dependências permitidas ficam em [arquitetura.toml](arquitetura.toml). Cada camada tem lista de permissões, então uma dependência nova só passa com alteração revisada da configuração. Não amplie permissões para fazer o lint passar.

| Código | Verificação |
|---|---|
| `ARQ001` | Import entre `pipeline/` e `web/`, em qualquer sentido. Vale para todos os arquivos dos dois módulos e não tem exceção configurável. |
| `ARQ002` | Camada importa outra camada interna fora de `pode_importar`, ou um destino sem camada. |
| `ARQ003` | Camada sem permissão para pacotes externos usa um pacote fora da biblioteca padrão. |
| `ARQ004` | Camada usa um módulo vetado em `proibidos`, como driver PostgreSQL, cliente HTTP ou hook do provider no DAG. |
| `ARQ005` | Import relativo sai do pacote de nível superior. |
| `ARQ006` | Arquivo dentro de `shopbrasil_pipeline/` ou `shopbrasil_web/` sem camada declarada. |
| `ARQ007` | Arquivo Python que não pode ser analisado. |

Direção das dependências:

- Pipeline: `dominio` usa só a biblioteca padrão. `aplicacao` usa `dominio`. `infraestrutura` usa `aplicacao` e `dominio`. A composição (`composicao.py` ou `composicao/`, no pacote ou na pasta de DAGs) é o único ponto, além da própria infraestrutura, que importa adaptadores concretos. Os DAGs usam `aplicacao`, `dominio` e a composição, sem drivers de banco, clientes HTTP ou hooks de PostgreSQL e HTTP.
- Web/API: `consultas` não depende de outras camadas, de pacotes externos nem de HTTP ou PostgreSQL. `apresentacao` usa `consultas` e não acessa banco. `infraestrutura` implementa contratos de `consultas`. A composição conecta as três.
- O `__init__.py` de cada pacote não importa camadas internas.

Os imports são resolvidos pelo caminho no repositório. `from ..infraestrutura import X`, `from .. import infraestrutura` e `from shopbrasil_pipeline.infraestrutura import X` recebem a mesma regra. Também entram `importlib.import_module("...")` e `__import__("...")` com nome literal, e imports dentro de funções ou de `if TYPE_CHECKING:`.

Arquivos Python dos módulos fora dos pacotes, como testes e pontos de entrada em `web/` ou `pipeline/`, seguem apenas a fronteira entre módulos. A revisão continua responsável pelo que imports não revelam, como SQL no DAG ou no controller.

## Uso nos PRs

O workflow [arquitetura.yml](../.github/workflows/arquitetura.yml) executa o lint e os testes em cada PR. Registre também o comando e o resultado no PR, conforme o [template](../.github/pull_request_template.md).
