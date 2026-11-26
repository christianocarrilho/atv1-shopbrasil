# aplicacao

**Autor:** Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Coordena as operações do pipeline e declara os contratos de catálogo e persistência.

- [execucao.py](execucao.py): identificação da run e data de referência (#4).
- [publicacao.py](publicacao.py): caso de uso `PublicarSnapshot` e contrato `RepositorioPublicacao` (#17). Recusa coleção vazia, item que não seja `MetricasCategoria` e código de categoria repetido antes de chamar o repositório.
- [alertas.py](alertas.py): mensagens `[SHOPBRASIL][...]` dos callbacks e o alerta simulado (#14), escritas direto no log da task.
