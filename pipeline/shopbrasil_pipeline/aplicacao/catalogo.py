"""Contrato da fonte do catálogo e caso de uso da coleta (#13).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

A coleta devolve uma projeção pequena do catálogo com os campos que a validação (#6) e o
cálculo (#15) usam: ``id``, ``category`` e ``price``. A projeção não valida nada: preserva o
texto da categoria e o tipo JSON da categoria e do preço para que o ``ValidarProdutosOperator`` recuse
catálogo vazio, categoria em branco, booleano ou texto no lugar do preço. Números trafegam como
strings decimais, porque o JSON do XCom os converteria em float.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

TIPOS_JSON = ("number", "string", "boolean", "null", "array", "object", "ausente")


@dataclass(frozen=True)
class ProdutoColetado:
    """Projeção de um produto com o tipo JSON original da categoria e do preço.

    ``categoria`` só é preenchida quando o valor é texto, e ``preco`` só quando é número (como
    string decimal). Nos outros casos o valor fica ``None`` e o tipo diz o que veio.
    """

    id: str | None
    categoria: str | None
    tipo_categoria: str
    preco: str | None
    tipo_preco: str

    def para_xcom(self) -> dict[str, object]:
        return {
            "id": self.id,
            "categoria": self.categoria,
            "tipo_categoria": self.tipo_categoria,
            "preco": self.preco,
            "tipo_preco": self.tipo_preco,
        }

    @classmethod
    def de_xcom(cls, valor: Mapping[str, object]) -> "ProdutoColetado":
        return cls(
            id=valor.get("id"),  # type: ignore[arg-type]
            categoria=valor.get("categoria"),  # type: ignore[arg-type]
            tipo_categoria=str(valor.get("tipo_categoria")),
            preco=valor.get("preco"),  # type: ignore[arg-type]
            tipo_preco=str(valor.get("tipo_preco")),
        )


@dataclass(frozen=True)
class CatalogoColetado:
    """Resposta da fonte. ``tipo_raiz`` diferente de ``array`` indica estrutura inválida."""

    tipo_raiz: str
    produtos: tuple[ProdutoColetado, ...]

    def para_xcom(self) -> dict[str, object]:
        return {"tipo_raiz": self.tipo_raiz, "produtos": [p.para_xcom() for p in self.produtos]}

    @classmethod
    def de_xcom(cls, valor: Mapping[str, object]) -> "CatalogoColetado":
        produtos = valor.get("produtos") or []
        return cls(
            tipo_raiz=str(valor.get("tipo_raiz")),
            produtos=tuple(ProdutoColetado.de_xcom(p) for p in produtos),  # type: ignore[union-attr]
        )


def tipo_json(valor: object) -> str:
    """Nome do tipo JSON de um valor decodificado com números como ``Decimal``."""
    if valor is None:
        return "null"
    if isinstance(valor, bool):
        return "boolean"
    if isinstance(valor, (Decimal, int, float)):
        return "number"
    if isinstance(valor, str):
        return "string"
    if isinstance(valor, Sequence):
        return "array"
    if isinstance(valor, Mapping):
        return "object"
    return type(valor).__name__


def projetar_catalogo(resposta: object) -> CatalogoColetado:
    """Reduz a resposta decodificada aos campos da validação, sem descartar itens inválidos."""
    raiz = tipo_json(resposta)
    if raiz != "array":
        return CatalogoColetado(tipo_raiz=raiz, produtos=())
    produtos = []
    for item in resposta:  # type: ignore[union-attr]
        if not isinstance(item, Mapping):
            produtos.append(ProdutoColetado(None, None, "ausente", None, "ausente"))
            continue
        tipo_preco = tipo_json(item["price"]) if "price" in item else "ausente"
        tipo_categoria = tipo_json(item["category"]) if "category" in item else "ausente"
        identificador = item.get("id")
        produtos.append(
            ProdutoColetado(
                id=str(identificador) if tipo_json(identificador) in ("number", "string") else None,
                categoria=item["category"] if tipo_categoria == "string" else None,
                tipo_categoria=tipo_categoria,
                preco=str(item["price"]) if tipo_preco == "number" else None,
                tipo_preco=tipo_preco,
            )
        )
    return CatalogoColetado(tipo_raiz=raiz, produtos=tuple(produtos))


class FonteCatalogo(Protocol):
    """Fonte do catálogo de produtos. Erros de transporte e de leitura são propagados."""

    def buscar_produtos(self) -> object:
        """Devolve a resposta decodificada, com números como ``Decimal``."""
        ...


def coletar_catalogo(fonte: FonteCatalogo) -> CatalogoColetado:
    """Busca o catálogo na fonte e devolve a projeção para a validação."""
    return projetar_catalogo(fonte.buscar_produtos())
