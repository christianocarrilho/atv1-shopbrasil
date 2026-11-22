"""Regras de validação do catálogo coletado (#6).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Confere a projeção entregue pela #13 antes de qualquer cálculo. Catálogo que não é lista, catálogo
vazio, categoria que não é texto ou está em branco, e preço que não é número, não é finito ou é
negativo, preço com mais de duas casas ou acima do limite de ``numeric(12,2)``, e categoria com espaços
nas extremidades tornam o catálogo inteiro inválido, para que a publicação não ocorra e os dados já
publicados fiquem como estão. Preço negativo é recusado aqui porque o esquema da #3 exige
``preco_minimo >= 0``. Uma categoria observada antes e ausente agora é normal: só os produtos
recebidos entram no resultado. A saída mantém um registro por produto com o código da categoria e
o preço como string decimal, sem arredondar, para a #15.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from shopbrasil_pipeline.aplicacao.catalogo import CatalogoColetado

MAX_PROBLEMAS_NA_MENSAGEM = 10
# Preços são gravados em numeric(12,2) (especificação): até duas casas e dez dígitos inteiros.
PRECO_MAXIMO = Decimal("9999999999.99")


class CatalogoInvalidoError(ValueError):
    """O catálogo não pode seguir para o cálculo; ``problemas`` lista cada motivo encontrado."""

    def __init__(self, problemas: list[str]) -> None:
        self.problemas = problemas
        resumo = "; ".join(problemas[:MAX_PROBLEMAS_NA_MENSAGEM])
        extra = len(problemas) - MAX_PROBLEMAS_NA_MENSAGEM
        if extra > 0:
            resumo += f"; e mais {extra} problema(s)"
        super().__init__(f"catálogo inválido: {resumo}")


@dataclass(frozen=True)
class ProdutoValido:
    """Campos necessários ao cálculo: código da categoria e preço decimal em texto."""

    codigo_categoria: str
    preco: str

    def para_xcom(self) -> dict[str, str]:
        return {"codigo_categoria": self.codigo_categoria, "preco": self.preco}


def _problema_preco(produto) -> str | None:
    if produto.tipo_preco != "number":
        return f"preço do tipo {produto.tipo_preco}"
    try:
        valor = Decimal(produto.preco)
    except (InvalidOperation, TypeError):
        return f"preço {produto.preco!r} não é decimal"
    if not valor.is_finite():
        return f"preço {produto.preco} não é finito"
    if valor < 0:
        return f"preço {produto.preco} negativo"
    if valor.as_tuple().exponent < -2 and valor != valor.quantize(Decimal("0.01")):
        return f"preço {produto.preco} com mais de duas casas decimais"
    if valor > PRECO_MAXIMO:
        return f"preço {produto.preco} acima do limite de numeric(12,2)"
    return None


def validar_catalogo(catalogo: CatalogoColetado) -> tuple[ProdutoValido, ...]:
    """Devolve os produtos validados ou levanta ``CatalogoInvalidoError`` com todos os motivos."""
    if catalogo.tipo_raiz != "array":
        raise CatalogoInvalidoError([f"a resposta deveria ser uma lista, recebido {catalogo.tipo_raiz}"])
    if not catalogo.produtos:
        raise CatalogoInvalidoError(["catálogo vazio"])
    problemas: list[str] = []
    validos: list[ProdutoValido] = []
    for posicao, produto in enumerate(catalogo.produtos):
        rotulo = f"produto {produto.id if produto.id is not None else f'na posição {posicao}'}"
        motivos = []
        if produto.tipo_categoria != "string":
            motivos.append(f"categoria do tipo {produto.tipo_categoria}")
        elif not produto.categoria or not produto.categoria.strip():
            motivos.append("categoria em branco")
        elif produto.categoria != produto.categoria.strip():
            # O código é a chave da categoria; " electronics" criaria uma categoria distinta de "electronics".
            motivos.append(f"categoria {produto.categoria!r} com espaços nas extremidades")
        problema_preco = _problema_preco(produto)
        if problema_preco:
            motivos.append(problema_preco)
        if motivos:
            problemas.append(f"{rotulo}: {', '.join(motivos)}")
        else:
            validos.append(ProdutoValido(codigo_categoria=produto.categoria, preco=produto.preco))  # type: ignore[arg-type]
    if problemas:
        raise CatalogoInvalidoError(problemas)
    return tuple(validos)
