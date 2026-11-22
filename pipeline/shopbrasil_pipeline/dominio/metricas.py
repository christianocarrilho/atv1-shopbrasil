"""Contrato e cálculo das métricas de preço de uma categoria (#15).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Calcula preço médio, mínimo, máximo e quantidade com ``Decimal``, sem float em nenhuma etapa.
A média é calculada sobre os preços recebidos e arredondada uma única vez para duas casas com
``ROUND_HALF_UP``, o mesmo critério do ``numeric`` do PostgreSQL para valores positivos, para
que o banco grave exatamente o valor calculado. O transporte entre tasks usa strings decimais,
porque o JSON do XCom converteria números em float e perderia a precisão.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, localcontext

CENTAVOS = Decimal("0.01")
MODO_ARREDONDAMENTO = ROUND_HALF_UP


def _duas_casas(valor: Decimal) -> Decimal:
    return valor.quantize(CENTAVOS, rounding=MODO_ARREDONDAMENTO)


def _decimal_finito(valor: object, campo: str) -> Decimal:
    # bool é subclasse de int e float perde precisão: os dois ficam fora do contrato.
    if not isinstance(valor, Decimal):
        raise TypeError(f"{campo} deve ser Decimal, recebido {type(valor).__name__}")
    if not valor.is_finite():
        raise ValueError(f"{campo} deve ser finito, recebido {valor}")
    return valor


def _decimal_de_texto(valor: object, campo: str) -> Decimal:
    if not isinstance(valor, str):
        raise TypeError(f"{campo} deve ser string decimal, recebido {type(valor).__name__}")
    try:
        return _decimal_finito(Decimal(valor), campo)
    except ArithmeticError as erro:
        raise ValueError(f"{campo} não é decimal: {valor!r}") from erro


@dataclass(frozen=True)
class PrecosCategoria:
    """Entrada do cálculo: código observado na coleta e preços dos produtos da categoria."""

    codigo_categoria: str
    precos: tuple[Decimal, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.codigo_categoria, str) or not self.codigo_categoria.strip():
            raise ValueError("codigo_categoria deve ser texto não vazio")
        if not self.precos:
            raise ValueError(f"categoria {self.codigo_categoria!r} sem preços")
        for preco in self.precos:
            _decimal_finito(preco, "preço")

    def para_xcom(self) -> dict[str, object]:
        return {"codigo_categoria": self.codigo_categoria, "precos": [str(p) for p in self.precos]}

    @classmethod
    def de_xcom(cls, valor: Mapping[str, object]) -> "PrecosCategoria":
        precos = valor["precos"]
        if not isinstance(precos, list):
            raise TypeError("precos deve ser lista de strings decimais")
        return cls(
            codigo_categoria=valor["codigo_categoria"],  # type: ignore[arg-type]
            precos=tuple(_decimal_de_texto(p, "preço") for p in precos),
        )


@dataclass(frozen=True)
class MetricasCategoria:
    """Saída do cálculo: código da categoria e as quatro métricas publicadas."""

    codigo_categoria: str
    preco_medio: Decimal
    preco_minimo: Decimal
    preco_maximo: Decimal
    quantidade_produtos: int

    def para_xcom(self) -> dict[str, object]:
        return {
            "codigo_categoria": self.codigo_categoria,
            "preco_medio": str(self.preco_medio),
            "preco_minimo": str(self.preco_minimo),
            "preco_maximo": str(self.preco_maximo),
            "quantidade_produtos": self.quantidade_produtos,
        }

    @classmethod
    def de_xcom(cls, valor: Mapping[str, object]) -> "MetricasCategoria":
        quantidade = valor["quantidade_produtos"]
        if isinstance(quantidade, bool) or not isinstance(quantidade, int):
            raise TypeError("quantidade_produtos deve ser inteiro")
        codigo = valor["codigo_categoria"]
        if not isinstance(codigo, str) or not codigo.strip():
            raise ValueError("codigo_categoria deve ser texto não vazio")
        return cls(
            codigo_categoria=codigo,
            preco_medio=_decimal_de_texto(valor["preco_medio"], "preco_medio"),
            preco_minimo=_decimal_de_texto(valor["preco_minimo"], "preco_minimo"),
            preco_maximo=_decimal_de_texto(valor["preco_maximo"], "preco_maximo"),
            quantidade_produtos=quantidade,
        )


def calcular_metricas(entrada: PrecosCategoria) -> MetricasCategoria:
    """Calcula as quatro métricas de uma categoria.

    Soma e divisão usam precisão suficiente para que o único arredondamento seja o da média
    para duas casas. Mínimo e máximo são preços observados e só recebem a escala de duas casas
    da coluna ``numeric(12,2)``.
    """
    precos: Iterable[Decimal] = entrada.precos
    quantidade = len(entrada.precos)
    with localcontext() as contexto:
        contexto.prec = 50
        media = sum(precos, Decimal(0)) / quantidade
    return MetricasCategoria(
        codigo_categoria=entrada.codigo_categoria,
        preco_medio=_duas_casas(media),
        preco_minimo=_duas_casas(min(entrada.precos)),
        preco_maximo=_duas_casas(max(entrada.precos)),
        quantidade_produtos=quantidade,
    )
