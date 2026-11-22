"""Contrato e cálculo das métricas de preço de uma categoria (#15).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Calcula preço médio, mínimo, máximo e quantidade com ``Decimal``, sem float em nenhuma etapa.
A média é calculada de forma exata, como fração, e arredondada uma única vez para duas casas com
``ROUND_HALF_UP`` (empate para longe do zero), o mesmo critério do ``numeric`` do PostgreSQL,
para que o banco grave exatamente o valor calculado. O transporte entre tasks usa strings decimais,
porque o JSON do XCom converteria números em float e perderia a precisão.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from fractions import Fraction

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
        if not isinstance(self.precos, tuple):
            object.__setattr__(self, "precos", tuple(self.precos))
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

    def __post_init__(self) -> None:
        # Recusa aqui, com mensagem clara, o que violaria os CHECKs da #3 dentro da transação.
        if not isinstance(self.codigo_categoria, str) or not self.codigo_categoria.strip():
            raise ValueError("codigo_categoria deve ser texto não vazio")
        for campo in ("preco_medio", "preco_minimo", "preco_maximo"):
            valor = _decimal_finito(getattr(self, campo), campo)
            if valor.as_tuple().exponent != -2:
                raise ValueError(f"{campo} deve ter exatamente duas casas, recebido {valor}")
        quantidade = self.quantidade_produtos
        if isinstance(quantidade, bool) or not isinstance(quantidade, int):
            raise TypeError("quantidade_produtos deve ser inteiro")
        if quantidade <= 0:
            raise ValueError("quantidade_produtos deve ser maior que zero")
        if not (Decimal(0) <= self.preco_minimo <= self.preco_medio <= self.preco_maximo):
            raise ValueError("as métricas devem obedecer 0 <= mínimo <= médio <= máximo")

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
        return cls(
            codigo_categoria=valor["codigo_categoria"],  # type: ignore[arg-type]
            preco_medio=_decimal_de_texto(valor["preco_medio"], "preco_medio"),
            preco_minimo=_decimal_de_texto(valor["preco_minimo"], "preco_minimo"),
            preco_maximo=_decimal_de_texto(valor["preco_maximo"], "preco_maximo"),
            quantidade_produtos=valor["quantidade_produtos"],  # type: ignore[arg-type]
        )


def _media_em_centavos(precos: tuple[Decimal, ...]) -> Decimal:
    """Média exata (como fração) arredondada uma única vez para centavos, empate longe do zero."""
    centavos = sum((Fraction(p) for p in precos), Fraction(0)) * 100 / len(precos)
    sinal = -1 if centavos < 0 else 1
    inteiro = math.floor(abs(centavos) + Fraction(1, 2))
    return Decimal(sinal * inteiro).scaleb(-2)


def calcular_metricas(entrada: PrecosCategoria) -> MetricasCategoria:
    """Calcula as quatro métricas de uma categoria.

    A média é exata até o único arredondamento para duas casas. Mínimo e máximo são preços
    observados e só recebem a escala de duas casas da coluna ``numeric(12,2)``.
    """
    return MetricasCategoria(
        codigo_categoria=entrada.codigo_categoria,
        preco_medio=_media_em_centavos(entrada.precos),
        preco_minimo=_duas_casas(min(entrada.precos)),
        preco_maximo=_duas_casas(max(entrada.precos)),
        quantidade_produtos=len(entrada.precos),
    )
