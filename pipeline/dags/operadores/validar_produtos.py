"""Operador que valida o catálogo antes do cálculo (#6).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

``ValidarProdutosOperator`` é um ``BaseOperator`` que recebe a projeção do catálogo (XCom de
Buscar Produtos), aplica as regras da aplicação e devolve só os campos necessários ao cálculo.
Um catálogo inválido não melhora com retentativas, então a falha usa ``AirflowFailException``:
a task falha na primeira tentativa, as etapas de análise e publicação não rodam e os dados já
publicados ficam preservados. As regras ficam em ``aplicacao/validacao.py``; aqui só há a
tradução entre Airflow e aplicação.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.sdk import BaseOperator

from shopbrasil_pipeline.aplicacao.catalogo import CatalogoColetado
from shopbrasil_pipeline.aplicacao.validacao import CatalogoInvalidoError, validar_catalogo


class ValidarProdutosOperator(BaseOperator):
    """Valida o catálogo coletado e devolve ``{"produtos": [{codigo_categoria, preco}]}``."""

    template_fields = ("catalogo",)

    def __init__(self, *, catalogo: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.catalogo = catalogo

    def execute(self, context) -> dict[str, object]:
        if not isinstance(self.catalogo, Mapping):
            raise AirflowFailException(f"catálogo em formato inesperado: {type(self.catalogo).__name__}")
        try:
            produtos = validar_catalogo(CatalogoColetado.de_xcom(self.catalogo))
        except CatalogoInvalidoError as erro:
            for problema in erro.problemas:
                self.log.error("Validação do catálogo: %s", problema)
            raise AirflowFailException(str(erro)) from erro
        categorias = sorted({p.codigo_categoria for p in produtos})
        self.log.info("Catálogo válido: %d produto(s) em %d categoria(s): %s", len(produtos), len(categorias), categorias)
        return {"produtos": [p.para_xcom() for p in produtos]}
