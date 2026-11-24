"""Testes das regras de validação do catálogo (#6).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987
"""

from __future__ import annotations

import json
import unittest
from decimal import Decimal

from shopbrasil_pipeline.aplicacao.catalogo import projetar_catalogo
from shopbrasil_pipeline.aplicacao.validacao import CatalogoInvalidoError, validar_catalogo


def catalogo(*produtos):
    return projetar_catalogo(json.loads(json.dumps(list(produtos)), parse_float=Decimal, parse_int=Decimal))


class ValidacaoTest(unittest.TestCase):
    def test_catalogo_valido_mantem_um_registro_por_produto_sem_arredondar(self):
        validos = validar_catalogo(catalogo(
            {"id": 1, "category": "electronics", "price": 64},
            {"id": 2, "category": "electronics", "price": 10.50},
        ))
        self.assertEqual([p.para_xcom() for p in validos], [
            {"codigo_categoria": "electronics", "preco": "64"},
            {"codigo_categoria": "electronics", "preco": "10.5"},
        ])

    def test_categoria_ausente_do_dia_e_normal(self):
        # Só jewelery chegou; categorias vistas antes não são exigidas.
        self.assertEqual(len(validar_catalogo(catalogo({"id": 5, "category": "jewelery", "price": 695}))), 1)

    def test_rejeita_catalogo_vazio_e_raiz_que_nao_e_lista(self):
        with self.assertRaisesRegex(CatalogoInvalidoError, "catálogo vazio"):
            validar_catalogo(catalogo())
        with self.assertRaisesRegex(CatalogoInvalidoError, "lista, recebido object"):
            validar_catalogo(projetar_catalogo({"products": []}))

    def test_rejeita_categoria_em_branco_ou_nao_textual(self):
        for categoria, motivo in (("  ", "categoria em branco"), ("", "categoria em branco"),
                                  (None, "categoria do tipo null"), (7, "categoria do tipo number")):
            with self.subTest(categoria=categoria), self.assertRaisesRegex(CatalogoInvalidoError, motivo):
                validar_catalogo(catalogo({"id": 1, "category": categoria, "price": 1}))
        with self.assertRaisesRegex(CatalogoInvalidoError, "categoria do tipo ausente"):
            validar_catalogo(catalogo({"id": 1, "price": 1}))

    def test_rejeita_preco_invalido(self):
        casos = ((True, "preço do tipo boolean"), ("10.00", "preço do tipo string"), (None, "preço do tipo null"),
                 (-1, "negativo"), ([1], "preço do tipo array"))
        for preco, motivo in casos:
            with self.subTest(preco=preco), self.assertRaisesRegex(CatalogoInvalidoError, motivo):
                validar_catalogo(catalogo({"id": 1, "category": "x", "price": preco}))
        with self.assertRaisesRegex(CatalogoInvalidoError, "não é finito"):
            validar_catalogo(projetar_catalogo([{"id": 1, "category": "x", "price": Decimal("NaN")}]))

    def test_um_produto_invalido_invalida_o_catalogo_e_lista_todos_os_motivos(self):
        with self.assertRaises(CatalogoInvalidoError) as erro:
            validar_catalogo(catalogo(
                {"id": 1, "category": "x", "price": 1},
                {"id": 2, "category": " ", "price": True},
                "texto",
            ))
        self.assertEqual(erro.exception.problemas, [
            "produto 2: categoria em branco, preço do tipo boolean",
            "produto na posição 2: categoria do tipo ausente, preço do tipo ausente",
        ])


    def test_rejeita_preco_fora_de_numeric_12_2(self):
        for preco, motivo in ((10.005, "mais de duas casas"), (10000000000, "acima do limite")):
            with self.subTest(preco=preco), self.assertRaisesRegex(CatalogoInvalidoError, motivo):
                validar_catalogo(catalogo({"id": 1, "category": "x", "price": preco}))
        # Zeros à direita não são casas a mais.
        produto = projetar_catalogo([{"id": 1, "category": "x", "price": Decimal("10.500")}])
        self.assertEqual("10.500", validar_catalogo(produto)[0].preco)

    def test_rejeita_categoria_com_espacos_nas_extremidades(self):
        for categoria in (" electronics", "electronics ", "\telectronics"):
            with self.subTest(categoria=categoria), self.assertRaisesRegex(CatalogoInvalidoError, "espaços nas extremidades"):
                validar_catalogo(catalogo({"id": 1, "category": categoria, "price": 1}))
        # Espaço interno faz parte do código real da FakeStore.
        self.assertEqual("men's clothing", validar_catalogo(catalogo({"id": 1, "category": "men's clothing", "price": 1}))[0].codigo_categoria)


if __name__ == "__main__":
    unittest.main()
