"""Tentativas de escrita com a conta da Web/API, executadas no contêiner da web (#9).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Usa a mesma configuração que a composição lê do ambiente do contêiner e imprime, em uma linha,
o usuário conectado e o nome do erro de cada tentativa de escrita ou DDL.
"""

import psycopg

from shopbrasil_web.composicao import configuracao_do_ambiente

parametros = configuracao_do_ambiente().parametros()
TENTATIVAS = [
    "INSERT INTO public.categoria (codigo, descricao) VALUES ('intrusa', 'intrusa')",
    "UPDATE public.metricas_categoria SET preco_medio = 0",
    "DELETE FROM public.metricas_categoria",
    "TRUNCATE public.metricas_categoria_historico",
    "CREATE TABLE public.intrusa (id integer)",
]

with psycopg.connect(**parametros) as conexao:
    usuario = conexao.execute("SELECT current_user").fetchone()[0]

resultados = []
for sql in TENTATIVAS:
    try:
        with psycopg.connect(**parametros) as conexao:
            conexao.execute(sql)
        resultados.append("ESCREVEU")
    except psycopg.Error as erro:
        resultados.append(type(erro).__name__)

print(usuario + "|" + ",".join(resultados))
