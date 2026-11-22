-- Autor: Christiano Henrique Carrilho Lopes da Silva
-- Código de pessoa: 1691353
-- Matrícula: 262987
-- Dados de teste do cenário e2e/web (#9), gravados pela conta administrativa.
-- electronics: 20/11 e 22/11 no snapshot; 21/11 só no histórico, com valor que não pode aparecer
-- na série, porque a Web/API lê só o snapshot. jewelery: 22/11 e 24/11 (última data).
-- men's clothing: cadastrada sem métricas. 21/11 e 23/11 ficam sem registros no snapshot.
INSERT INTO public.categoria (codigo, descricao) VALUES
    ('electronics', 'Eletrônicos'),
    ('jewelery', 'Joias'),
    ('men''s clothing', 'Moda masculina');

INSERT INTO public.metricas_categoria
    (categoria_id, data_referencia, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
SELECT c.id, v.data::date, v.medio, v.minimo, v.maximo, v.n
  FROM (VALUES
        ('electronics', '2026-11-20', 20.00, 15.00, 25.00, 2),
        ('electronics', '2026-11-22', 10.00, 10.00, 10.01, 3),
        ('jewelery',    '2026-11-22', 10.01, 10.00, 10.01, 2),
        ('jewelery',    '2026-11-24', 1234567890.12, 0.10, 9999999999.99, 4)
       ) AS v(codigo, data, medio, minimo, maximo, n)
  JOIN public.categoria c ON c.codigo = v.codigo;

INSERT INTO public.metricas_categoria_historico
    (run_id, categoria_id, data_execucao, preco_medio, preco_minimo, preco_maximo, quantidade_produtos)
SELECT 'scheduled__2026-11-21T09:00:00+00:00', id, '2026-11-21', 99.99, 99.99, 99.99, 1
  FROM public.categoria WHERE codigo = 'electronics';
