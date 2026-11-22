-- Autor: Christiano Henrique Carrilho Lopes da Silva
-- Código de pessoa: 1691353
-- Matrícula: 262987
-- Fixture E2E isolada da #2: tabela de sonda criada pela conta administrativa após os
-- privilégios padrão, para verificar escrita do pipeline, leitura da web e o limite do pool.
-- Não faz parte do modelo analítico; o DDL real é o db/schema.sql da #3.

CREATE TABLE IF NOT EXISTS e2e_ambiente_sonda (
    id         integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id     text        NOT NULL,
    map_index  integer     NOT NULL,
    inicio     timestamptz NOT NULL,
    fim        timestamptz NOT NULL
);
