-- Autor: Christiano Henrique Carrilho Lopes da Silva
-- Código de pessoa: 1691353
-- Matrícula: 262987
-- Fixture E2E da #2: SQL propositalmente inválido para confirmar que o db-init termina com
-- código diferente de zero quando um script de esquema falha (ON_ERROR_STOP).

CREATE TABLE e2e_ambiente_invalida (id integer PRIMARY KEY;
