-- Autor: Christiano Henrique Carrilho Lopes da Silva
-- Código de pessoa: 1691353
-- Matrícula: 262987
-- Fixture E2E da #3: aplica o db/schema.sql real e depois falha de propósito, para confirmar
-- que o db-init desfaz o esquema inteiro quando um comando posterior do arquivo falha.

\i /db/schema.sql

CREATE TABLE e2e_esquema_invalida (id integer PRIMARY KEY;
