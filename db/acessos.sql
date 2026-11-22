-- Autor: Christiano Henrique Carrilho Lopes da Silva
-- Código de pessoa: 1691353
-- Matrícula: 262987
-- Provisiona os bancos e as contas do ambiente: metadados do Airflow, banco analítico,
-- conta de escrita do pipeline e conta de leitura da Web/API.
-- O script é idempotente e sincroniza as senhas do .env a cada execução do db-init.
-- Os privilégios padrão valem para as tabelas que a conta administrativa criar depois,
-- por isso o db/schema.sql da #3 recebe os GRANTs sem repeti-los no DDL.
--
-- Variáveis psql esperadas (passadas por db/aplicar.sh):
--   admin_user, airflow_db, airflow_user, airflow_password,
--   analytic_db, writer_user, writer_password, reader_user, reader_password

\set ON_ERROR_STOP on

-- Contas. CREATE ROLE só roda quando a conta não existe; ALTER ROLE mantém a senha alinhada ao .env.
SELECT format('CREATE ROLE %I LOGIN', :'airflow_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'airflow_user')
\gexec
SELECT format('CREATE ROLE %I LOGIN', :'writer_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'writer_user')
\gexec
SELECT format('CREATE ROLE %I LOGIN', :'reader_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'reader_user')
\gexec

SELECT format('ALTER ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
              :'airflow_user', :'airflow_password')
\gexec
SELECT format('ALTER ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
              :'writer_user', :'writer_password')
\gexec
SELECT format('ALTER ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
              :'reader_user', :'reader_password')
\gexec

-- Bancos. CREATE DATABASE não roda em bloco transacional, por isso usa \gexec.
SELECT format('CREATE DATABASE %I OWNER %I', :'airflow_db', :'airflow_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'airflow_db')
\gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'analytic_db', :'admin_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'analytic_db')
\gexec

-- Cada conta conecta apenas ao banco que usa.
SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', :'airflow_db')
\gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', :'analytic_db')
\gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I, %I', :'analytic_db', :'writer_user', :'reader_user')
\gexec

\connect :"analytic_db"

-- O esquema public pertence ao administrador; as contas da aplicação não executam DDL.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SELECT format('GRANT USAGE ON SCHEMA public TO %I, %I', :'writer_user', :'reader_user')
\gexec

-- Objetos já existentes (reaplicação sobre um volume com tabelas).
SELECT format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %I', :'writer_user')
\gexec
SELECT format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %I', :'writer_user')
\gexec
SELECT format('GRANT SELECT ON ALL TABLES IN SCHEMA public TO %I', :'reader_user')
\gexec

-- Objetos que o administrador criar depois, como as tabelas do db/schema.sql.
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
              'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', :'admin_user', :'writer_user')
\gexec
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
              'GRANT USAGE, SELECT ON SEQUENCES TO %I', :'admin_user', :'writer_user')
\gexec
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
              'GRANT SELECT ON TABLES TO %I', :'admin_user', :'reader_user')
\gexec
