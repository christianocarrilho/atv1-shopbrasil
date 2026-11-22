-- Autor: Christiano Henrique Carrilho Lopes da Silva
-- Código de pessoa: 1691353
-- Matrícula: 262987
-- Esquema analítico compartilhado entre pipeline e Web/API (#3): categoria, snapshot diário
-- de métricas e histórico por run, no esquema public do banco shopbrasil.
-- O db-init aplica este arquivo com psql --single-transaction e ON_ERROR_STOP, por isso o DDL
-- usa apenas comandos transacionais, sem BEGIN/COMMIT próprios, e IF NOT EXISTS para ser
-- reaplicado sobre um volume existente sem perder dados.
-- Os GRANTs vêm dos privilégios padrão de db/acessos.sql e não se repetem aqui.
-- Nomes, tipos e regras de transporte para a #15 estão em docs/modelo-dados.md.

-- Preços em NUMERIC(12,2): escala 2 exigida pela especificação e até 9.999.999.999,99,
-- folga ampla para o catálogo sem aceitar valores absurdos.

CREATE TABLE IF NOT EXISTS public.categoria (
    id        integer GENERATED ALWAYS AS IDENTITY,
    codigo    text    NOT NULL,
    descricao text    NOT NULL,
    CONSTRAINT categoria_pk PRIMARY KEY (id),
    CONSTRAINT categoria_codigo_uk UNIQUE (codigo),
    CONSTRAINT categoria_codigo_preenchido_ck CHECK (btrim(codigo) <> '')
);

CREATE TABLE IF NOT EXISTS public.metricas_categoria (
    id                  integer       GENERATED ALWAYS AS IDENTITY,
    categoria_id        integer       NOT NULL,
    data_referencia     date          NOT NULL,
    preco_medio         numeric(12,2) NOT NULL,
    preco_minimo        numeric(12,2) NOT NULL,
    preco_maximo        numeric(12,2) NOT NULL,
    quantidade_produtos integer       NOT NULL,
    CONSTRAINT metricas_categoria_pk PRIMARY KEY (id),
    CONSTRAINT metricas_categoria_categoria_fk FOREIGN KEY (categoria_id)
        REFERENCES public.categoria (id) ON DELETE RESTRICT,
    CONSTRAINT metricas_categoria_data_categoria_uk UNIQUE (data_referencia, categoria_id),
    CONSTRAINT metricas_categoria_quantidade_ck CHECK (quantidade_produtos > 0),
    CONSTRAINT metricas_categoria_precos_ck
        CHECK (preco_minimo >= 0 AND preco_minimo <= preco_medio AND preco_medio <= preco_maximo)
);

-- Sem coluna id: a chave natural (run_id, categoria_id) é a unicidade exigida e serve de PK.
CREATE TABLE IF NOT EXISTS public.metricas_categoria_historico (
    run_id              text          NOT NULL,
    categoria_id        integer       NOT NULL,
    data_execucao       date          NOT NULL,
    preco_medio         numeric(12,2) NOT NULL,
    preco_minimo        numeric(12,2) NOT NULL,
    preco_maximo        numeric(12,2) NOT NULL,
    quantidade_produtos integer       NOT NULL,
    CONSTRAINT metricas_categoria_historico_pk PRIMARY KEY (run_id, categoria_id),
    CONSTRAINT metricas_categoria_historico_categoria_fk FOREIGN KEY (categoria_id)
        REFERENCES public.categoria (id) ON DELETE RESTRICT,
    CONSTRAINT metricas_categoria_historico_run_id_ck CHECK (btrim(run_id) <> ''),
    CONSTRAINT metricas_categoria_historico_quantidade_ck CHECK (quantidade_produtos > 0),
    CONSTRAINT metricas_categoria_historico_precos_ck
        CHECK (preco_minimo >= 0 AND preco_minimo <= preco_medio AND preco_medio <= preco_maximo)
);

COMMENT ON TABLE public.categoria IS
    'Cadastro de categorias da FakeStore. Linhas permanecem quando a categoria some da coleta.';
COMMENT ON COLUMN public.categoria.codigo IS
    'Valor de category da FakeStore; chave de negócio usada pelo contrato da #15.';
COMMENT ON TABLE public.metricas_categoria IS
    'Snapshot diário por categoria, substituído a cada publicação bem-sucedida da data. Lido pela Web/API.';
COMMENT ON COLUMN public.metricas_categoria.data_referencia IS
    'Data da ocorrência agendada em America/Sao_Paulo.';
COMMENT ON TABLE public.metricas_categoria_historico IS
    'Histórico append por run do Airflow; o reprocessamento da mesma run preserva as linhas existentes.';
COMMENT ON COLUMN public.metricas_categoria_historico.run_id IS
    'run_id do DAG run do Airflow que publicou a linha.';
COMMENT ON COLUMN public.metricas_categoria_historico.data_execucao IS
    'Data de referência da run (ocorrência agendada em America/Sao_Paulo), preservada no reprocessamento.';
