"""Testes do lint de arquitetura da ShopBrasil (#26).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Monta repositórios temporários com a configuração versionada e confere exemplos
permitidos e proibidos de imports absolutos, relativos e dinâmicos. Usa
``unittest`` para rodar sem dependências além da biblioteca padrão.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

QUALIDADE = Path(__file__).resolve().parent.parent
REPOSITORIO = QUALIDADE.parent
SCRIPT = QUALIDADE / "lint_arquitetura.py"
CONFIG = QUALIDADE / "arquitetura.toml"

sys.path.insert(0, str(QUALIDADE))
import lint_arquitetura as lint  # noqa: E402

ESTRUTURA_BASE = [
    "pipeline/shopbrasil_pipeline/__init__.py",
    "pipeline/shopbrasil_pipeline/dominio/__init__.py",
    "pipeline/shopbrasil_pipeline/dominio/metricas.py",
    "pipeline/shopbrasil_pipeline/aplicacao/__init__.py",
    "pipeline/shopbrasil_pipeline/aplicacao/contratos.py",
    "pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py",
    "pipeline/shopbrasil_pipeline/infraestrutura/__init__.py",
    "pipeline/shopbrasil_pipeline/infraestrutura/fakestore.py",
    "pipeline/shopbrasil_pipeline/infraestrutura/postgres.py",
    "pipeline/shopbrasil_pipeline/composicao.py",
    "pipeline/dags/callbacks.py",
    "pipeline/dags/shopbrasil_dag.py",
    "web/shopbrasil_web/__init__.py",
    "web/shopbrasil_web/consultas/__init__.py",
    "web/shopbrasil_web/consultas/metricas.py",
    "web/shopbrasil_web/apresentacao/__init__.py",
    "web/shopbrasil_web/apresentacao/controller.py",
    "web/shopbrasil_web/infraestrutura/__init__.py",
    "web/shopbrasil_web/infraestrutura/repositorio.py",
    "web/shopbrasil_web/composicao.py",
]


class RepositorioTemporario:
    """Cria a estrutura de camadas e permite sobrescrever arquivos."""

    def __init__(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.raiz = Path(self._tmp.name)
        for caminho in ESTRUTURA_BASE:
            self.escrever(caminho, "")

    def escrever(self, caminho: str, conteudo: str) -> None:
        alvo = self.raiz / caminho
        alvo.parent.mkdir(parents=True, exist_ok=True)
        alvo.write_text(textwrap.dedent(conteudo), encoding="utf-8")

    def verificar(self) -> list[lint.Violacao]:
        config = lint.carregar_configuracao(CONFIG)
        violacoes, _ = lint.Analisador(self.raiz, config).verificar()
        return violacoes

    def fechar(self) -> None:
        self._tmp.cleanup()


class BaseTeste(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = RepositorioTemporario()
        self.addCleanup(self.repo.fechar)

    def assertSemViolacoes(self) -> None:
        violacoes = self.repo.verificar()
        self.assertEqual([], violacoes, "\n" + "\n".join(v.formatar() for v in violacoes))

    def assertViolacao(self, arquivo: str, conteudo: str, codigo: str, trecho: str = "") -> lint.Violacao:
        self.repo.escrever(arquivo, conteudo)
        violacoes = [v for v in self.repo.verificar() if v.arquivo == arquivo]
        encontradas = [v for v in violacoes if v.codigo == codigo and trecho in v.mensagem]
        self.assertTrue(
            encontradas,
            f"esperava {codigo} contendo {trecho!r} em {arquivo}; obtido:\n"
            + "\n".join(v.formatar() for v in violacoes),
        )
        return encontradas[0]


class TestePermitidos(BaseTeste):
    def test_arquitetura_conforme_passa(self) -> None:
        r = self.repo
        r.escrever("pipeline/shopbrasil_pipeline/dominio/metricas.py", """
            from __future__ import annotations
            from dataclasses import dataclass
            from decimal import Decimal, ROUND_HALF_UP
            import statistics
            from . import __name__ as _pacote
        """)
        r.escrever("pipeline/shopbrasil_pipeline/aplicacao/contratos.py", """
            from typing import Protocol
            from shopbrasil_pipeline.dominio.metricas import Decimal
        """)
        r.escrever("pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py", """
            from .contratos import Protocol
            from ..dominio import metricas
            from .. import dominio
            import shopbrasil_pipeline
            import shopbrasil_pipeline.dominio.metricas as m
        """)
        r.escrever("pipeline/shopbrasil_pipeline/infraestrutura/fakestore.py", """
            import requests
            from shopbrasil_pipeline.aplicacao.contratos import Protocol
            from ..dominio.metricas import Decimal
        """)
        r.escrever("pipeline/shopbrasil_pipeline/infraestrutura/postgres.py", """
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            from .fakestore import requests
            from ..aplicacao import contratos
        """)
        r.escrever("pipeline/shopbrasil_pipeline/composicao.py", """
            from shopbrasil_pipeline.infraestrutura.postgres import PostgresHook
            from shopbrasil_pipeline.infraestrutura import fakestore
            from shopbrasil_pipeline.aplicacao.casos_uso import m
        """)
        r.escrever("pipeline/dags/shopbrasil_dag.py", """
            import pendulum
            from airflow.sdk import dag, task, task_group
            from airflow.models import BaseOperator
            from shopbrasil_pipeline.aplicacao import casos_uso
            from shopbrasil_pipeline.composicao import fakestore
            import callbacks
        """)
        r.escrever("web/shopbrasil_web/consultas/metricas.py", """
            from dataclasses import dataclass
            from datetime import date
            from typing import Protocol
        """)
        r.escrever("web/shopbrasil_web/apresentacao/controller.py", """
            from http import HTTPStatus
            from fastapi import APIRouter
            from shopbrasil_web.consultas.metricas import Protocol
            from ..consultas import metricas
        """)
        r.escrever("web/shopbrasil_web/infraestrutura/repositorio.py", """
            import psycopg
            from ..consultas.metricas import Protocol
        """)
        r.escrever("web/shopbrasil_web/composicao.py", """
            from .apresentacao import controller
            from .infraestrutura.repositorio import psycopg
            from shopbrasil_web.consultas import metricas
        """)
        self.assertSemViolacoes()

    def test_arquivos_fora_do_pacote_so_respeitam_a_fronteira(self) -> None:
        self.repo.escrever("pipeline/tests/test_algo.py", """
            from shopbrasil_pipeline.infraestrutura import postgres
            import pytest
        """)
        self.repo.escrever("web/main.py", "from shopbrasil_web.infraestrutura import repositorio\n")
        self.assertSemViolacoes()

    def test_composicao_pode_ficar_na_pasta_de_dags(self) -> None:
        self.repo.escrever("pipeline/dags/composicao.py", """
            from shopbrasil_pipeline.infraestrutura.postgres import PostgresHook
            from airflow.providers.postgres.hooks.postgres import PostgresHook
        """)
        self.repo.escrever("pipeline/dags/shopbrasil_dag.py", "import composicao\n")
        self.assertSemViolacoes()


class TesteFronteiraEntreModulos(BaseTeste):
    def test_pipeline_nao_importa_web_absoluto(self) -> None:
        v = self.assertViolacao(
            "pipeline/shopbrasil_pipeline/infraestrutura/postgres.py",
            "from shopbrasil_web.consultas.metricas import X\n",
            "ARQ001", "módulo pipeline não pode importar web",
        )
        self.assertIn("db/", v.correcao)

    def test_web_nao_importa_pipeline_absoluto(self) -> None:
        self.assertViolacao(
            "web/shopbrasil_web/infraestrutura/repositorio.py",
            "import shopbrasil_pipeline.dominio.metricas\n",
            "ARQ001", "módulo web não pode importar pipeline",
        )

    def test_web_nao_importa_pipeline_pela_raiz_do_repositorio(self) -> None:
        self.assertViolacao(
            "web/shopbrasil_web/composicao.py",
            "from pipeline.shopbrasil_pipeline import dominio\n",
            "ARQ001",
        )

    def test_web_nao_importa_dags(self) -> None:
        self.assertViolacao("web/shopbrasil_web/composicao.py", "import callbacks\n", "ARQ001")

    def test_fronteira_vale_fora_das_camadas(self) -> None:
        self.assertViolacao("pipeline/tests/test_x.py", "import shopbrasil_web\n", "ARQ001")

    def test_import_relativo_atravessando_modulos(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py",
            "from ....web.shopbrasil_web.consultas import metricas\n",
            "ARQ001",
        )

    def test_import_dinamico_literal(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/composicao.py",
            "import importlib\nmodulo = importlib.import_module('shopbrasil_web.consultas')\n",
            "ARQ001",
        )


class TesteDirecaoDasCamadasPipeline(BaseTeste):
    def test_dominio_nao_usa_terceiros(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/dominio/metricas.py",
            "from airflow.sdk import task\n",
            "ARQ003", "pacote externo 'airflow'",
        )

    def test_dominio_nao_usa_http_da_biblioteca_padrao(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/dominio/metricas.py",
            "from urllib import request\n",
            "ARQ004", "urllib.request",
        )

    def test_dominio_nao_importa_aplicacao(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/dominio/metricas.py",
            "from ..aplicacao.contratos import Repositorio\n",
            "ARQ002", "pipeline_dominio não pode importar pipeline_aplicacao",
        )

    def test_aplicacao_nao_importa_infraestrutura_absoluto(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py",
            "from shopbrasil_pipeline.infraestrutura.postgres import Publicador\n",
            "ARQ002", "pipeline_aplicacao não pode importar pipeline_infraestrutura",
        )

    def test_aplicacao_nao_importa_infraestrutura_relativo(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py",
            "from ..infraestrutura.postgres import Publicador\n",
            "ARQ002", "pipeline_infraestrutura",
        )

    def test_aplicacao_nao_importa_submodulo_via_pacote_pai(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py",
            "from .. import dominio, infraestrutura\n",
            "ARQ002", "pipeline_infraestrutura",
        )

    def test_aplicacao_nao_importa_composicao(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py",
            "import shopbrasil_pipeline.composicao as c\n",
            "ARQ002", "pipeline_composicao",
        )

    def test_aplicacao_nao_usa_airflow_nem_driver(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py",
            "import psycopg2\n",
            "ARQ003", "psycopg2",
        )

    def test_infraestrutura_nao_importa_dags(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/infraestrutura/postgres.py",
            "import callbacks\n",
            "ARQ002", "pipeline_dags",
        )

    def test_infraestrutura_nao_importa_composicao(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/infraestrutura/postgres.py",
            "from .. import composicao\n",
            "ARQ002", "pipeline_composicao",
        )

    def test_dag_nao_importa_dominio(self) -> None:
        # O DAG aciona a aplicação; tipos e regras do domínio chegam por ela (AGENTS.md).
        self.assertViolacao(
            "pipeline/dags/shopbrasil_dag.py",
            "from shopbrasil_pipeline.dominio.metricas import Decimal\n",
            "ARQ002",
            "pipeline_dags não pode importar pipeline_dominio",
        )

    def test_dag_nao_importa_adaptador_concreto(self) -> None:
        v = self.assertViolacao(
            "pipeline/dags/shopbrasil_dag.py",
            "from shopbrasil_pipeline.infraestrutura.postgres import Publicador\n",
            "ARQ002", "pipeline_dags não pode importar pipeline_infraestrutura",
        )
        self.assertIn("composição", v.correcao)

    def test_dag_nao_usa_hook_postgres_nem_http(self) -> None:
        self.assertViolacao(
            "pipeline/dags/shopbrasil_dag.py",
            "from airflow.providers.postgres.hooks.postgres import PostgresHook\n",
            "ARQ004", "airflow.providers.postgres",
        )
        self.assertViolacao("pipeline/dags/shopbrasil_dag.py", "import requests\n", "ARQ004", "requests")

    def test_raiz_do_pacote_sem_dependencias_internas(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/__init__.py",
            "from .infraestrutura import postgres\n",
            "ARQ002", "pipeline_raiz",
        )

    def test_arquivo_sem_camada_dentro_do_pacote(self) -> None:
        v = self.assertViolacao(
            "pipeline/shopbrasil_pipeline/utilitarios.py", "import os\n", "ARQ006",
        )
        self.assertIn("arquitetura.toml", v.correcao)


class TesteDirecaoDasCamadasWeb(BaseTeste):
    def test_apresentacao_nao_importa_infraestrutura(self) -> None:
        self.assertViolacao(
            "web/shopbrasil_web/apresentacao/controller.py",
            "from ..infraestrutura.repositorio import RepositorioPostgres\n",
            "ARQ002", "web_apresentacao não pode importar web_infraestrutura",
        )

    def test_apresentacao_nao_acessa_banco(self) -> None:
        self.assertViolacao(
            "web/shopbrasil_web/apresentacao/controller.py", "import psycopg\n", "ARQ004", "psycopg",
        )

    def test_apresentacao_nao_importa_composicao(self) -> None:
        self.assertViolacao(
            "web/shopbrasil_web/apresentacao/controller.py",
            "from shopbrasil_web import composicao\n",
            "ARQ002", "web_composicao",
        )

    def test_consultas_nao_depende_de_http_nem_postgres(self) -> None:
        self.assertViolacao("web/shopbrasil_web/consultas/metricas.py", "import fastapi\n", "ARQ003")
        self.assertViolacao("web/shopbrasil_web/consultas/metricas.py", "from http import HTTPStatus\n", "ARQ004")
        self.assertViolacao("web/shopbrasil_web/consultas/metricas.py", "import psycopg\n", "ARQ003")

    def test_consultas_nao_importa_infraestrutura(self) -> None:
        self.assertViolacao(
            "web/shopbrasil_web/consultas/metricas.py",
            "from shopbrasil_web.infraestrutura import repositorio\n",
            "ARQ002", "web_infraestrutura",
        )

    def test_infraestrutura_nao_importa_apresentacao(self) -> None:
        self.assertViolacao(
            "web/shopbrasil_web/infraestrutura/repositorio.py",
            "from ..apresentacao import controller\n",
            "ARQ002", "web_apresentacao",
        )


class TesteImportsRelativosInvalidos(BaseTeste):
    def test_relativo_que_sai_do_pacote(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py",
            "from ... import dags\n",
            "ARQ005",
        )

    def test_relativo_em_arquivo_de_dag(self) -> None:
        self.assertViolacao("pipeline/dags/shopbrasil_dag.py", "from . import callbacks\n", "ARQ005")

    def test_relativo_alem_da_raiz_do_repositorio(self) -> None:
        self.assertViolacao(
            "pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py",
            "from ...... import algo\n",
            "ARQ005",
        )


class TesteCli(BaseTeste):
    def executar(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--raiz", str(self.repo.raiz), *args],
            capture_output=True, text=True, check=False,
        )

    def test_sucesso_retorna_zero(self) -> None:
        resultado = self.executar()
        self.assertEqual(0, resultado.returncode, resultado.stdout + resultado.stderr)
        self.assertIn("OK:", resultado.stdout)

    def test_violacao_retorna_um_com_localizacao(self) -> None:
        self.repo.escrever(
            "pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py",
            "import os\nfrom ..infraestrutura import postgres\n",
        )
        resultado = self.executar()
        self.assertEqual(1, resultado.returncode)
        self.assertIn(
            "pipeline/shopbrasil_pipeline/aplicacao/casos_uso.py:2:1: ARQ002", resultado.stdout,
        )
        self.assertIn("correção:", resultado.stdout)
        self.assertIn("FALHOU: 1 violação", resultado.stdout)

    def test_erro_de_sintaxe_falha(self) -> None:
        self.repo.escrever("web/shopbrasil_web/consultas/metricas.py", "def (:\n")
        resultado = self.executar()
        self.assertEqual(1, resultado.returncode)
        self.assertIn("ARQ007", resultado.stdout)

    def test_configuracao_invalida_retorna_dois(self) -> None:
        config = self.repo.raiz / "ruim.toml"
        config.write_text(
            CONFIG.read_text(encoding="utf-8").replace(
                'pode_importar = ["web_consultas"]\nterceiros = ["*"]\nproibidos = []',
                'pode_importar = ["pipeline_dominio"]\nterceiros = ["*"]\nproibidos = []',
                1,
            ),
            encoding="utf-8",
        )
        resultado = self.executar("--config", str(config))
        self.assertEqual(2, resultado.returncode, resultado.stdout + resultado.stderr)
        self.assertIn("não têm exceção", resultado.stderr)


class TesteRepositorioReal(unittest.TestCase):
    def test_repositorio_atual_passa(self) -> None:
        resultado = subprocess.run(
            [sys.executable, str(SCRIPT), "--raiz", str(REPOSITORIO)],
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(0, resultado.returncode, resultado.stdout + resultado.stderr)


if __name__ == "__main__":
    unittest.main()
