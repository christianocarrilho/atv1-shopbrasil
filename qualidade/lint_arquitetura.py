#!/usr/bin/env python3
"""Lint de arquitetura da ShopBrasil (#26).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Verifica imports absolutos, relativos e dinâmicos com nome literal nos módulos
``pipeline/`` e ``web/`` contra as camadas declaradas em ``arquitetura.toml``.
Usa apenas a biblioteca padrão (``ast`` e ``tomllib``) para não depender das
versões de Airflow e da tecnologia Web/API que a #1 define. Os imports são
resolvidos pelo caminho no repositório, então ``from ..infraestrutura import X``
e ``from shopbrasil_pipeline.infraestrutura import X`` recebem a mesma regra.

Uso::

    python3 qualidade/lint_arquitetura.py [--raiz DIR] [--config ARQ] [--listar]

Códigos de saída: 0 sem violações, 1 com violações, 2 erro de uso ou config.
"""

from __future__ import annotations

import argparse
import ast
import sys
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

VERSAO_MINIMA = (3, 11)
CONFIG_PADRAO = Path(__file__).resolve().with_name("arquitetura.toml")
DIRETORIOS_IGNORADOS = {"__pycache__", "node_modules", "venv", "site-packages"}
STDLIB = frozenset(getattr(sys, "stdlib_module_names", ())) | {"__future__"}


class ErroConfiguracao(Exception):
    """Configuração inválida ou incoerente."""


@dataclass(frozen=True)
class Modulo:
    nome: str
    raiz: str
    raizes_import: tuple[str, ...]
    pacotes: tuple[str, ...]


@dataclass(frozen=True)
class Camada:
    nome: str
    modulo: str
    caminhos: tuple[str, ...]
    pode_importar: frozenset[str]
    terceiros: frozenset[str]
    proibidos: tuple[str, ...]
    orientacao: str

    @property
    def libera_terceiros(self) -> bool:
        return "*" in self.terceiros


@dataclass
class Configuracao:
    modulos: dict[str, Modulo]
    camadas: dict[str, Camada]
    raizes_pacote: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class Violacao:
    arquivo: str
    linha: int
    coluna: int
    codigo: str
    mensagem: str
    correcao: str

    def formatar(self) -> str:
        texto = f"{self.arquivo}:{self.linha}:{self.coluna}: {self.codigo} {self.mensagem}"
        if self.correcao:
            texto += f"\n    correção: {self.correcao}"
        return texto


@dataclass(frozen=True)
class Importacao:
    linha: int
    coluna: int
    trecho: str
    nivel: int
    modulo: str | None
    nomes: tuple[str, ...]


@dataclass(frozen=True)
class Destino:
    """Resultado da resolução de um import."""

    tipo: str  # "interno", "stdlib", "terceiro" ou "fora_do_pacote"
    nome: str
    caminho: str | None = None


# ----------------------------------------------------------------- configuração


def _normalizar(caminho: str) -> str:
    return str(PurePosixPath(caminho.strip().strip("/")))


def _tupla_texto(valor, campo: str, dono: str) -> tuple[str, ...]:
    if not isinstance(valor, list) or not all(isinstance(v, str) for v in valor):
        raise ErroConfiguracao(f"{dono}: '{campo}' deve ser uma lista de textos")
    return tuple(valor)


def carregar_configuracao(caminho: Path) -> Configuracao:
    import tomllib

    try:
        dados = tomllib.loads(caminho.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ErroConfiguracao(f"configuração não encontrada: {caminho}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ErroConfiguracao(f"{caminho}: TOML inválido: {exc}") from exc

    modulos: dict[str, Modulo] = {}
    for nome, bruto in dados.get("modulos", {}).items():
        dono = f"modulos.{nome}"
        raiz = bruto.get("raiz")
        if not isinstance(raiz, str):
            raise ErroConfiguracao(f"{dono}: 'raiz' é obrigatória")
        modulos[nome] = Modulo(
            nome=nome,
            raiz=_normalizar(raiz),
            raizes_import=tuple(
                _normalizar(r)
                for r in _tupla_texto(bruto.get("raizes_import", [raiz]), "raizes_import", dono)
            ),
            pacotes=tuple(
                _normalizar(p) for p in _tupla_texto(bruto.get("pacotes", []), "pacotes", dono)
            ),
        )
    if len(modulos) < 2:
        raise ErroConfiguracao("declare pelo menos dois módulos em [modulos]")

    camadas: dict[str, Camada] = {}
    for nome, bruto in dados.get("camadas", {}).items():
        dono = f"camadas.{nome}"
        modulo = bruto.get("modulo")
        if modulo not in modulos:
            raise ErroConfiguracao(f"{dono}: módulo desconhecido {modulo!r}")
        caminhos = tuple(
            _normalizar(c) for c in _tupla_texto(bruto.get("caminhos", []), "caminhos", dono)
        )
        if not caminhos:
            raise ErroConfiguracao(f"{dono}: declare ao menos um caminho")
        raiz_modulo = modulos[modulo].raiz
        for c in caminhos:
            if not _contem(raiz_modulo, c):
                raise ErroConfiguracao(f"{dono}: caminho {c!r} está fora de {raiz_modulo!r}")
        camadas[nome] = Camada(
            nome=nome,
            modulo=modulo,
            caminhos=caminhos,
            pode_importar=frozenset(_tupla_texto(bruto.get("pode_importar", []), "pode_importar", dono)),
            terceiros=frozenset(_tupla_texto(bruto.get("terceiros", []), "terceiros", dono)),
            proibidos=_tupla_texto(bruto.get("proibidos", []), "proibidos", dono),
            orientacao=str(bruto.get("orientacao", "")),
        )

    for camada in camadas.values():
        for alvo in camada.pode_importar:
            if alvo not in camadas:
                raise ErroConfiguracao(f"camadas.{camada.nome}: camada desconhecida {alvo!r} em pode_importar")
            if camadas[alvo].modulo != camada.modulo:
                raise ErroConfiguracao(
                    f"camadas.{camada.nome}: {alvo!r} pertence a outro módulo; "
                    "imports entre pipeline e web não têm exceção"
                )

    vistos: dict[str, str] = {}
    for camada in camadas.values():
        for c in camada.caminhos:
            if c in vistos:
                raise ErroConfiguracao(f"caminho {c!r} declarado em {vistos[c]!r} e {camada.nome!r}")
            vistos[c] = camada.nome

    raizes_pacote = frozenset(
        c.nome
        for c in camadas.values()
        if all(PurePosixPath(p).name == "__init__.py" and str(PurePosixPath(p).parent) in modulos[c.modulo].pacotes for p in c.caminhos)
    )
    return Configuracao(modulos=modulos, camadas=camadas, raizes_pacote=raizes_pacote)


# ------------------------------------------------------------------ utilitários


def _contem(base: str, caminho: str) -> bool:
    return caminho == base or caminho.startswith(base + "/")


def _corresponde(nome: str, prefixo: str) -> bool:
    return nome == prefixo or nome.startswith(prefixo + ".")


class Analisador:
    def __init__(self, raiz: Path, config: Configuracao):
        self.raiz = raiz
        self.config = config

    # ------------------------------------------------------------ classificação

    def modulo_de(self, caminho: str) -> Modulo | None:
        candidatos = [m for m in self.config.modulos.values() if _contem(m.raiz, caminho)]
        return max(candidatos, key=lambda m: len(m.raiz), default=None)

    def camada_de(self, caminho: str) -> Camada | None:
        melhor: tuple[int, Camada] | None = None
        for camada in self.config.camadas.values():
            for c in camada.caminhos:
                if _contem(c, caminho) and (melhor is None or len(c) > melhor[0]):
                    melhor = (len(c), camada)
        if melhor is None and (self.raiz / caminho).is_dir():
            return self.camada_de(f"{caminho}/__init__.py")
        return melhor[1] if melhor else None

    def pacote_de(self, caminho: str) -> str | None:
        modulo = self.modulo_de(caminho)
        if modulo is None:
            return None
        return next((p for p in modulo.pacotes if _contem(p, caminho)), None)

    # ---------------------------------------------------------------- arquivos

    def arquivos(self) -> list[str]:
        encontrados: set[str] = set()
        for modulo in self.config.modulos.values():
            base = self.raiz / modulo.raiz
            if not base.is_dir():
                continue
            for arquivo in base.rglob("*.py"):
                relativo = arquivo.relative_to(self.raiz)
                if any(p in DIRETORIOS_IGNORADOS or p.startswith(".") for p in relativo.parts[:-1]):
                    continue
                encontrados.add(relativo.as_posix())
        return sorted(encontrados)

    # --------------------------------------------------------------- resolução

    def _existe_modulo(self, caminho: str) -> bool:
        alvo = self.raiz / caminho
        return alvo.is_dir() or alvo.with_name(alvo.name + ".py").is_file()

    def _raizes_busca(self, modulo: Modulo | None) -> list[str]:
        ordem: list[str] = []
        if modulo is not None:
            ordem += sorted(modulo.raizes_import, key=len, reverse=True)
        for outro in self.config.modulos.values():
            if outro is not modulo:
                ordem += sorted(outro.raizes_import, key=len, reverse=True)
        return ordem

    def _resolver_absoluto(self, nome: str, modulo: Modulo | None) -> Destino:
        partes = nome.split(".")
        if partes[0] in STDLIB:
            return Destino("stdlib", nome)
        for raiz_import in self._raizes_busca(modulo):
            if self._existe_modulo(f"{raiz_import}/{partes[0]}"):
                return Destino("interno", nome, "/".join([raiz_import, *partes]))
        # Import pela raiz do repositório, como "pipeline.shopbrasil_pipeline".
        for outro in self.config.modulos.values():
            if partes[0] == outro.raiz and (self.raiz / outro.raiz).is_dir():
                return Destino("interno", nome, "/".join(partes))
        return Destino("terceiro", nome)

    def _raiz_import_do_arquivo(self, arquivo: str, modulo: Modulo | None) -> str | None:
        if modulo is None:
            return None
        candidatos = [r for r in modulo.raizes_import if _contem(r, arquivo)]
        return max(candidatos, key=len, default=None)

    def resolver(self, arquivo: str, imp: Importacao) -> list[Destino]:
        modulo = self.modulo_de(arquivo)
        if imp.nivel == 0:
            base = self._resolver_absoluto(imp.modulo or "", modulo)
            if not imp.nomes:
                return [base]
            if base.tipo != "interno":
                return [base] + [
                    Destino(base.tipo, f"{base.nome}.{n}") for n in imp.nomes if n != "*"
                ]
            return self._expandir_nomes(base, imp.nomes)

        diretorio = PurePosixPath(arquivo).parent
        for _ in range(imp.nivel - 1):
            if str(diretorio) in ("", "."):
                rotulo = "." * imp.nivel + (imp.modulo or "")
                return [Destino("fora_do_pacote", rotulo)]
            diretorio = diretorio.parent
        base_dir = str(diretorio)
        rotulo = "." * imp.nivel + (imp.modulo or "")
        partes_base = [] if base_dir in ("", ".") else [base_dir]
        caminho = "/".join(partes_base + (imp.modulo.split(".") if imp.modulo else [])) or "."

        raiz_import = self._raiz_import_do_arquivo(arquivo, modulo)
        topo = None
        if raiz_import is not None:
            resto = PurePosixPath(arquivo).relative_to(raiz_import).parts
            topo = f"{raiz_import}/{resto[0]}" if len(resto) > 1 else None
        destino_modulo = self.modulo_de(caminho)
        if destino_modulo is not None and modulo is not None and destino_modulo.nome != modulo.nome:
            return self._expandir_nomes(Destino("interno", rotulo, caminho), imp.nomes)
        if topo is None or not _contem(topo, base_dir) or base_dir in ("", "."):
            return [Destino("fora_do_pacote", rotulo, caminho)]
        return self._expandir_nomes(Destino("interno", rotulo, caminho), imp.nomes)

    def _expandir_nomes(self, base: Destino, nomes: tuple[str, ...]) -> list[Destino]:
        if not nomes:
            return [base]
        destinos: list[Destino] = []
        usa_base = False
        for nome in nomes:
            candidato = f"{base.caminho}/{nome}"
            if nome != "*" and self._existe_modulo(candidato):
                destinos.append(Destino("interno", f"{base.nome}.{nome}".lstrip("."), candidato))
            else:
                usa_base = True
        if usa_base:
            destinos.insert(0, base)
        return destinos

    # --------------------------------------------------------------- verificação

    def verificar(self) -> tuple[list[Violacao], dict[str, str | None]]:
        violacoes: list[Violacao] = []
        classificacao: dict[str, str | None] = {}
        for arquivo in self.arquivos():
            camada = self.camada_de(arquivo)
            classificacao[arquivo] = camada.nome if camada else None
            violacoes.extend(self.verificar_arquivo(arquivo))
        return violacoes, classificacao

    def verificar_arquivo(self, arquivo: str) -> list[Violacao]:
        modulo = self.modulo_de(arquivo)
        camada = self.camada_de(arquivo)
        violacoes: list[Violacao] = []
        fonte = (self.raiz / arquivo).read_text(encoding="utf-8")
        try:
            arvore = ast.parse(fonte, filename=arquivo)
        except SyntaxError as exc:
            return [Violacao(arquivo, exc.lineno or 1, (exc.offset or 0) + 1, "ARQ007",
                             f"não foi possível analisar o arquivo: {exc.msg}",
                             "Corrija a sintaxe para que os imports possam ser verificados.")]

        pacote = self.pacote_de(arquivo)
        if pacote is not None and camada is None:
            sugestoes = ", ".join(
                c.nome for c in self.config.camadas.values() if modulo and c.modulo == modulo.nome
            )
            violacoes.append(Violacao(
                arquivo, 1, 1, "ARQ006",
                f"arquivo dentro de {pacote}/ sem camada declarada",
                f"Mova o arquivo para uma camada ({sugestoes}) ou declare o caminho em qualidade/arquitetura.toml com revisão.",
            ))

        linhas = fonte.splitlines()
        for imp in _coletar_importacoes(arvore, linhas):
            for destino in self.resolver(arquivo, imp):
                v = self._avaliar(arquivo, modulo, camada, imp, destino)
                if v is not None:
                    violacoes.append(v)
        return _sem_duplicatas(violacoes)

    def _avaliar(self, arquivo: str, modulo: Modulo | None, camada: Camada | None,
                 imp: Importacao, destino: Destino) -> Violacao | None:
        def violacao(codigo: str, mensagem: str, correcao: str) -> Violacao:
            return Violacao(arquivo, imp.linha, imp.coluna, codigo, f"{mensagem} [{imp.trecho}]", correcao)

        if destino.tipo == "fora_do_pacote":
            return violacao(
                "ARQ005",
                f"import relativo '{destino.nome}' sai do pacote de nível superior",
                "Use import absoluto pelo pacote do próprio módulo ou mova o código para dentro do pacote.",
            )

        if destino.tipo == "interno":
            assert destino.caminho is not None
            alvo_modulo = self.modulo_de(destino.caminho)
            if modulo is not None and alvo_modulo is not None and alvo_modulo.nome != modulo.nome:
                return violacao(
                    "ARQ001",
                    f"módulo {modulo.nome} não pode importar {alvo_modulo.nome} ('{destino.nome}' -> {destino.caminho})",
                    "Pipeline e Web/API se comunicam apenas pelo esquema PostgreSQL em db/. "
                    "Duplique o tipo de leitura no próprio módulo ou consulte o banco.",
                )
            if camada is None:
                return None
            alvo = self.camada_de(destino.caminho)
            if alvo is not None and (
                alvo.nome == camada.nome
                or alvo.nome in camada.pode_importar
                or (alvo.nome in self.config.raizes_pacote and alvo.modulo == camada.modulo)
            ):
                return None
            alvo_rotulo = alvo.nome if alvo else f"destino sem camada ({destino.caminho})"
            permitidas = ", ".join(sorted(camada.pode_importar)) or "nenhuma outra camada"
            return violacao(
                "ARQ002",
                f"camada {camada.nome} não pode importar {alvo_rotulo} ('{destino.nome}'); permitidas: {permitidas}",
                camada.orientacao,
            )

        if camada is None:
            return None
        proibido = next((p for p in camada.proibidos if _corresponde(destino.nome, p)), None)
        if proibido is not None:
            return violacao(
                "ARQ004",
                f"camada {camada.nome} não pode usar '{destino.nome}' (vetado por '{proibido}')",
                camada.orientacao,
            )
        if destino.tipo == "terceiro":
            topo = destino.nome.split(".")[0]
            if not camada.libera_terceiros and topo not in camada.terceiros:
                return violacao(
                    "ARQ003",
                    f"camada {camada.nome} não pode depender do pacote externo '{topo}'",
                    camada.orientacao,
                )
        return None


def _sem_duplicatas(violacoes: list[Violacao]) -> list[Violacao]:
    vistos: set[tuple] = set()
    unicas = []
    for v in violacoes:
        chave = (v.arquivo, v.linha, v.coluna, v.codigo, v.mensagem)
        if chave not in vistos:
            vistos.add(chave)
            unicas.append(v)
    return unicas


def _texto_literal(no: ast.AST) -> str | None:
    return no.value if isinstance(no, ast.Constant) and isinstance(no.value, str) else None


def _coletar_importacoes(arvore: ast.AST, linhas: list[str]) -> list[Importacao]:
    def trecho(no: ast.AST) -> str:
        indice = getattr(no, "lineno", 1) - 1
        return linhas[indice].strip() if 0 <= indice < len(linhas) else ""

    encontrados: list[Importacao] = []
    for no in ast.walk(arvore):
        if isinstance(no, ast.Import):
            for alias in no.names:
                encontrados.append(Importacao(no.lineno, no.col_offset + 1, trecho(no), 0, alias.name, ()))
        elif isinstance(no, ast.ImportFrom):
            encontrados.append(Importacao(
                no.lineno, no.col_offset + 1, trecho(no), no.level, no.module,
                tuple(a.name for a in no.names),
            ))
        elif isinstance(no, ast.Call) and no.args:
            funcao = no.func
            nome_funcao = funcao.attr if isinstance(funcao, ast.Attribute) else getattr(funcao, "id", None)
            alvo = _texto_literal(no.args[0])
            if nome_funcao in ("import_module", "__import__") and alvo and not alvo.startswith("."):
                encontrados.append(Importacao(no.lineno, no.col_offset + 1, trecho(no), 0, alvo, ()))
    return sorted(encontrados, key=lambda i: (i.linha, i.coluna))


# ------------------------------------------------------------------------- CLI


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lint_arquitetura",
        description="Verifica os limites entre módulos e camadas da ShopBrasil.",
    )
    parser.add_argument("--raiz", type=Path, default=Path(__file__).resolve().parent.parent,
                        help="raiz do repositório (padrão: diretório pai de qualidade/)")
    parser.add_argument("--config", type=Path, default=CONFIG_PADRAO,
                        help="arquivo TOML com módulos e camadas")
    parser.add_argument("--listar", action="store_true",
                        help="lista a camada atribuída a cada arquivo verificado")
    args = parser.parse_args(argv)

    if sys.version_info < VERSAO_MINIMA:
        print(f"erro: requer Python {'.'.join(map(str, VERSAO_MINIMA))}+", file=sys.stderr)
        return 2
    raiz = args.raiz.resolve()
    if not raiz.is_dir():
        print(f"erro: raiz inexistente: {raiz}", file=sys.stderr)
        return 2
    try:
        config = carregar_configuracao(args.config)
    except ErroConfiguracao as exc:
        print(f"erro de configuração: {exc}", file=sys.stderr)
        return 2

    analisador = Analisador(raiz, config)
    violacoes, classificacao = analisador.verificar()

    if args.listar:
        for arquivo, camada in classificacao.items():
            print(f"{arquivo}: {camada or '(fora das camadas: só a fronteira entre módulos)'}")
        print()

    for v in violacoes:
        print(v.formatar())
    total = len(classificacao)
    if violacoes:
        arquivos = len({v.arquivo for v in violacoes})
        print(f"\nFALHOU: {len(violacoes)} violação(ões) em {arquivos} arquivo(s); {total} arquivo(s) Python verificado(s).")
        return 1
    print(f"OK: {total} arquivo(s) Python verificado(s), nenhuma violação de arquitetura.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
