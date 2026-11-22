"""Casos de uso de leitura da Web/API (#9).

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Compartilhados pela API JSON e pela página das #19–#21. Recebem o repositório pelo construtor e
não conhecem HTTP nem SQL. A ordenação e a grade diária da série ficam aqui, e não no SQL, para
que a regra de lacunas (um ponto por dia, `None` sem observação, sem interpolar nem zerar) seja
a mesma qualquer que seja o adaptador.
"""

from __future__ import annotations

from datetime import date, timedelta

from shopbrasil_web.consultas.modelos import (
    Categoria,
    MetricasDaData,
    PontoEvolucao,
    SerieEvolucao,
)
from shopbrasil_web.consultas.repositorio import RepositorioMetricas

# Um ano e um dia cobrem um ano bissexto inteiro; o limite evita séries enormes por engano.
MAX_DIAS_INTERVALO = 366


class IntervaloInvalidoError(ValueError):
    """Intervalo com início depois do fim ou maior que MAX_DIAS_INTERVALO dias."""


class CategoriaNaoEncontradaError(LookupError):
    """Código de categoria que não está no cadastro."""

    def __init__(self, codigo: str) -> None:
        super().__init__(f"categoria {codigo!r} não cadastrada")
        self.codigo = codigo


class ConsultarMetricasDaData:
    """Métricas do snapshot de uma data, ordenadas pelo código da categoria."""

    def __init__(self, repositorio: RepositorioMetricas) -> None:
        self._repositorio = repositorio

    def executar(self, data: date) -> MetricasDaData:
        linhas = sorted(self._repositorio.metricas_da_data(data), key=lambda m: m.categoria.codigo)
        return MetricasDaData(data=data, metricas=tuple(linhas))


class ConsultarUltimaData:
    """Data mais recente com métricas, para a página abrir nela; `None` sem nenhuma publicação."""

    def __init__(self, repositorio: RepositorioMetricas) -> None:
        self._repositorio = repositorio

    def executar(self) -> date | None:
        return self._repositorio.ultima_data()


class ListarCategorias:
    """Categorias cadastradas, ordenadas pelo código, para o filtro da evolução."""

    def __init__(self, repositorio: RepositorioMetricas) -> None:
        self._repositorio = repositorio

    def executar(self) -> tuple[Categoria, ...]:
        return tuple(sorted(self._repositorio.categorias(), key=lambda c: c.codigo))


class ConsultarEvolucao:
    """Série diária do preço médio de uma categoria no intervalo inclusivo [inicio, fim]."""

    def __init__(self, repositorio: RepositorioMetricas) -> None:
        self._repositorio = repositorio

    def executar(self, codigo: str, inicio: date, fim: date) -> SerieEvolucao:
        if inicio > fim:
            raise IntervaloInvalidoError(f"início {inicio.isoformat()} depois do fim {fim.isoformat()}")
        dias = (fim - inicio).days + 1
        if dias > MAX_DIAS_INTERVALO:
            raise IntervaloInvalidoError(f"intervalo de {dias} dias; o máximo é {MAX_DIAS_INTERVALO}")
        categoria = self._repositorio.categoria(codigo)
        if categoria is None:
            raise CategoriaNaoEncontradaError(codigo)
        observados = {
            o.data: o.preco_medio
            for o in self._repositorio.precos_medios(codigo, inicio, fim)
            if inicio <= o.data <= fim
        }
        pontos = tuple(
            PontoEvolucao(data=dia, preco_medio=observados.get(dia))
            for dia in (inicio + timedelta(days=i) for i in range(dias))
        )
        return SerieEvolucao(categoria=categoria, inicio=inicio, fim=fim, pontos=pontos)
