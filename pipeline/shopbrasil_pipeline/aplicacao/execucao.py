"""Identificação de uma execução do pipeline.

Autor: Christiano Henrique Carrilho Lopes da Silva
Código de pessoa: 1691353
Matrícula: 262987

Recebe do DAG o ``run_id`` e a ocorrência agendada e devolve a identificação que as etapas
seguintes compartilham. A saída é um dicionário de textos para trafegar pelo XCom sem
serializador próprio.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from shopbrasil_pipeline.dominio.calendario import data_referencia


class OcorrenciaAusenteError(ValueError):
    """A run não tem ocorrência agendada da qual derivar a data de referência."""


@dataclass(frozen=True)
class Execucao:
    """Run do Airflow e data de referência das métricas que ela publica."""

    run_id: str
    data_referencia: date

    def para_xcom(self) -> dict[str, str]:
        return {"run_id": self.run_id, "data_referencia": self.data_referencia.isoformat()}

    @classmethod
    def de_xcom(cls, valor: dict[str, str]) -> "Execucao":
        return cls(run_id=valor["run_id"], data_referencia=date.fromisoformat(valor["data_referencia"]))


def identificar_execucao(run_id: str, ocorrencia: datetime | None) -> Execucao:
    """Monta a identificação da run a partir da ocorrência agendada.

    A data de referência não usa o relógio. Sem ocorrência, a run é rejeitada; o reprocessamento
    é feito limpando as tasks da run original, que preserva ``run_id`` e ocorrência.
    """
    if not run_id:
        raise ValueError("run_id obrigatório")
    if ocorrencia is None:
        raise OcorrenciaAusenteError(
            f"run {run_id!r} sem data lógica; reprocesse limpando as tasks da run agendada"
        )
    return Execucao(run_id=run_id, data_referencia=data_referencia(ocorrencia))
