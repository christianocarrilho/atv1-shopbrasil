#!/usr/bin/env bash
# Autor: Christiano Henrique Carrilho Lopes da Silva
# Código de pessoa: 1691353
# Matrícula: 262987
# Comando único da suíte E2E (#23). Executa os cenários em sequência, cada um no próprio projeto
# Compose isolado, guarda a saída de cada um e imprime o resultado por cenário. Termina com código
# diferente de zero se algum cenário falhar.
#
# Uso: e2e/executar_todos.sh [cenário ...]   (padrão: todos, na ordem abaixo)
# Saída: tabelas por cenário em $E2E_RESULTADOS (padrão: e2e/resultados/<data-hora>/).

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CENARIOS=("$@")
[[ ${#CENARIOS[@]} -eq 0 ]] && CENARIOS=(ambiente esquema pipeline publicacao web integrado)
SAIDA="${E2E_RESULTADOS:-$ROOT/e2e/resultados/$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$SAIDA"

declare -a LINHAS=()
FALHAS=0
for cenario in "${CENARIOS[@]}"; do
  script="$ROOT/e2e/$cenario/executar.sh"
  [[ -x "$script" ]] || { echo "cenário desconhecido: $cenario" >&2; exit 2; }
  echo "=== $cenario"
  inicio=$(date +%s)
  "$script" 2>&1 | tee "$SAIDA/$cenario.log"
  codigo=${PIPESTATUS[0]}
  duracao=$(( $(date +%s) - inicio ))
  pass=$(grep -c '^\[PASS\]' "$SAIDA/$cenario.log")
  fail=$(grep -c '^\[FAIL\]' "$SAIDA/$cenario.log")
  if [[ $codigo -eq 0 ]]; then status=PASS; else status=FAIL; FALHAS=$((FALHAS + 1)); fi
  LINHAS+=("| $cenario | $status | $pass | $fail | ${duracao}s |")
done

{
  echo "| Cenário | Resultado | PASS | FAIL | Duração |"
  echo "|---|---|---|---|---|"
  printf '%s\n' "${LINHAS[@]}"
} | tee "$SAIDA/resumo.md"
echo "Saídas em $SAIDA"
exit $(( FALHAS > 0 ))
