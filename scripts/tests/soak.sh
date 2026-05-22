#!/usr/bin/env bash
# ============================================================================
# SOAK TEST — carga baixa e prolongada
# ----------------------------------------------------------------------------
# Propósito:
#   Detectar memory leak / goroutine leak / conexão SQS não fechada.
#   Envia 1 ev/s por DURATION segundos e captura `docker stats` em CSV
#   pra você gráficar depois (Excel, gnuplot, o que for).
#
# Métricas capturadas no CSV:
#   timestamp,container,cpu_pct,mem_usage,mem_pct
#
# Como interpretar:
#   - Memória RSS deve ser PLANA depois do warm-up (~1min).
#   - Cresce linearmente -> leak. Suspeitar de:
#       * goroutine não fechada (workers não respeitando context)
#       * conexão SQS persistente acumulando
#       * slice/map crescendo sem bound
#   - CPU deve ser baixo e estável (1 ev/s é trivial).
#
# Frase pra defesa em vídeo:
#   "Soak de 30min a 1 ev/s. Memória do processor estabilizou em ~XMB
#    após warm-up. Sem crescimento -> sem goroutine leak / sem conexão
#    pendente. Graceful shutdown também testado no fim."
#
# Uso:
#   DURATION=1800 bash scripts/tests/soak.sh   # 30 min (default)
#   DURATION=300  bash scripts/tests/soak.sh   # 5 min teste rápido
# ============================================================================
set -euo pipefail
export USERPROFILE="${USERPROFILE:-$HOME}"

# uuidgen não existe no git-bash do Windows. Geramos um UUID v4 conforme
# RFC 4122 a partir de /dev/urandom — presente no git-bash, Mac e Linux.
# Os nibbles de versão (4) e variante (8/9/a/b) são fixados manualmente.
gen_uuid() {
  local h
  h=$(od -An -tx1 -N16 /dev/urandom | tr -d ' \n')
  printf '%s-%s-4%s-%x%s-%s\n' \
    "${h:0:8}" "${h:8:4}" "${h:13:3}" \
    "$(( 0x${h:16:1} & 0x3 | 0x8 ))" "${h:17:3}" "${h:20:12}"
}

DURATION="${DURATION:-1800}"
RATE="${RATE:-1}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-10}"
ENDPOINT="http://localhost:4566"
RAW_QUEUE="$ENDPOINT/000000000000/raw-events"

STATS_FILE="soak_stats_$(date +%Y%m%d_%H%M%S).csv"
echo "timestamp,container,cpu_pct,mem_usage,mem_pct" > "$STATS_FILE"

echo "==> Soak test: ${DURATION}s a ${RATE} ev/s. Stats -> $STATS_FILE"

# Collector em background. --no-stream é OBRIGATÓRIO no Windows
# senão `docker stats` bloqueia (TTY emulation). Em Mac/Linux também funciona.
(
  while true; do
    ts=$(date +%s)
    docker stats --no-stream --format "$ts,{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}" >> "$STATS_FILE" 2>/dev/null || true
    sleep "$SAMPLE_INTERVAL"
  done
) &
STATS_PID=$!

# Garante kill do collector mesmo se script for interrompido
cleanup() {
  echo ""
  echo "==> parando collector (pid=$STATS_PID)"
  kill "$STATS_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

SLEEP=$(awk -v r="$RATE" 'BEGIN { printf "%.4f\n", 1/r }')
END=$(( $(date +%s) + DURATION ))
COUNT=0
LAST_REPORT=$(date +%s)

while [[ "$(date +%s)" -lt "$END" ]]; do
  uuid=$(gen_uuid)
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  body=$(printf '{"event_id":"%s","developer_id":"dev-soak","metric_type":"commits","value":1,"repository":"org/soak","timestamp":"%s"}' "$uuid" "$ts")
  aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs send-message \
    --queue-url "$RAW_QUEUE" --message-body "$body" \
    --output text --query 'MessageId' > /dev/null
  COUNT=$((COUNT + 1))

  NOW=$(date +%s)
  if (( NOW - LAST_REPORT >= 30 )); then
    REMAINING=$((END - NOW))
    echo "  [$(date +%H:%M:%S)] enviados=$COUNT restam=${REMAINING}s"
    LAST_REPORT=$NOW
  fi

  sleep "$SLEEP"
done

echo ""
echo "=============================================="
echo " RESULTADO"
echo "=============================================="
echo " total_enviados : $COUNT"
echo " duração        : ${DURATION}s"
echo " stats_csv      : $STATS_FILE"
echo "=============================================="
echo ""
echo "Análise sugerida:"
echo "  # Última amostra de memória por container:"
echo "  tail -n 20 $STATS_FILE"
echo ""
echo "  # Importar no Excel / sheets pra plotar mem_pct vs timestamp."
echo "  # Esperado: linha plana após warm-up."
