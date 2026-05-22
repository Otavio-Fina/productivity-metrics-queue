#!/usr/bin/env bash
# ============================================================================
# LOAD TEST — taxa controlada
# ----------------------------------------------------------------------------
# Propósito:
#   Medir throughput sustentável do pipeline. Envia N eventos a R ev/s,
#   espera a raw-events drenar e calcula o throughput efetivo end-to-end.
#
# Métricas reportadas:
#   - send_duration  : tempo pra empurrar N mensagens no SQS
#   - drain_duration : tempo do último send até raw-events ficar vazia
#   - effective_eps  : N / (send + drain) — eventos efetivamente processados/s
#
# Como interpretar:
#   - Se rodar duas vezes com mesmo N/R e drain_duration crescer
#     -> seu worker pool tá saturado. Subir WORKER_COUNT no compose.
#   - LocalStack tem teto de ~150-200 ev/s no SQS interno. Acima disso o
#     gargalo é o LocalStack, NÃO o seu código (mencionar no vídeo).
#
# Frase pra defesa em vídeo:
#   "Aguentei X ev/s sustentado com WORKER_COUNT=Y. A drenagem foi linear,
#    o que indica que o pool tava balanceado com a taxa de chegada. Pra
#    saturar, subi pra 2X ev/s e observei in-flight crescente."
#
# Uso:
#   N=500  RATE=50  bash scripts/tests/load.sh
#   N=2000 RATE=200 bash scripts/tests/load.sh   # tentativa de saturar
# ============================================================================
set -euo pipefail

# Workaround AWS CLI v2 + git-bash no Windows: USERPROFILE precisa existir
# no subprocess senão pathlib.Path.home() crasha. No-op em Mac/Linux.
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

N="${N:-500}"
RATE="${RATE:-50}"
ENDPOINT="http://localhost:4566"
RAW_QUEUE="$ENDPOINT/000000000000/raw-events"

SLEEP=$(awk -v r="$RATE" 'BEGIN { printf "%.4f\n", 1/r }')

echo "==> Load test: N=$N, target rate=$RATE ev/s (sleep=${SLEEP}s entre sends)"
START=$(date +%s)

for i in $(seq 1 "$N"); do
  uuid=$(gen_uuid)
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  body=$(printf '{"event_id":"%s","developer_id":"dev-load","metric_type":"commits","value":1,"repository":"org/load","timestamp":"%s"}' "$uuid" "$ts")
  aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs send-message \
    --queue-url "$RAW_QUEUE" --message-body "$body" \
    --output text --query 'MessageId' > /dev/null
  sleep "$SLEEP"
done

SEND_END=$(date +%s)
SEND_DUR=$((SEND_END - START))
echo "==> $N enviados em ${SEND_DUR}s (target era $(awk -v n="$N" -v r="$RATE" 'BEGIN { printf "%.1f", n/r }')s)"

echo "==> aguardando raw-events drenar (visible + in-flight == 0)..."
while true; do
  attrs=$(aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs get-queue-attributes \
    --queue-url "$RAW_QUEUE" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --output text --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]')
  vis=$(echo "$attrs" | awk '{print $1}')
  inv=$(echo "$attrs" | awk '{print $2}')
  total=$((vis + inv))
  echo "  fila: visible=$vis in-flight=$inv"
  [[ "$total" -eq 0 ]] && break
  sleep 2
done

DRAIN_END=$(date +%s)
DRAIN_DUR=$((DRAIN_END - SEND_END))
TOTAL_DUR=$((DRAIN_END - START))
EPS=$(awk -v n="$N" -v t="$TOTAL_DUR" 'BEGIN { if (t==0) print "inf"; else printf "%.1f", n/t }')

echo ""
echo "=============================================="
echo " RESULTADO"
echo "=============================================="
echo " send_duration  : ${SEND_DUR}s"
echo " drain_duration : ${DRAIN_DUR}s  (tempo pós-envio até fila vazia)"
echo " total_duration : ${TOTAL_DUR}s"
echo " effective_eps  : ${EPS} ev/s"
echo "=============================================="
echo ""
echo "Verificação rápida:"
echo "  aws --endpoint-url=$ENDPOINT dynamodb scan --table-name events --select COUNT"
echo "  curl -s http://localhost:8080/metrics/dev-load/summary | jq ."
