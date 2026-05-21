#!/usr/bin/env bash
# ============================================================================
# BURST TEST — pico instantâneo via send-message-batch
# ----------------------------------------------------------------------------
# Propósito:
#   Simular um spike: jogar N eventos na fila em segundos e validar que o
#   sistema absorve sem perder nada. Usa send-message-batch (10 msgs/call)
#   pra empurrar rápido sem precisar de binário Go.
#
# Métricas reportadas:
#   - burst_send_duration : tempo pra empurrar N (deve ser <10s pra N=1000)
#   - peak_visible        : pico de mensagens visíveis na fila
#   - drain_duration      : tempo do fim do envio até a fila zerar
#
# Como interpretar:
#   - peak_visible alto + drain rápido = pool grande absorve bem.
#   - Se rodar e events_table.count < N -> PERDA. Bug grave no Processor.
#   - Watch out: se LocalStack reclamar de throttle, o pico é só do lado dele.
#
# Frase pra defesa em vídeo:
#   "Empurrei N eventos em <X segundos>. Pico de visíveis foi P. O pool
#    drenou em D segundos. Zero perda confirmada pelo count na tabela events."
#
# Uso:
#   N=1000 bash scripts/tests/burst.sh
# ============================================================================
set -euo pipefail
export USERPROFILE="${USERPROFILE:-$HOME}"

N="${N:-1000}"
ENDPOINT="http://localhost:4566"
RAW_QUEUE="$ENDPOINT/000000000000/raw-events"

# SQS send-message-batch aceita no máximo 10 entries por call
BATCH_SIZE=10
BATCHES=$(( (N + BATCH_SIZE - 1) / BATCH_SIZE ))

echo "==> Burst test: N=$N em $BATCHES batches de até $BATCH_SIZE"
SEND_START=$(date +%s)

build_entry() {
  local id="$1"
  local uuid ts body
  uuid=$(uuidgen | tr 'A-Z' 'a-z')
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  body=$(printf '{"event_id":"%s","developer_id":"dev-burst","metric_type":"commits","value":1,"repository":"org/burst","timestamp":"%s"}' "$uuid" "$ts")
  # escape JSON pra usar dentro de outro JSON
  body_escaped=$(printf '%s' "$body" | jq -Rs .)
  printf '{"Id":"%s","MessageBody":%s}' "$id" "$body_escaped"
}

sent=0
for b in $(seq 1 "$BATCHES"); do
  entries=""
  for i in $(seq 1 "$BATCH_SIZE"); do
    [[ "$sent" -ge "$N" ]] && break
    sent=$((sent + 1))
    entry=$(build_entry "$i")
    if [[ -z "$entries" ]]; then
      entries="$entry"
    else
      entries="$entries,$entry"
    fi
  done
  aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs send-message-batch \
    --queue-url "$RAW_QUEUE" \
    --entries "[$entries]" \
    --output text --query 'Successful[*].MessageId' | wc -l > /dev/null
done

SEND_END=$(date +%s)
BURST_DUR=$((SEND_END - SEND_START))
echo "==> $sent eventos enviados em ${BURST_DUR}s ($(awk -v n="$sent" -v t="$BURST_DUR" 'BEGIN { if (t==0) print "muito rápido"; else printf "%.0f ev/s", n/t }'))"

echo "==> monitorando drenagem (peak + total)..."
PEAK=0
while true; do
  attrs=$(aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs get-queue-attributes \
    --queue-url "$RAW_QUEUE" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --output text --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]')
  vis=$(echo "$attrs" | awk '{print $1}')
  inv=$(echo "$attrs" | awk '{print $2}')
  [[ "$vis" -gt "$PEAK" ]] && PEAK="$vis"
  total=$((vis + inv))
  echo "  vis=$vis  inflight=$inv  (peak_visible=$PEAK)"
  [[ "$total" -eq 0 ]] && break
  sleep 2
done

DRAIN_END=$(date +%s)
DRAIN_DUR=$((DRAIN_END - SEND_END))

echo ""
echo "=============================================="
echo " RESULTADO"
echo "=============================================="
echo " burst_send_duration : ${BURST_DUR}s"
echo " peak_visible        : $PEAK"
echo " drain_duration      : ${DRAIN_DUR}s"
echo "=============================================="
echo ""
echo "Verificar zero perda (deve ser >= $N considerando outros eventos):"
echo "  curl -s http://localhost:8080/metrics/dev-burst/summary | jq ."
