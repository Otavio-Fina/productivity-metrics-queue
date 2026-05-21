#!/usr/bin/env bash
# ============================================================================
# DLQ STRESS — 100 eventos inválidos
# ----------------------------------------------------------------------------
# Propósito:
#   Validar em escala que o caminho DLQ funciona corretamente:
#     - eventos inválidos NUNCA são deletados pelo Processor
#     - SQS move automaticamente pra DLQ após maxReceiveCount=3 receives
#     - logs do Processor mostram exatamente 3 tentativas por mensagem
#
# Envia N×4 mensagens, uma pra cada regra de validação:
#   - UUID inválido
#   - value negativo
#   - review_time > 1440
#   - timestamp no futuro
#
# Como interpretar:
#   raw-events-dlq.count == N*4  -> DLQ tá funcionando
#   raw-events.count     == 0    -> nada ficou preso
#   eventos perdidos             -> não tolerado; refletir sobre o Processor
#                                   estar deletando mensagens inválidas (bug).
#
# Logs esperados no Processor (rodar `docker compose logs processor | grep validation`):
#   ~N*4*3 linhas de "validation failed" (cada msg é recebida 3x antes de ir pra DLQ)
#
# Frase pra defesa em vídeo:
#   "Mandei N*4 inválidos. RedrivePolicy(maxReceiveCount=3) garantiu que após
#    3 tentativas o SQS mover sozinho pra DLQ. Comprovei: DLQ=N*4, raw-events=0,
#    logs mostram ~3 tentativas por mensagem. Nenhum evento inválido foi deletado
#    pelo Processor — política deliberada, validação não retenta."
#
# Uso:
#   N=25 bash scripts/tests/dlq.sh   # 100 mensagens totais (default)
# ============================================================================
set -euo pipefail
export USERPROFILE="${USERPROFILE:-$HOME}"

N="${N:-25}"
ENDPOINT="http://localhost:4566"
RAW_QUEUE="$ENDPOINT/000000000000/raw-events"
DLQ_QUEUE="$ENDPOINT/000000000000/raw-events-dlq"

TOTAL=$((N * 4))
echo "==> DLQ stress: $TOTAL mensagens inválidas ($N por regra)"

# Mede baseline da DLQ pra calcular delta no fim (evita interferência de runs anteriores)
BASELINE=$(aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs get-queue-attributes \
  --queue-url "$DLQ_QUEUE" --attribute-names ApproximateNumberOfMessages \
  --output text --query 'Attributes.ApproximateNumberOfMessages')
echo "==> baseline raw-events-dlq: $BASELINE mensagens"

send() {
  local body="$1"
  aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs send-message \
    --queue-url "$RAW_QUEUE" --message-body "$body" \
    --output text --query 'MessageId' > /dev/null
}

future_ts="2099-01-01T00:00:00Z"

echo "==> enviando..."
for i in $(seq 1 "$N"); do
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  # regra 1: UUID inválido
  send "$(printf '{"event_id":"not-a-uuid-%d","developer_id":"dev-bad","metric_type":"commits","value":1,"repository":"org/bad","timestamp":"%s"}' "$i" "$ts")"
  # regra 2: value negativo
  send "$(printf '{"event_id":"%s","developer_id":"dev-bad","metric_type":"commits","value":-1,"repository":"org/bad","timestamp":"%s"}' "$(uuidgen | tr A-Z a-z)" "$ts")"
  # regra 3: review_time > 1440
  send "$(printf '{"event_id":"%s","developer_id":"dev-bad","metric_type":"review_time_minutes","value":9999,"repository":"org/bad","timestamp":"%s"}' "$(uuidgen | tr A-Z a-z)" "$ts")"
  # regra 4: timestamp no futuro
  send "$(printf '{"event_id":"%s","developer_id":"dev-bad","metric_type":"commits","value":1,"repository":"org/bad","timestamp":"%s"}' "$(uuidgen | tr A-Z a-z)" "$future_ts")"
done
echo "==> $TOTAL inválidos enviados"

# Cada mensagem precisa ser recebida 3x. Visibility timeout default LocalStack
# é 30s, mas LocalStack normalmente devolve rápido. Damos margem generosa.
WAIT="${WAIT:-90}"
echo "==> aguardando ${WAIT}s para SQS completar 3 receives + redrive..."
sleep "$WAIT"

raw=$(aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs get-queue-attributes \
  --queue-url "$RAW_QUEUE" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
  --output text --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]')
raw_vis=$(echo "$raw" | awk '{print $1}')
raw_inv=$(echo "$raw" | awk '{print $2}')

dlq_now=$(aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs get-queue-attributes \
  --queue-url "$DLQ_QUEUE" --attribute-names ApproximateNumberOfMessages \
  --output text --query 'Attributes.ApproximateNumberOfMessages')
DELTA=$((dlq_now - BASELINE))

echo ""
echo "=============================================="
echo " RESULTADO"
echo "=============================================="
echo " raw-events.visible    : $raw_vis  (esperado: 0)"
echo " raw-events.inflight   : $raw_inv  (esperado: 0)"
echo " raw-events-dlq.delta  : $DELTA    (esperado: $TOTAL)"
echo "=============================================="

if [[ "$DELTA" -eq "$TOTAL" && "$raw_vis" -eq 0 && "$raw_inv" -eq 0 ]]; then
  echo " PASS — todos os inválidos chegaram na DLQ sem perda"
else
  echo " ATENÇÃO — números fora do esperado. Aumentar WAIT ou investigar."
fi

echo ""
echo "Verificar tentativas nos logs (esperado ~$((TOTAL * 3)) ocorrências):"
echo "  docker compose logs processor | grep -c 'validation'"
