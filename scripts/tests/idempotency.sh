#!/usr/bin/env bash
# ============================================================================
# IDEMPOTENCY STRESS — mesmo event_id em concorrência
# ----------------------------------------------------------------------------
# Propósito:
#   Provar que o Aggregator é idempotente sob race condition. Envia o MESMO
#   event_id N vezes em paralelo (background &) e valida que:
#     1) a tabela `events` tem exatamente 1 linha pra esse event_id
#     2) o summary do developer tem events_processed incrementado em 1 (não N)
#
# Como interpretar:
#   - Count = 1 e summary correto -> idempotência OK.
#   - Count = N ou summary somando N -> race condition. Refletir sobre:
#       * Put na tabela `events` deve usar ConditionExpression
#         "attribute_not_exists(event_id)" — falha com ConditionalCheckFailed
#         pra duplicatas, e essa falha indica "já processei, pular summary".
#       * OU usar TransactWriteItems pra atomic Put(events) + Update(summary).
#       * NÃO basta "checar antes de inserir" (GetItem + Put) — janela de race.
#
# Frase pra defesa em vídeo:
#   "Disparei N writes concorrentes do mesmo event_id. ConditionExpression
#    no Put garante que só uma transação ganha; as demais retornam
#    ConditionalCheckFailed, que o handler trata como duplicata silenciosa
#    e NÃO atualiza o summary. Resultado: events=1, events_processed=1."
#
# Uso:
#   N=50 bash scripts/tests/idempotency.sh
# ============================================================================
set -euo pipefail
export USERPROFILE="${USERPROFILE:-$HOME}"

N="${N:-50}"
ENDPOINT="http://localhost:4566"
RAW_QUEUE="$ENDPOINT/000000000000/raw-events"

# event_id fixo e único pra este teste — gerado uma vez por execução
# pra não colidir com runs anteriores
DUP_ID=$(uuidgen | tr 'A-Z' 'a-z')
TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
BODY=$(printf '{"event_id":"%s","developer_id":"dev-idem","metric_type":"commits","value":7,"repository":"org/idem","timestamp":"%s"}' "$DUP_ID" "$TS")

echo "==> Idempotency test: enviando event_id=$DUP_ID  N=$N vezes em paralelo"

for i in $(seq 1 "$N"); do
  aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs send-message \
    --queue-url "$RAW_QUEUE" --message-body "$BODY" \
    --output text --query 'MessageId' > /dev/null &
done
wait

echo "==> $N sends concluídos. Aguardando pipeline drenar..."
while true; do
  attrs=$(aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs get-queue-attributes \
    --queue-url "$RAW_QUEUE" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --output text --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]')
  vis=$(echo "$attrs" | awk '{print $1}')
  inv=$(echo "$attrs" | awk '{print $2}')
  total=$((vis + inv))
  echo "  raw-events: visible=$vis in-flight=$inv"
  [[ "$total" -eq 0 ]] && break
  sleep 2
done
sleep 3

echo ""
echo "==> Verificação 1: linhas em `events` para event_id=$DUP_ID (esperado: 1)"
ROWS=$(aws --endpoint-url="$ENDPOINT" --no-cli-pager dynamodb get-item \
  --table-name events \
  --key "{\"event_id\":{\"S\":\"$DUP_ID\"}}" \
  --output json 2>/dev/null | jq -r 'if .Item then 1 else 0 end')
echo "    encontrado: $ROWS"

echo ""
echo "==> Verificação 2: summary de dev-idem (esperado: events_processed=1, total_commits=7)"
curl -s http://localhost:8080/metrics/dev-idem/summary | jq .

echo ""
echo "=============================================="
echo " RESULTADO"
echo "=============================================="
if [[ "$ROWS" -eq 1 ]]; then
  echo " events_row_count : $ROWS  (PASS)"
else
  echo " events_row_count : $ROWS  (FAIL — esperado 1)"
fi
echo " summary acima ^ deve mostrar events_processed=1 e total_commits=7"
echo "=============================================="
