#!/usr/bin/env bash
# Seeds raw-events with 15 valid + 1 duplicated (sent 2x) + 4 invalid events.
#
# Expected end state (after ~30s):
#   - DynamoDB events table:    16 rows  (15 unique valid + 1 from the duplicated pair counted once)
#   - DynamoDB developer_summary: 4 rows  (dev-123, dev-456, dev-789, dev-001)
#   - SQS raw-events-dlq:        4 messages (one per validation rule)
#
# Each invalid event targets a DIFFERENT brief rule, so DLQ count tells us
# which rule (if any) is silently passing.

set -euo pipefail

export USERPROFILE="${USERPROFILE:-$HOME}"

ENDPOINT="http://localhost:4566"
RAW_QUEUE="$ENDPOINT/000000000000/raw-events"

send_event() {
  local event_id="$1" developer_id="$2" metric_type="$3" value="$4" repository="$5" timestamp="$6" label="$7"
  local body
  body=$(printf '{"event_id":"%s","developer_id":"%s","metric_type":"%s","value":%s,"repository":"%s","timestamp":"%s"}' \
    "$event_id" "$developer_id" "$metric_type" "$value" "$repository" "$timestamp")
  aws --endpoint-url="$ENDPOINT" --no-cli-pager sqs send-message \
    --queue-url "$RAW_QUEUE" \
    --message-body "$body" \
    --output text --query 'MessageId' > /dev/null
  echo "  sent: $label"
}

echo "==> 15 valid events (4 devs, mix of metric_types)"
send_event "a1b2c3d4-0001-4001-8001-000000000001" "dev-123" "commits"             5  "org/web-app" "2026-05-19T09:00:00Z" "dev-123 commits=5"
send_event "a1b2c3d4-0002-4002-8002-000000000002" "dev-123" "commits"             3  "org/web-app" "2026-05-19T10:00:00Z" "dev-123 commits=3"
send_event "a1b2c3d4-0003-4003-8003-000000000003" "dev-123" "pull_requests"       1  "org/web-app" "2026-05-19T11:00:00Z" "dev-123 PR=1"
send_event "a1b2c3d4-0004-4004-8004-000000000004" "dev-123" "review_time_minutes" 45 "org/web-app" "2026-05-19T12:00:00Z" "dev-123 review=45"
send_event "a1b2c3d4-0005-4005-8005-000000000005" "dev-123" "review_time_minutes" 60 "org/web-app" "2026-05-19T13:00:00Z" "dev-123 review=60"

send_event "a1b2c3d4-0006-4006-8006-000000000006" "dev-456" "commits"             8  "org/api"     "2026-05-19T14:00:00Z" "dev-456 commits=8"
send_event "a1b2c3d4-0007-4007-8007-000000000007" "dev-456" "commits"             2  "org/api"     "2026-05-19T15:00:00Z" "dev-456 commits=2"
send_event "a1b2c3d4-0008-4008-8008-000000000008" "dev-456" "pull_requests"       2  "org/api"     "2026-05-19T16:00:00Z" "dev-456 PR=2"
send_event "a1b2c3d4-0009-4009-8009-000000000009" "dev-456" "review_time_minutes" 30 "org/api"     "2026-05-19T17:00:00Z" "dev-456 review=30"

send_event "a1b2c3d4-000a-400a-800a-00000000000a" "dev-789" "commits"             10 "org/cli"     "2026-05-20T08:00:00Z" "dev-789 commits=10"
send_event "a1b2c3d4-000b-400b-800b-00000000000b" "dev-789" "pull_requests"       3  "org/cli"     "2026-05-20T09:00:00Z" "dev-789 PR=3"
send_event "a1b2c3d4-000c-400c-800c-00000000000c" "dev-789" "pull_requests"       1  "org/cli"     "2026-05-20T10:00:00Z" "dev-789 PR=1"
send_event "a1b2c3d4-000d-400d-800d-00000000000d" "dev-789" "review_time_minutes" 90 "org/cli"     "2026-05-20T11:00:00Z" "dev-789 review=90"

send_event "a1b2c3d4-000e-400e-800e-00000000000e" "dev-001" "commits"             4  "org/docs"    "2026-05-20T12:00:00Z" "dev-001 commits=4"
send_event "a1b2c3d4-000f-400f-800f-00000000000f" "dev-001" "pull_requests"       5  "org/docs"    "2026-05-20T13:00:00Z" "dev-001 PR=5"

echo ""
echo "==> 1 duplicate event_id sent 2x (Aggregator must count it once)"
DUP_ID="a1b2c3d4-dddd-4ddd-8ddd-dddddddddddd"
send_event "$DUP_ID" "dev-456" "commits" 7 "org/api" "2026-05-19T18:00:00Z" "DUP first send"
send_event "$DUP_ID" "dev-456" "commits" 7 "org/api" "2026-05-19T18:00:00Z" "DUP second send (must NOT double-count)"

echo ""
echo "==> 4 invalid events (each tests a distinct validation rule)"
send_event "not-a-uuid"                           "dev-999" "commits"             1    "org/bad" "2026-05-20T08:00:00Z" "INVALID: event_id is not a UUID"
send_event "a1b2c3d4-bad1-4bad-8bad-000000000001" "dev-999" "commits"             -5   "org/bad" "2026-05-20T08:00:00Z" "INVALID: negative value"
send_event "a1b2c3d4-bad2-4bad-8bad-000000000002" "dev-999" "review_time_minutes" 9999 "org/bad" "2026-05-20T08:00:00Z" "INVALID: review_time > 1440"
send_event "a1b2c3d4-bad3-4bad-8bad-000000000003" "dev-999" "commits"             1    "org/bad" "2099-01-01T00:00:00Z" "INVALID: timestamp in the future"

echo ""
echo "==> done. Wait ~30s for invalid events to reach DLQ, then verify:"
echo ""
echo "# 1) events table has 16 rows (15 unique + 1 duplicate counted once)"
echo "aws --endpoint-url=$ENDPOINT dynamodb scan --table-name events --select COUNT"
echo ""
echo "# 2) DLQ has 4 messages (one per validation rule)"
echo "aws --endpoint-url=$ENDPOINT sqs get-queue-attributes \\"
echo "  --queue-url $ENDPOINT/000000000000/raw-events-dlq \\"
echo "  --attribute-names ApproximateNumberOfMessages"
echo ""
echo "# 3) Idempotency check: dev-456 commits=17 (8+2+7), NOT 24"
echo "curl -s http://localhost:8080/metrics/dev-456/summary | jq ."
echo ""
echo "# 4) Avg calculation: dev-123 avg_review_time_minutes=52.5 (= 105/2)"
echo "curl -s http://localhost:8080/metrics/dev-123/summary | jq ."
echo ""
echo "# 5) Health probe"
echo "curl -s http://localhost:8080/health | jq ."
