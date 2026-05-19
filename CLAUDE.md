# productivity-metrics-queue

Developer metrics pipeline — a 5-day Go interview challenge. Two services move
developer productivity events through SQS, aggregate them into DynamoDB, and expose
a REST API. The full brief is in `requitements.md` — it is the source of truth.

## Working with this repo (collaboration rules — always apply)

The repo owner is doing this challenge to learn and must defend every decision on
video. These rules override default assistant behavior:

1. **Guide, don't edit.** Do NOT edit project/challenge files. Tell the owner which
   file to open and exactly what content to put, with a detailed explanation of
   *why*. The owner applies every change by hand and reviews it. Exceptions: files
   the owner explicitly asks Claude to write, and `.claude/` / memory config.

2. **Follow `requitements.md` to the letter.** Content the brief spells out —
   commands, JSON schemas, names, structure — is reproduced verbatim. Deviate only
   where the brief marks something as optional or "sugerida". Where the brief is
   silent, it is a free engineering choice — say so explicitly.

3. **No over-engineering.** Build only what `requitements.md` asks — no extra
   abstractions, helpers, or speculative features.

4. **Teach while guiding.** Every instruction includes the reasoning, trade-offs,
   and gotchas, so the owner can explain it on video.

## System

```
[SQS raw-events] -> Processor -> [SQS processed-events] -> Aggregator -> DynamoDB
                                                                             |
                                                                          REST API
```

Two Go services (1.21+), LocalStack (SQS + DynamoDB), everything up with
`docker-compose up`. Clean Architecture expected.

## Processor (Serviço 1)

- Consumes `raw-events`; concurrent worker pool, worker count from an env var.
- Validates each event:
  - `event_id` — required, valid UUID v4
  - `developer_id` — required, non-empty
  - `metric_type` — one of `commits`, `pull_requests`, `review_time_minutes`
  - `value` — `>= 0`; for `review_time_minutes` the max is `1440`
  - `timestamp` — required, must not be in the future
- Valid event -> enrich with `processed_at` + `processor_id` -> publish to
  `processed-events`.
- Invalid event -> not published -> reaches `raw-events-dlq` after 3 receive
  attempts. Never delete an invalid message; let SQS redrive it.
- Retry with exponential backoff on SQS/transport errors only — not on validation
  failures (those are permanent).

## Aggregator (Serviço 2)

- Consumes `processed-events`.
- Idempotent: the same `event_id` arriving twice must not double-count.
- Persists to DynamoDB:
  - `events` — one row per event, PK `event_id`
  - `developer_summary` — one row per developer, PK `developer_id`, updated
    incrementally on each event.
- REST API:
  - `GET /metrics/:developer_id` — all events for a developer
  - `GET /metrics/:developer_id/summary` — aggregated summary
  - `GET /health` — verifies SQS + DynamoDB connectivity

## Event schemas

Raw event — `raw-events` (input to Processor):

```json
{
  "event_id": "uuid-v4",
  "developer_id": "dev-123",
  "metric_type": "commits | pull_requests | review_time_minutes",
  "value": 15,
  "repository": "org/repo-name",
  "timestamp": "2026-04-15T10:30:00Z"
}
```

Processed event — `processed-events` (Processor output / Aggregator input):

```json
{
  "event_id": "uuid-v4",
  "developer_id": "dev-123",
  "metric_type": "commits",
  "value": 15,
  "repository": "org/repo-name",
  "timestamp": "2026-04-15T10:30:00Z",
  "processed_at": "2026-04-15T10:30:05Z",
  "processor_id": "processor-instance-1"
}
```

Summary response — `GET /metrics/:developer_id/summary`:

```json
{
  "developer_id": "dev-123",
  "total_commits": 142,
  "total_pull_requests": 38,
  "avg_review_time_minutes": 45.2,
  "events_processed": 195,
  "last_activity": "2026-04-15T10:30:00Z"
}
```

## Infrastructure

- LocalStack provides SQS + DynamoDB; queues and tables are created automatically on
  startup (`infra/localstack/init-aws.sh`).
- Queues: `raw-events`, `raw-events-dlq`, `processed-events`, `processed-events-dlq`.
  DLQ redrive `maxReceiveCount` = 3.
- DynamoDB tables: `events` (PK `event_id`), `developer_summary` (PK `developer_id`).

## Non-functional requirements

- `docker-compose up` brings everything up with no manual steps.
- All config via env vars — endpoints, queue names, table names, worker count,
  processor id. Nothing hardcoded.
- Structured JSON logs, correlated by `event_id`.
- Graceful shutdown — drain in-flight workers, close connections cleanly.
- Unit tests for validation and processing/aggregation logic.
- Clear README: how to run, test, and use the API.

## Clean Architecture rule

- `domain/` — stdlib only, no external imports (entities, value objects, rules).
- `usecase/` — imports `domain/` and interfaces only; never imports `infra/`.
- `infra/` — implements those interfaces; AWS SDK and HTTP libraries live here only.

## Definition of done

The "Critérios de Avaliação > Essenciais" checklist in `requitements.md`, plus: code
compiles and `go test ./...` passes in both services.

## Differentials (optional, only if time allows)

Multi-stage Dockerfile to scratch/distroless, OpenTelemetry tracing across both
services, Makefile (build/test/lint/run), Swagger/OpenAPI docs.

## Conventions

- Two independent Go modules — one `go.mod` per service, as the suggested layout
  shows.
- When scope is ambiguous, make a decision and document it in the README — the brief
  explicitly evaluates how ambiguity is handled.
