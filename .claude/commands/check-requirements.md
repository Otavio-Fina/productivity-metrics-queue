---
description: Check repo progress against the requitements.md evaluation criteria
allowed-tools: Read, Glob, Grep, Bash(go build ./...), Bash(go test ./...)
---

Check how far the implementation has progressed against `requitements.md`.

Steps:

1. Read `requitements.md` — focus on the "Critérios de Avaliação" section
   (Essenciais, Qualidade de Código, Diferenciais).
2. Scan the repo to assess each item. Use Glob/Grep/Read — do not assume.
   - Essenciais: does `docker-compose.yml` define localstack + processor +
     aggregator? Does `infra/localstack/init-aws.sh` create the 4 queues + 2
     tables? Do both services build? Is there a DLQ redrive policy? Do tests pass?
   - Qualidade de Código: are `domain/`, `usecase/`, `infra/` layers present and
     separated? Are there interfaces for the SQS/DynamoDB adapters? Any `panic`
     calls? Worker pool + context + graceful shutdown present?
   - Diferenciais: multi-stage Dockerfile, OpenTelemetry, Makefile, Swagger.
3. Optionally run `go build ./...` and `go test ./...` per service to confirm.

Report each item as DONE / PARTIAL / MISSING with a one-line reason and a
file:line reference where relevant. End with the single most important next step.

This command is read-only — report findings, do not change any code.
