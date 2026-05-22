# productivity-metrics-queue

Pipeline de métricas de produtividade de desenvolvedores em Go.
Dois serviços + SQS + DynamoDB, tudo em LocalStack, subindo com **um único
`docker-compose up`**.

> Desafio técnico de 5 dias para vaga de Pleno Go (Bestera — time de AI Coding
> Tools). A descrição completa está em [`requitements.md`](./requitements.md).

---

## Sumário

1. [TL;DR](#tldr)
2. [Arquitetura](#arquitetura)
3. [Como rodar](#como-rodar)
4. [API REST](#api-rest)
5. [Observabilidade (OpenTelemetry + Jaeger)](#observabilidade-opentelemetry--jaeger)
6. [Layout do projeto](#layout-do-projeto)
7. [Decisões de design](#decisões-de-design)
8. [Variáveis de ambiente](#variáveis-de-ambiente)
9. [Testes](#testes)
10. [Mapeamento dos Critérios de Avaliação](#mapeamento-dos-critérios-de-avaliação)
11. [Trade-offs e o que faria diferente com mais tempo](#trade-offs-e-o-que-faria-diferente-com-mais-tempo)

---

## TL;DR

```bash
docker-compose up -d                  # sobe LocalStack + Jaeger + processor + aggregator
bash scripts/seed.sh                  # publica 15 válidos + 1 duplicado + 4 inválidos
curl http://localhost:8080/health     # {"status":"healthy"}
curl http://localhost:8080/metrics/dev-123/summary
```

UI do Jaeger (traces ponta-a-ponta entre os dois serviços):
<http://localhost:16686>.

---

## Arquitetura

```
[SQS: raw-events]
        │
        ▼
┌───────────────┐
│   Processor   │   Worker pool. Valida cada evento, enriquece com
│               │   processed_at + processor_id e publica adiante.
└───────────────┘
        │
        ▼
[SQS: processed-events]
        │
        ▼
┌───────────────┐                    ┌──────────────────────┐
│  Aggregator   │  ───────────────►  │  DynamoDB            │
│               │                    │   • events           │
│  Worker pool  │                    │   • developer_summary│
│  + API REST   │                    └──────────────────────┘
└───────────────┘
        │
        ▼  HTTP :8080
   GET /metrics/:developer_id
   GET /metrics/:developer_id/summary
   GET /health
```

**Por que duas filas e dois serviços?** Para separar responsabilidades e
permitir escalar cada lado de forma independente: validar/enriquecer é
CPU-leve, agregar/persistir é I/O-pesado. O contrato entre os dois é só o
schema da mensagem (não há acoplamento de runtime).

**Por que LocalStack?** O brief exige tudo rodando localmente via Compose. SQS
e DynamoDB são serviços AWS de fato; o LocalStack reproduz a API com fidelidade
(inclusive `TransactWriteItems`, redrive policy, GSIs).

---

## Como rodar

**Pré-requisitos:** Docker + Compose (V1 ou V2 — o `Makefile` detecta os dois),
Go 1.21+ apenas se for rodar os testes localmente (não é necessário para
`docker-compose up`).

```bash
# Sobe tudo (LocalStack cria filas e tabelas no startup automaticamente)
docker-compose up -d

# Aguarda o healthcheck do LocalStack passar
docker-compose ps

# Popula a fila raw-events com a massa de teste
bash scripts/seed.sh

# Consulta
curl http://localhost:8080/metrics/dev-123
curl http://localhost:8080/metrics/dev-123/summary

# Derruba mantendo dados; "make clean" remove volumes também
docker-compose down
```

Portas expostas:

| Porta | Serviço |
|------:|---------|
| `8080`  | API REST do Aggregator |
| `4566`  | LocalStack (SQS + DynamoDB) |
| `16686` | Jaeger UI |

Atalhos via Makefile:

```bash
make help     # lista alvos
make up       # docker-compose up -d
make run      # up + segue logs
make logs     # segue logs
make test     # go test ./... nos dois serviços
make build    # go build ./... nos dois serviços
make lint     # go vet ./... nos dois serviços
make clean    # docker-compose down -v (limpa volumes)
```

### Scripts de carga e de borda

Em `scripts/tests/` (todos shell, AWS CLI apontando para LocalStack):

| Script | Para que serve |
|--------|----------------|
| `idempotency.sh` | Envia N cópias do mesmo `event_id` em paralelo — confirma que o `developer_summary` incrementa **uma vez só**. |
| `dlq.sh` | Dispara cada regra de validação isoladamente e checa o DLQ. |
| `burst.sh` | Burst curto de mensagens para observar o worker pool. |
| `load.sh` | Carga sustentada moderada. |
| `soak.sh` | Carga longa (estabilidade). |

---

## API REST

Spec completa em [`docs/openapi.yaml`](./docs/openapi.yaml).
Cole o conteúdo em <https://editor.swagger.io> para visualizar interativamente.

### `GET /health`

Verifica conectividade real com SQS (`GetQueueAttributes`) **e** DynamoDB
(`DescribeTable`).

```bash
curl http://localhost:8080/health
# {"status":"healthy"}
```

### `GET /metrics/:developer_id`

Lista paginada (cursor-based) dos eventos do desenvolvedor.
Parâmetros opcionais: `limit` (default 10), `cursor` (do `next_cursor` da
página anterior).

```bash
curl 'http://localhost:8080/metrics/dev-123?limit=5'
```

```json
{
  "items": [
    {
      "event_id": "550e8400-e29b-41d4-a716-446655440000",
      "developer_id": "dev-123",
      "metric_type": "commits",
      "value": 15,
      "repository": "org/repo-name",
      "timestamp": "2026-04-15T10:30:00Z",
      "processed_at": "2026-04-15T10:30:05Z",
      "processor_id": "processor-1"
    }
  ],
  "next_cursor": "550e8400-e29b-41d4-a716-446655440000"
}
```

### `GET /metrics/:developer_id/summary`

```bash
curl http://localhost:8080/metrics/dev-123/summary
```

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

`avg_review_time_minutes` é calculada na leitura como
`total_review_time_minutes / review_time_events_count` — guardar soma + contagem
preserva precisão a cada novo evento.

---

## Observabilidade (OpenTelemetry + Jaeger)

Os dois serviços instrumentam **automaticamente** todas as chamadas AWS via o
middleware `otelaws`. Cada `ReceiveMessage`, `SendMessage`, `DeleteMessage`,
`TransactWriteItems`, `Query`, `GetItem` vira um span.

O middleware também injeta o cabeçalho `traceparent` no `MessageAttributes` do
SQS no `SendMessage`. Isso significa que **um trace atravessa os dois
serviços** — não é só log com `event_id` no payload: é tracing distribuído de
verdade.

```bash
# após rodar seed.sh:
open http://localhost:16686
# Service: processor → veja o trace seguir para o aggregator no mesmo ID
```

Se `OTEL_EXPORTER_OTLP_ENDPOINT` estiver vazio, o tracer vira no-op (graceful
degrade — útil para rodar os testes sem subir o Jaeger).

---

## Layout do projeto

```
.
├── docker-compose.yml                # LocalStack, Jaeger, Processor, Aggregator
├── Makefile                          # build, test, lint, run, clean
├── docs/
│   └── openapi.yaml                  # spec OpenAPI 3.0.3 da API REST
├── infra/
│   └── localstack/
│       └── init-aws.sh               # cria filas (com DLQ) e tabelas no startup
├── scripts/
│   ├── seed.sh                       # 15 válidos + 1 duplicado + 4 inválidos
│   └── tests/                        # idempotency, dlq, burst, load, soak
└── services/
    ├── processor/                    # serviço 1
    │   ├── cmd/main.go               # wiring + graceful shutdown
    │   ├── Dockerfile                # multi-stage → scratch
    │   └── internal/
    │       ├── domain/               # ★ regras puras (sem AWS/HTTP)
    │       │   ├── events.go         #   RawEvent, ProcessedEvent, IsRawEventValid
    │       │   └── events_test.go    #   table-driven cobrindo TODAS as regras
    │       ├── usecase/              # ★ orquestração; depende só de domain + interfaces
    │       │   └── processor.go      #   HandleProcess: valida → enriquece → publica
    │       └── infra/                # ★ adapters; única camada que conhece AWS SDK
    │           ├── config/           #   carga de env vars
    │           ├── otel/             #   bootstrap OTel + OTLP gRPC
    │           ├── queue/            #   consumer + publisher SQS (backoff aqui)
    │           └── worker/           #   pool: 1 fetcher + N workers
    │
    └── aggregator/                   # serviço 2
        ├── cmd/main.go               # wiring + errgroup (pool ‖ HTTP)
        ├── Dockerfile                # multi-stage → scratch
        └── internal/
            ├── domain/               # ★ ProcessedEvent, SummaryRecord, EventsPage
            ├── usecase/              # ★ aggregate (write path) + query (read path)
            └── infra/
                ├── api/              # Gin handlers + router + server
                ├── config/
                ├── otel/
                ├── queue/            # consumer SQS
                ├── repository/       # ★ DynamoDB (TransactWriteItems = idempotência)
                │   └── dynamodb_test.go
                └── worker/
```

★ = camadas Clean Architecture, dependências apontando só pra dentro
(`infra → usecase → domain`).

---

## Decisões de design

### Clean Architecture com fronteiras estritas

- `domain/` — entidades, regras de negócio. **Zero imports de AWS, HTTP ou
  qualquer infra.** Compila isolado.
- `usecase/` — orquestração. Só conhece `domain` e interfaces de saída
  (`Publisher`, `Repository`). Nada de SQS / DynamoDB / Gin.
- `infra/` — adapters concretos. Única camada que importa AWS SDK / Gin.

Plug acontece em `cmd/main.go` — é o único lugar onde "tudo se conhece".

### Worker pool simples e enxuto

[`services/processor/internal/infra/worker/pool.go`](services/processor/internal/infra/worker/pool.go),
[`services/aggregator/internal/infra/worker/pool.go`](services/aggregator/internal/infra/worker/pool.go)

- 1 goroutine "fetcher" puxa batches do SQS e empurra no canal.
- N goroutines "worker" consomem do canal e executam o use case.
- `WORKER_COUNT` configurável por env var (default 2).
- Shutdown: `ctx.Done()` para o fetcher → `close(jobs)` → workers drenam
  o que está no canal → `wg.Wait()` retorna. **Nenhum job é perdido.**

No Aggregator, pool + HTTP server rodam em paralelo com `errgroup`: ambos
compartilham `gctx`, então um SIGTERM derruba os dois em sequência limpa.

### Retry com backoff exponencial — só no publisher

[`services/processor/internal/infra/queue/publisher.go`](services/processor/internal/infra/queue/publisher.go)

- Apenas o publisher tem retry (3 tentativas, 200ms → 400ms → 800ms).
- Validação **não retenta** — erro permanente, não transiente.
- Cancelamento de `ctx` aborta o retry imediatamente (graceful shutdown).
- O retryer do AWS SDK é **desligado** (`aws.NopRetryer{}`) — ter dois
  mecanismos sobrepostos confunde métricas e estoura latência.

### Idempotência atômica no Aggregator

[`services/aggregator/internal/infra/repository/dynamodb.go`](services/aggregator/internal/infra/repository/dynamodb.go)

Uma única `TransactWriteItems`:

1. **Put** em `events` com `ConditionExpression "attribute_not_exists(event_id)"`.
2. **Update** em `developer_summary` (`ADD` atômico nos contadores).

Se o mesmo `event_id` chegar duas vezes, a transação inteira é cancelada com
`ConditionalCheckFailed`. O handler trata isso como sucesso silencioso
(log de warn + ack). **Resultado: o summary só incrementa uma vez por evento,
mesmo sob duplicatas concorrentes.**

Isso vale at-least-once delivery do SQS sem precisar de cache externo nem de
locks.

### DLQ por convenção, não por código

- Mensagem com erro de validação → worker **não chama `DeleteMessage`**.
- SQS reentrega; após `maxReceiveCount=3` (configurado em `init-aws.sh`),
  cai em `raw-events-dlq` automaticamente.
- Erro transitório segue o mesmo caminho — só que o Aggregator é idempotente,
  então o dado não duplica.

### Validação fail-slow no domínio

[`services/processor/internal/domain/events.go`](services/processor/internal/domain/events.go)

`IsRawEventValid` acumula **todos** os erros em uma passada e retorna a
lista. O worker loga o conjunto inteiro — quem produz o evento errado vê
todos os problemas de uma vez, em vez de pingar de retorno em retorno.

### `sum + count` em vez de `avg` na summary

Calcular a média a cada novo evento perderia precisão (média de médias é
viciada quando os pesos diferem). Guardamos `total_review_time_minutes` e
`review_time_events_count`; a média é divisão simples no handler HTTP.

### GSI + paginação por cursor

A tabela `events` tem um GSI `developer_id-index` (configurado em
`init-aws.sh`). `GET /metrics/:developer_id` faz um `Query` nesse GSI.
Paginação usa `event_id` como cursor — o handler devolve `next_cursor` quando
há mais dados; cliente passa de volta em `cursor=…`.

### Logs estruturados + correlação por `event_id`

Tudo em JSON via `slog`. Em cada worker, `slog.With(..., "event_id", id,
"developer_id", id, "worker_id", N)` decora todos os logs subsequentes
daquele job. Combinado com o tracing distribuído (acima), dá pra correlacionar
um evento da entrada até o registro no DynamoDB.

### Graceful shutdown ponta-a-ponta

`signal.NotifyContext(SIGINT|SIGTERM)` → `ctx` viaja por todo o stack:

1. Fetcher do pool sai do loop.
2. Canal de jobs fecha; workers drenam o que está em voo.
3. (Aggregator) HTTP server faz `Shutdown` com timeout.
4. `defer shutdownOtel(5s)` faz flush dos spans batched antes do processo
   morrer — sem isso perderíamos os últimos N spans.

---

## Variáveis de ambiente

Tudo configurável; nada hardcoded. Defaults definidos em
[`docker-compose.yml`](./docker-compose.yml).

### Processor

| Variável | Default no compose | Para que serve |
|----------|---------------------|----------------|
| `AWS_ENDPOINT_URL` | `http://localstack:4566` | LocalStack; vazio = AWS real. |
| `AWS_REGION` | `us-east-1` | Região AWS. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `test` / `test` | LocalStack ignora; SDK exige presença. |
| `RAW_EVENTS_QUEUE_URL` | `…/raw-events` | Fila de entrada. |
| `PROCESSED_EVENTS_QUEUE_URL` | `…/processed-events` | Fila de saída. |
| `PROCESSOR_ID` | `processor-1` | Vai no `processor_id` do evento enriquecido. |
| `WORKER_COUNT` | `5` | Tamanho do pool. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `jaeger:4317` | OTLP gRPC; vazio = tracer no-op. |
| `OTEL_SERVICE_NAME` | `processor` | Aparece como nome do serviço no Jaeger. |

### Aggregator

| Variável | Default no compose | Para que serve |
|----------|---------------------|----------------|
| `AWS_ENDPOINT_URL` | `http://localstack:4566` | Idem. |
| `AWS_REGION` | `us-east-1` | Idem. |
| `PROCESSED_EVENTS_QUEUE_URL` | `…/processed-events` | Fila de entrada. |
| `EVENTS_TABLE_NAME` | `events` | DynamoDB. |
| `SUMMARY_TABLE_NAME` | `developer_summary` | DynamoDB. |
| `EVENTS_GSI_NAME` | `developer_id-index` | GSI para o `GET /metrics/:dev`. |
| `WORKER_COUNT` | `5` | Tamanho do pool. |
| `HTTP_PORT` | `8080` | Porta da API REST. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `jaeger:4317` | Idem processor. |
| `OTEL_SERVICE_NAME` | `aggregator` | Idem processor. |

---

## Testes

```bash
make test
# ou, por serviço:
cd services/processor  && go test ./...
cd services/aggregator && go test ./...
```

| Arquivo | O que cobre |
|---------|-------------|
| [`services/processor/internal/domain/events_test.go`](services/processor/internal/domain/events_test.go) | **Todas** as regras de `IsRawEventValid`: happy path por `metric_type`, falha por campo, fronteiras inclusivas (`value=0`, `review_time=1440`, `timestamp=now`) e **fail-slow** (5+ erros acumulados em uma passada). |
| [`services/aggregator/internal/infra/repository/dynamodb_test.go`](services/aggregator/internal/infra/repository/dynamodb_test.go) | Builder da `UpdateExpression` por `metric_type` — única lógica não-trivial do repositório. Inclui caso de tipo não suportado. |

---

## Mapeamento dos Critérios de Avaliação

Tabelas abaixo mapeiam cada item do `requitements.md` ao local onde está
implementado. Pensadas para servir como guia de revisão (humana **ou** por
IA).

### Essenciais (vai/não vai)

| Critério | Implementação |
|----------|---------------|
| `docker-compose up` funciona sem intervenção manual | [`docker-compose.yml`](./docker-compose.yml) + healthcheck no LocalStack + [`infra/localstack/init-aws.sh`](./infra/localstack/init-aws.sh) cria filas e tabelas no startup. |
| Dois containers de serviço rodando | `processor` e `aggregator` em [`docker-compose.yml`](./docker-compose.yml). |
| Comunicação exclusivamente via SQS | Não há HTTP/RPC entre os serviços. Único elo: a fila `processed-events`. |
| Processor valida e publica na segunda fila | [`services/processor/internal/usecase/processor.go`](services/processor/internal/usecase/processor.go) + [`…/infra/queue/publisher.go`](services/processor/internal/infra/queue/publisher.go). |
| Aggregator consome, persiste no DynamoDB e expõe API | [`services/aggregator/cmd/main.go`](services/aggregator/cmd/main.go) compõe `worker.Pool` + `api.Server` em `errgroup`. |
| DLQ funcionando para mensagens com falha | Worker não acka erro de validação → SQS redrive após `maxReceiveCount=3`. Verifique com `bash scripts/tests/dlq.sh`. |
| Código compila e testes passam | `make build && make test`. |

### Qualidade de Código (peso alto)

| Critério | Exemplo concreto |
|----------|------------------|
| **Clean Architecture** | `internal/{domain,usecase,infra}` em cada serviço; dependências apontam só para dentro. `usecase` define interfaces (`Publisher`, `Repository`, `Queries`) — `infra` implementa. |
| **Go idiomático** | Erros tipados (`domain.ValidationFailure`), `errors.As/Is`, interfaces pequenas no ponto de uso, `context.Context` em toda chamada I/O. |
| **Concorrência** | Worker pool com canal bufferizado + `sync.WaitGroup` em [`services/processor/internal/infra/worker/pool.go`](services/processor/internal/infra/worker/pool.go); `errgroup` paralelizando pool + HTTP server em [`services/aggregator/cmd/main.go`](services/aggregator/cmd/main.go). |
| **Tratamento de erros** | Validação devolve **lista** de erros (fail-slow); publisher diferencia transiente × cancelamento × permanente; repositório distingue `ConditionalCheckFailed` de erro real; sem `panic` em nenhum caminho. |
| **Testabilidade** | Interfaces nos pontos de junção (`Publisher`, `Repository`, `DynamoAPI`, `SQSSendMessageAPI`, `Queries`) — qualquer adapter é mockável; já existem testes table-driven no domínio e no repositório. |
| **Comunicação entre serviços** | Schema da mensagem é o único contrato. Definido em [`services/processor/internal/domain/events.go`](services/processor/internal/domain/events.go) (`ProcessedEvent`) e replicado em [`services/aggregator/internal/domain/events.go`](services/aggregator/internal/domain/events.go). |

### Diferenciais (4/4)

| Diferencial | Onde está |
|-------------|-----------|
| **Dockerfile multi-stage → scratch** | [`services/processor/Dockerfile`](services/processor/Dockerfile) e [`services/aggregator/Dockerfile`](services/aggregator/Dockerfile) (`FROM scratch`, binário estaticamente linkado com `-ldflags="-s -w"`). |
| **Tracing distribuído com OpenTelemetry** | Auto-instrumentação via `otelaws` middleware ([processor main.go:74](services/processor/cmd/main.go), [aggregator main.go:73](services/aggregator/cmd/main.go)). `traceparent` viaja em `MessageAttributes` do SQS — trace atravessa os dois serviços. Visualizável no Jaeger UI (porta 16686). |
| **Makefile com comandos úteis** | [`Makefile`](./Makefile): `build`, `test`, `lint`, `up`, `down`, `run`, `logs`, `clean`. Detecta Compose V1 vs V2 e funciona em Windows + macOS + Linux. |
| **Documentação OpenAPI / Swagger** | [`docs/openapi.yaml`](./docs/openapi.yaml) — OpenAPI 3.0.3 completa (3 paths, 6 schemas, exemplos). Visualizável em <https://editor.swagger.io>. |

---

## Trade-offs e o que faria diferente com mais tempo

Item explícito do brief (a "autocrítica" do vídeo). Lista honesta do que ficou
de fora ou poderia ser melhor:

- **Sem testes de integração.** O domínio é coberto por table-driven; o builder
  de `UpdateExpression` tem teste unitário. Mas não há teste ponta-a-ponta com
  LocalStack subindo (testcontainers seria o caminho natural).
- **Worker count fixo.** `WORKER_COUNT` é env var, mas não há auto-scaling
  baseado em backlog (`ApproximateNumberOfMessages`). Em prod eu adicionaria.
- **DLQ sem dashboard nem alerta.** Hoje só dá pra inspecionar via
  `aws sqs receive-message`. Métrica Prometheus em cima de
  `ApproximateNumberOfMessages` da DLQ resolveria.
- **Sem métricas Prometheus.** Só temos traces. Para SLOs reais (taxa de erro,
  latência P95) faltariam counters/histograms.
- **API sem auth e sem rate limiting.** É local. Em prod entraria atrás de um
  gateway com JWT + throttling por developer_id.
- **`uuid.Parse` aceita qualquer versão.** O brief pede UUID v4; a validação
  atual aceita v1/v5 também (qualquer UUID parseável). Acrescentar
  `id.Version() == 4` seria 2 linhas.
- **Schema de mensagem duplicado.** `ProcessedEvent` está definido em ambos os
  serviços (módulos separados). Está consistente, mas um `pkg/contracts/`
  compartilhado seria mais defensivo contra drift. A duplicação é deliberada
  aqui — o brief sugere módulos `go.mod` independentes por serviço — mas
  com mais tempo eu extrairia um schema versionado (JSON Schema ou
  protobuf).
- **Idempotência sem TTL.** A condição `attribute_not_exists` cresce com a
  tabela `events`. Em escala real, adicionaria TTL na própria tabela para
  expirar registros antigos (mantendo só o necessário para deduplicação na
  janela útil).
