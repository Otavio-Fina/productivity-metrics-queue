package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/infra/config"
	otelinit "github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/infra/otel"
	"github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/infra/queue"
	"github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/infra/worker"
	"github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/usecase"
)

func main() {
	// Requisito não-funcional do brief:
	// "logs estruturados (JSON), correlacionados por event_id".
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	appCfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}

	// ctx cancela em SIGINT/SIGTERM → gatilho do graceful shutdown.
	// Pool.Run vê ctx.Done(), para o fetchLoop, fecha o canal de jobs,
	// drena workers em voo e retorna. O `defer stop()` desfaz o handler
	// no fim, boa prática mesmo quando o processo está saindo.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// OTel tracer global. Se OTEL_EXPORTER_OTLP_ENDPOINT estiver vazio,
	// vira no-op (graceful degrade para dev/tests sem Jaeger).
	// Shutdown com timeout próprio faz flush dos spans batched antes do
	// processo morrer, sem isso perderíamos os últimos N spans.
	shutdownOtel, err := otelinit.Init(ctx)
	if err != nil {
		slog.Error("otel init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		sCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownOtel(sCtx)
	}()

	// AWS config. Desligamos o retryer do SDK (NopRetryer = max 1 tentativa,
	// sem delay) porque o nosso publisher já tem backoff próprio. Ter dois
	// mecanismos de retry sobrepostos confunde métricas e estoura latência.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(appCfg.AWSRegion),
		awsconfig.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }),
	)
	if err != nil {
		slog.Error("aws config load failed", "error", err)
		os.Exit(1)
	}

	// otelaws instrumenta TODAS as chamadas AWS subsequentes: cada
	// SendMessage / ReceiveMessage / DeleteMessage vira span automático.
	// Bônus: o middleware já injeta `traceparent` no MessageAttributes
	// do SendMessage — preparação pra propagação cross-service via SQS.
	otelaws.AppendMiddlewares(&awsCfg.APIOptions)

	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if appCfg.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(appCfg.AWSEndpoint)
		}
	})

	consumer := queue.NewSQSConsumer(sqsClient, appCfg.RawEventsQueueURL)
	publisher := queue.NewSQSPublisher(sqsClient, appCfg.ProcessedEventsQueueURL)
	us := usecase.NewProcessorUs(publisher, appCfg.ProcessorID)
	pool := worker.NewPool(consumer, us, appCfg.WorkerCount)

	slog.Info("processor: starting",
		"processor_id", appCfg.ProcessorID,
		"worker_count", appCfg.WorkerCount,
		"raw_queue", appCfg.RawEventsQueueURL,
		"processed_queue", appCfg.ProcessedEventsQueueURL,
		"aws_endpoint", appCfg.AWSEndpoint,
	)

	// Bloqueia aqui. O pool roda os workers e o fetcher em goroutines, e responde ao ctx para graceful shutdown.
	pool.Run(ctx)

	slog.Info("processor: stopped")
}
