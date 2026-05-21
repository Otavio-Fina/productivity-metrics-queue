package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"golang.org/x/sync/errgroup"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/infra/api"
	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/infra/config"
	otelinit "github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/infra/otel"
	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/infra/queue"
	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/infra/repository"
	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/infra/worker"
	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/usecase"
)

func main() {
	// slog default = JSON em stdout. Atende ao requisito não-funcional:
	// "logs estruturados (JSON), correlacionados por event_id".
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	appCfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}

	// ctx cancela em SIGINT/SIGTERM → gatilho do graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// OTel tracer global. Endpoint vazio = no-op (dev/tests sem Jaeger).
	// Shutdown com timeout próprio garante flush dos spans batched antes
	// do processo morrer — sem isso perderíamos os últimos N spans.
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

	// NopRetryer desliga o retry do SDK. Aqui o motivo é diferente do
	// Processor (que tinha publisher com backoff): no Aggregator queremos
	// que erros transientes virem rápido em "não-ack" → SQS reentrega.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(appCfg.AWSRegion),
		awsconfig.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }),
	)
	if err != nil {
		slog.Error("aws config load failed", "error", err)
		os.Exit(1)
	}

	// otelaws instrumenta TODAS as chamadas AWS subsequentes: cada
	// ReceiveMessage / DeleteMessage (SQS) e GetItem / TransactWriteItems
	// / Query (DynamoDB) vira span automático.
	otelaws.AppendMiddlewares(&awsCfg.APIOptions)

	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if appCfg.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(appCfg.AWSEndpoint)
		}
	})
	dynamoClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if appCfg.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(appCfg.AWSEndpoint)
		}
	})

	repo := repository.NewDynamoRepo(
		dynamoClient,
		appCfg.EventsTableName,
		appCfg.SummaryTableName,
		appCfg.EventsGSIName,
	)
	aggregateUs := usecase.NewAggregateUs(repo)
	queryUs := usecase.NewQueryUs(repo)

	consumer := queue.NewSQSConsumer(sqsClient, appCfg.ProcessedEventsQueueURL)
	pool := worker.NewPool(consumer, aggregateUs, appCfg.WorkerCount)

	healthProbe := buildHealthProbe(sqsClient, appCfg.ProcessedEventsQueueURL, repo)
	handlers := api.NewHandlers(queryUs, healthProbe)
	router := api.NewRouter(handlers)
	server := api.NewServer(":"+appCfg.HTTPPort, router)

	slog.Info("aggregator: starting",
		"worker_count", appCfg.WorkerCount,
		"http_port", appCfg.HTTPPort,
		"processed_queue", appCfg.ProcessedEventsQueueURL,
		"events_table", appCfg.EventsTableName,
		"summary_table", appCfg.SummaryTableName,
		"events_gsi", appCfg.EventsGSIName,
		"aws_endpoint", appCfg.AWSEndpoint,
	)

	// Dois runners em paralelo: worker pool e HTTP server. Cancelamento
	// de ctx (SIGTERM) propaga pelo gctx do errgroup. Wait bloqueia até
	// ambos terminarem o drain.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		pool.Run(gctx)
		return nil
	})
	g.Go(func() error {
		return server.Run(gctx)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("aggregator: stopped with error", "error", err)
		os.Exit(1)
	}
	slog.Info("aggregator: stopped")
}

func buildHealthProbe(
	sqsClient *sqs.Client,
	queueURL string,
	repo *repository.DynamoRepo,
) api.HealthProbe {
	return func(ctx context.Context) error {
		if _, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(queueURL),
		}); err != nil {
			return err
		}
		return repo.HealthCheck(ctx)
	}
}
