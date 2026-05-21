package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/infra/queue"
)

type Handler interface {
	HandleAggregate(ctx context.Context, e domain.ProcessedEvent) error
}

// Pool orquestra o processamento concorrente: 1 fetcher + N workers
// + canal bufferizado. Drena em voo quando ctx é cancelado.
type Pool struct {
	consumer    *queue.SQSConsumer
	handler     Handler
	workerCount int
}

func NewPool(consumer *queue.SQSConsumer, h Handler, workerCount int) *Pool {
	return &Pool{
		consumer:    consumer,
		handler:     h,
		workerCount: workerCount,
	}
}

func (p *Pool) Run(ctx context.Context) {
	jobs := make(chan queue.Job, p.workerCount)

	var wg sync.WaitGroup
	for i := 0; i < p.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobs {
				p.processOne(ctx, workerID, job)
			}
		}(i)
	}

	p.fetchLoop(ctx, jobs)
	close(jobs)
	wg.Wait()
}

func (p *Pool) fetchLoop(ctx context.Context, jobs chan<- queue.Job) {
	for {
		if ctx.Err() != nil {
			return
		}

		batch, err := p.consumer.ReceiveBatch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			slog.ErrorContext(ctx, "receive batch failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
			continue
		}

		for _, job := range batch {
			select {
			case <-ctx.Done():
				return
			case jobs <- job:
			}
		}
	}
}

// processOne classifica o erro retornado pelo use case em 2 categorias
// (mais simples que o Processor, porque aqui não há validation failures):
//   - nil   → ack (delete)
//   - err   → log + NÃO ack → SQS reentrega → DLQ
func (p *Pool) processOne(ctx context.Context, workerID int, job queue.Job) {
	logger := slog.With(
		"worker_id", workerID,
		"event_id", job.Event.EventID,
		"developer_id", job.Event.DeveloperID,
	)

	err := p.handler.HandleAggregate(ctx, job.Event)
	if err != nil {
		logger.ErrorContext(ctx, "aggregate failed", "error", err)
		return
	}

	if delErr := p.consumer.Delete(ctx, job.ReceiptHandle); delErr != nil {
		logger.ErrorContext(ctx, "failed to ack message", "error", delErr)
	}
}
