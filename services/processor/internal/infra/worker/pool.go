package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/domain"
	"github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/infra/queue"
	"github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/usecase"
)

// Pool orquestra o processamento concorrente do Processor: compõe consumer
// SQS, use case e N workers em paralelo. Roda até o ctx ser cancelado;
// nesse momento drena o trabalho em voo e retorna limpo.
type Pool struct {
	consumer    *queue.SQSConsumer
	usecase     *usecase.ProcessorUs
	workerCount int
}

func NewPool(consumer *queue.SQSConsumer, us *usecase.ProcessorUs, workerCount int) *Pool {
	return &Pool{
		consumer:    consumer,
		usecase:     us,
		workerCount: workerCount,
	}
}

// Run sobe N workers + 1 fetcher e bloqueia até ctx ser cancelado.
// Quando cancela, fecha o canal de jobs e espera os workers drenarem
// antes de retornar.
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

// fetchLoop puxa batches do SQS e empurra jobs no canal. Retorna quando
// ctx é cancelado. Erros de SQS-level (rede, throttling) são logados e
// retentados após um sleep curto — não derrubam o serviço.
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

// processOne é o trabalho de um worker para uma mensagem: chama o use case,
// classifica o erro retornado, e decide acknowledgar (Delete) ou deixar
// o SQS reentregar.
func (p *Pool) processOne(ctx context.Context, workerID int, job queue.Job) {
	logger := slog.With(
		"worker_id", workerID,
		"event_id", job.Event.EventID,
		"developer_id", job.Event.DeveloperID,
	)

	err := p.usecase.HandleProcess(ctx, job.Event)

	if err == nil {
		if delErr := p.consumer.Delete(ctx, job.ReceiptHandle); delErr != nil {
			// Ack falhou: a mensagem vai redelivery. Aggregator é idempotente,
			// então duplicata não conta duas vezes — at-least-once é OK.
			logger.ErrorContext(ctx, "failed to ack message", "error", delErr)
		}
		return
	}

	var vf domain.ValidationFailure
	if errors.As(err, &vf) {
		logger.WarnContext(ctx, "validation failed",
			"errors", []domain.ErrorValidation(vf))
		// Permanente: não deleta. SQS redrive → DLQ após maxReceiveCount.
		return
	}

	logger.ErrorContext(ctx, "transient processing error", "error", err)
	// Transitório: não deleta. SQS reentrega após visibility timeout.
}
