package usecase

import (
	"context"
	"log/slog"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
)

// Repository é a interface que o caso de uso espera da infra (DynamoDB).
// Manter como interface aqui permite mockar em testes e proteger o use case
// de qualquer detalhe de AWS SDK.
type Repository interface {
	// SaveEventAndUpdateSummary grava o evento e atualiza o agregado do dev
	// numa única operação atômica. newEvent=false significa que o event_id
	// já existia → idempotência segurada, não é erro.
	SaveEventAndUpdateSummary(ctx context.Context, e domain.ProcessedEvent) (newEvent bool, err error)
}

type AggregateUs struct {
	repo Repository
}

func NewAggregateUs(repo Repository) *AggregateUs {
	return &AggregateUs{repo: repo}
}

// HandleAggregate é o ponto de entrada do use case "agregar evento".
// Retorna nil pra:
//   - sucesso (novo evento agregado)
//   - duplicata (já agregamos esse event_id; loga e ignora)
// Retorna error pra falhas transientes (DynamoDB throttle, rede). O worker
// NÃO faz ack quando há erro → SQS reentrega → DLQ após maxReceiveCount.
func (a *AggregateUs) HandleAggregate(ctx context.Context, e domain.ProcessedEvent) error {
	newEvent, err := a.repo.SaveEventAndUpdateSummary(ctx, e)
	if err != nil {
		return err
	}
	if !newEvent {
		slog.WarnContext(ctx, "duplicate event ignored",
			"event_id", e.EventID,
			"developer_id", e.DeveloperID,
		)
	}
	return nil
}
