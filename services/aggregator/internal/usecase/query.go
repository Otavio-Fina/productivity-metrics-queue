package usecase

import (
	"context"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
)

// QueryRepository é a interface de leitura. Mesma camada de adapter da
// Repository do AggregateUs — na prática, o mesmo struct implementa as duas.
// Interfaces separadas seguem Interface Segregation: o handler de query não
// precisa conhecer o método de escrita.
type QueryRepository interface {
	GetEventsByDeveloper(ctx context.Context, developerID string, limit int, cursor string) (domain.EventsPage, error)
	GetSummary(ctx context.Context, developerID string) (record domain.SummaryRecord, found bool, err error)
}

type QueryUs struct {
	repo QueryRepository
}

func NewQueryUs(repo QueryRepository) *QueryUs {
	return &QueryUs{repo: repo}
}

// GetEvents lê uma página de eventos do dev. Limit é clampado:
// valor inválido (<=0) cai pro default 20; valor exagerado (>100) é
// limitado a 100 pra proteger DynamoDB de Query custosa.
func (q *QueryUs) GetEvents(ctx context.Context, developerID string, limit int, cursor string) (domain.EventsPage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return q.repo.GetEventsByDeveloper(ctx, developerID, limit, cursor)
}

func (q *QueryUs) GetSummary(ctx context.Context, developerID string) (domain.SummaryRecord, bool, error) {
	return q.repo.GetSummary(ctx, developerID)
}
