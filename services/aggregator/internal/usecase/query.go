package usecase

import (
	"context"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
)

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
