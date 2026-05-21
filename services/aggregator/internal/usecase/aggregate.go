package usecase

import (
	"context"
	"log/slog"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
)

type Repository interface {
	SaveEventAndUpdateSummary(ctx context.Context, e domain.ProcessedEvent) (newEvent bool, err error)
}

type AggregateUs struct {
	repo Repository
}

func NewAggregateUs(repo Repository) *AggregateUs {
	return &AggregateUs{repo: repo}
}

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
