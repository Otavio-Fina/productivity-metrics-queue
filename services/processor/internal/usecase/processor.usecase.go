package usecase

import (
	"context"
	"time"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/domain"
)

type Publisher interface {
	Publish(ctx context.Context, e domain.ProcessedEvent) error
}

type ProcessorUs struct {
	publisher   Publisher
	processorID string
}

func NewProcessorUs(publisher Publisher, processorID string) *ProcessorUs {
	return &ProcessorUs{
		publisher:   publisher,
		processorID: processorID,
	}
}

func (us *ProcessorUs) HandleProcess(ctx context.Context, rawEvent domain.RawEvent) error {
	timeNow := time.Now().UTC()

	if valid, errrs := domain.IsRawEventValid(rawEvent, timeNow); !valid {
		return domain.ValidationFailure(errrs)
	}

	processed := domain.ProcessedEvent{
		RawEvent:    rawEvent,
		ProcessedAt: timeNow,
		ProcessorID: us.processorID,
	}

	return us.publisher.Publish(ctx, processed)
}
