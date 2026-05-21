package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
)

// fakeRepo permite controlar newEvent/err e inspecionar quantas vezes
// e com qual evento o use case chamou o repositório.
type fakeRepo struct {
	newEvent bool
	err      error
	calls    int
	gotEvent domain.ProcessedEvent
}

func (f *fakeRepo) SaveEventAndUpdateSummary(_ context.Context, e domain.ProcessedEvent) (bool, error) {
	f.calls++
	f.gotEvent = e
	return f.newEvent, f.err
}

func sampleEvent() domain.ProcessedEvent {
	return domain.ProcessedEvent{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  domain.MetricCommit,
		Value:       3,
		Timestamp:   time.Date(2026, 4, 15, 10, 30, 0, 0, time.UTC),
		ProcessedAt: time.Date(2026, 4, 15, 10, 30, 5, 0, time.UTC),
		ProcessorID: "processor-1",
	}
}

// Happy path: evento inédito chega, repo retorna newEvent=true, use case
// devolve nil — worker dá ack e a fila avança.
func TestHandleAggregate_NewEvent_ReturnsNil(t *testing.T) {
	repo := &fakeRepo{newEvent: true}
	us := NewAggregateUs(repo)

	if err := us.HandleAggregate(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("esperava nil, got %v", err)
	}
	if repo.calls != 1 {
		t.Errorf("esperava 1 chamada ao repo, got %d", repo.calls)
	}
}

// Idempotência: repo detecta duplicata (ConditionalCheckFailed → newEvent=false).
// O use case TEM que retornar nil para o worker dar ack e não criar loop
// infinito de reentrega. Esse é o teste que defende o requisito funcional
// "se o mesmo event_id chegar duas vezes, não duplicar".
func TestHandleAggregate_Duplicate_ReturnsNilWithoutError(t *testing.T) {
	repo := &fakeRepo{newEvent: false}
	us := NewAggregateUs(repo)

	if err := us.HandleAggregate(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("duplicata deveria retornar nil, got %v", err)
	}
}

// Erro transiente do DynamoDB sobe pro worker — sem ack → SQS reentrega →
// DLQ após maxReceiveCount.
func TestHandleAggregate_RepoError_Propagates(t *testing.T) {
	wantErr := errors.New("dynamodb throttled")
	repo := &fakeRepo{err: wantErr}
	us := NewAggregateUs(repo)

	err := us.HandleAggregate(context.Background(), sampleEvent())
	if !errors.Is(err, wantErr) {
		t.Fatalf("esperava propagação do erro do repo, got %v", err)
	}
}
