package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/domain"
)

// fPublisher implementa Publisher in-memory pra inspecionar o que o
// use case publicou. err != nil simula falha transiente (rede/SQS).
type fPublisher struct {
	published []domain.ProcessedEvent
	err       error
}

func (f *fPublisher) Publish(_ context.Context, e domain.ProcessedEvent) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, e)
	return nil
}

func validRawEvent() domain.RawEvent {
	return domain.RawEvent{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  domain.MetricCommit,
		Value:       5,
		Repository:  "org/repo",
		Timestamp:   time.Now().UTC().Add(-1 * time.Hour),
	}
}

// Happy path: evento válido vira ProcessedEvent enriquecido e é publicado
// exatamente uma vez.
func TestHandleProcess_ValidEvent_PublishesEnriched(t *testing.T) {
	pub := &fPublisher{}
	us := NewProcessorUs(pub, "processor-1")

	before := time.Now().UTC()
	err := us.HandleProcess(context.Background(), validRawEvent())
	after := time.Now().UTC()
	if err != nil {
		t.Fatalf("esperava sucesso, got %v", err)
	}

	if len(pub.published) != 1 {
		t.Fatalf("esperava 1 evento publicado, got %d", len(pub.published))
	}
	got := pub.published[0]
	if got.ProcessorID != "processor-1" {
		t.Errorf("processor_id: got %q want %q", got.ProcessorID, "processor-1")
	}
	// ProcessedAt tem que estar dentro da janela de execução do teste —
	// é assim que se testa time.Now() sem injetar um Clock.
	if got.ProcessedAt.Before(before) || got.ProcessedAt.After(after) {
		t.Errorf("processed_at fora da janela do teste: %v (esperado entre %v e %v)",
			got.ProcessedAt, before, after)
	}
}

// Evento inválido NÃO pode ser publicado e o erro tem que ser do tipo
// ValidationFailure. O worker pool decide ack/não-ack inspecionando esse
// tipo via errors.As — se quebrar, mensagem ruim volta no loop.
func TestHandleProcess_InvalidEvent_DoesNotPublishAndReturnsValidationFailure(t *testing.T) {
	pub := &fPublisher{}
	us := NewProcessorUs(pub, "processor-1")

	raw := validRawEvent()
	raw.EventID = "" // qualquer falha de validação serve

	err := us.HandleProcess(context.Background(), raw)
	if err == nil {
		t.Fatal("esperava erro de validação, got nil")
	}
	var vf domain.ValidationFailure
	if !errors.As(err, &vf) {
		t.Fatalf("esperava ValidationFailure, got %T: %v", err, err)
	}
	if len(pub.published) != 0 {
		t.Fatalf("não esperava publicação em evento inválido, got %d", len(pub.published))
	}
}

// Erro do publisher (transiente) tem que subir pro chamador. O worker
// trata como retry (não-ack) → SQS reentrega após visibility timeout.
func TestHandleProcess_PublisherError_Propagates(t *testing.T) {
	wantErr := errors.New("sqs unavailable")
	pub := &fPublisher{err: wantErr}
	us := NewProcessorUs(pub, "processor-1")

	err := us.HandleProcess(context.Background(), validRawEvent())
	if !errors.Is(err, wantErr) {
		t.Fatalf("esperava erro do publisher, got %v", err)
	}
}
