package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
)

func init() {
	// Silencia o banner DEBUG do Gin durante go test.
	gin.SetMode(gin.TestMode)
}

// fakeQueries satisfaz a interface Queries do pacote api com valores
// fixos por teste — sem mocks gerados, sem libs externas.
type fakeQueries struct {
	page    domain.EventsPage
	summary domain.SummaryRecord
	found   bool
	err     error
}

func (f *fakeQueries) GetEvents(_ context.Context, _ string, _ int, _ string) (domain.EventsPage, error) {
	return f.page, f.err
}

func (f *fakeQueries) GetSummary(_ context.Context, _ string) (domain.SummaryRecord, bool, error) {
	return f.summary, f.found, f.err
}

func newTestRouter(q *fakeQueries, h HealthProbe) *gin.Engine {
	r := gin.New()
	hs := NewHandlers(q, h)
	r.GET("/metrics/:developer_id", hs.GetEvents)
	r.GET("/metrics/:developer_id/summary", hs.GetSummary)
	r.GET("/health", hs.GetHealth)
	return r
}

// Avg é derivada na leitura: total_review_time_minutes / review_time_events_count.
// 180/4 = 45 — qualquer mudança nessa fórmula tem que passar por aqui.
func TestGetSummary_ComputesAverageFromTotals(t *testing.T) {
	q := &fakeQueries{
		summary: domain.SummaryRecord{
			DeveloperID:            "dev-1",
			TotalReviewTimeMinutes: 180,
			ReviewTimeEventsCount:  4,
			TotalCommits:           7,
			EventsProcessed:        11,
			LastActivity:           time.Date(2026, 4, 15, 10, 30, 0, 0, time.UTC),
		},
		found: true,
	}
	r := newTestRouter(q, func(context.Context) error { return nil })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics/dev-1/summary", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp summaryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.AvgReviewTimeMinutes != 45 {
		t.Errorf("avg: got %v, want 45", resp.AvgReviewTimeMinutes)
	}
	if resp.TotalCommits != 7 {
		t.Errorf("total_commits: got %d, want 7", resp.TotalCommits)
	}
}

// Guarda contra divisão por zero: dev sem eventos de review_time tem que
// retornar avg = 0 (não NaN, não +Inf, não panic).
func TestGetSummary_NoReviewTimeEvents_AvgIsZero(t *testing.T) {
	q := &fakeQueries{
		summary: domain.SummaryRecord{
			DeveloperID:           "dev-1",
			ReviewTimeEventsCount: 0,
			TotalCommits:          3,
			EventsProcessed:       3,
		},
		found: true,
	}
	r := newTestRouter(q, func(context.Context) error { return nil })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics/dev-1/summary", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", w.Code)
	}
	var resp summaryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.AvgReviewTimeMinutes != 0 {
		t.Errorf("avg deveria ser 0 quando não há eventos de review, got %v", resp.AvgReviewTimeMinutes)
	}
}

// found=false → 404 (não 200 com payload vazio). Cliente HTTP precisa
// distinguir "dev existe mas sem atividade" de "dev nunca apareceu".
func TestGetSummary_NotFound_Returns404(t *testing.T) {
	q := &fakeQueries{found: false}
	r := newTestRouter(q, func(context.Context) error { return nil })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics/missing/summary", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", w.Code)
	}
}

func TestGetHealth_Ok(t *testing.T) {
	r := newTestRouter(&fakeQueries{}, func(context.Context) error { return nil })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", w.Code)
	}
}

// Health=503 quando uma dependência (SQS ou DynamoDB) falha — é o sinal
// que orquestradores (k8s/ECS) usam pra parar de mandar tráfego pra ti.
func TestGetHealth_DependencyDown_Returns503(t *testing.T) {
	probe := func(context.Context) error { return errors.New("dynamodb unreachable") }
	r := newTestRouter(&fakeQueries{}, probe)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", w.Code)
	}
}
