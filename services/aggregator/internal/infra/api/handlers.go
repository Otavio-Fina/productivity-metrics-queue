package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
)

// Queries é o que o handler precisa do mundo do use case. Interface fina
// pra manter o handler desacoplado do struct concreto QueryUs.
type Queries interface {
	GetEvents(ctx context.Context, developerID string) ([]domain.ProcessedEvent, error)
	GetSummary(ctx context.Context, developerID string) (domain.SummaryRecord, bool, error)
}

// HealthProbe valida saúde das dependências externas (SQS + DynamoDB).
// Composta em main.go com closures sobre os clientes concretos.
type HealthProbe func(ctx context.Context) error

type Handlers struct {
	queries Queries
	health  HealthProbe
}

func NewHandlers(queries Queries, health HealthProbe) *Handlers {
	return &Handlers{queries: queries, health: health}
}

// summaryResponse é o shape EXATO especificado no brief — campos JSON
// inclusive a ordem. Não reutilizamos domain.SummaryRecord porque ele tem
// total_review_time_minutes + review_time_events_count (formato persistido,
// não API).
type summaryResponse struct {
	DeveloperID          string    `json:"developer_id"`
	TotalCommits         int64     `json:"total_commits"`
	TotalPullRequests    int64     `json:"total_pull_requests"`
	AvgReviewTimeMinutes float64   `json:"avg_review_time_minutes"`
	EventsProcessed      int64     `json:"events_processed"`
	LastActivity         time.Time `json:"last_activity"`
}

func (h *Handlers) GetEvents(c *gin.Context) {
	developerID := c.Param("developer_id")
	if developerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "developer_id is required"})
		return
	}

	events, err := h.queries.GetEvents(c.Request.Context(), developerID)
	if err != nil {
		slog.ErrorContext(c.Request.Context(), "get events failed",
			"error", err, "developer_id", developerID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, events)
}

func (h *Handlers) GetSummary(c *gin.Context) {
	developerID := c.Param("developer_id")
	if developerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "developer_id is required"})
		return
	}

	rec, found, err := h.queries.GetSummary(c.Request.Context(), developerID)
	if err != nil {
		slog.ErrorContext(c.Request.Context(), "get summary failed",
			"error", err, "developer_id", developerID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "developer not found"})
		return
	}

	resp := summaryResponse{
		DeveloperID:       rec.DeveloperID,
		TotalCommits:      rec.TotalCommits,
		TotalPullRequests: rec.TotalPullRequests,
		EventsProcessed:   rec.EventsProcessed,
		LastActivity:      rec.LastActivity,
	}
	if rec.ReviewTimeEventsCount > 0 {
		resp.AvgReviewTimeMinutes = float64(rec.TotalReviewTimeMinutes) / float64(rec.ReviewTimeEventsCount)
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handlers) GetHealth(c *gin.Context) {
	if err := h.health(c.Request.Context()); err != nil {
		slog.WarnContext(c.Request.Context(), "health check failed", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "healthy"})
}
