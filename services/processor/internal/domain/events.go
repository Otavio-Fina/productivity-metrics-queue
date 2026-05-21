package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// structs de entrada

type MetricType string

const (
	MetricPR            MetricType = "pull_requests"
	MetricCommit        MetricType = "commits"
	MetricReviewTimeMin MetricType = "review_time_minutes"
)

type RawEvent struct {
	EventID     string     `json:"event_id"`
	DeveloperID string     `json:"developer_id"`
	MetricType  MetricType `json:"metric_type"`
	Value       int64      `json:"value"`
	Repository  string     `json:"repository"`
	Timestamp   time.Time  `json:"timestamp"`
}

// structs de saida

type ProcessedEvent struct {
	RawEvent
	ProcessedAt time.Time `json:"processed_at"`
	ProcessorID string    `json:"processor_id"`
}

// struct gerais

type ErrorValidation struct {
	Message   string    `json:"message"`
	Field     string    `json:"field"`
	Timestamp time.Time `json:"timestamp"`
}

type ValidationFailure []ErrorValidation

func (v ValidationFailure) Error() string {
	return fmt.Sprintf("validation failed: %d error(s)", len(v))
}

const reviewTimeMaxMinutes = 1440

func IsRawEventValid(event RawEvent, now time.Time) (bool, []ErrorValidation) {
	var errors []ErrorValidation

	// event_id validation
	if strings.TrimSpace(event.EventID) == "" {
		errors = append(errors, ErrorValidation{
			Message: "event_id is required",
			Field:   "event_id",
		})
	}

	if id, _ := uuid.Parse(event.EventID); id == uuid.Nil {
		errors = append(errors, ErrorValidation{
			Message: "event_id must be a valid UUID v4",
			Field:   "event_id",
		})
	}

	// developer_id validation
	if strings.TrimSpace(event.DeveloperID) == "" {
		errors = append(errors, ErrorValidation{
			Message: "developer_id is required",
			Field:   "developer_id",
		})
	}

	// metric_type validation
	if event.MetricType != MetricPR && event.MetricType != MetricCommit && event.MetricType != MetricReviewTimeMin {
		errors = append(errors, ErrorValidation{
			Message: "metric_type must be one of: pull_requests, commits, review_time_minutes",
			Field:   "metric_type",
		})
	}

	// value validation
	if event.Value < 0 {
		errors = append(errors, ErrorValidation{
			Message: "value must be non-negative",
			Field:   "value",
		})
	}

	if event.MetricType == MetricReviewTimeMin && event.Value > reviewTimeMaxMinutes {
		errors = append(errors, ErrorValidation{
			Message: fmt.Sprintf("value for review_time_minutes must be between 0 and %d", reviewTimeMaxMinutes),
			Field:   "value",
		})
	}

	// timestamp validation
	if event.Timestamp.IsZero() {
		errors = append(errors, ErrorValidation{
			Message: "timestamp is required",
			Field:   "timestamp",
		})
	}

	if event.Timestamp.After(now) {
		errors = append(errors, ErrorValidation{
			Message: "timestamp cannot be in the future",
			Field:   "timestamp",
		})
	}

	return len(errors) == 0, errors
}
