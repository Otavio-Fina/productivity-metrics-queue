package domain

import "time"

type MetricType string

const (
	MetricPR            MetricType = "pull_requests"
	MetricCommit        MetricType = "commits"
	MetricReviewTimeMin MetricType = "review_time_minutes"
)

// ProcessedEvent espelha o contrato emitido pelo Processor na fila
// processed-events. Cada serviço define o próprio tipo (Clean Architecture
// proíbe imports cruzados de domain) — o que une os dois é o contrato JSON.
type ProcessedEvent struct {
	EventID     string     `json:"event_id"`
	DeveloperID string     `json:"developer_id"`
	MetricType  MetricType `json:"metric_type"`
	Value       int64      `json:"value"`
	Repository  string     `json:"repository"`
	Timestamp   time.Time  `json:"timestamp"`
	ProcessedAt time.Time  `json:"processed_at"`
	ProcessorID string     `json:"processor_id"`
}
