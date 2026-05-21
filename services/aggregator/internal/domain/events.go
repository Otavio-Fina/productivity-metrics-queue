package domain

import "time"

type MetricType string

const (
	MetricPR            MetricType = "pull_requests"
	MetricCommit        MetricType = "commits"
	MetricReviewTimeMin MetricType = "review_time_minutes"
)

type ProcessedEvent struct {
	EventID     string     `json:"event_id"     dynamodbav:"event_id"`
	DeveloperID string     `json:"developer_id" dynamodbav:"developer_id"`
	MetricType  MetricType `json:"metric_type"  dynamodbav:"metric_type"`
	Value       int64      `json:"value"        dynamodbav:"value"`
	Repository  string     `json:"repository"   dynamodbav:"repository"`
	Timestamp   time.Time  `json:"timestamp"    dynamodbav:"timestamp"`
	ProcessedAt time.Time  `json:"processed_at" dynamodbav:"processed_at"`
	ProcessorID string     `json:"processor_id" dynamodbav:"processor_id"`
}

type EventsPage struct {
	Events     []ProcessedEvent
	NextCursor string
}
