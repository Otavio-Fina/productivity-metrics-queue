package domain

import "time"

type SummaryRecord struct {
	DeveloperID            string    `json:"developer_id"            dynamodbav:"developer_id"`
	TotalCommits           int64     `json:"total_commits"           dynamodbav:"total_commits"`
	TotalPullRequests      int64     `json:"total_pull_requests"     dynamodbav:"total_pull_requests"`
	TotalReviewTimeMinutes int64     `json:"total_review_time_minutes" dynamodbav:"total_review_time_minutes"`
	ReviewTimeEventsCount  int64     `json:"review_time_events_count"  dynamodbav:"review_time_events_count"`
	EventsProcessed        int64     `json:"events_processed"        dynamodbav:"events_processed"`
	LastActivity           time.Time `json:"last_activity"           dynamodbav:"last_activity"`
}
