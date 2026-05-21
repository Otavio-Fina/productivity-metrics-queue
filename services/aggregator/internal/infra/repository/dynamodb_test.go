package repository

import (
	"strconv"
	"testing"
	"time"

	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
)

func TestBuildSummaryUpdate(t *testing.T) {
	ts := time.Date(2026, 4, 15, 10, 30, 0, 0, time.UTC)

	cases := []struct {
		name           string
		metricType     domain.MetricType
		value          int64
		wantExpression string
	}{
		{
			name:           "commits",
			metricType:     domain.MetricCommit,
			value:          15,
			wantExpression: "ADD total_commits :v, events_processed :one SET last_activity = :ts",
		},
		{
			name:           "pull_requests",
			metricType:     domain.MetricPR,
			value:          3,
			wantExpression: "ADD total_pull_requests :v, events_processed :one SET last_activity = :ts",
		},
		{
			name:           "review_time_minutes",
			metricType:     domain.MetricReviewTimeMin,
			value:          45,
			wantExpression: "ADD total_review_time_minutes :v, review_time_events_count :one, events_processed :one SET last_activity = :ts",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event := domain.ProcessedEvent{
				DeveloperID: "dev-1",
				MetricType:  tc.metricType,
				Value:       tc.value,
				Timestamp:   ts,
			}
			expr, values, err := buildSummaryUpdate(event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if expr != tc.wantExpression {
				t.Errorf("expression mismatch:\n got  %q\n want %q", expr, tc.wantExpression)
			}

			vAttr, ok := values[":v"].(*dtypes.AttributeValueMemberN)
			if !ok {
				t.Fatalf(":v not a Number attribute")
			}
			if vAttr.Value != strconv.FormatInt(tc.value, 10) {
				t.Errorf(":v value mismatch: got %q want %q", vAttr.Value, strconv.FormatInt(tc.value, 10))
			}

			oneAttr, ok := values[":one"].(*dtypes.AttributeValueMemberN)
			if !ok || oneAttr.Value != "1" {
				t.Errorf(":one expected to be Number 1")
			}

			tsAttr, ok := values[":ts"].(*dtypes.AttributeValueMemberS)
			if !ok {
				t.Fatalf(":ts not a String attribute")
			}
			if tsAttr.Value != "2026-04-15T10:30:00Z" {
				t.Errorf(":ts value mismatch: got %q", tsAttr.Value)
			}
		})
	}
}

func TestBuildSummaryUpdate_UnsupportedMetricType(t *testing.T) {
	event := domain.ProcessedEvent{
		MetricType: "unknown",
		Timestamp:  time.Now(),
	}
	_, _, err := buildSummaryUpdate(event)
	if err == nil {
		t.Fatal("expected error for unsupported metric type, got nil")
	}
}
