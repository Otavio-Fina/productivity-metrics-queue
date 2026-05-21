package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	AWSEndpoint             string
	AWSRegion               string
	ProcessedEventsQueueURL string
	EventsTableName         string
	SummaryTableName        string
	EventsGSIName           string
	WorkerCount             int
	HTTPPort                string
}

func Load() (Config, error) {
	cfg := Config{
		AWSEndpoint:             os.Getenv("AWS_ENDPOINT_URL"),
		AWSRegion:               getOrDefault("AWS_REGION", "us-east-1"),
		ProcessedEventsQueueURL: os.Getenv("PROCESSED_EVENTS_QUEUE_URL"),
		EventsTableName:         os.Getenv("EVENTS_TABLE_NAME"),
		SummaryTableName:        os.Getenv("SUMMARY_TABLE_NAME"),
		EventsGSIName:           getOrDefault("EVENTS_GSI_NAME", "developer_id-index"),
		HTTPPort:                getOrDefault("HTTP_PORT", "8080"),
	}

	workerCountStr := getOrDefault("WORKER_COUNT", "2")
	n, err := strconv.Atoi(workerCountStr)
	if err != nil || n <= 0 {
		return Config{}, fmt.Errorf("invalid WORKER_COUNT %q: must be a positive integer", workerCountStr)
	}
	cfg.WorkerCount = n

	if cfg.ProcessedEventsQueueURL == "" {
		return Config{}, fmt.Errorf("PROCESSED_EVENTS_QUEUE_URL is required")
	}
	if cfg.EventsTableName == "" {
		return Config{}, fmt.Errorf("EVENTS_TABLE_NAME is required")
	}
	if cfg.SummaryTableName == "" {
		return Config{}, fmt.Errorf("SUMMARY_TABLE_NAME is required")
	}

	return cfg, nil
}

func getOrDefault(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
