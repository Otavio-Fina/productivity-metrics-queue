package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	AWSEndpoint             string
	AWSRegion               string
	RawEventsQueueURL       string
	ProcessedEventsQueueURL string
	WorkerCount             int
	ProcessorID             string
}

func Load() (Config, error) {
	cfg := Config{
		AWSEndpoint:             os.Getenv("AWS_ENDPOINT_URL"),
		AWSRegion:               getOrDefault("AWS_REGION", "us-east-1"),
		RawEventsQueueURL:       os.Getenv("RAW_EVENTS_QUEUE_URL"),
		ProcessedEventsQueueURL: os.Getenv("PROCESSED_EVENTS_QUEUE_URL"),
		ProcessorID:             os.Getenv("PROCESSOR_ID"),
	}

	workerCountStr := getOrDefault("WORKER_COUNT", "2")
	n, err := strconv.Atoi(workerCountStr)
	if err != nil || n <= 0 {
		return Config{}, fmt.Errorf("invalid WORKER_COUNT %q: must be a positive integer", workerCountStr)
	}
	cfg.WorkerCount = n

	if cfg.RawEventsQueueURL == "" {
		return Config{}, fmt.Errorf("RAW_EVENTS_QUEUE_URL is required")
	}
	if cfg.ProcessedEventsQueueURL == "" {
		return Config{}, fmt.Errorf("PROCESSED_EVENTS_QUEUE_URL is required")
	}
	if cfg.ProcessorID == "" {
		return Config{}, fmt.Errorf("PROCESSOR_ID is required")
	}

	return cfg, nil
}

func getOrDefault(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
