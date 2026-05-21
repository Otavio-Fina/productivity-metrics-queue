package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/domain"
)

type SQSSendMessageAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type SQSPublisher struct {
	client         SQSSendMessageAPI
	targetQueueURL string
	maxAttempts    int
	initialBackoff time.Duration
}

func NewSQSPublisher(client SQSSendMessageAPI, targetQueueURL string) *SQSPublisher {
	return &SQSPublisher{
		client:         client,
		targetQueueURL: targetQueueURL,
		maxAttempts:    3,
		initialBackoff: 200 * time.Millisecond,
	}
}

// dobra o backoff a cada falha
// (100ms → 200ms → 400ms → 800ms). Aborta imediatamente se ctx for
// cancelado (graceful shutdown).
func (p *SQSPublisher) Publish(ctx context.Context, e domain.ProcessedEvent) error {
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal processed event: %w", err)
	}

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(p.targetQueueURL),
		MessageBody: aws.String(string(body)),
	}

	backoff := p.initialBackoff

	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		_, err = p.client.SendMessage(ctx, input)
		if err == nil {
			return nil
		}

		// Cancelamento não é retryable, um abort do context
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		if attempt == p.maxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}

	return fmt.Errorf("publish to sqs after %d attempts: %w", p.maxAttempts, err)
}
