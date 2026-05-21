package queue

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/processor/internal/domain"
)

type SQSReceiveDeleteAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

type Job struct {
	Event         domain.RawEvent
	ReceiptHandle string
}

type SQSConsumer struct {
	client   SQSReceiveDeleteAPI
	queueURL string
}

func NewSQSConsumer(client SQSReceiveDeleteAPI, queueURL string) *SQSConsumer {
	return &SQSConsumer{client: client, queueURL: queueURL}
}

func (c *SQSConsumer) ReceiveBatch(ctx context.Context) ([]Job, error) {
	resSQS, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     20,
	})
	if err != nil {
		return nil, err
	}

	jobs := make([]Job, 0, len(resSQS.Messages))
	for _, msg := range resSQS.Messages {
		var event domain.RawEvent
		if err := json.Unmarshal([]byte(*msg.Body), &event); err != nil {
			slog.WarnContext(ctx, "failed to decode raw event",
				"error", err,
				"message_id", *msg.MessageId,
				"body", *msg.Body)
			continue
		}
		jobs = append(jobs, Job{
			Event:         event,
			ReceiptHandle: *msg.ReceiptHandle,
		})
	}
	return jobs, nil
}

func (c *SQSConsumer) Delete(ctx context.Context, receiptHandle string) error {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}
