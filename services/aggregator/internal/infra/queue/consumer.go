package queue

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
)

type SQSReceiveDeleteAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// Job é uma mensagem da fila processed-events já decodificada e pronta
// pro use case. Carrega o ReceiptHandle pra que o worker consiga deletar
// (ack) depois do processamento bem-sucedido.
type Job struct {
	Event         domain.ProcessedEvent
	ReceiptHandle string
}

// SQSConsumer encapsula ReceiveMessage + DeleteMessage. Faz long-poll na
// fila e devolve eventos decodificados; mensagens com JSON malformado são
// puladas (sem delete → SQS redrive pra DLQ depois de 3 receives).
type SQSConsumer struct {
	client   SQSReceiveDeleteAPI
	queueURL string
}

func NewSQSConsumer(client SQSReceiveDeleteAPI, queueURL string) *SQSConsumer {
	return &SQSConsumer{client: client, queueURL: queueURL}
}

func (c *SQSConsumer) ReceiveBatch(ctx context.Context) ([]Job, error) {
	res, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     20,
	})
	if err != nil {
		return nil, err
	}

	jobs := make([]Job, 0, len(res.Messages))
	for _, msg := range res.Messages {
		var event domain.ProcessedEvent
		if err := json.Unmarshal([]byte(*msg.Body), &event); err != nil {
			slog.WarnContext(ctx, "failed to decode processed event",
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

// Delete acknowledga uma mensagem — chamado pelo worker depois do use case
// retornar nil. Em qualquer outro caminho (erro transiente), NÃO chamar:
// o SQS reentrega depois do visibility timeout (e eventualmente DLQ).
func (c *SQSConsumer) Delete(ctx context.Context, receiptHandle string) error {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}
