package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
)

type DynamoAPI interface {
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
}

type DynamoRepo struct {
	client         DynamoAPI
	eventsTable    string
	summaryTable   string
	developerIDGSI string
}

func NewDynamoRepo(client DynamoAPI, eventsTable, summaryTable, developerIDGSI string) *DynamoRepo {
	return &DynamoRepo{
		client:         client,
		eventsTable:    eventsTable,
		summaryTable:   summaryTable,
		developerIDGSI: developerIDGSI,
	}
}

// SaveEventAndUpdateSummary grava o evento na tabela events e atualiza o
// agregado em developer_summary em UMA única transação atômica.
// Idempotência vem da ConditionExpression no Put: se event_id já existe,
// a transação inteira é cancelada com ConditionalCheckFailed —
// interpretamos como duplicata (retorna newEvent=false, err=nil).
func (r *DynamoRepo) SaveEventAndUpdateSummary(ctx context.Context, e domain.ProcessedEvent) (bool, error) {
	eventItem, err := attributevalue.MarshalMap(e)
	if err != nil {
		return false, fmt.Errorf("marshal event item: %w", err)
	}

	updateExpr, exprValues, err := buildSummaryUpdate(e)
	if err != nil {
		return false, err
	}

	_, err = r.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []dtypes.TransactWriteItem{
			{
				Put: &dtypes.Put{
					TableName:           aws.String(r.eventsTable),
					Item:                eventItem,
					ConditionExpression: aws.String("attribute_not_exists(event_id)"),
				},
			},
			{
				Update: &dtypes.Update{
					TableName: aws.String(r.summaryTable),
					Key: map[string]dtypes.AttributeValue{
						"developer_id": &dtypes.AttributeValueMemberS{Value: e.DeveloperID},
					},
					UpdateExpression:          aws.String(updateExpr),
					ExpressionAttributeValues: exprValues,
				},
			},
		},
	})
	if err == nil {
		return true, nil
	}

	if isConditionalCheckFailed(err) {
		return false, nil
	}
	return false, fmt.Errorf("transact write: %w", err)
}

// buildSummaryUpdate constrói a UpdateExpression conforme o metric_type.
// Sempre incrementa events_processed e atualiza last_activity (last-write-wins).
// O switch é a única lógica não-trivial no repository — vale ter teste unitário
// pra cada caso.
func buildSummaryUpdate(e domain.ProcessedEvent) (string, map[string]dtypes.AttributeValue, error) {
	ts, err := e.Timestamp.MarshalText()
	if err != nil {
		return "", nil, fmt.Errorf("marshal timestamp: %w", err)
	}
	values := map[string]dtypes.AttributeValue{
		":v":   &dtypes.AttributeValueMemberN{Value: strconv.FormatInt(e.Value, 10)},
		":one": &dtypes.AttributeValueMemberN{Value: "1"},
		":ts":  &dtypes.AttributeValueMemberS{Value: string(ts)},
	}

	//atomico
	switch e.MetricType {
	case domain.MetricCommit:
		return "ADD total_commits :v, events_processed :one SET last_activity = :ts", values, nil
	case domain.MetricPR:
		return "ADD total_pull_requests :v, events_processed :one SET last_activity = :ts", values, nil
	case domain.MetricReviewTimeMin:
		return "ADD total_review_time_minutes :v, review_time_events_count :one, events_processed :one SET last_activity = :ts", values, nil
	default:
		return "", nil, fmt.Errorf("unsupported metric_type %q", e.MetricType)
	}
}

// isConditionalCheckFailed inspeciona o erro da transação pra detectar a
// causa "condição não satisfeita" — que pra nós significa duplicata. O SDK
// pode reportar de duas formas: como TransactionCanceledException (esperado)
// ou via smithy.APIError genérico (fallback defensivo).
func isConditionalCheckFailed(err error) bool {
	var canceledErr *dtypes.TransactionCanceledException
	if errors.As(err, &canceledErr) {
		for _, reason := range canceledErr.CancellationReasons {
			if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
				return true
			}
		}
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "ConditionalCheckFailedException" {
		return true
	}
	return false
}

// GetEventsByDeveloper faz Query no GSI por developer_id. Retorna slice
// vazio (não erro) quando o dev não tem eventos.
func (r *DynamoRepo) GetEventsByDeveloper(ctx context.Context, developerID string) ([]domain.ProcessedEvent, error) {
	//TODO: paginar se tiver muitos eventos — por ora assumimos poucos por dev, mas pode ser necessário no futuro.
	out, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.eventsTable),
		IndexName:              aws.String(r.developerIDGSI),
		KeyConditionExpression: aws.String("developer_id = :d"),
		ExpressionAttributeValues: map[string]dtypes.AttributeValue{
			":d": &dtypes.AttributeValueMemberS{Value: developerID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("query events by developer: %w", err)
	}

	events := make([]domain.ProcessedEvent, 0, len(out.Items))
	for _, item := range out.Items {
		var e domain.ProcessedEvent
		if err := attributevalue.UnmarshalMap(item, &e); err != nil {
			return nil, fmt.Errorf("unmarshal event: %w", err)
		}
		events = append(events, e)
	}
	return events, nil
}

// GetSummary retorna o agregado do dev. found=false quando não há registro.
func (r *DynamoRepo) GetSummary(ctx context.Context, developerID string) (domain.SummaryRecord, bool, error) {
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.summaryTable),
		Key: map[string]dtypes.AttributeValue{
			"developer_id": &dtypes.AttributeValueMemberS{Value: developerID},
		},
	})
	if err != nil {
		return domain.SummaryRecord{}, false, fmt.Errorf("get summary: %w", err)
	}
	if len(out.Item) == 0 {
		return domain.SummaryRecord{}, false, nil
	}
	var rec domain.SummaryRecord
	if err := attributevalue.UnmarshalMap(out.Item, &rec); err != nil {
		return domain.SummaryRecord{}, false, fmt.Errorf("unmarshal summary: %w", err)
	}
	return rec, true, nil
}

// HealthCheck é uma probe barata: DescribeTable na tabela events.
// Confirma conectividade com DynamoDB sem custo de IO real.
func (r *DynamoRepo) HealthCheck(ctx context.Context) error {
	_, err := r.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(r.eventsTable),
	})
	return err
}
