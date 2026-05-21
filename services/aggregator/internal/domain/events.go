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
//
// As tags dynamodbav são obrigatórias: sem elas, attributevalue.MarshalMap
// grava os atributos com o NOME DO CAMPO GO (EventID, DeveloperID...), e o
// Put falha porque a PK da tabela é "event_id" e não "EventID".
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

// EventsPage é o retorno paginado de uma listagem de eventos por developer.
// NextCursor vazio significa que não há mais páginas.
// O cursor é o event_id do último item retornado — o cliente devolve esse
// valor em ?cursor=... pra pedir a próxima página.
type EventsPage struct {
	Events     []ProcessedEvent
	NextCursor string
}
