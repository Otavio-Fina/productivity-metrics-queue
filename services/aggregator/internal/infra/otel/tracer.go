package otel

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Init configura o tracer provider global com exporter OTLP gRPC.
//
// Endpoint vem de OTEL_EXPORTER_OTLP_ENDPOINT (ex: "jaeger:4317").
// Service name vem de OTEL_SERVICE_NAME.
//
// Endpoint vazio = OTel desligado (no-op shutdown). Permite rodar testes
// e dev local sem subir Jaeger.
//
// Retorna função de shutdown que deve ser chamada com defer no main —
// garante flush dos spans batched antes do processo morrer.
func Init(ctx context.Context) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	// Resource schemaless: SDK mescla com resource.Environment() (que lê
	// OTEL_SERVICE_NAME e usa o schema interno do SDK). Se o nosso resource
	// tivesse SchemaURL fixo, esse merge falha com "conflicting Schema URL"
	// sempre que SDK e o semconv importado divergem de versão. Schemaless
	// mescla com qualquer schema. Só precisamos de service.name pro Jaeger
	// agrupar os traces.
	res := resource.NewSchemaless(
		semconv.ServiceName(os.Getenv("OTEL_SERVICE_NAME")),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return tp.Shutdown, nil
}
