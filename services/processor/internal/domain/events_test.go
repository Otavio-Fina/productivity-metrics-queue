package domain

import (
	"testing"
	"time"
)

// validRawEvent retorna um evento que passa em todas as regras.
// Os testes mutam um campo de cada vez pra isolar a falha sob teste.
func validRawEvent(now time.Time) RawEvent {
	return RawEvent{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  MetricCommit,
		Value:       3,
		Repository:  "org/repo",
		Timestamp:   now.Add(-1 * time.Hour),
	}
}

func TestIsRawEventValid_HappyPath_PorMetricType(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)

	for _, mt := range []MetricType{MetricCommit, MetricPR, MetricReviewTimeMin} {
		t.Run(string(mt), func(t *testing.T) {
			ev := validRawEvent(now)
			ev.MetricType = mt

			valid, errs := IsRawEventValid(ev, now)
			if !valid {
				t.Fatalf("esperava válido, got errors: %+v", errs)
			}
			if len(errs) != 0 {
				t.Fatalf("esperava nenhum erro, got %d: %+v", len(errs), errs)
			}
		})
	}
}

func TestIsRawEventValid_Falhas(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	mutate := func(f func(*RawEvent)) RawEvent {
		ev := validRawEvent(now)
		f(&ev)
		return ev
	}

	cases := []struct {
		name      string
		event     RawEvent
		wantField string
	}{
		{
			name:      "event_id ausente",
			event:     mutate(func(e *RawEvent) { e.EventID = "" }),
			wantField: "event_id",
		},
		{
			name:      "event_id com espaços apenas",
			event:     mutate(func(e *RawEvent) { e.EventID = "   " }),
			wantField: "event_id",
		},
		{
			name:      "event_id não é UUID",
			event:     mutate(func(e *RawEvent) { e.EventID = "not-a-uuid" }),
			wantField: "event_id",
		},
		{
			name:      "developer_id vazio",
			event:     mutate(func(e *RawEvent) { e.DeveloperID = "" }),
			wantField: "developer_id",
		},
		{
			name:      "developer_id com espaços apenas",
			event:     mutate(func(e *RawEvent) { e.DeveloperID = "  " }),
			wantField: "developer_id",
		},
		{
			name:      "metric_type vazio",
			event:     mutate(func(e *RawEvent) { e.MetricType = "" }),
			wantField: "metric_type",
		},
		{
			name:      "metric_type desconhecido",
			event:     mutate(func(e *RawEvent) { e.MetricType = "deploys" }),
			wantField: "metric_type",
		},
		{
			name:      "value negativo em commits",
			event:     mutate(func(e *RawEvent) { e.Value = -1 }),
			wantField: "value",
		},
		{
			name: "review_time_minutes acima de 1440",
			event: mutate(func(e *RawEvent) {
				e.MetricType = MetricReviewTimeMin
				e.Value = 1441
			}),
			wantField: "value",
		},
		{
			name:      "timestamp zero",
			event:     mutate(func(e *RawEvent) { e.Timestamp = time.Time{} }),
			wantField: "timestamp",
		},
		{
			name:      "timestamp no futuro",
			event:     mutate(func(e *RawEvent) { e.Timestamp = now.Add(1 * time.Hour) }),
			wantField: "timestamp",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			valid, errs := IsRawEventValid(tc.event, now)
			if valid {
				t.Fatalf("esperava inválido, mas passou. errors=%+v", errs)
			}

			found := false
			for _, e := range errs {
				if e.Field == tc.wantField {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("nenhum erro no campo %q. errors=%+v", tc.wantField, errs)
			}
		})
	}
}

// Fronteiras inclusivas: valores no limite devem PASSAR.
// Documenta o contrato (>= 0, <= 1440, timestamp == now ok).
func TestIsRawEventValid_FronteirasPassam(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		event RawEvent
	}{
		{
			name: "value zero é válido",
			event: func() RawEvent {
				e := validRawEvent(now)
				e.Value = 0
				return e
			}(),
		},
		{
			name: "review_time_minutes no limite 1440",
			event: func() RawEvent {
				e := validRawEvent(now)
				e.MetricType = MetricReviewTimeMin
				e.Value = 1440
				return e
			}(),
		},
		{
			name: "timestamp == now não conta como futuro",
			event: func() RawEvent {
				e := validRawEvent(now)
				e.Timestamp = now
				return e
			}(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			valid, errs := IsRawEventValid(tc.event, now)
			if !valid {
				t.Fatalf("esperava válido, got errors: %+v", errs)
			}
		})
	}
}

// Fail-slow: o domínio acumula TODOS os erros em uma passada em vez de
// retornar no primeiro. Isso é importante porque o log do worker captura o
// quadro completo para o autor do evento — e a decisão do Processor é
// binária (não-ack), independente de quantos campos quebraram.
func TestIsRawEventValid_FailSlow_AcumulaMultiplosErros(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	ev := RawEvent{
		EventID:     "",
		DeveloperID: "",
		MetricType:  "",
		Value:       -5,
		Timestamp:   time.Time{},
	}

	valid, errs := IsRawEventValid(ev, now)
	if valid {
		t.Fatalf("esperava inválido")
	}

	// Esperamos erros em todos os 5 campos quebrados. event_id vazio dispara
	// 2 erros (required + UUID inválido), então o total esperado é >= 6.
	wantFields := map[string]bool{
		"event_id":     false,
		"developer_id": false,
		"metric_type":  false,
		"value":        false,
		"timestamp":    false,
	}
	for _, e := range errs {
		if _, ok := wantFields[e.Field]; ok {
			wantFields[e.Field] = true
		}
	}
	for field, seen := range wantFields {
		if !seen {
			t.Errorf("esperava erro no campo %q, mas não veio. errors=%+v", field, errs)
		}
	}
}
