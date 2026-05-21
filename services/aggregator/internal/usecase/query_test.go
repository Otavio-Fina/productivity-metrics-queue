package usecase

import (
	"context"
	"testing"

	"github.com/Otavio-Fina/productivity-metrics-queue/services/aggregator/internal/domain"
)

// fakeQueryRepo captura o limit recebido pelo repositório — único ponto
// sob teste neste arquivo. GetSummary existe só pra satisfazer a interface.
type fakeQueryRepo struct {
	gotLimit int
}

func (f *fakeQueryRepo) GetEventsByDeveloper(_ context.Context, _ string, limit int, _ string) (domain.EventsPage, error) {
	f.gotLimit = limit
	return domain.EventsPage{}, nil
}

func (f *fakeQueryRepo) GetSummary(_ context.Context, _ string) (domain.SummaryRecord, bool, error) {
	return domain.SummaryRecord{}, false, nil
}

// O contrato é: limit inválido (<=0) → default 20; limit excessivo (>100)
// → cap em 100. Defende o DynamoDB de Query custosa vinda do cliente HTTP.
func TestGetEvents_LimitClamp(t *testing.T) {
	cases := []struct {
		name    string
		input   int
		wantArg int
	}{
		{"zero vira default 20", 0, 20},
		{"negativo vira default 20", -1, 20},
		{"valor válido passa direto", 50, 50},
		{"acima de 100 é limitado a 100", 500, 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeQueryRepo{}
			us := NewQueryUs(repo)
			if _, err := us.GetEvents(context.Background(), "dev-1", tc.input, ""); err != nil {
				t.Fatalf("erro inesperado: %v", err)
			}
			if repo.gotLimit != tc.wantArg {
				t.Errorf("limit passado ao repo: got %d, want %d", repo.gotLimit, tc.wantArg)
			}
		})
	}
}
