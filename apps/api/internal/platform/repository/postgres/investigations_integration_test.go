//go:build integration

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/investigations"
)

type testInvestigationOutboxFactory struct{}

func (testInvestigationOutboxFactory) PrepareSourceCollect(
	investigationID, jobID, outboxID, sourceCode string,
	command investigations.CreateCommand,
	_ time.Time,
) (investigations.OutboxMessage, error) {
	return sourceCollectRequestedOutboxMessage(outboxID, jobID, investigationID, 1), nil
}

func TestInvestigationCreationStoreIsIdempotentAndTransactional(t *testing.T) {
	ctx := context.Background()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)
	store := NewInvestigationStore(pool)

	sources, err := store.ResolveSources(ctx, []string{"fixture"})
	if err != nil {
		t.Fatalf("ResolveSources() error = %v", err)
	}
	name := "Ada Lovelace"
	command := investigations.CreateCommand{
		IdempotencyKey: "00000000-0000-4000-8000-000000000091",
		RequestHash:    strings.Repeat("a", 64),
		RawSubject:     []byte(`{"subject":{"type":"person","name":"Ada Lovelace"}}`),
		Subject: investigations.Subject{
			Type: "person",
			Name: &name,
		},
		Limits: investigations.Limits{
			Depth:       1,
			EntityLimit: 50,
			Sources:     []string{"fixture"},
		},
	}
	command.InvestigationID = "10000000-0000-4000-8000-000000000091"
	jobID := "20000000-0000-4000-8000-000000000091"
	outboxID := "30000000-0000-4000-8000-000000000091"
	message, err := testInvestigationOutboxFactory{}.PrepareSourceCollect(
		command.InvestigationID, jobID, outboxID, "fixture", command, time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("prepare initial outbox: %v", err)
	}
	command.InitialJobs = []investigations.InitialJob{{
		JobID:  jobID,
		Source: sources[0],
		Outbox: message,
	}}
	accepted, err := store.Create(ctx, command, sources)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if accepted.StatusURL == "" || accepted.ID == "" {
		t.Fatalf("accepted = %#v, want id and status URL", accepted)
	}

	var status, stage string
	var stateVersion int
	var rawSubject, normalizedSubject, limits []byte
	if err := pool.QueryRow(ctx, `
		select status, stage, state_version, raw_subject::text,
			normalized_subject::text, limits::text
		from investigations where id = $1
	`, accepted.ID).Scan(&status, &stage, &stateVersion, &rawSubject, &normalizedSubject, &limits); err != nil {
		t.Fatalf("read investigation: %v", err)
	}
	if status != "running" || stage != "subject_discovery" || stateVersion != 1 {
		t.Fatalf("investigation state = %s/%s/%d, want running/subject_discovery/1", status, stage, stateVersion)
	}
	if len(rawSubject) == 0 || len(normalizedSubject) == 0 || len(limits) == 0 {
		t.Fatal("investigation subject or limits were not stored")
	}

	var jobs, outboxes, idem int
	var storedOperation, payloadOperation string
	if err := pool.QueryRow(ctx, `
		select
			(select count(*) from source_jobs where investigation_id = $1),
			(select count(*) from outbox_messages where aggregate_id in
				(select id from source_jobs where investigation_id = $1)),
			(select count(*) from api_idempotency_keys
				where scope = 'investigation' and idempotency_key = $2),
			(select operation from source_jobs where investigation_id = $1),
			(select payload->'payload'->>'operation' from outbox_messages
				where aggregate_id in
					(select id from source_jobs where investigation_id = $1))
	`, accepted.ID, command.IdempotencyKey).Scan(
		&jobs, &outboxes, &idem, &storedOperation, &payloadOperation,
	); err != nil {
		t.Fatalf("count creation records: %v", err)
	}
	if jobs != 1 || outboxes != 1 || idem != 1 {
		t.Fatalf("creation records = jobs %d outboxes %d idem %d, want 1/1/1", jobs, outboxes, idem)
	}
	if storedOperation != "search_subject" || payloadOperation != storedOperation {
		t.Fatalf("stored source operation = %q and payload operation = %q, want both search_subject",
			storedOperation, payloadOperation)
	}

	replayed, err := store.Create(ctx, command, sources)
	if err != nil {
		t.Fatalf("Create(replay) error = %v", err)
	}
	if replayed != accepted {
		t.Fatalf("replayed = %#v, want %#v", replayed, accepted)
	}

	conflictCommand := command
	conflictCommand.RequestHash = strings.Repeat("b", 64)
	conflictCommand.Limits.Sources = []string{"unknown-source"}
	creator, err := investigations.NewCreator(store, testInvestigationOutboxFactory{})
	if err != nil {
		t.Fatalf("NewCreator() error = %v", err)
	}
	if _, err := creator.Create(ctx, conflictCommand); err == nil {
		t.Fatal("creator.Create(different hash/unknown source) error = nil, want conflict")
	} else {
		var conflict *investigations.IdempotencyConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("creator.Create(different hash/unknown source) error = %v, want conflict", err)
		}
	}

	conflictCommand.RequestHash = strings.Repeat("c", 64)
	conflictCommand.Limits.Sources = []string{"fixture"}
	conflictCommand.InvestigationID = command.InvestigationID
	conflictCommand.InitialJobs = command.InitialJobs
	if _, err := store.Create(ctx, conflictCommand, sources); err == nil {
		t.Fatal("Create(different hash) error = nil, want conflict")
	} else {
		var conflict *investigations.IdempotencyConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("Create(different hash) error = %v, want conflict", err)
		}
	}

	var investigationCount int
	if err := pool.QueryRow(ctx, "select count(*) from investigations").Scan(&investigationCount); err != nil {
		t.Fatalf("count investigations: %v", err)
	}
	if investigationCount < 1 {
		t.Fatal("investigation was not created")
	}
}

func TestInvestigationCreationRejectsUnknownAndDisabledSources(t *testing.T) {
	ctx := context.Background()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)
	store := NewInvestigationStore(pool)

	if _, err := store.ResolveSources(ctx, []string{"unknown-source"}); err == nil {
		t.Fatal("ResolveSources(unknown) error = nil")
	} else {
		var selection *investigations.SourceSelectionError
		if !errors.As(err, &selection) || len(selection.Unknown) != 1 {
			t.Fatalf("ResolveSources(unknown) error = %v", err)
		}
	}

	const disabledCode = "disabled-fixture"
	if _, err := pool.Exec(ctx, `
		insert into sources (id, code, name, mode, enabled)
		values ('90000000-0000-4000-8000-000000000091', $1, 'Disabled Fixture', 'file', false)
		on conflict (code) do update set enabled = false
	`, disabledCode); err != nil {
		t.Fatalf("insert disabled source: %v", err)
	}
	if _, err := store.ResolveSources(ctx, []string{disabledCode}); err == nil {
		t.Fatal("ResolveSources(disabled) error = nil")
	} else {
		var selection *investigations.SourceSelectionError
		if !errors.As(err, &selection) || len(selection.Disabled) != 1 {
			t.Fatalf("ResolveSources(disabled) error = %v", err)
		}
	}
}
