package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/investigations"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/handlers"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/apperrors"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

type statusReaderFunc func(context.Context, uuid.UUID) (investigations.StatusSnapshot, error)

func (read statusReaderFunc) ReadStatus(ctx context.Context, id uuid.UUID) (investigations.StatusSnapshot, error) {
	return read(ctx, id)
}

func TestInvestigationStatusRoute(t *testing.T) {
	compiler, contracts := statusOpenAPICompiler(t)
	const schemaPath = "https://example.test/openapi#/components/schemas/"
	statusSchema, err := compiler.Compile(schemaPath + "InvestigationStatus")
	if err != nil {
		t.Fatal(err)
	}
	problemSchema, err := compiler.Compile(schemaPath + "Problem")
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	upperCaseID := uuid.MustParse("aaaaaaaa-bbbb-4aaa-8bbb-cccccccccccc")
	now := time.Date(2026, 11, 28, 12, 0, 30, 0, time.UTC)
	retryAt := now.Add(30 * time.Second)
	const privateError = "private source diagnostics"
	for _, tc := range []struct {
		name   string
		id     string
		jobs   []investigations.StatusJob
		count  int
		err    error
		status int
	}{
		{name: "empty", id: id.String(), status: http.StatusOK},
		{name: "partial result", id: id.String(), status: http.StatusOK, jobs: []investigations.StatusJob{
			{SourceCode: "fixture", Status: "succeeded", AttemptNo: 1, UpdatedAt: now},
			{SourceCode: "fixture", Status: "retry_wait", AttemptNo: 1, UpdatedAt: now, NextAttemptAt: &retryAt, ErrorCode: "SOURCE_TIMEOUT", ErrorAt: &now},
		}},
		{name: "isolated failure", id: id.String(), status: http.StatusOK, jobs: []investigations.StatusJob{
			{SourceCode: "fixture", Status: "failed", AttemptNo: 1, UpdatedAt: now, ErrorCode: privateError, ErrorAt: &now},
		}},
		{name: "review only", id: id.String(), status: http.StatusOK, count: 1},
		{name: "source action only", id: id.String(), status: http.StatusOK, jobs: []investigations.StatusJob{
			{ID: id, SourceCode: "fixture", Status: "action_required", AttemptNo: 1, UpdatedAt: now},
		}},
		{name: "unknown investigation", id: id.String(), status: http.StatusNotFound, err: apperrors.ErrNotFound},
		{name: "database error", id: id.String(), status: http.StatusInternalServerError, err: errors.New(privateError)},
		{name: "malformed UUID", id: "bad-id", status: http.StatusBadRequest},
		{name: "wrapped UUID", id: "x" + id.String() + "y", status: http.StatusBadRequest},
		{name: "compact UUID", id: strings.ReplaceAll(id.String(), "-", ""), status: http.StatusBadRequest},
		{name: "uppercase UUID", id: strings.ToUpper(upperCaseID.String()), status: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			reader := statusReaderFunc(func(_ context.Context, got uuid.UUID) (investigations.StatusSnapshot, error) {
				calls++
				if want := uuid.MustParse(tc.id); got != want {
					t.Fatalf("lookup ID = %s, want %s", got, want)
				}
				return investigations.StatusSnapshot{
					Status: "running", Stage: "subject_discovery", StateVersion: 1,
					UpdatedAt: now, Jobs: tc.jobs, PendingReviewCount: tc.count,
				}, tc.err
			})
			handler := handlers.New(&readinessStub{}, time.Second, config.Investigation{}, nil, reader)
			router := NewRouter(slog.New(slog.NewTextHandler(io.Discard, nil)), handler)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, businessBaseURL+"/investigations/"+tc.id+"/status", nil))
			if response.Code != tc.status || strings.Contains(response.Body.String(), privateError) {
				t.Fatalf("response = %d %s", response.Code, response.Body)
			}
			if tc.status == http.StatusBadRequest && calls != 0 || tc.status != http.StatusBadRequest && calls != 1 {
				t.Fatalf("read calls = %d", calls)
			}
			if tc.status != http.StatusOK {
				if got := response.Header().Get("Content-Type"); got != "application/problem+json" {
					t.Fatalf("problem content type = %q", got)
				}
				validateStatusJSON(t, problemSchema, response.Body.Bytes())
				return
			}

			actual := validateStatusJSON(t, statusSchema, response.Body.Bytes())
			if tc.name == "partial result" {
				example, err := os.ReadFile(filepath.Join(contracts, "examples", "http", "investigation-status.json"))
				if err != nil {
					t.Fatal(err)
				}
				if want := validateStatusJSON(t, statusSchema, example); !bytes.Equal(actual, want) {
					t.Fatalf("status example mismatch\ngot  %s\nwant %s", actual, want)
				}
			}
			var dto generated.InvestigationStatus
			if err := json.Unmarshal(actual, &dto); err != nil {
				t.Fatal(err)
			}
			hasSourceAction := tc.name == "source action only"
			if dto.Status != "running" || dto.RequiresAction != (tc.count > 0 || hasSourceAction) ||
				dto.PendingReviewCount != tc.count || (dto.ReviewsUrl != nil) != (tc.count > 0) {
				t.Fatalf("status or review fields = %s", actual)
			}
			if dto.ReviewsUrl != nil && *dto.ReviewsUrl != businessBaseURL+"/investigations/"+id.String()+"/reviews" {
				t.Fatalf("reviews URL = %q", *dto.ReviewsUrl)
			}
			if hasSourceAction && (len(dto.SourceActions) != 1 || dto.SourceActions[0].JobId != id ||
				dto.SourceActions[0].ActionCode != "SOURCE_ACTION_REQUIRED" || dto.SourceActions[0].ActionUrl != nil) {
				t.Fatalf("source actions = %s", actual)
			}
			if tc.name == "isolated failure" && (len(dto.Sources) != 1 || dto.Sources[0].Status != "failed" ||
				dto.Sources[0].ErrorCode == nil || *dto.Sources[0].ErrorCode != "SOURCE_ERROR") {
				t.Fatalf("source failure = %s", actual)
			}
		})
	}
}

func statusOpenAPICompiler(t *testing.T) (*jsonschema.Compiler, string) {
	t.Helper()
	contracts := filepath.Join("..", "..", "..", "..", "..", "..", "contracts")
	raw, err := os.ReadFile(filepath.Join(contracts, "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var spec any
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	const resource = "https://example.test/openapi"
	if err := compiler.AddResource(resource, spec); err != nil {
		t.Fatal(err)
	}
	return compiler, contracts
}

func validateStatusJSON(t *testing.T, schema *jsonschema.Schema, raw []byte) []byte {
	t.Helper()
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(value); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
