//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/handlers"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/router"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

func TestListSourcesUsesRegisteredFixture(t *testing.T) {
	ctx := context.Background()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := handlers.New(pool, time.Second, config.Investigation{}, NewInvestigationStore(pool))
	server := router.NewRouter(logger, handler)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/sources", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /sources = %d, body: %s", response.Code, response.Body)
	}
	var catalog generated.SourceCatalog
	if err := json.Unmarshal(response.Body.Bytes(), &catalog); err != nil {
		t.Fatal(err)
	}
	if len(catalog.Items) != 1 || catalog.UpdatedAt.IsZero() {
		t.Fatalf("unexpected catalog: %#v", catalog)
	}
	fixture := catalog.Items[0]
	if fixture.Code != "fixture" || fixture.Name != "Fixture" || fixture.Mode != "file" || !fixture.Enabled {
		t.Fatalf("unexpected fixture: %#v", fixture)
	}
	if len(fixture.Roles) != 1 || fixture.Roles[0] != generated.SourceCatalogItemRolesDiscovery ||
		len(fixture.EntityTypes) != 1 || fixture.EntityTypes[0] != generated.EntityTypePerson ||
		fixture.Health.State != generated.SourceHealthStateUnknown {
		t.Fatalf("unexpected fixture capabilities: %#v", fixture)
	}
}
