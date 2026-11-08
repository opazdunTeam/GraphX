package postgres

import (
	"strings"
	"testing"

	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

func TestOpenDoesNotExposeInvalidDSN(t *testing.T) {
	const secret = "sensitive-password"
	_, err := Open(t.Context(), config.Postgres{DSN: "://user:" + secret + "@localhost/database"})
	if err == nil {
		t.Fatal("Open() error = nil, want invalid DSN error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("Open() error exposes DSN secret: %v", err)
	}
}
