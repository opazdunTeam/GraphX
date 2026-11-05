//go:build integration

package postgres

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

func testPostgresConfig() config.Postgres {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://test_user:test_password@localhost:5433/graphx_test?sslmode=disable"
	}

	return config.Postgres{
		DSN:            dsn,
		MaxConns:       5,
		MinConns:       1,
		ConnectTimeout: 5 * time.Second,
		HealthTimeout:  time.Second,
	}
}

func TestOpen(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("pool.Ping() error = %v", err)
	}
	if pool.Config().MaxConns != 5 || pool.Config().MinConns != 1 {
		t.Errorf(
			"pool limits = %d/%d, want 5/1",
			pool.Config().MaxConns,
			pool.Config().MinConns,
		)
	}
}

func TestStoreWithinTransaction(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	store := NewStore(pool)
	var value int
	if err := store.WithinTransaction(ctx, func(txStore *Store) error {
		return txStore.db.QueryRow(ctx, "select 1").Scan(&value)
	}); err != nil {
		t.Fatalf("WithinTransaction() error = %v", err)
	}
	if value != 1 {
		t.Errorf("transaction query value = %d, want 1", value)
	}

	wantErr := errors.New("rollback transaction")
	err = store.WithinTransaction(ctx, func(*Store) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("WithinTransaction() error = %v, want %v", err, wantErr)
	}
}

func TestTransactionalStoreRejectsNestedTransaction(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	store := NewStore(pool)
	err = store.WithinTransaction(ctx, func(txStore *Store) error {
		return txStore.WithinTransaction(ctx, func(*Store) error { return nil })
	})
	if err == nil {
		t.Fatal("nested WithinTransaction() error = nil, want error")
	}
}
