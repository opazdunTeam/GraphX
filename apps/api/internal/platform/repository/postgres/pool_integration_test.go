//go:build integration

package postgres

import (
	"context"
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
	const table = "graphx_transaction_probe"
	if _, err := pool.Exec(ctx, "drop table if exists "+table); err != nil {
		t.Fatalf("drop transaction probe: %v", err)
	}
	if _, err := pool.Exec(ctx, "create table "+table+" (value integer not null)"); err != nil {
		t.Fatalf("create transaction probe: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, "drop table if exists "+table)
	})

	if err := store.WithinTransaction(ctx, func(txStore *Store) error {
		_, err := txStore.db.Exec(ctx, "insert into "+table+" (value) values (1)")
		return err
	}); err != nil {
		t.Fatalf("WithinTransaction() error = %v", err)
	}

	var committed int
	if err := pool.QueryRow(ctx, "select count(*) from "+table+" where value = 1").Scan(&committed); err != nil {
		t.Fatalf("read committed value: %v", err)
	}
	if committed != 1 {
		t.Errorf("committed rows = %d, want 1", committed)
	}

	wantErr := errors.New("rollback transaction")
	err = store.WithinTransaction(ctx, func(txStore *Store) error {
		if _, insertErr := txStore.db.Exec(ctx, "insert into "+table+" (value) values (2)"); insertErr != nil {
			return insertErr
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("WithinTransaction() error = %v, want %v", err, wantErr)
	}

	var rolledBack int
	if err := pool.QueryRow(ctx, "select count(*) from "+table+" where value = 2").Scan(&rolledBack); err != nil {
		t.Fatalf("read rolled back value: %v", err)
	}
	if rolledBack != 0 {
		t.Errorf("rolled back rows = %d, want 0", rolledBack)
	}

	if err := store.WithinTransaction(ctx, nil); err == nil {
		t.Fatal("WithinTransaction(nil) error = nil, want error")
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
