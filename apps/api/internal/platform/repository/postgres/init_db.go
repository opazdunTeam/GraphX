package postgres

import (
	"GraphX/internal/platform/config"
	"testing"
)

func TestInitDB(t *testing.T) {
	var cfg config.Config

	cfg.Postgres.Host = "localhost"
	cfg.Postgres.Port = "5432"
	cfg.Postgres.User = "test_user"
	cfg.Postgres.Password = "test_password"
	cfg.Postgres.Name = "test_db"
	cfg.Postgres.Sslmode = "disable"

	db, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB() error = %v", err)
	}

	if db == nil {
		t.Fatal("InitDB() returned nil DB")
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB() error = %v", err)
	}

	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	if got := sqlDB.Stats().MaxOpenConnections; got != 100 {
		t.Errorf("MaxOpenConnections = %d, want 100", got)
	}
}