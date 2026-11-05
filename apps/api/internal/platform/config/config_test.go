package config

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

var apiEnvKeys = []string{
	"APP_ENV",
	"LOG_LEVEL",
	"API_HTTP_ADDR",
	"API_READ_HEADER_TIMEOUT",
	"API_READ_TIMEOUT",
	"API_WRITE_TIMEOUT",
	"API_IDLE_TIMEOUT",
	"API_SHUTDOWN_TIMEOUT",
	"POSTGRES_DSN",
	"POSTGRES_MAX_CONNS",
	"POSTGRES_MIN_CONNS",
	"POSTGRES_CONNECT_TIMEOUT",
	"POSTGRES_HEALTH_TIMEOUT",
}

func cleanAPIEnv(t *testing.T) {
	t.Helper()

	for _, key := range apiEnvKeys {
		value, exists := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}

		t.Cleanup(func() {
			if exists {
				_ = os.Setenv(key, value)
				return
			}
			_ = os.Unsetenv(key)
		})
	}
}

func TestLoadAPI_Defaults(t *testing.T) {
	cleanAPIEnv(t)
	t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")

	cfg, err := LoadAPI()
	if err != nil {
		t.Fatalf("LoadAPI() error = %v", err)
	}

	if cfg.Environment != "local" {
		t.Errorf("Environment = %q, want local", cfg.Environment)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, slog.LevelInfo)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP.Addr = %q, want :8080", cfg.HTTP.Addr)
	}
	if cfg.HTTP.ReadHeaderTimeout != 5*time.Second {
		t.Errorf("HTTP.ReadHeaderTimeout = %v, want 5s", cfg.HTTP.ReadHeaderTimeout)
	}
	if cfg.HTTP.ShutdownTimeout != 15*time.Second {
		t.Errorf("HTTP.ShutdownTimeout = %v, want 15s", cfg.HTTP.ShutdownTimeout)
	}
	if cfg.Postgres.MaxConns != 20 || cfg.Postgres.MinConns != 2 {
		t.Errorf("Postgres pool = %d/%d, want 20/2", cfg.Postgres.MaxConns, cfg.Postgres.MinConns)
	}
	if cfg.Postgres.ConnectTimeout != 5*time.Second {
		t.Errorf("Postgres.ConnectTimeout = %v, want 5s", cfg.Postgres.ConnectTimeout)
	}
	if cfg.Postgres.HealthTimeout != 2*time.Second {
		t.Errorf("Postgres.HealthTimeout = %v, want 2s", cfg.Postgres.HealthTimeout)
	}
}

func TestLoadAPI_Overrides(t *testing.T) {
	cleanAPIEnv(t)
	t.Setenv("APP_ENV", "test")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("API_HTTP_ADDR", "127.0.0.1:9090")
	t.Setenv("API_READ_TIMEOUT", "3s")
	t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")
	t.Setenv("POSTGRES_MAX_CONNS", "8")
	t.Setenv("POSTGRES_MIN_CONNS", "1")
	t.Setenv("POSTGRES_HEALTH_TIMEOUT", "250ms")

	cfg, err := LoadAPI()
	if err != nil {
		t.Fatalf("LoadAPI() error = %v", err)
	}

	if cfg.Environment != "test" || cfg.LogLevel != slog.LevelDebug {
		t.Errorf("process config = %q/%v, want test/debug", cfg.Environment, cfg.LogLevel)
	}
	if cfg.HTTP.Addr != "127.0.0.1:9090" || cfg.HTTP.ReadTimeout != 3*time.Second {
		t.Errorf("HTTP config = %q/%v", cfg.HTTP.Addr, cfg.HTTP.ReadTimeout)
	}
	if cfg.Postgres.MaxConns != 8 || cfg.Postgres.MinConns != 1 {
		t.Errorf("Postgres pool = %d/%d, want 8/1", cfg.Postgres.MaxConns, cfg.Postgres.MinConns)
	}
	if cfg.Postgres.HealthTimeout != 250*time.Millisecond {
		t.Errorf("Postgres.HealthTimeout = %v, want 250ms", cfg.Postgres.HealthTimeout)
	}
}

func TestLoadProcess_DoesNotRequirePostgres(t *testing.T) {
	cleanAPIEnv(t)

	cfg, err := LoadProcess()
	if err != nil {
		t.Fatalf("LoadProcess() error = %v", err)
	}
	if cfg.Environment != "local" {
		t.Errorf("Environment = %q, want local", cfg.Environment)
	}
}

func TestLoadAPI_Validation(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "missing DSN", key: "POSTGRES_DSN", value: ""},
		{name: "invalid log level", key: "LOG_LEVEL", value: "verbose"},
		{name: "invalid HTTP address", key: "API_HTTP_ADDR", value: "8080"},
		{name: "non-positive duration", key: "API_READ_TIMEOUT", value: "0s"},
		{name: "invalid max connections", key: "POSTGRES_MAX_CONNS", value: "many"},
		{name: "negative min connections", key: "POSTGRES_MIN_CONNS", value: "-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanAPIEnv(t)
			t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")
			t.Setenv(tt.key, tt.value)

			if _, err := LoadAPI(); err == nil {
				t.Fatal("LoadAPI() error = nil, want validation error")
			}
		})
	}
}

func TestLoadAPI_MinConnectionsMustNotExceedMax(t *testing.T) {
	cleanAPIEnv(t)
	t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")
	t.Setenv("POSTGRES_MAX_CONNS", "2")
	t.Setenv("POSTGRES_MIN_CONNS", "3")

	if _, err := LoadAPI(); err == nil {
		t.Fatal("LoadAPI() error = nil, want pool bounds validation error")
	}
}
