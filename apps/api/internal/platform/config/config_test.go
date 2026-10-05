package config

import (
	"log/slog"
	"testing"
	"time"
)

func TestLoadAPIDefaults(t *testing.T) {
	t.Setenv("APP_ENV", "local")
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("API_HTTP_ADDR", ":8080")
	t.Setenv("API_SHUTDOWN_TIMEOUT", "15s")

	cfg, err := LoadAPI()
	if err != nil {
		t.Fatalf("LoadAPI() error = %v", err)
	}
	if cfg.Environment != "local" || cfg.LogLevel != slog.LevelInfo {
		t.Fatalf("unexpected process config: %+v", cfg.Process)
	}
	if cfg.HTTPAddr != ":8080" || cfg.ShutdownTimeout != 15*time.Second {
		t.Fatalf("unexpected api config: %+v", cfg)
	}
}

func TestLoadAPIRejectsInvalidTimeout(t *testing.T) {
	t.Setenv("API_SHUTDOWN_TIMEOUT", "0s")
	if _, err := LoadAPI(); err == nil {
		t.Fatal("LoadAPI() expected an error")
	}
}
