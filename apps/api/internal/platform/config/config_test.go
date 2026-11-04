package config

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func createConfigFile(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")

	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	return path
}

func TestInitConfig_Success(t *testing.T) {
	t.Setenv("DB_PASSWORD", "env-password")

	yamlContent := `
server:
  host: "localhost:8080"
  log_level: "debug"
  read_header_timeout: 5
  read_timeout: 10
  write_timeout: 15
  idle_timeout: 30
  retries: 3

postgres:
  host: "localhost"
  user: "postgres"
  name: "qr_service"
  password: "yaml-password"
  port: "5432"
  sslmode: "disable"

process:
  env: "local"
  log_level: DEBUG
`

	path := createConfigFile(t, yamlContent)

	cfg, err := InitConfig(path)
	if err != nil {
		t.Fatalf("InitConfig() returned error: %v", err)
	}

	if cfg.Server.Host != "localhost:8080" {
		t.Errorf("Server.Host = %q, want %q",
			cfg.Server.Host, "localhost:8080")
	}

	if cfg.Server.Read_timeout != 10 {
		t.Errorf("Server.Read_timeout = %d, want %d",
			cfg.Server.Read_timeout, 10)
	}

	if cfg.Server.Retries != 3 {
		t.Errorf("Server.Retries = %d, want %d",
			cfg.Server.Retries, 3)
	}

	if cfg.Postgres.Host != "localhost" {
		t.Errorf("Postgres.Host = %q, want %q",
			cfg.Postgres.Host, "localhost")
	}

	if cfg.Postgres.Password != "env-password" {
		t.Errorf("Postgres.Password = %q, want %q",
			cfg.Postgres.Password, "env-password")
	}

	if cfg.Process.Env != "local" {
		t.Errorf("Process.Env = %q, want %q",
			cfg.Process.Env, "local")
	}

	if cfg.Process.LogLevel != slog.LevelDebug {
		t.Errorf("Process.LogLevel = %v, want %v",
			cfg.Process.LogLevel, slog.LevelDebug)
	}
}

func TestInitConfig_PasswordFromEnv(t *testing.T) {
	t.Setenv("DB_PASSWORD", "secret-from-env")

	path := createConfigFile(t, `
postgres:
  password: "secret-from-yaml"
`)

	cfg, err := InitConfig(path)
	if err != nil {
		t.Fatalf("InitConfig() returned error: %v", err)
	}

	if cfg.Postgres.Password != "secret-from-env" {
		t.Errorf("Postgres.Password = %q, want %q",
			cfg.Postgres.Password, "secret-from-env")
	}
}

func TestInitConfig_EmptyEnvPassword(t *testing.T) {
	t.Setenv("DB_PASSWORD", "")

	path := createConfigFile(t, `
postgres:
  password: "secret-from-yaml"
`)

	cfg, err := InitConfig(path)
	if err != nil {
		t.Fatalf("InitConfig() returned error: %v", err)
	}

	if cfg.Postgres.Password != "" {
		t.Errorf("Postgres.Password = %q, want empty string",
			cfg.Postgres.Password)
	}
}

func TestInitConfig_FileNotFound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")

	_, err := InitConfig(path)

	if err == nil {
		t.Fatal("InitConfig() expected error, got nil")
	}

	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("InitConfig() error = %v, want os.ErrNotExist", err)
	}
}

func TestInitConfig_InvalidYAML(t *testing.T) {
	path := createConfigFile(t, `
server: [
`)

	_, err := InitConfig(path)

	if err == nil {
		t.Fatal("InitConfig() expected YAML parsing error, got nil")
	}
}

func TestInitConfig_InvalidFieldType(t *testing.T) {
	path := createConfigFile(t, `
server:
  read_timeout: abc
`)

	_, err := InitConfig(path)

	if err == nil {
		t.Fatal("InitConfig() expected type parsing error, got nil")
	}
}
