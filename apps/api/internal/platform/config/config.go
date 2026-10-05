package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

const (
	defaultEnvironment     = "local"
	defaultLogLevel        = "info"
	defaultHTTPAddr        = ":8080"
	defaultShutdownTimeout = 15 * time.Second
)

type Process struct {
	Environment string
	LogLevel    slog.Level
}

type API struct {
	Process
	HTTPAddr        string
	ShutdownTimeout time.Duration
}

func LoadProcess() (Process, error) {
	level, err := parseLogLevel(envOrDefault("LOG_LEVEL", defaultLogLevel))
	if err != nil {
		return Process{}, err
	}

	environment := strings.TrimSpace(envOrDefault("APP_ENV", defaultEnvironment))
	if environment == "" {
		return Process{}, fmt.Errorf("APP_ENV must not be empty")
	}

	return Process{Environment: environment, LogLevel: level}, nil
}

func LoadAPI() (API, error) {
	process, err := LoadProcess()
	if err != nil {
		return API{}, err
	}

	shutdownTimeout, err := time.ParseDuration(envOrDefault("API_SHUTDOWN_TIMEOUT", defaultShutdownTimeout.String()))
	if err != nil || shutdownTimeout <= 0 {
		return API{}, fmt.Errorf("API_SHUTDOWN_TIMEOUT must be a positive duration")
	}

	httpAddr := strings.TrimSpace(envOrDefault("API_HTTP_ADDR", defaultHTTPAddr))
	if httpAddr == "" {
		return API{}, fmt.Errorf("API_HTTP_ADDR must not be empty")
	}

	return API{
		Process:         process,
		HTTPAddr:        httpAddr,
		ShutdownTimeout: shutdownTimeout,
	}, nil
}

func parseLogLevel(raw string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToLower(strings.TrimSpace(raw)))); err != nil {
		return 0, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	return level, nil
}

func envOrDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
