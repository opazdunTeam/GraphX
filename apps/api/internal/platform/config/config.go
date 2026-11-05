package config

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultEnvironment            = "local"
	defaultLogLevel               = "info"
	defaultHTTPAddr               = ":8080"
	defaultReadHeaderTimeout      = 5 * time.Second
	defaultReadTimeout            = 15 * time.Second
	defaultWriteTimeout           = 30 * time.Second
	defaultIdleTimeout            = 60 * time.Second
	defaultShutdownTimeout        = 15 * time.Second
	defaultPostgresMaxConns       = 20
	defaultPostgresMinConns       = 2
	defaultPostgresConnectTimeout = 5 * time.Second
	defaultPostgresHealthTimeout  = 2 * time.Second
)

// Config contains all settings required by the API process.
type Config struct {
	Process
	HTTP     HTTP
	Postgres Postgres
}

// Process contains settings shared by executable entrypoints.
type Process struct {
	Environment string
	LogLevel    slog.Level
}

// HTTP contains HTTP server lifecycle and timeout settings.
type HTTP struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
}

// Postgres contains pool settings. DSN must never be written to logs.
type Postgres struct {
	DSN            string
	MaxConns       int32
	MinConns       int32
	ConnectTimeout time.Duration
	HealthTimeout  time.Duration
}

// LoadAPI reads and validates the environment-only API configuration.
func LoadAPI() (Config, error) {
	process, err := LoadProcess()
	if err != nil {
		return Config{}, err
	}

	httpConfig, err := loadHTTP()
	if err != nil {
		return Config{}, err
	}

	postgresConfig, err := loadPostgres()
	if err != nil {
		return Config{}, err
	}

	return Config{
		Process:  process,
		HTTP:     httpConfig,
		Postgres: postgresConfig,
	}, nil
}

// LoadProcess reads configuration shared by lightweight process entrypoints.
func LoadProcess() (Process, error) {
	level, err := parseLogLevel(envOrDefault("LOG_LEVEL", defaultLogLevel))
	if err != nil {
		return Process{}, err
	}

	environment := strings.TrimSpace(envOrDefault("APP_ENV", defaultEnvironment))
	if environment == "" {
		return Process{}, fmt.Errorf("APP_ENV must not be empty")
	}

	return Process{
		Environment: environment,
		LogLevel:    level,
	}, nil
}

func loadHTTP() (HTTP, error) {
	addr := strings.TrimSpace(envOrDefault("API_HTTP_ADDR", defaultHTTPAddr))
	if addr == "" {
		return HTTP{}, fmt.Errorf("API_HTTP_ADDR must not be empty")
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return HTTP{}, fmt.Errorf("API_HTTP_ADDR must be a valid host:port: %w", err)
	}

	readHeaderTimeout, err := positiveDuration("API_READ_HEADER_TIMEOUT", defaultReadHeaderTimeout)
	if err != nil {
		return HTTP{}, err
	}
	readTimeout, err := positiveDuration("API_READ_TIMEOUT", defaultReadTimeout)
	if err != nil {
		return HTTP{}, err
	}
	writeTimeout, err := positiveDuration("API_WRITE_TIMEOUT", defaultWriteTimeout)
	if err != nil {
		return HTTP{}, err
	}
	idleTimeout, err := positiveDuration("API_IDLE_TIMEOUT", defaultIdleTimeout)
	if err != nil {
		return HTTP{}, err
	}
	shutdownTimeout, err := positiveDuration("API_SHUTDOWN_TIMEOUT", defaultShutdownTimeout)
	if err != nil {
		return HTTP{}, err
	}

	return HTTP{
		Addr:              addr,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		ShutdownTimeout:   shutdownTimeout,
	}, nil
}

func loadPostgres() (Postgres, error) {
	dsn := strings.TrimSpace(os.Getenv("POSTGRES_DSN"))
	if dsn == "" {
		return Postgres{}, fmt.Errorf("POSTGRES_DSN is required")
	}

	maxConns, err := positiveInt32("POSTGRES_MAX_CONNS", defaultPostgresMaxConns)
	if err != nil {
		return Postgres{}, err
	}
	minConns, err := nonNegativeInt32("POSTGRES_MIN_CONNS", defaultPostgresMinConns)
	if err != nil {
		return Postgres{}, err
	}
	if minConns > maxConns {
		return Postgres{}, fmt.Errorf("POSTGRES_MIN_CONNS must not exceed POSTGRES_MAX_CONNS")
	}

	connectTimeout, err := positiveDuration("POSTGRES_CONNECT_TIMEOUT", defaultPostgresConnectTimeout)
	if err != nil {
		return Postgres{}, err
	}
	healthTimeout, err := positiveDuration("POSTGRES_HEALTH_TIMEOUT", defaultPostgresHealthTimeout)
	if err != nil {
		return Postgres{}, err
	}

	return Postgres{
		DSN:            dsn,
		MaxConns:       maxConns,
		MinConns:       minConns,
		ConnectTimeout: connectTimeout,
		HealthTimeout:  healthTimeout,
	}, nil
}

func parseLogLevel(raw string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToLower(strings.TrimSpace(raw)))); err != nil {
		return 0, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	return level, nil
}

func positiveDuration(key string, fallback time.Duration) (time.Duration, error) {
	value, err := time.ParseDuration(strings.TrimSpace(envOrDefault(key, fallback.String())))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return value, nil
}

func positiveInt32(key string, fallback int) (int32, error) {
	value, err := parseInt32(key, fallback)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}
	return value, nil
}

func nonNegativeInt32(key string, fallback int) (int32, error) {
	value, err := parseInt32(key, fallback)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, fmt.Errorf("%s must not be negative", key)
	}
	return value, nil
}

func parseInt32(key string, fallback int) (int32, error) {
	raw := strings.TrimSpace(envOrDefault(key, strconv.Itoa(fallback)))
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s must be a 32-bit integer: %w", key, err)
	}
	return int32(value), nil
}

func envOrDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
