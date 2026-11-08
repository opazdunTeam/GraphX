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
	defaultLogFormat              = LogFormatJSON
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
	defaultInvestigationDepth     = 1
	maxInvestigationDepth         = 2
	defaultInvestigationEntities  = 50
	maxInvestigationEntities      = 100
)

type LogFormat string

const (
	LogFormatJSON LogFormat = "json"
	LogFormatText LogFormat = "text"
)

type Config struct {
	Process
	HTTP          HTTP
	Postgres      Postgres
	Investigation Investigation
}

type Process struct {
	Environment string
	LogLevel    slog.Level
	LogFormat   LogFormat
}

type HTTP struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
}

type Postgres struct {
	DSN            string
	MaxConns       int32
	MinConns       int32
	ConnectTimeout time.Duration
	HealthTimeout  time.Duration
}

type Investigation struct {
	DefaultDepth       int
	MaxDepth           int
	DefaultEntityLimit int
	MaxEntityLimit     int
}

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

	investigationConfig, err := loadInvestigation()
	if err != nil {
		return Config{}, err
	}

	return Config{
		Process:       process,
		HTTP:          httpConfig,
		Postgres:      postgresConfig,
		Investigation: investigationConfig,
	}, nil
}

func LoadProcess() (Process, error) {
	level, err := parseLogLevel(envOrDefault("LOG_LEVEL", defaultLogLevel))
	if err != nil {
		return Process{}, err
	}
	format, err := parseLogFormat(envOrDefault("LOG_FORMAT", string(defaultLogFormat)))
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
		LogFormat:   format,
	}, nil
}

func loadInvestigation() (Investigation, error) {
	defaultDepth, err := nonNegativeInt("INVESTIGATION_DEFAULT_DEPTH", defaultInvestigationDepth)
	if err != nil {
		return Investigation{}, err
	}
	maxDepth, err := nonNegativeInt("INVESTIGATION_MAX_DEPTH", maxInvestigationDepth)
	if err != nil {
		return Investigation{}, err
	}
	if defaultDepth > maxDepth {
		return Investigation{}, fmt.Errorf("INVESTIGATION_DEFAULT_DEPTH must not exceed INVESTIGATION_MAX_DEPTH")
	}
	if maxDepth > maxInvestigationDepth {
		return Investigation{}, fmt.Errorf("INVESTIGATION_MAX_DEPTH must not exceed the OpenAPI maximum of %d", maxInvestigationDepth)
	}

	defaultEntityLimit, err := positiveInt("INVESTIGATION_DEFAULT_ENTITY_LIMIT", defaultInvestigationEntities)
	if err != nil {
		return Investigation{}, err
	}
	maxEntityLimit, err := positiveInt("INVESTIGATION_MAX_ENTITY_LIMIT", maxInvestigationEntities)
	if err != nil {
		return Investigation{}, err
	}
	if defaultEntityLimit > maxEntityLimit {
		return Investigation{}, fmt.Errorf("INVESTIGATION_DEFAULT_ENTITY_LIMIT must not exceed INVESTIGATION_MAX_ENTITY_LIMIT")
	}
	if maxEntityLimit > maxInvestigationEntities {
		return Investigation{}, fmt.Errorf("INVESTIGATION_MAX_ENTITY_LIMIT must not exceed the OpenAPI maximum of %d", maxInvestigationEntities)
	}

	return Investigation{
		DefaultDepth:       defaultDepth,
		MaxDepth:           maxDepth,
		DefaultEntityLimit: defaultEntityLimit,
		MaxEntityLimit:     maxEntityLimit,
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

func parseLogFormat(raw string) (LogFormat, error) {
	format := LogFormat(strings.ToLower(strings.TrimSpace(raw)))
	if format != LogFormatJSON && format != LogFormatText {
		return "", fmt.Errorf("LOG_FORMAT must be json or text")
	}
	return format, nil
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

func positiveInt(key string, fallback int) (int, error) {
	value, err := parseInt(key, fallback)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}
	return value, nil
}

func nonNegativeInt(key string, fallback int) (int, error) {
	value, err := parseInt(key, fallback)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, fmt.Errorf("%s must not be negative", key)
	}
	return value, nil
}

func parseInt(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(envOrDefault(key, strconv.Itoa(fallback)))
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
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
