package config

import (
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

var apiEnvKeys = []string{
	"APP_ENV",
	"LOG_LEVEL",
	"LOG_FORMAT",
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
	"RABBITMQ_URL",
	"RABBITMQ_PREFETCH_SOURCE",
	"RABBITMQ_CONNECT_TIMEOUT",
	"PUBLISHER_METRICS_ADDR",
	"S3_ENDPOINT",
	"S3_REGION",
	"S3_BUCKET_ARTIFACTS",
	"S3_ACCESS_KEY",
	"S3_SECRET_KEY",
	"S3_USE_PATH_STYLE",
	"S3_CONNECT_TIMEOUT",
	"S3_READ_TIMEOUT",
	"SOURCE_RESULT_TERMINAL_MAX_BYTES",
	"SOURCE_RESULT_MANIFEST_MAX_BYTES",
	"INVESTIGATION_DEFAULT_DEPTH",
	"INVESTIGATION_MAX_DEPTH",
	"INVESTIGATION_DEFAULT_ENTITY_LIMIT",
	"INVESTIGATION_MAX_ENTITY_LIMIT",
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
	if cfg.LogFormat != LogFormatJSON {
		t.Errorf("LogFormat = %q, want json", cfg.LogFormat)
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
	if cfg.Investigation.DefaultDepth != 1 || cfg.Investigation.MaxDepth != 2 ||
		cfg.Investigation.DefaultEntityLimit != 50 || cfg.Investigation.MaxEntityLimit != 100 {
		t.Errorf("Investigation = %#v, want documented defaults", cfg.Investigation)
	}
}

func TestLoadAPI_Overrides(t *testing.T) {
	cleanAPIEnv(t)
	t.Setenv("APP_ENV", "test")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")
	t.Setenv("API_HTTP_ADDR", "127.0.0.1:9090")
	t.Setenv("API_READ_TIMEOUT", "3s")
	t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")
	t.Setenv("POSTGRES_MAX_CONNS", "8")
	t.Setenv("POSTGRES_MIN_CONNS", "1")
	t.Setenv("POSTGRES_HEALTH_TIMEOUT", "250ms")
	t.Setenv("INVESTIGATION_DEFAULT_DEPTH", "0")
	t.Setenv("INVESTIGATION_MAX_DEPTH", "1")
	t.Setenv("INVESTIGATION_DEFAULT_ENTITY_LIMIT", "25")
	t.Setenv("INVESTIGATION_MAX_ENTITY_LIMIT", "75")

	cfg, err := LoadAPI()
	if err != nil {
		t.Fatalf("LoadAPI() error = %v", err)
	}

	if cfg.Environment != "test" || cfg.LogLevel != slog.LevelDebug || cfg.LogFormat != LogFormatText {
		t.Errorf("process config = %q/%v/%q, want test/debug/text", cfg.Environment, cfg.LogLevel, cfg.LogFormat)
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
	if cfg.Investigation.DefaultDepth != 0 || cfg.Investigation.MaxDepth != 1 ||
		cfg.Investigation.DefaultEntityLimit != 25 || cfg.Investigation.MaxEntityLimit != 75 {
		t.Errorf("Investigation = %#v, want environment overrides", cfg.Investigation)
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

func TestLoadPublisherRequiresValidRabbitMQURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "missing URL"},
		{name: "unsupported scheme", url: "http://rabbitmq:5672"},
		{name: "missing host", url: "amqp:///vhost"},
		{name: "invalid port", url: "amqp://rabbitmq:port/vhost"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cleanAPIEnv(t)
			t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")
			t.Setenv("RABBITMQ_URL", test.url)
			if _, err := LoadPublisher(); err == nil {
				t.Fatal("LoadPublisher() error = nil, want configuration error")
			}
		})
	}

	cleanAPIEnv(t)
	t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")
	t.Setenv("RABBITMQ_URL", "amqps://publisher:secret@rabbitmq.example:5671/graphx")
	cfg, err := LoadPublisher()
	if err != nil {
		t.Fatalf("LoadPublisher() error = %v", err)
	}
	if cfg.RabbitMQ.URL != "amqps://publisher:secret@rabbitmq.example:5671/graphx" {
		t.Fatal("LoadPublisher() did not preserve the configured RabbitMQ URL")
	}
	if cfg.PublisherMetricsAddr != defaultPublisherMetricsAddr {
		t.Fatalf("PublisherMetricsAddr = %q, want %q", cfg.PublisherMetricsAddr, defaultPublisherMetricsAddr)
	}

	cleanAPIEnv(t)
	t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")
	t.Setenv("RABBITMQ_URL", "amqp://graphx:secret@rabbitmq:5672/graphx")
	t.Setenv("PUBLISHER_METRICS_ADDR", "0.0.0.0:9092")
	cfg, err = LoadPublisher()
	if err != nil {
		t.Fatalf("LoadPublisher() with metrics address override error = %v", err)
	}
	if cfg.PublisherMetricsAddr != "0.0.0.0:9092" {
		t.Fatalf("PublisherMetricsAddr = %q, want 0.0.0.0:9092", cfg.PublisherMetricsAddr)
	}
}

func TestLoadPublisherRejectsInvalidMetricsAddress(t *testing.T) {
	const privateMarker = "private-metrics-marker"
	tests := []string{"", "9091", "localhost", "127.0.0.1:service", "127.0.0.1:65536",
		"http://user:" + privateMarker + "@localhost:9091"}
	for _, address := range tests {
		t.Run(address, func(t *testing.T) {
			cleanAPIEnv(t)
			t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")
			t.Setenv("RABBITMQ_URL", "amqp://graphx:secret@rabbitmq:5672/graphx")
			t.Setenv("PUBLISHER_METRICS_ADDR", address)
			_, err := LoadPublisher()
			if err == nil {
				t.Fatal("LoadPublisher() error = nil, want metrics address validation error")
			}
			if strings.Contains(err.Error(), privateMarker) {
				t.Fatal("LoadPublisher() error exposed the configured address")
			}
		})
	}
}

func TestLoadConsumer(t *testing.T) {
	cleanAPIEnv(t)
	t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")
	t.Setenv("RABBITMQ_URL", "amqp://graphx:secret@localhost:5672/graphx")
	t.Setenv("S3_ENDPOINT", "http://localhost:9000")
	t.Setenv("S3_BUCKET_ARTIFACTS", "graphx-artifacts")
	t.Setenv("S3_ACCESS_KEY", "access")
	t.Setenv("S3_SECRET_KEY", "secret")

	cfg, err := LoadConsumer()
	if err != nil {
		t.Fatalf("LoadConsumer() error = %v", err)
	}
	if cfg.Consumer.Prefetch != defaultRabbitMQPrefetch ||
		cfg.Consumer.ConnectTimeout != defaultRabbitMQConnectTimeout ||
		cfg.Consumer.MaxMessageBytes != defaultResultMessageMaxBytes {
		t.Fatalf("consumer settings = %#v", cfg.Consumer)
	}
	if cfg.ObjectStore.Region != "local" ||
		cfg.ObjectStore.MaxManifestBytes != defaultResultManifestMaxBytes ||
		!cfg.ObjectStore.PathStyle {
		t.Fatalf("object store settings = %#v", cfg.ObjectStore)
	}
}

func TestLoadConsumerRejectsInvalidObjectStoreConfiguration(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "endpoint scheme", key: "S3_ENDPOINT", value: "ftp://localhost:9000"},
		{name: "endpoint credentials", key: "S3_ENDPOINT", value: "http://user:pass@localhost:9000"},
		{name: "manifest size", key: "SOURCE_RESULT_MANIFEST_MAX_BYTES", value: "0"},
		{name: "prefetch", key: "RABBITMQ_PREFETCH_SOURCE", value: "65536"},
		{name: "path style", key: "S3_USE_PATH_STYLE", value: "sometimes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cleanAPIEnv(t)
			t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")
			t.Setenv("RABBITMQ_URL", "amqp://graphx:secret@localhost:5672/graphx")
			t.Setenv("S3_ENDPOINT", "http://localhost:9000")
			t.Setenv("S3_BUCKET_ARTIFACTS", "graphx-artifacts")
			t.Setenv("S3_ACCESS_KEY", "access")
			t.Setenv("S3_SECRET_KEY", "secret")
			t.Setenv(test.key, test.value)
			if _, err := LoadConsumer(); err == nil {
				t.Fatal("LoadConsumer() error = nil, want configuration error")
			}
		})
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
		{name: "invalid log format", key: "LOG_FORMAT", value: "pretty"},
		{name: "invalid HTTP address", key: "API_HTTP_ADDR", value: "8080"},
		{name: "non-positive duration", key: "API_READ_TIMEOUT", value: "0s"},
		{name: "invalid max connections", key: "POSTGRES_MAX_CONNS", value: "many"},
		{name: "negative min connections", key: "POSTGRES_MIN_CONNS", value: "-1"},
		{name: "depth over contract maximum", key: "INVESTIGATION_MAX_DEPTH", value: "3"},
		{name: "entity limit over contract maximum", key: "INVESTIGATION_MAX_ENTITY_LIMIT", value: "101"},
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

func TestLoadAPI_InvestigationDefaultsMustNotExceedMaximums(t *testing.T) {
	tests := []struct {
		defaultKey string
		maxKey     string
	}{
		{defaultKey: "INVESTIGATION_DEFAULT_DEPTH", maxKey: "INVESTIGATION_MAX_DEPTH"},
		{defaultKey: "INVESTIGATION_DEFAULT_ENTITY_LIMIT", maxKey: "INVESTIGATION_MAX_ENTITY_LIMIT"},
	}

	for _, tt := range tests {
		t.Run(tt.defaultKey, func(t *testing.T) {
			cleanAPIEnv(t)
			t.Setenv("POSTGRES_DSN", "postgres://graphx:secret@localhost:5432/graphx")
			t.Setenv(tt.defaultKey, "2")
			t.Setenv(tt.maxKey, "1")
			if _, err := LoadAPI(); err == nil {
				t.Fatal("LoadAPI() error = nil, want limits validation error")
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
