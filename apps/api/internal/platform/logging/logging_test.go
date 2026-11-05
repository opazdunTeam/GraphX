package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

func TestNewLoggerFormats(t *testing.T) {
	tests := []struct {
		name       string
		format     config.LogFormat
		wantPrefix string
	}{
		{name: "json", format: config.LogFormatJSON, wantPrefix: "{"},
		{name: "text", format: config.LogFormatText, wantPrefix: "time="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			newLogger(&output, "test-service", slog.LevelInfo, tt.format).Info("started")

			if !strings.HasPrefix(output.String(), tt.wantPrefix) {
				t.Fatalf("log = %q, want prefix %q", output.String(), tt.wantPrefix)
			}
			if !strings.Contains(output.String(), "test-service") {
				t.Fatalf("log = %q, want service field", output.String())
			}
		})
	}
}
