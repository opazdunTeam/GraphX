package logging

import (
	"io"
	"log/slog"
	"os"

	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

func New(service string, level slog.Level, format config.LogFormat) *slog.Logger {
	return newLogger(os.Stdout, service, level, format)
}

func newLogger(output io.Writer, service string, level slog.Level, format config.LogFormat) *slog.Logger {
	options := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if format == config.LogFormatText {
		handler = slog.NewTextHandler(output, options)
	} else {
		handler = slog.NewJSONHandler(output, options)
	}

	return slog.New(handler).With("service", service)
}
