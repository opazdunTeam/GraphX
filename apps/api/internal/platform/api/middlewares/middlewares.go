package middlewares

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"time"
	"uuid"

	"github.com/gin-gonic/gin"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/utils"
)

type contextKey uint8

const (
	requestIDContextKey contextKey = iota
	requestIDHeader                = "X-Request-ID"
)

func RecoverMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				requestID := GetRequestID(c.Request.Context())

				logger.Error(
					"panic recovered",
					"request_id", requestID,
					"panic_type", fmt.Sprintf("%T", recovered),
				)

				utils.WriteProblem(
					c,
					http.StatusInternalServerError,
					"INTERNAL_ERROR",
					"Internal server error",
					"An unexpected error occurred.",
				)

				c.Abort()
			}
		}()

		c.Next()
	}
}

var requestIDPattern = regexp.MustCompile(
	`^[A-Za-z0-9._:-]{1,128}$`,
)

func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader(requestIDHeader)

		if !requestIDPattern.MatchString(requestID) {
			requestID = uuid.New().String()
		}

		ctx := context.WithValue(
			c.Request.Context(),
			requestIDContextKey,
			requestID,
		)

		c.Request = c.Request.WithContext(ctx)
		c.Request.Header.Set(requestIDHeader, requestID)

		c.Header(requestIDHeader, requestID)

		c.Next()
	}
}

func LoggerMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		requestID := GetRequestID(c.Request.Context())

		c.Next()
		logger.InfoContext(c.Request.Context(), "http request",
			"request_id", requestID,
			"method", c.Request.Method,
			"path", c.FullPath(),
			"status", c.Writer.Status(),
			"duration_ms", time.Since(started).Milliseconds(),
		)
	}
}

func GetRequestID(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDContextKey).(string)

	return requestID
}
