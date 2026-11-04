package middlewares

import (
	"GraphX/internal/platform/api/utils"
	"GraphX/internal/platform/consts"
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"time"
	"uuid"

	"github.com/gin-gonic/gin"
)

func RecoverMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				requestID := GetRequestID(c.Request.Context())

				logger.Error(
					"panic recovered",
					"request_id", requestID,
					"panic", recovered,
				)

				utils.WriteProblem(
					c,
					http.StatusInternalServerError,
					"internal_error",
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
		requestID := c.GetHeader(consts.RequestIDKey)

		if !requestIDPattern.MatchString(requestID) {
			requestID = uuid.New().String()
		}

		ctx := context.WithValue(
			c.Request.Context(),
			consts.RequestIDKey,
			requestID,
		)

		c.Request = c.Request.WithContext(ctx)

		// Вернуть ID клиенту в ответе.
		c.Header(consts.RequestIDKey, requestID)

		c.Next()
	}
}

func LoggerMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		traceID := GetRequestID(c.Request.Context())

		c.Next()
		logger.InfoContext(c.Request.Context(), "http request",
			"trace_id", traceID,
			"method", c.Request.Method,
			"path", c.FullPath(),
			"status", c.Writer.Status(),
			"duration_ms", time.Since(started).Milliseconds(),
		)
	}
}

func GetRequestID(ctx context.Context) string {
	requestID, _ := ctx.Value(consts.RequestIDKey).(string)

	return requestID
}
