package maperrors

import (
	"GraphX/internal/platform/api/utils"
	"GraphX/internal/platform/apperrors"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

type ErrorHandler struct {
	logger *slog.Logger
}

func HandleError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, apperrors.ErrNotFound):
		utils.WriteProblem(
			c,
			http.StatusNotFound,
			"NOT_FOUND",
			"Resource not found",
			"The requested resource does not exist.",
		)

	case errors.Is(err, apperrors.ErrInvalidInput):
		utils.WriteProblem(
			c,
			http.StatusBadRequest,
			"INVALID_INPUT",
			"Invalid input",
			"The request contains invalid data.",
		)

	default:
		utils.WriteProblem(
			c,
			http.StatusInternalServerError,
			"INTERNAL_ERROR",
			"Internal server error",
			"An unexpected error occurred.",
		)
	}
}
