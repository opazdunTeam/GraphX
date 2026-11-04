package utils

import (
	"GraphX/internal/platform/dto"

	"github.com/gin-gonic/gin"
)

func WriteProblem(c *gin.Context, status int, code string, title string, detail string) {
	c.Header("Content-Type", "application/problem+json")

	c.JSON(status, dto.ProblemResponse{
		Type:   "about:blank",
		Title:  title,
		Status: status,
		Code:   code,
		Detail: detail,
	})
}
