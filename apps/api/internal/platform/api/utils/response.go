package utils

import (
	"github.com/gin-gonic/gin"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
)

func WriteProblem(c *gin.Context, status int, code string, title string, detail string) {
	c.Header("Content-Type", "application/problem+json")

	c.JSON(status, generated.Problem{
		Type:   "about:blank",
		Title:  title,
		Status: status,
		Code:   code,
		Detail: detail,
	})
}
