package apperrors

import "errors"

var (
	ErrInvalidInput = errors.New("ErrInvalidInput")
	ErrNotFound     = errors.New("ErrNotFound")
)
