package orchestration

import "fmt"

type Status string

const (
	StatusPending        Status = "pending"
	StatusQueued         Status = "queued"
	StatusRunning        Status = "running"
	StatusRetryWait      Status = "retry_wait"
	StatusSucceeded      Status = "succeeded"
	StatusNotFound       Status = "not_found"
	StatusActionRequired Status = "action_required"
	StatusUnavailable    Status = "unavailable"
	StatusFailed         Status = "failed"
	StatusCancelled      Status = "cancelled"
)

var allowedTransitions = map[Status]map[Status]bool{
	StatusPending: {
		StatusQueued: true, StatusRunning: true, StatusRetryWait: true,
		StatusSucceeded: true, StatusNotFound: true, StatusActionRequired: true,
		StatusUnavailable: true, StatusFailed: true, StatusCancelled: true,
	},
	StatusQueued: {
		StatusRunning: true, StatusRetryWait: true, StatusSucceeded: true,
		StatusNotFound: true, StatusActionRequired: true, StatusUnavailable: true,
		StatusFailed: true, StatusCancelled: true,
	},
	StatusRunning: {
		StatusRetryWait: true, StatusSucceeded: true, StatusNotFound: true,
		StatusActionRequired: true, StatusUnavailable: true, StatusFailed: true,
		StatusCancelled: true,
	},
	StatusRetryWait:      {StatusPending: true, StatusCancelled: true},
	StatusActionRequired: {StatusPending: true, StatusFailed: true, StatusCancelled: true},
}

func IsTerminal(status Status) bool {
	switch status {
	case StatusSucceeded, StatusNotFound, StatusUnavailable, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

func CanTransition(from, to Status) bool {
	if from == to {
		return true
	}
	return allowedTransitions[from][to]
}

func AllowedTransition(from, to Status) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("invalid source job status transition: %s -> %s", from, to)
	}
	return nil
}

func ValidateAttemptTransition(from, to Status, oldAttempt, newAttempt int) error {
	if err := AllowedTransition(from, to); err != nil {
		return err
	}

	startsNewAttempt := (from == StatusRetryWait || from == StatusActionRequired) && to == StatusPending
	if startsNewAttempt {
		if newAttempt != oldAttempt+1 {
			return fmt.Errorf("a new source job attempt must increment attempt_no by one")
		}
		return nil
	}
	if newAttempt != oldAttempt {
		return fmt.Errorf("attempt_no may change only when a new attempt starts")
	}
	return nil
}
