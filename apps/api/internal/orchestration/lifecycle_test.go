package orchestration

import "testing"

func TestDocumentedStatusTransitionMatrix(t *testing.T) {
	statuses := []Status{
		StatusPending, StatusQueued, StatusRunning, StatusRetryWait,
		StatusSucceeded, StatusNotFound, StatusActionRequired,
		StatusUnavailable, StatusFailed, StatusCancelled,
	}
	allowed := map[Status]map[Status]bool{
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

	for _, from := range statuses {
		for _, to := range statuses {
			if from == to {
				continue
			}
			want := allowed[from][to]
			if got := CanTransition(from, to); got != want {
				t.Errorf("%s -> %s: got %t, want %t", from, to, got, want)
			}
		}
	}
}

func TestTerminalStatesCannotTransition(t *testing.T) {
	for _, terminal := range []Status{
		StatusSucceeded, StatusNotFound, StatusUnavailable, StatusFailed, StatusCancelled,
	} {
		if !IsTerminal(terminal) {
			t.Errorf("%s is not terminal", terminal)
		}
		for _, next := range []Status{StatusPending, StatusQueued, StatusRunning, StatusRetryWait} {
			if CanTransition(terminal, next) {
				t.Errorf("terminal %s unexpectedly transitions to %s", terminal, next)
			}
		}
	}
}

func TestNewAttemptMustIncrementOnce(t *testing.T) {
	if err := ValidateAttemptTransition(StatusRetryWait, StatusPending, 1, 2); err != nil {
		t.Fatalf("valid retry rejected: %v", err)
	}
	if err := ValidateAttemptTransition(StatusActionRequired, StatusPending, 2, 3); err != nil {
		t.Fatalf("valid action-required retry rejected: %v", err)
	}
	for _, test := range []struct {
		from, to  Status
		old, next int
	}{
		{StatusRetryWait, StatusPending, 1, 1},
		{StatusRetryWait, StatusPending, 1, 3},
		{StatusRunning, StatusSucceeded, 2, 3},
	} {
		if err := ValidateAttemptTransition(test.from, test.to, test.old, test.next); err == nil {
			t.Errorf("%s -> %s attempt %d -> %d was accepted", test.from, test.to, test.old, test.next)
		}
	}
}
