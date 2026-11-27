package postgres

import (
	"fmt"
	"testing"

	"github.com/opazdunTeam/GraphX/apps/api/internal/orchestration"
)

func TestValidateOutboxMessage(t *testing.T) {
	tests := []struct {
		name      string
		message   orchestration.OutboxMessage
		jobID     string
		attemptNo int
		wantError bool
	}{
		{name: "complete", message: testOutboxMessage{id: "message", messageType: "type", aggregateID: "job", operation: "search_subject", attemptNo: 2, payloadJSON: `{}`}, jobID: "job", attemptNo: 2},
		{name: "nil", jobID: "job", attemptNo: 2, wantError: true},
		{name: "missing id", message: testOutboxMessage{messageType: "type", aggregateID: "job", operation: "search_subject", attemptNo: 2, payloadJSON: `{}`}, jobID: "job", attemptNo: 2, wantError: true},
		{name: "missing type", message: testOutboxMessage{id: "message", aggregateID: "job", operation: "search_subject", attemptNo: 2, payloadJSON: `{}`}, jobID: "job", attemptNo: 2, wantError: true},
		{name: "missing operation", message: testOutboxMessage{id: "message", messageType: "type", aggregateID: "job", attemptNo: 2, payloadJSON: `{}`}, jobID: "job", attemptNo: 2, wantError: true},
		{name: "missing payload", message: testOutboxMessage{id: "message", messageType: "type", aggregateID: "job", operation: "search_subject", attemptNo: 2}, jobID: "job", attemptNo: 2, wantError: true},
		{name: "wrong aggregate", message: testOutboxMessage{id: "message", messageType: "type", aggregateID: "other", operation: "search_subject", attemptNo: 2, payloadJSON: `{}`}, jobID: "job", attemptNo: 2, wantError: true},
		{name: "wrong attempt", message: testOutboxMessage{id: "message", messageType: "type", aggregateID: "job", operation: "search_subject", attemptNo: 1, payloadJSON: `{}`}, jobID: "job", attemptNo: 2, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateOutboxMessage(test.message, test.jobID, test.attemptNo)
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, wantError %t", err, test.wantError)
			}
		})
	}
}

type testOutboxMessage struct {
	id              string
	messageType     string
	aggregateID     string
	investigationID string
	operation       string
	attemptNo       int
	payloadJSON     string
}

func (m testOutboxMessage) OutboxID() string        { return m.id }
func (m testOutboxMessage) MessageType() string     { return m.messageType }
func (m testOutboxMessage) AggregateID() string     { return m.aggregateID }
func (m testOutboxMessage) InvestigationID() string { return m.investigationID }
func (m testOutboxMessage) Operation() string       { return m.operation }
func (m testOutboxMessage) AttemptNo() int          { return m.attemptNo }
func (m testOutboxMessage) JSONPayload() string     { return m.payloadJSON }

func sourceCollectRequestedOutboxMessage(
	messageID, jobID, investigationID string,
	attemptNo int,
) orchestration.OutboxMessage {
	payloadJSON := fmt.Sprintf(`{
		"message_id":%q,
		"type":"source.collect.requested.v1",
		"schema_version":1,
		"occurred_at":"2026-11-19T12:00:00Z",
		"correlation_id":%q,
		"causation_id":null,
		"producer":"graphx-api",
		"root_command_id":%q,
		"payload":{
			"job_id":%q,
			"attempt_no":%d,
			"investigation_id":%q,
			"source":"fixture",
			"operation":"search_subject",
			"purpose":"subject_discovery",
			"query":{"type":"person","name":"Ada Lovelace"},
			"limits":{"max_records":20,"max_bytes":1048576,"timeout_seconds":30},
			"deadline":"2026-11-19T12:00:30Z"
		}
	}`, messageID, investigationID, messageID, jobID, attemptNo, investigationID)
	return testOutboxMessage{
		id:              messageID,
		messageType:     "source.collect.requested.v1",
		aggregateID:     jobID,
		investigationID: investigationID,
		operation:       "search_subject",
		attemptNo:       attemptNo,
		payloadJSON:     payloadJSON,
	}
}
