package rabbitmq

import (
	"testing"

	"github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"
)

func TestFailedResultRejectsDuplicateArtifactRefs(t *testing.T) {
	payload := SourceCollectFailedPayload{Artifacts: []ingestion.ArtifactReference{
		{LocalRef: "artifact:1"},
		{LocalRef: "artifact:1"},
	}}
	if err := ValidateSourceCollectFailed(payload); err == nil {
		t.Fatal("expected duplicate artifact local_ref error")
	}
}
