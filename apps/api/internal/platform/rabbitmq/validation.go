package rabbitmq

import "github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"

func ValidateSourceCollectFailed(payload SourceCollectFailedPayload) error {
	return ingestion.ValidateArtifactRefs(payload.Artifacts, payload.JobID, payload.AttemptNo)
}
