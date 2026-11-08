package rabbitmq

import "github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"

func ValidateSourceCollectFailed(payload SourceCollectFailedPayload) error {
	return ingestion.ValidateUniqueArtifactRefs(payload.Artifacts)
}
