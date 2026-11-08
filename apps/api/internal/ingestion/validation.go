package ingestion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/contractjson"
)

func ValidateSourceResult(result SourceResult, rawPayload []byte) error {
	digest, err := sourceResultDigest(rawPayload)
	if err != nil {
		return err
	}
	if digest != result.Digest {
		return errors.New("source result digest does not match RFC 8785 canonical payload")
	}
	artifactRefs, err := validateArtifactRefs(result.Artifacts)
	if err != nil {
		return err
	}
	if result.Manifest != nil {
		if _, exists := artifactRefs[result.Manifest.LocalRef]; exists {
			return errors.New("manifest and source artifact local_ref values must be unique")
		}
	}
	if result.Manifest == nil {
		if result.RecordCount != len(result.Records) {
			return errors.New("inline record_count must equal records length")
		}
		if err := validateContent(result.Records, result.Claims, result.Artifacts); err != nil {
			return err
		}
	}
	return nil
}

func ValidateManifest(
	result SourceResult,
	manifest SourceResultManifest,
	storedBytes []byte,
) error {
	if result.Manifest == nil {
		return errors.New("manifest result has no manifest reference")
	}
	actualHash := sha256.Sum256(storedBytes)
	if result.Manifest.ByteSize != int64(len(storedBytes)) ||
		result.Manifest.SHA256 != hex.EncodeToString(actualHash[:]) {
		return errors.New("manifest hash or byte size does not match stored object")
	}
	if result.JobID != manifest.JobID || result.AttemptNo != manifest.AttemptNo ||
		result.RecordCount != manifest.RecordCount {
		return errors.New("manifest metadata does not match completed result")
	}
	if manifest.RecordCount != len(manifest.Records) {
		return errors.New("manifest record_count must equal records length")
	}
	if err := validateContent(manifest.Records, manifest.Claims, result.Artifacts); err != nil {
		return err
	}
	return nil
}

func sourceResultDigest(rawPayload []byte) (string, error) {
	value, err := contractjson.Decode(rawPayload)
	if err != nil {
		return "", fmt.Errorf("validate source result JSON: %w", err)
	}
	payload, ok := value.(map[string]any)
	if !ok {
		return "", errors.New("source result payload must be a JSON object")
	}
	// json.Number values survive re-encoding; only digest is removed before RFC 8785.
	delete(payload, "digest")
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode completed payload for digest: %w", err)
	}
	canonical, err := contractjson.Canonicalize(encodedPayload)
	if err != nil {
		return "", fmt.Errorf("canonicalize completed payload: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func validateContent(
	records []SourceRecord,
	claims []Claim,
	artifacts []ArtifactReference,
) error {
	artifactRefs, err := validateArtifactRefs(artifacts)
	if err != nil {
		return err
	}

	recordRefs := make(map[string]struct{}, len(records))
	for _, record := range records {
		if _, exists := recordRefs[record.LocalRef]; exists {
			return fmt.Errorf("duplicate source record local_ref %q", record.LocalRef)
		}
		recordRefs[record.LocalRef] = struct{}{}
		if _, exists := artifactRefs[record.ArtifactLocalRef]; !exists {
			return fmt.Errorf("record %q references unknown artifact %q", record.LocalRef, record.ArtifactLocalRef)
		}
	}

	for _, claim := range claims {
		if _, exists := recordRefs[claim.SubjectLocalRef]; !exists {
			return fmt.Errorf("claim subject references unknown record %q", claim.SubjectLocalRef)
		}
		if claim.ObjectLocalRef != nil {
			if _, exists := recordRefs[*claim.ObjectLocalRef]; !exists {
				return fmt.Errorf("claim object references unknown record %q", *claim.ObjectLocalRef)
			}
		}
		if err := validateClaim(claim); err != nil {
			return err
		}
	}
	return nil
}

func validateClaim(claim Claim) error {
	switch claim.ClaimKind {
	case "relationship":
		if claim.ObjectLocalRef == nil {
			return errors.New("relationship claim requires object_local_ref")
		}
	case "attribute":
		if claim.ObjectLocalRef != nil {
			return errors.New("attribute claim forbids object_local_ref")
		}
		if !meaningful(claim.RawValue) && !meaningful(claim.NormalizedValue) {
			return errors.New("attribute claim requires a meaningful value")
		}
	case "event":
		if claim.ObjectLocalRef == nil && !meaningful(claim.RawValue) && !meaningful(claim.NormalizedValue) {
			return errors.New("event claim requires an object or meaningful value")
		}
	}
	if err := validateBoundary("valid_from", claim.ValidFrom, claim.ValidFromPrecision); err != nil {
		return err
	}
	if err := validateBoundary("valid_to", claim.ValidTo, claim.ValidToPrecision); err != nil {
		return err
	}
	if claim.ValidFrom != nil && claim.ValidTo != nil &&
		claim.ValidFromPrecision != "approximate" && claim.ValidToPrecision != "approximate" {
		from, _ := time.Parse(time.DateOnly, *claim.ValidFrom)
		to, _ := time.Parse(time.DateOnly, *claim.ValidTo)
		latestTo := latestPossibleDate(to, claim.ValidToPrecision)
		if !latestTo.After(from) {
			return errors.New("valid_to cannot be later than valid_from")
		}
	}
	if claim.TemporalBasis == "unknown" &&
		(claim.ValidFrom != nil || claim.ValidTo != nil || claim.SnapshotAt != nil) {
		return errors.New("unknown temporal_basis forbids temporal values")
	}
	if claim.TemporalBasis == "snapshot" {
		if claim.SnapshotAt == nil || claim.ValidFrom != nil || claim.ValidTo != nil ||
			claim.ValidFromPrecision != "unknown" || claim.ValidToPrecision != "unknown" {
			return errors.New("snapshot temporal form is invalid")
		}
	} else if claim.SnapshotAt != nil {
		return errors.New("snapshot_at is only valid for snapshot claims")
	}
	return nil
}

func validateArtifactRefs(artifacts []ArtifactReference) (map[string]struct{}, error) {
	refs := make(map[string]struct{}, len(artifacts))
	for _, artifact := range artifacts {
		if _, exists := refs[artifact.LocalRef]; exists {
			return nil, fmt.Errorf("duplicate artifact local_ref %q", artifact.LocalRef)
		}
		refs[artifact.LocalRef] = struct{}{}
	}
	return refs, nil
}

// ValidateUniqueArtifactRefs enforces result-scoped artifact identities for completed and failed results.
func ValidateUniqueArtifactRefs(artifacts []ArtifactReference) error {
	_, err := validateArtifactRefs(artifacts)
	return err
}

func latestPossibleDate(anchor time.Time, precision string) time.Time {
	switch precision {
	case "year":
		return time.Date(anchor.Year(), time.December, 31, 0, 0, 0, 0, time.UTC)
	case "month":
		return time.Date(anchor.Year(), anchor.Month()+1, 0, 0, 0, 0, 0, time.UTC)
	default:
		return anchor
	}
}

func validateBoundary(name string, value *string, precision string) error {
	if precision == "unknown" {
		if value != nil {
			return fmt.Errorf("%s must be null when precision is unknown", name)
		}
		return nil
	}
	if value == nil {
		return fmt.Errorf("%s is required when precision is known", name)
	}
	date, err := time.Parse(time.DateOnly, *value)
	if err != nil {
		return fmt.Errorf("parse %s: %w", name, err)
	}
	if precision == "month" && date.Day() != 1 {
		return fmt.Errorf("%s month precision must use day 1 anchor", name)
	}
	if precision == "year" && (date.Month() != time.January || date.Day() != 1) {
		return fmt.Errorf("%s year precision must use January 1 anchor", name)
	}
	return nil
}

func meaningful(value any) bool {
	switch value := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(value) != ""
	case []any:
		return len(value) > 0
	case map[string]any:
		return len(value) > 0
	default:
		return true
	}
}
