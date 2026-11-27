package ingestion

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

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
	if err := validateRawLocatorNumbers(rawPayload); err != nil {
		return err
	}
	if invalidPersistedText(result.AdapterVersion) ||
		invalidPersistedText(result.ParserVersion) ||
		invalidPersistedText(result.MappingVersion) ||
		invalidPersistedText(result.NormalizationVersion) {
		return errors.New("source result versions must not be blank")
	}
	artifactRefs, err := validateArtifactRefs(result.Artifacts)
	if err != nil {
		return err
	}
	if err := validateSourceArtifactKinds(result.Artifacts); err != nil {
		return err
	}
	if err := validateArtifactTransport(result.Artifacts, result.JobID, result.AttemptNo); err != nil {
		return err
	}
	if result.Manifest != nil {
		if _, exists := artifactRefs[result.Manifest.LocalRef]; exists {
			return errors.New("manifest and source artifact local_ref values must be unique")
		}
		allArtifacts := append(append([]ArtifactReference{}, result.Artifacts...), *result.Manifest)
		if _, err := validateArtifactRefs(allArtifacts); err != nil {
			return err
		}
		if err := validateArtifactTransport(allArtifacts, result.JobID, result.AttemptNo); err != nil {
			return err
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
	if err := validateRawLocatorNumbers(storedBytes); err != nil {
		return err
	}
	if !strings.EqualFold(result.JobID, manifest.JobID) || result.AttemptNo != manifest.AttemptNo ||
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
	if err := validateSourceArtifactKinds(artifacts); err != nil {
		return err
	}
	artifactRefs, err := validateArtifactRefs(artifacts)
	if err != nil {
		return err
	}

	recordRefs := make(map[string]struct{}, len(records))
	schemaVersionsByArtifact := make(map[string]string)
	for _, record := range records {
		if invalidPersistedText(record.LocalRef) ||
			invalidPersistedText(record.ArtifactLocalRef) ||
			invalidPersistedText(record.RecordType) ||
			(record.ExternalID != nil && invalidPersistedText(*record.ExternalID)) {
			return fmt.Errorf("record %q contains a blank required text value", record.LocalRef)
		}
		if _, exists := recordRefs[record.LocalRef]; exists {
			return fmt.Errorf("duplicate source record local_ref %q", record.LocalRef)
		}
		recordRefs[record.LocalRef] = struct{}{}
		if _, exists := artifactRefs[record.ArtifactLocalRef]; !exists {
			return fmt.Errorf("record %q references unknown artifact %q", record.LocalRef, record.ArtifactLocalRef)
		}
		if invalidPersistedText(record.SchemaVersion) {
			return fmt.Errorf("record %q has a blank schema_version", record.LocalRef)
		}
		if err := validateJSONBCompatible("raw_data", record.RawData); err != nil {
			return err
		}
		if schemaVersion, exists := schemaVersionsByArtifact[record.ArtifactLocalRef]; exists &&
			schemaVersion != record.SchemaVersion {
			return fmt.Errorf(
				"artifact %q contains records with different schema versions",
				record.ArtifactLocalRef,
			)
		}
		schemaVersionsByArtifact[record.ArtifactLocalRef] = record.SchemaVersion
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

func validateSourceArtifactKinds(artifacts []ArtifactReference) error {
	for _, artifact := range artifacts {
		if artifact.Kind == "manifest" {
			return errors.New("source artifacts cannot use manifest kind")
		}
	}
	return nil
}

func validateClaim(claim Claim) error {
	if invalidPersistedText(claim.SubjectLocalRef) ||
		invalidPersistedText(claim.Predicate) ||
		(claim.ObjectLocalRef != nil && invalidPersistedText(*claim.ObjectLocalRef)) {
		return errors.New("claim reference and predicate fields must not be blank")
	}
	if claim.EvidenceExcerpt != nil && strings.ContainsRune(*claim.EvidenceExcerpt, '\x00') {
		return errors.New("evidence_excerpt must not contain U+0000")
	}
	for name, value := range map[string]any{
		"raw_value": claim.RawValue, "normalized_value": claim.NormalizedValue,
		"qualifiers": claim.Qualifiers, "evidence_locator": claim.EvidenceLocator,
	} {
		if err := validateJSONBCompatible(name, value); err != nil {
			return err
		}
	}
	if err := validateEvidenceLocator(claim.EvidenceLocator); err != nil {
		return err
	}
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

func validateEvidenceLocator(locator map[string]any) error {
	jsonPointer, hasJSON := locator["json_pointer"]
	csvRow, hasCSV := locator["csv_row"]
	page, hasPage := locator["page_number"]
	fragment, hasFragment := locator["fragment"]
	if boolCount(hasJSON)+boolCount(hasCSV)+boolCount(hasPage || hasFragment) != 1 {
		return errors.New("evidence_locator must identify a JSON pointer, CSV row, or document page/fragment")
	}
	if hasJSON {
		pointer, valid := jsonPointer.(string)
		if !valid || !validJSONPointer(pointer) {
			return errors.New("evidence json_pointer must be a valid RFC 6901 pointer")
		}
		return nil
	}
	if hasCSV {
		if !positiveInteger(csvRow) {
			return errors.New("evidence csv_row must be a positive integer")
		}
		return nil
	}
	if hasPage && !positiveInteger(page) {
		return errors.New("evidence page_number must be a positive integer")
	}
	if hasFragment {
		text, ok := fragment.(string)
		if !ok || blankText(text) || strings.ContainsRune(text, '\x00') {
			return errors.New("evidence fragment must be a non-empty string")
		}
	}
	return nil
}

func validJSONPointer(value string) bool {
	if value == "" {
		return true
	}
	if !strings.HasPrefix(value, "/") {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] != '~' {
			continue
		}
		if index+1 >= len(value) || (value[index+1] != '0' && value[index+1] != '1') {
			return false
		}
		index++
	}
	return true
}

func positiveInteger(value any) bool {
	const maxSafeInteger = float64(1<<53 - 1)
	valid := func(number float64) bool {
		return number >= 1 && number <= maxSafeInteger && math.Trunc(number) == number
	}
	switch number := value.(type) {
	case int:
		return valid(float64(number))
	case float64:
		return valid(number)
	case json.Number:
		parsed, err := number.Float64()
		return err == nil && valid(parsed)
	default:
		return false
	}
}

func validateRawLocatorNumbers(document []byte) error {
	var payload struct {
		Claims []struct {
			EvidenceLocator map[string]json.RawMessage `json:"evidence_locator"`
		} `json:"claims"`
	}
	if err := json.Unmarshal(document, &payload); err != nil {
		return fmt.Errorf("decode locator number tokens: %w", err)
	}
	for _, claim := range payload.Claims {
		for _, field := range []string{"csv_row", "page_number"} {
			raw, exists := claim.EvidenceLocator[field]
			if !exists {
				continue
			}
			token := strings.TrimSpace(string(raw))
			if token == "" || token[0] == '"' || token == "null" {
				continue
			}
			if !exactPositiveSafeIntegerToken(token) {
				return fmt.Errorf("evidence %s must be an exact safe integer", field)
			}
		}
	}
	return nil
}

func exactPositiveSafeIntegerToken(token string) bool {
	exponent := 0
	if index := strings.IndexAny(token, "eE"); index >= 0 {
		parsed, err := strconv.Atoi(token[index+1:])
		if err != nil {
			return false
		}
		exponent = parsed
		token = token[:index]
	}
	if strings.HasPrefix(token, "-") {
		return false
	}
	fractionDigits := 0
	if index := strings.IndexByte(token, '.'); index >= 0 {
		fractionDigits = len(token) - index - 1
		token = token[:index] + token[index+1:]
	}
	digits := strings.TrimLeft(token, "0")
	if digits == "" {
		return false
	}
	if exponent < fractionDigits {
		if exponent < fractionDigits-len(digits) {
			return false
		}
		remove := fractionDigits - exponent
		if strings.Trim(digits[len(digits)-remove:], "0") != "" {
			return false
		}
		digits = strings.TrimLeft(digits[:len(digits)-remove], "0")
		if digits == "" {
			return false
		}
	} else {
		scale := exponent - fractionDigits
		if scale > 16 || len(digits)+scale > len("9007199254740991") {
			return false
		}
		digits += strings.Repeat("0", scale)
	}
	const maximum = "9007199254740991"
	return len(digits) < len(maximum) || len(digits) == len(maximum) && digits <= maximum
}

func invalidPersistedText(value string) bool {
	return blankText(value) || strings.ContainsRune(value, '\x00')
}

func blankText(value string) bool {
	return strings.TrimFunc(value, func(char rune) bool {
		return unicode.IsSpace(char) || unicode.IsControl(char)
	}) == ""
}

func validateJSONBCompatible(name string, value any) error {
	pending := []any{value}
	for len(pending) > 0 {
		last := len(pending) - 1
		item := pending[last]
		pending = pending[:last]
		switch item := item.(type) {
		case string:
			if strings.ContainsRune(item, '\x00') {
				return fmt.Errorf("%s must not contain U+0000", name)
			}
		case []any:
			pending = append(pending, item...)
		case map[string]any:
			for key, child := range item {
				pending = append(pending, key, child)
			}
		}
	}
	return nil
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func BuildProcessingRunInputs(
	result SourceResult,
	manifest *SourceResultManifest,
) ([]ProcessingRunInput, error) {
	var records []SourceRecord
	var claims []Claim
	if result.Manifest == nil {
		if manifest != nil {
			return nil, errors.New("inline result cannot use manifest content")
		}
		records = result.Records
		claims = result.Claims
	} else {
		if manifest == nil {
			return nil, errors.New("manifest result requires manifest content")
		}
		if !strings.EqualFold(result.JobID, manifest.JobID) || result.AttemptNo != manifest.AttemptNo ||
			result.RecordCount != manifest.RecordCount {
			return nil, errors.New("manifest metadata does not match completed result")
		}
		records = manifest.Records
		claims = manifest.Claims
	}
	if result.RecordCount != len(records) {
		return nil, errors.New("record_count must equal records length")
	}
	if err := validateContent(records, claims, result.Artifacts); err != nil {
		return nil, err
	}
	if invalidPersistedText(result.ParserVersion) ||
		invalidPersistedText(result.MappingVersion) ||
		invalidPersistedText(result.NormalizationVersion) {
		return nil, errors.New("processing run versions must not be empty")
	}

	byArtifact := make(map[string][]SourceRecord)
	for _, record := range records {
		byArtifact[record.ArtifactLocalRef] = append(byArtifact[record.ArtifactLocalRef], record)
	}
	runs := make([]ProcessingRunInput, 0, len(byArtifact))
	for _, artifact := range result.Artifacts {
		artifactRecords := byArtifact[artifact.LocalRef]
		if len(artifactRecords) == 0 {
			continue
		}
		schemaVersion := artifactRecords[0].SchemaVersion
		if invalidPersistedText(schemaVersion) {
			return nil, fmt.Errorf("artifact %q has an empty record schema version", artifact.LocalRef)
		}
		for _, record := range artifactRecords[1:] {
			if record.SchemaVersion != schemaVersion {
				return nil, fmt.Errorf(
					"artifact %q contains records with different schema versions",
					artifact.LocalRef,
				)
			}
		}
		runs = append(runs, ProcessingRunInput{
			ArtifactLocalRef: artifact.LocalRef, ProcessingScope: "source_collection",
			SchemaVersion: schemaVersion, ParserVersion: result.ParserVersion,
			MappingVersion:       result.MappingVersion,
			NormalizationVersion: result.NormalizationVersion,
			Records:              artifactRecords,
		})
	}
	return runs, nil
}

func validateArtifactRefs(artifacts []ArtifactReference) (map[string]int, error) {
	refs, _, err := orderArtifactRefs(artifacts)
	return refs, err
}

func OrderArtifactRefs(artifacts []ArtifactReference) ([]ArtifactReference, error) {
	_, ordered, err := orderArtifactRefs(artifacts)
	return ordered, err
}

func orderArtifactRefs(artifacts []ArtifactReference) (map[string]int, []ArtifactReference, error) {
	refs := make(map[string]int, len(artifacts))
	for index, artifact := range artifacts {
		if _, exists := refs[artifact.LocalRef]; exists {
			return nil, nil, fmt.Errorf("duplicate artifact local_ref %q", artifact.LocalRef)
		}
		refs[artifact.LocalRef] = index
	}
	const (
		visiting = iota + 1
		ordered
	)
	state := make([]uint8, len(artifacts))
	result := make([]ArtifactReference, 0, len(artifacts))
	var visit func(int) error
	visit = func(index int) error {
		switch state[index] {
		case ordered:
			return nil
		case visiting:
			return errors.New("artifact parent references cannot contain a cycle")
		}
		state[index] = visiting
		artifact := artifacts[index]
		if artifact.ParentLocalRef != nil {
			parent, exists := refs[*artifact.ParentLocalRef]
			if !exists {
				return fmt.Errorf("artifact %q references unknown parent %q", artifact.LocalRef, *artifact.ParentLocalRef)
			}
			if err := visit(parent); err != nil {
				return err
			}
		}
		result = append(result, artifact)
		state[index] = ordered
		return nil
	}
	for index := range artifacts {
		if err := visit(index); err != nil {
			return nil, nil, err
		}
	}
	return refs, result, nil
}

func validateArtifactTransport(artifacts []ArtifactReference, jobID string, attemptNo int) error {
	allowedMetadata := map[string]struct{}{
		"etag": {}, "last-modified": {}, "content-language": {},
	}
	for _, artifact := range artifacts {
		if invalidPersistedText(artifact.LocalRef) ||
			invalidPersistedText(artifact.MediaType) ||
			utf8.RuneCountInString(artifact.LocalRef) > 200 ||
			strings.IndexFunc(artifact.LocalRef, unicode.IsControl) >= 0 {
			return fmt.Errorf("artifact %q has an invalid local_ref", artifact.LocalRef)
		}
		encodedRef := base64.RawURLEncoding.EncodeToString([]byte(artifact.LocalRef))
		expectedKey := fmt.Sprintf("artifacts/v1/%s/%d/%s", strings.ToLower(jobID), attemptNo, encodedRef)
		if artifact.ObjectKey != expectedKey || len(artifact.ObjectKey) > 1024 {
			return fmt.Errorf("artifact %q has an invalid object_key", artifact.LocalRef)
		}
		parsed, err := url.Parse(artifact.SourceURI)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
			parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" ||
			parsed.Fragment != "" || utf8.RuneCountInString(artifact.SourceURI) > 2048 {
			return fmt.Errorf("artifact %q has an unsafe source_uri", artifact.LocalRef)
		}
		if portText := parsed.Port(); portText != "" {
			port, portErr := strconv.Atoi(portText)
			if portErr != nil || port < 1 || port > 65535 || portText != strconv.Itoa(port) {
				return fmt.Errorf("artifact %q has an unsafe source_uri port", artifact.LocalRef)
			}
		}
		for name, value := range artifact.HTTPMetadata {
			if _, allowed := allowedMetadata[name]; !allowed || value == "" ||
				utf8.RuneCountInString(value) > 512 || strings.ContainsAny(value, "\x00\r\n") {
				return fmt.Errorf("artifact %q has unsafe HTTP metadata", artifact.LocalRef)
			}
		}
		if artifact.OriginalFilename != nil {
			filename := *artifact.OriginalFilename
			if filename != strings.Trim(filename, " .") || filename == "" ||
				utf8.RuneCountInString(filename) > 255 || strings.ContainsAny(filename, "/\\") ||
				strings.IndexFunc(filename, unicode.IsControl) >= 0 {
				return fmt.Errorf("artifact %q has an unsafe original_filename", artifact.LocalRef)
			}
		}
	}
	return nil
}

func ValidateArtifactRefs(
	artifacts []ArtifactReference,
	jobID string,
	attemptNo int,
) error {
	if _, err := validateArtifactRefs(artifacts); err != nil {
		return err
	}
	return validateArtifactTransport(artifacts, jobID, attemptNo)
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
		return !blankText(value)
	case []any:
		return len(value) > 0
	case map[string]any:
		return len(value) > 0
	default:
		return true
	}
}
