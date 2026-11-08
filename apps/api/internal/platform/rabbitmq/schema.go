package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/opazdunTeam/GraphX/apps/api/internal/contractjson"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const manifestSchemaID = "https://graphx.local/schemas/ingestion/source-result-manifest.v1.schema.json"

var messageSchemaIDs = map[string]string{
	SourceCollectRequestedType: "https://graphx.local/schemas/messages/source-collect-requested.v1.schema.json",
	SourceCollectCompletedType: "https://graphx.local/schemas/messages/source-collect-completed.v1.schema.json",
	SourceCollectFailedType:    "https://graphx.local/schemas/messages/source-collect-failed.v1.schema.json",
}

type SchemaRegistry struct {
	messages map[string]*jsonschema.Schema
	manifest *jsonschema.Schema
}

func NewSchemaRegistry(schemaRoot string) (*SchemaRegistry, error) {
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()

	err := filepath.WalkDir(schemaRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		value, err := decodeJSON(data)
		if err != nil {
			return fmt.Errorf("decode schema %s: %w", path, err)
		}
		document, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("schema %s must be a JSON object", path)
		}
		id, ok := document["$id"].(string)
		if !ok || id == "" {
			return fmt.Errorf("schema %s has no $id", path)
		}
		if err := compiler.AddResource(id, document); err != nil {
			return fmt.Errorf("register schema %s: %w", path, err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load contract schemas: %w", err)
	}

	registry := &SchemaRegistry{messages: make(map[string]*jsonschema.Schema)}
	for messageType, id := range messageSchemaIDs {
		schema, err := compiler.Compile(id)
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", messageType, err)
		}
		registry.messages[messageType] = schema
	}
	registry.manifest, err = compiler.Compile(manifestSchemaID)
	if err != nil {
		return nil, fmt.Errorf("compile manifest schema: %w", err)
	}
	return registry, nil
}

func (r *SchemaRegistry) ValidateMessage(raw []byte) (ValidatedMessage, error) {
	value, err := decodeJSON(raw)
	if err != nil {
		return ValidatedMessage{}, err
	}
	header, ok := value.(map[string]any)
	if !ok {
		return ValidatedMessage{}, errors.New("message must be a JSON object")
	}
	messageType, ok := header["type"].(string)
	if !ok {
		return ValidatedMessage{}, errors.New("message type must be a string")
	}
	version, ok := header["schema_version"].(json.Number)
	numericVersion, versionErr := version.Float64()
	if !ok || versionErr != nil || numericVersion != 1 {
		return ValidatedMessage{}, fmt.Errorf("unsupported schema version for %q", messageType)
	}
	schema, ok := r.messages[messageType]
	if !ok {
		return ValidatedMessage{}, fmt.Errorf("unsupported message type %q", messageType)
	}
	if err := schema.Validate(value); err != nil {
		return ValidatedMessage{}, fmt.Errorf("validate %s: %w", messageType, err)
	}
	var wireEnvelope struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &wireEnvelope); err != nil {
		return ValidatedMessage{}, fmt.Errorf("decode validated message payload: %w", err)
	}
	return ValidatedMessage{
		Type: messageType, SchemaVersion: 1, Raw: raw, Payload: wireEnvelope.Payload,
	}, nil
}

func (r *SchemaRegistry) ValidateManifest(raw []byte) error {
	value, err := decodeJSON(raw)
	if err != nil {
		return err
	}
	if err := r.manifest.Validate(value); err != nil {
		return fmt.Errorf("validate source result manifest: %w", err)
	}
	return nil
}

func decodeJSON(raw []byte) (any, error) {
	return contractjson.Decode(raw)
}
