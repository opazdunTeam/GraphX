package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/rabbitmq"
)

func main() {
	if err := run(filepath.Join("..", "..", "contracts")); err != nil {
		fmt.Fprintln(os.Stderr, "contract check failed:", err)
		os.Exit(1)
	}
	fmt.Println("contract schemas and examples are valid")
}

func run(contractsRoot string) error {
	registry, err := rabbitmq.NewSchemaRegistry(filepath.Join(contractsRoot, "schemas"))
	if err != nil {
		return err
	}
	ingestionPaths, err := filepath.Glob(filepath.Join(contractsRoot, "examples", "ingestion", "*.json"))
	if err != nil {
		return err
	}
	if len(ingestionPaths) == 0 {
		return fmt.Errorf("no ingestion examples found")
	}
	for _, path := range ingestionPaths {
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		switch filepath.Base(path) {
		case "source-result-manifest.v1.json":
			if validateErr := registry.ValidateManifest(raw); validateErr != nil {
				return fmt.Errorf("%s: %w", path, validateErr)
			}
		default:
			return fmt.Errorf("no validator registered for ingestion example %s", path)
		}
	}
	paths, err := filepath.Glob(filepath.Join(contractsRoot, "examples", "messages", "*.json"))
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("no message examples found")
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		message, err := registry.ValidateMessage(raw)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		switch message.Type {
		case rabbitmq.SourceCollectCompletedType:
			completed, decodeErr := rabbitmq.Decode[ingestion.SourceResult](
				message, rabbitmq.SourceCollectCompletedType,
			)
			if decodeErr != nil {
				return fmt.Errorf("%s: %w", path, decodeErr)
			}
			if validateErr := ingestion.ValidateSourceResult(
				completed.Payload, message.Payload,
			); validateErr != nil {
				return fmt.Errorf("%s: %w", path, validateErr)
			}
			if completed.Payload.Manifest == nil {
				continue
			}
			manifestPath := filepath.Join(
				contractsRoot, "examples", "ingestion", "source-result-manifest.v1.json",
			)
			manifest, err := os.ReadFile(manifestPath)
			if err != nil {
				return err
			}
			if err := registry.ValidateManifest(manifest); err != nil {
				return fmt.Errorf("%s: %w", manifestPath, err)
			}
			var manifestDocument ingestion.SourceResultManifest
			if err := json.Unmarshal(manifest, &manifestDocument); err != nil {
				return fmt.Errorf("%s: decode manifest: %w", manifestPath, err)
			}
			if err := ingestion.ValidateManifest(
				completed.Payload, manifestDocument, manifest,
			); err != nil {
				return fmt.Errorf("%s: %w", manifestPath, err)
			}
		case rabbitmq.SourceCollectFailedType:
			failed, decodeErr := rabbitmq.Decode[rabbitmq.SourceCollectFailedPayload](
				message, rabbitmq.SourceCollectFailedType,
			)
			if decodeErr != nil {
				return fmt.Errorf("%s: %w", path, decodeErr)
			}
			if validateErr := rabbitmq.ValidateSourceCollectFailed(failed.Payload); validateErr != nil {
				return fmt.Errorf("%s: %w", path, validateErr)
			}
		}
	}
	return nil
}
