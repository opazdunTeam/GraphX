package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	generatorModule = "github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.2"
	configPath      = "oapi-codegen.yaml"
	specPath        = "../../contracts/openapi.yaml"
	generatedPath   = "internal/platform/api/generated/openapi.gen.go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Println("generated Go API is current and preserves the required contract shape")
}

func run() error {
	if _, err := os.Stat(configPath); err != nil {
		return fmt.Errorf("run check-api from apps/api: %w", err)
	}

	tempDir, err := os.MkdirTemp("", "graphx-openapi-check-")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	tempOutput := filepath.Join(tempDir, "openapi.gen.go")
	command := exec.Command(
		"go", "run", generatorModule,
		"--config", configPath,
		"--o", tempOutput,
		specPath,
	)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("generate Go API: %w\n%s", err, output)
	}

	actual, err := os.ReadFile(generatedPath)
	if err != nil {
		return fmt.Errorf("read committed generated API: %w", err)
	}
	expected, err := os.ReadFile(tempOutput)
	if err != nil {
		return fmt.Errorf("read regenerated API: %w", err)
	}
	if !bytes.Equal(actual, expected) {
		return fmt.Errorf("%s is stale; run make generate-api", generatedPath)
	}

	generated := string(actual)
	assertions := map[string]bool{
		"health operations are excluded": !strings.Contains(generated, "GetLiveness(") &&
			!strings.Contains(generated, "GetReadiness(") &&
			!strings.Contains(generated, "/health/live"),
		"business routes are generated": strings.Contains(generated, `options.BaseURL+"/investigations"`) &&
			strings.Contains(generated, `options.BaseURL+"/sources"`) &&
			strings.Contains(generated, `options.BaseURL+"/artifacts/:artifact_id/content"`),
		"subject union helpers are generated": strings.Contains(generated, "AsNamedSubjectInput()") &&
			strings.Contains(generated, "AsIdentifiedSubjectInput()"),
		"request limits remain optional": strings.Contains(generated, "Depth *int") &&
			strings.Contains(generated, "EntityLimit *int") &&
			strings.Contains(generated, "`json:\"depth,omitempty\"`") &&
			strings.Contains(generated, "`json:\"entity_limit,omitempty\"`"),
	}
	for name, passed := range assertions {
		if !passed {
			return fmt.Errorf("generated Go API assertion failed: %s", name)
		}
	}

	return nil
}
