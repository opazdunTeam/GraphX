package main

import (
	"fmt"
	"os"
	"os/exec"
)

const testDSN = "postgres://test_user:test_password@localhost:5433/graphx_test?sslmode=disable"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	composeArgs := []string{"compose", "-f", "docker-compose.test.yaml"}
	if err := execute("docker", append(composeArgs, "up", "-d", "--wait")...); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("start integration database: %w", err)
	}

	testCommand := exec.Command(
		"go",
		"test",
		"-tags=integration",
		"./internal/platform/repository/postgres",
		"-v",
	)
	testCommand.Env = append(os.Environ(), "POSTGRES_TEST_DSN="+testDSN)
	testCommand.Stdout = os.Stdout
	testCommand.Stderr = os.Stderr
	testErr := testCommand.Run()
	downErr := execute("docker", append(composeArgs, "down", "-v")...)

	if testErr != nil {
		return fmt.Errorf("run integration tests: %w", testErr)
	}
	if downErr != nil {
		return fmt.Errorf("remove integration database: %w", downErr)
	}
	return nil
}

func execute(name string, args ...string) error {
	command := exec.Command(name, args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	return command.Run()
}
