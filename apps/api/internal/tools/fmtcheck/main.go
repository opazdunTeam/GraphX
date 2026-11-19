package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func main() {
	output, err := exec.Command("gofmt", "-l", "./cmd", "./internal").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "run gofmt: %v\n%s", err, output)
		os.Exit(1)
	}

	if files := strings.TrimSpace(string(output)); files != "" {
		fmt.Fprintf(os.Stderr, "Go files require formatting:\n%s\n", files)
		os.Exit(1)
	}
}
