package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

func ensureJobGraphDB(ctx context.Context, opts options) error {
	_ = ctx
	dbPath := filepath.Join("db", "job_graph.db")
	if _, err := os.Stat(dbPath); err == nil {
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("stat %s failed: %w", dbPath, err)
	}

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return fmt.Errorf("create db dir failed: %w", err)
	}

	if _, err := os.Stat("classification.json"); err != nil {
		return fmt.Errorf("classification.json not found: %w", err)
	}

	log.Printf("[JOBGRAPH] %s missing, rebuilding via cmd/jobgraph...", dbPath)
	cmd := exec.Command("go", "run", "./cmd/jobgraph", "-input", "classification.json", "-db", dbPath, "-top", "10", "-emb-model", opts.embModel)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("rebuild job graph db failed: %w", err)
	}

	if _, err := os.Stat(dbPath); err != nil {
		return fmt.Errorf("job graph db still missing after rebuild: %w", err)
	}
	return nil
}
