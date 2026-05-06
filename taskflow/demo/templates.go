package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/OpenNSW/nsw-task-flow/orchestrator"
)

// loadTemplates scans all *.json files in templatesDir and registers them in the registry.
func loadTemplates(registry *orchestrator.TaskTemplateRegistry, templatesDir string) error {
	pattern := filepath.Join(templatesDir, "*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("glob %s: %w", pattern, err)
	}
	if len(files) == 0 {
		log.Printf("[Registry] WARNING: no template files found in %s", templatesDir)
	}

	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		var entry orchestrator.TaskTemplateEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		if entry.TemplateID == "" {
			return fmt.Errorf("%s: missing required field 'template_id'", path)
		}
		registry.Register(entry)
		log.Printf("[Registry] Loaded template: %s (workflow=%s)", entry.TemplateID, entry.WorkflowID)
	}
	return nil
}
