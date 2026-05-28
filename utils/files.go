package utils

import (
	"errors"
	"fmt"
	"load-balancer/models"
	"os"
	"path/filepath"
)

func FindProjectRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get cwd: %w", err)
	}

	dir := cwd
	for {
		if exists(filepath.Join(dir, "go.mod")) || exists(filepath.Join(dir, ".git")) {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", errors.New("project root not found (missing go.mod or .git)")
}

func FindConfigFile(root string) (string, error) {
	configFile := filepath.Join(root, models.DefaultConfigYAML)

	if exists(configFile) {
		return configFile, nil
	}

	return "", fmt.Errorf("config file not found in %s", root)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
