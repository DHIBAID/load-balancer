package config

import (
	"fmt"
	"load-balancer/models"
	"load-balancer/utils"
	"os"

	"gopkg.in/yaml.v3"
)

// ReadConfig loads a YAML config file from the project root directory.
// It searches for config.yaml and config.yml in that order.
func ReadConfig() (models.Config, string, error) {
	root, err := utils.FindProjectRoot()
	if err != nil {
		return models.Config{}, "", err
	}

	cfgPath, err := utils.FindConfigFile(root)
	if err != nil {
		return models.Config{}, "", err
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return models.Config{}, "", fmt.Errorf("read config: %w", err)
	}

	var cfg models.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return models.Config{}, "", fmt.Errorf("parse yaml: %w", err)
	}

	return cfg, cfgPath, nil
}
