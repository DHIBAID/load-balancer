package models

const DefaultConfigYAML = "config.yaml"

type Config struct {
	Listen      string   `yaml:"listen"`
	Backends    []string `yaml:"backends"`
	DialTimeout string   `yaml:"dial_timeout"`
	MetricsPort string   `yaml:"metrics_port"`
}
