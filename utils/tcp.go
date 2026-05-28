package utils

import (
	"load-balancer/models"
	"net/url"
	"time"
)

func NormalizeBackends(backends []string) []string {
	normalized := make([]string, 0, len(backends))
	for _, backend := range backends {
		addr := backend
		if u, err := url.Parse(backend); err == nil && u.Host != "" {
			addr = u.Host
		}
		normalized = append(normalized, addr)
	}
	return normalized
}

func GetString(cfg models.Config, key string) string {
	if v, ok := cfg[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func GetStringSlice(cfg models.Config, key string) []string {
	value, ok := cfg[key]
	if !ok {
		return nil
	}

	if items, ok := value.([]string); ok {
		return items
	}

	items, ok := value.([]any)
	if !ok {
		return nil
	}

	result := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			result = append(result, s)
		}
	}

	return result
}

func ParseDuration(value string) time.Duration {
	if value == "" {
		return 0
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return parsed
}
