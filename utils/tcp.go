package utils

import (
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
