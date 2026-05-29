package services

import (
	"load-balancer/models"
	"log"
	"net"
	"time"
)

// Check if a backend service is up and available by a small ping, if request fails or times out,
// we assume it is dead and remove it from the pool of available backends.

func CheckHealth(pool *models.BackendPool) {
	active, failed := pool.SnapshotBackends()

	healthyNext := make([]string, 0, len(active))
	failedNext := make([]string, 0, len(active)+len(failed))
	seen := make(map[string]struct{}, len(active)+len(failed))

	for _, backend := range active {
		if _, ok := seen[backend]; ok {
			continue
		}
		seen[backend] = struct{}{}
		if checkBackend(backend) {
			healthyNext = append(healthyNext, backend)
		} else {
			failedNext = append(failedNext, backend)
			log.Printf("Backend %s is down", backend)
		}
	}

	for _, backend := range failed {
		if _, ok := seen[backend]; ok {
			continue
		}
		seen[backend] = struct{}{}
		if checkBackend(backend) {
			healthyNext = append(healthyNext, backend)
			log.Printf("Backend %s recovered", backend)
		} else {
			failedNext = append(failedNext, backend)
		}
	}

	pool.ReplaceBackends(healthyNext, failedNext)
}

func checkBackend(backend string) bool {
	conn, err := net.DialTimeout("tcp", backend, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("\n")); err != nil {
		return false
	}
	return true
}
