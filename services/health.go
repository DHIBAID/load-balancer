package services

import (
	"load-balancer/models"
	"log"
	"net"
	"time"
)

func CheckHealth(pool *models.BackendPool) {
	active, failed := pool.SnapshotBackends()

	isUp := func(backend string) bool {
		conn, err := net.DialTimeout("tcp", backend, 2*time.Second)
		if err != nil {
			return false
		}
		defer conn.Close()
		_, err = conn.Write([]byte("\n"))
		return err == nil
	}

	healthy := make([]string, 0, len(active))
	failedNext := make([]string, 0, len(active)+len(failed))
	seen := make(map[string]struct{}, len(active)+len(failed))

	check := func(list []string, onUp, onDown func(string)) {
		for _, backend := range list {
			if _, ok := seen[backend]; ok {
				continue
			}
			seen[backend] = struct{}{}
			if isUp(backend) {
				onUp(backend)
			} else {
				onDown(backend)
			}
		}
	}

	check(active,
		func(b string) { healthy = append(healthy, b) },
		func(b string) { failedNext = append(failedNext, b); log.Printf("Backend %s is down", b) },
	)
	check(failed,
		func(b string) { healthy = append(healthy, b); log.Printf("Backend %s recovered", b) },
		func(b string) { failedNext = append(failedNext, b) },
	)

	pool.ReplaceBackends(healthy, failedNext)
}
