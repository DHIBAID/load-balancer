package services

import (
	"load-balancer/models"
	"log"
	"net"
	"time"
)

// Check if a backend service is up and available by a small ping, if request fails or times out,
// we assume it is dead and remove it from the pool of available backends.

func CheckHealth(pool models.BackendPool) {
	for i, backend := range pool.Addresses {
		// Make a TCP connection to the backend service, and send a newline byte
		// to check if it's alive
		// Using a timeout to avoid hanging if the backend is unresponsive
		conn, err := net.DialTimeout("tcp", backend, 2*time.Second) // 2 seconds timeout
		if err != nil {
			log.Printf("Backend %s is down: %v", backend, err)
			// Remove it from the pool of available backends
			if len(pool.Addresses) > 1 {
				// Ensure we don't remove the last backend
				pool.Addresses = append(pool.Addresses[:i], pool.Addresses[i+1:]...)
			} else if len(pool.Addresses) == 1 {
				// Exit, but log that all backends are down
				log.Printf("All backends are down! No available services.")
				return
			}
		} else {
			_, writeErr := conn.Write([]byte("\n"))
			if writeErr != nil {
				log.Printf("Failed to send newline to backend %s: %v", backend, writeErr)
			}
			log.Printf("Backend %s is healthy", backend)
			conn.Close() // Close the connection if it's successful
		}
	}
}
