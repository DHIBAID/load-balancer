package services

import (
	"encoding/json"
	"load-balancer/models"
	"log"
	"net/http"
)

func StartMetricsServer(config *models.Config, pool *models.BackendPool) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(pool.SnapshotMetrics()); err != nil {
			log.Printf("encode metrics error: %v", err)
		}
	})
	if err := http.ListenAndServe(config.MetricsPort, mux); err != nil {
		log.Fatalf("failed to start metrics server: %v", err)
	}
}
