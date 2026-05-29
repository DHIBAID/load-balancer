package services

import (
	"encoding/json"
	"load-balancer/models"
	"log"
	"net/http"
)

func SendMetrics(config *models.Config, pool *models.BackendPool, w http.ResponseWriter, r *http.Request) {
	_ = config
	_ = r

	metrics := pool.SnapshotMetrics()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		log.Printf("encode metrics error: %v", err)
	}
}

func StartMetricsServer(config *models.Config, pool *models.BackendPool) {
	// Create a new net/http server and listen on config.MetricsPort for incoming requests to /metrics
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		SendMetrics(config, pool, w, r)
	})

	if err := http.ListenAndServe(config.MetricsPort, mux); err != nil {
		log.Fatalf("failed to start metrics server: %v", err)
	}

}
