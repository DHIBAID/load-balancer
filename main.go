package main

import (
	"bufio"
	"load-balancer/config"
	"load-balancer/models"
	"load-balancer/services"
	"load-balancer/utils"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	cfg, cfgPath, err := config.ReadConfig()
	if err != nil {
		log.Fatalf("read config error: %v", err)
	}

	listenAddr := cfg.Listen
	if listenAddr == "" {
		log.Fatalf("missing listen address in config: %s", cfgPath)
	}

	timeout := utils.ParseDuration(cfg.DialTimeout)
	if timeout == 0 {
		timeout = 3 * time.Second
	}

	backends := utils.NormalizeBackends(cfg.Backends)
	if len(backends) == 0 {
		log.Fatalf("missing backends in config: %s", cfgPath)
	}

	pool := &models.BackendPool{}
	pool.InitBackends(backends)

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}
	log.Printf("tcp lb listening on %s", listenAddr)

	// Start a goroutine to periodically check the health of backends
	go func() {
		for {
			services.CheckHealth(pool)
			time.Sleep(10 * time.Second) // Check every 10 seconds
		}
	}()

	// Spawn HTTP server for metrics
	go func() {
		services.StartMetricsServer(&cfg, pool)
	}()

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}

		go handleClient(clientConn, pool, timeout)
	}

}

func handleClient(c net.Conn, pool *models.BackendPool, timeout time.Duration) {
	id, wm := pool.RegisterWorker()
	defer c.Close()
	defer pool.UnregisterWorker(id, wm)

	scanner := bufio.NewScanner(c)
	for scanner.Scan() {
		line := scanner.Text()
		atomic.AddUint64(&wm.Requests, 1)
		atomic.AddUint64(&wm.BytesIn, uint64(len(line)))
		backendAddr := pool.Pick()
		if backendAddr == "" {
			_, _ = c.Write([]byte("no backend available\n"))
			atomic.AddUint64(&wm.Errors, 1)
			continue
		}

		resp, err := forwardLine(backendAddr, timeout, line)
		if err != nil {
			log.Printf("dial %s error: %v", backendAddr, err)
			_, _ = c.Write([]byte("backend error\n"))
			atomic.AddUint64(&wm.Errors, 1)
			continue
		}

		_, _ = c.Write([]byte(resp + "\n"))
		atomic.AddUint64(&wm.BytesOut, uint64(len(resp)+1))
	}

	if err := scanner.Err(); err != nil {
		atomic.AddUint64(&wm.Errors, 1)
	}
}

func forwardLine(backendAddr string, timeout time.Duration, line string) (string, error) {
	conn, err := net.DialTimeout("tcp", backendAddr, timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	_, _ = reader.ReadString('\n')

	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	if _, err := conn.Write([]byte(line)); err != nil {
		return "", err
	}

	resp, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimSuffix(resp, "\n"), nil
}
