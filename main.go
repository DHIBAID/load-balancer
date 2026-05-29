package main

import (
	"bufio"
	"io"
	"load-balancer/config"
	"load-balancer/models"
	"load-balancer/services"
	"load-balancer/utils"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// pprof for performance profiling
	"net/http"
	_ "net/http/pprof"
)

const (
	msgNoBackend  = "no backend available"
	msgBackendErr = "backend error"
	defaultPoolSz = 8
)

var (
	readerPool = sync.Pool{New: func() any { return bufio.NewReader(strings.NewReader("")) }}
	writerPool = sync.Pool{New: func() any { return bufio.NewWriter(io.Discard) }}
)

func main() {
	// pprof server for performance profiling
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

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

	poolSize := cfg.BackendPoolSize
	if poolSize <= 0 {
		poolSize = defaultPoolSz
	}

	pool := models.NewBackendPool(poolSize, timeout)
	pool.ReplaceBackends(backends, nil)

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}
	log.Printf("tcp lb listening on %s", listenAddr)

	go func() {
		for {
			services.CheckHealth(pool)
			time.Sleep(10 * time.Second) // Check every 10 seconds
		}
	}()

	go services.StartMetricsServer(&cfg, pool)

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}

		go handleClient(clientConn, pool)
	}
}

func handleClient(c net.Conn, pool *models.BackendPool) {
	id, wm := pool.RegisterWorker()
	defer c.Close()
	defer pool.UnregisterWorker(id, wm)

	scanner := bufio.NewScanner(c)
	writer := bufio.NewWriter(c)
	for scanner.Scan() {
		line := scanner.Text()
		atomic.AddUint64(&wm.Requests, 1)
		atomic.AddUint64(&wm.BytesIn, uint64(len(line)))
		backend := pool.Pick()
		if backend == nil {
			if !writeClientLine(writer, wm, msgNoBackend) {
				return
			}
			atomic.AddUint64(&wm.Errors, 1)
			continue
		}

		bm := pool.BeginBackendRequest(backend.Addr)
		resp, err := forwardLine(pool, backend, line)
		pool.EndBackendRequest(backend.Addr, bm)
		if err != nil {
			log.Printf("backend %s error: %v", backend.Addr, err)
			if !writeClientLine(writer, wm, msgBackendErr) {
				return
			}
			atomic.AddUint64(&wm.Errors, 1)
			continue
		}

		if !writeClientLine(writer, wm, resp) {
			return
		}
	}

	if err := scanner.Err(); err != nil {
		atomic.AddUint64(&wm.Errors, 1)
	}
}

func forwardLine(pool *models.BackendPool, backend *models.Backend, line string) (string, error) {
	conn, err := pool.Acquire(backend)
	if err != nil {
		return "", err
	}

	reader := readerPool.Get().(*bufio.Reader)
	writer := writerPool.Get().(*bufio.Writer)
	reader.Reset(conn)
	writer.Reset(conn)
	usable := true

	defer func() {
		readerPool.Put(reader)
		writerPool.Put(writer)
		pool.Release(backend, conn, usable)
	}()

	if err := writeLine(writer, line); err != nil {
		usable = false
		return "", err
	}

	resp, err := reader.ReadString('\n')
	if err != nil {
		usable = false
		return "", err
	}

	return strings.TrimSuffix(resp, "\n"), nil
}

func writeClientLine(w *bufio.Writer, wm *models.WorkerMetrics, line string) bool {
	if err := writeLine(w, line); err != nil {
		atomic.AddUint64(&wm.Errors, 1)
		return false
	}
	atomic.AddUint64(&wm.BytesOut, uint64(len(line)+1))
	return true
}

func writeLine(w *bufio.Writer, line string) error {
	if _, err := w.WriteString(line); err != nil {
		return err
	}
	if err := w.WriteByte('\n'); err != nil {
		return err
	}
	return w.Flush()
}
