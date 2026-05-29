package main

import (
	"bufio"
	"bytes"
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
	readerPool     = sync.Pool{New: func() any { return bufio.NewReader(strings.NewReader("")) }}
	writerPool     = sync.Pool{New: func() any { return bufio.NewWriter(io.Discard) }}
	lineBufferPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
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

	clientReader := bufio.NewReaderSize(c, 64*1024)
	clientWriter := bufio.NewWriter(c)
	for {
		line, err := clientReader.ReadSlice('\n')
		if err != nil {
			if err == bufio.ErrBufferFull {
				buf := lineBufferPool.Get().(*bytes.Buffer)
				buf.Reset()
				buf.Write(line)
				for err == bufio.ErrBufferFull {
					line, err = clientReader.ReadSlice('\n')
					buf.Write(line)
				}
				if err != nil && err != io.EOF {
					lineBufferPool.Put(buf)
					atomic.AddUint64(&wm.Errors, 1)
					return
				}
				if buf.Len() == 0 && err == io.EOF {
					lineBufferPool.Put(buf)
					return
				}
				line = make([]byte, buf.Len())
				copy(line, buf.Bytes())
				lineBufferPool.Put(buf)
			} else if err == io.EOF {
				if len(line) == 0 {
					return
				}
			} else {
				atomic.AddUint64(&wm.Errors, 1)
				return
			}
		}

		if len(line) > 0 && line[len(line)-1] == '\n' {
			line = line[:len(line)-1]
		}
		atomic.AddUint64(&wm.Requests, 1)
		atomic.AddUint64(&wm.BytesIn, uint64(len(line)))
		backend := pool.Pick()
		if backend == nil {
			if _, err := clientWriter.WriteString(msgNoBackend); err != nil {
				atomic.AddUint64(&wm.Errors, 1)
				return
			}
			if err := clientWriter.WriteByte('\n'); err != nil {
				atomic.AddUint64(&wm.Errors, 1)
				return
			}
			if err := clientWriter.Flush(); err != nil {
				atomic.AddUint64(&wm.Errors, 1)
				return
			}
			atomic.AddUint64(&wm.BytesOut, uint64(len(msgNoBackend)+1))
			atomic.AddUint64(&wm.Errors, 1)
			continue
		}

		bm := pool.BeginBackendRequest(backend.Addr)
		bytesOut, err := forwardLine(pool, backend, line, clientWriter)
		pool.EndBackendRequest(backend.Addr, bm)
		if err != nil {
			log.Printf("backend %s error: %v", backend.Addr, err)
			if _, err := clientWriter.WriteString(msgBackendErr); err != nil {
				atomic.AddUint64(&wm.Errors, 1)
				return
			}
			if err := clientWriter.WriteByte('\n'); err != nil {
				atomic.AddUint64(&wm.Errors, 1)
				return
			}
			if err := clientWriter.Flush(); err != nil {
				atomic.AddUint64(&wm.Errors, 1)
				return
			}
			atomic.AddUint64(&wm.BytesOut, uint64(len(msgBackendErr)+1))
			atomic.AddUint64(&wm.Errors, 1)
			continue
		}

		atomic.AddUint64(&wm.BytesOut, uint64(bytesOut))
	}
}

func forwardLine(pool *models.BackendPool, backend *models.Backend, line []byte, clientWriter *bufio.Writer) (int, error) {
	conn, err := pool.Acquire(backend)
	if err != nil {
		return 0, err
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

	if _, err := writer.Write(line); err != nil {
		usable = false
		return 0, err
	}
	if err := writer.WriteByte('\n'); err != nil {
		usable = false
		return 0, err
	}
	if err := writer.Flush(); err != nil {
		usable = false
		return 0, err
	}

	total := 0
	for {
		chunk, err := reader.ReadSlice('\n')
		if len(chunk) > 0 {
			n, werr := clientWriter.Write(chunk)
			total += n
			if werr != nil {
				usable = false
				return total, werr
			}
		}
		if err == nil {
			if err := clientWriter.Flush(); err != nil {
				usable = false
				return total, err
			}
			return total, nil
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		usable = false
		return total, err
	}
}
