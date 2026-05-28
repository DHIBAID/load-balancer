package main

import (
	"flag"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type stats struct {
	sent  uint64
	recv  uint64
	errs  uint64
	conns uint64
}

func main() {
	target := flag.String("target", "127.0.0.1:9000", "load balancer address")
	conns := flag.Int("conns", 5000, "number of concurrent connections")
	duration := flag.Duration("duration", 2*time.Minute, "test duration")
	size := flag.Int("size", 1024, "payload size in bytes")
	flag.Parse()

	if *conns <= 0 || *size <= 0 {
		log.Fatal("conns and size must be > 0")
	}

	payload := makePayload(*size)
	deadline := time.Now().Add(*duration)

	var wg sync.WaitGroup
	var s stats

	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runClient(id, *target, payload, deadline, &s)
		}(i)
	}

	ticker := time.NewTicker(5 * time.Second)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				sent := atomic.LoadUint64(&s.sent)
				recv := atomic.LoadUint64(&s.recv)
				errs := atomic.LoadUint64(&s.errs)
				conns := atomic.LoadUint64(&s.conns)
				log.Printf("progress conns=%d sent=%d recv=%d errs=%d", conns, sent, recv, errs)
			case <-done:
				return
			}
		}
	}()

	wg.Wait()
	close(done)
	ticker.Stop()

	log.Printf("done conns=%d sent=%d recv=%d errs=%d", s.conns, s.sent, s.recv, s.errs)
}

func runClient(id int, target string, payload string, deadline time.Time, s *stats) {

	payloadBytes := []byte(payload + "\n")

	conn, err := net.Dial("tcp", target)
	if err != nil {
		atomic.AddUint64(&s.errs, 1)
		return
	}
	atomic.AddUint64(&s.conns, 1)
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 4096)
	for time.Now().Before(deadline) {
		if _, err := conn.Write(payloadBytes); err != nil {
			atomic.AddUint64(&s.errs, 1)
			log.Printf("read error: %v", err)
			return
		}
		atomic.AddUint64(&s.sent, 1)

		if _, err := conn.Read(buf); err != nil {
			atomic.AddUint64(&s.errs, 1)
			return
		}
		atomic.AddUint64(&s.recv, 1)
	}
}

func makePayload(size int) string {
	if size <= 1 {
		return "x"
	}

	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var b strings.Builder
	b.Grow(size)
	for i := 0; i < size; i++ {
		b.WriteByte(letters[rand.Intn(len(letters))])
	}
	return b.String()
}
