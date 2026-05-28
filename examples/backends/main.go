package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
)

type backend struct {
	name string
	addr string
}

func main() {
	basePort := flag.Int("base-port", 9001, "first backend port")
	flag.Parse()

	backends := []backend{
		{name: "backend-1", addr: fmt.Sprintf(":%d", *basePort)},
		{name: "backend-2", addr: fmt.Sprintf(":%d", *basePort+1)},
		{name: "backend-3", addr: fmt.Sprintf(":%d", *basePort+2)},
	}

	for i := range backends {
		b := backends[i]
		go serveBackend(b)
	}

	select {}
}

func serveBackend(b backend) {
	ln, err := net.Listen("tcp", b.addr)
	if err != nil {
		log.Fatalf("%s listen error: %v", b.name, err)
	}
	log.Printf("%s listening on %s", b.name, b.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("%s accept error: %v", b.name, err)
			continue
		}

		go handleConn(b, conn)
	}
}

func handleConn(b backend, conn net.Conn) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	// log.Printf("%s accepted connection from %s", b.name, remoteAddr)

	_, _ = fmt.Fprintf(conn, "%s ready\n", b.name)

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		// log.Printf("%s recv from %s: %s", b.name, remoteAddr, line)
		_, _ = fmt.Fprintf(conn, "%s: %s\n", b.name, line)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("%s read error from %s: %v", b.name, remoteAddr, err)
	}
}
