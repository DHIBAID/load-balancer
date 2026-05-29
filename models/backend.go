package models

import "net"

type Backend struct {
	Addr string
	Pool chan net.Conn
}
