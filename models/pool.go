package models

import "sync/atomic"

type BackendPool struct {
	Addresses []string
	Next  uint64
}

func (p *BackendPool) Pick() string {
	if len(p.Addresses) == 0 {
		return ""
	}
	idx := atomic.AddUint64(&p.Next, 1) - 1
	return p.Addresses[idx%uint64(len(p.Addresses))]
}
