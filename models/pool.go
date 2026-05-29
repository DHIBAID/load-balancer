package models

import (
	"sync"
	"sync/atomic"
)

type BackendPool struct {
	Next         uint64
	nextWorkerID uint64
	backends     atomic.Value
	workers      sync.Map
	backendStats sync.Map
	totalMetrics PoolMetrics
}

type WorkerMetrics struct {
	Requests uint64
	Errors   uint64
	BytesIn  uint64
	BytesOut uint64
}

type BackendMetrics struct {
	Active uint64
}

type PoolMetrics struct {
	ActiveConnections uint64 `json:"active_connections"`
	TotalRequests     uint64 `json:"total_requests"`
	TotalErrors       uint64 `json:"total_errors"`
	BytesIn           uint64 `json:"bytes_in"`
	BytesOut          uint64 `json:"bytes_out"`
	HealthyBackends   uint64 `json:"healthy_backends"`
	FailedBackends    uint64 `json:"failed_backends"`
}

type backendSnapshot struct {
	Active []string
	Failed []string
}

func (p *BackendPool) Pick() string {
	snapshot, ok := p.snapshot()
	if !ok || len(snapshot.Active) == 0 {
		return ""
	}

	active := snapshot.Active
	start := int(atomic.AddUint64(&p.Next, 1)-1) % len(active)
	selected := active[start]
	minActive := p.backendActive(selected)

	for i := 1; i < len(active); i++ {
		idx := (start + i) % len(active)
		candidate := active[idx]
		count := p.backendActive(candidate)
		if count < minActive {
			minActive = count
			selected = candidate
		}
	}

	return selected
}

func (p *BackendPool) backendActive(addr string) uint64 {
	value, ok := p.backendStats.Load(addr)
	if !ok {
		return 0
	}
	bm, ok := value.(*BackendMetrics)
	if !ok || bm == nil {
		return 0
	}
	return atomic.LoadUint64(&bm.Active)
}

func (p *BackendPool) SnapshotBackends() (active []string, failed []string) {
	snapshot, ok := p.snapshot()
	if !ok {
		return nil, nil
	}

	active = append([]string(nil), snapshot.Active...)
	failed = append([]string(nil), snapshot.Failed...)
	return active, failed
}

func (p *BackendPool) snapshot() (backendSnapshot, bool) {
	snapshot, ok := p.backends.Load().(backendSnapshot)
	return snapshot, ok
}

func (p *BackendPool) ReplaceBackends(active []string, failed []string) {
	p.backends.Store(backendSnapshot{
		Active: append([]string(nil), active...),
		Failed: append([]string(nil), failed...),
	})
}

func (p *BackendPool) RegisterWorker() (uint64, *WorkerMetrics) {
	id := atomic.AddUint64(&p.nextWorkerID, 1)
	wm := &WorkerMetrics{}
	p.workers.Store(id, wm)
	return id, wm
}

func (p *BackendPool) BeginBackendRequest(addr string) *BackendMetrics {
	value, _ := p.backendStats.LoadOrStore(addr, &BackendMetrics{})
	bm, ok := value.(*BackendMetrics)
	if !ok || bm == nil {
		return nil
	}
	atomic.AddUint64(&bm.Active, 1)
	return bm
}

func (p *BackendPool) EndBackendRequest(_ string, bm *BackendMetrics) {
	if bm == nil {
		return
	}
	atomic.AddUint64(&bm.Active, ^uint64(0))
}

func (p *BackendPool) UnregisterWorker(id uint64, wm *WorkerMetrics) {
	if wm != nil {
		atomic.AddUint64(&p.totalMetrics.TotalRequests, atomic.LoadUint64(&wm.Requests))
		atomic.AddUint64(&p.totalMetrics.TotalErrors, atomic.LoadUint64(&wm.Errors))
		atomic.AddUint64(&p.totalMetrics.BytesIn, atomic.LoadUint64(&wm.BytesIn))
		atomic.AddUint64(&p.totalMetrics.BytesOut, atomic.LoadUint64(&wm.BytesOut))
	}
	p.workers.Delete(id)
}

func (p *BackendPool) SnapshotMetrics() PoolMetrics {
	var live WorkerMetrics
	activeConnections := uint64(0)
	p.workers.Range(func(_, value any) bool {
		wm, ok := value.(*WorkerMetrics)
		if !ok || wm == nil {
			return true
		}
		activeConnections++
		live.Requests += atomic.LoadUint64(&wm.Requests)
		live.Errors += atomic.LoadUint64(&wm.Errors)
		live.BytesIn += atomic.LoadUint64(&wm.BytesIn)
		live.BytesOut += atomic.LoadUint64(&wm.BytesOut)
		return true
	})

	totals := PoolMetrics{
		ActiveConnections: activeConnections,
		TotalRequests:     atomic.LoadUint64(&p.totalMetrics.TotalRequests) + live.Requests,
		TotalErrors:       atomic.LoadUint64(&p.totalMetrics.TotalErrors) + live.Errors,
		BytesIn:           atomic.LoadUint64(&p.totalMetrics.BytesIn) + live.BytesIn,
		BytesOut:          atomic.LoadUint64(&p.totalMetrics.BytesOut) + live.BytesOut,
	}
	if snapshot, ok := p.snapshot(); ok {
		totals.HealthyBackends = uint64(len(snapshot.Active))
		totals.FailedBackends = uint64(len(snapshot.Failed))
	}

	return totals
}
