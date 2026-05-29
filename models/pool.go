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
	totalMetrics PoolMetrics
}

type backendSnapshot struct {
	Active []string
	Failed []string
}

type WorkerMetrics struct {
	Requests uint64
	Errors   uint64
	BytesIn  uint64
	BytesOut uint64
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

func (p *BackendPool) Pick() string {
	active, _ := p.SnapshotBackends()
	if len(active) == 0 {
		return ""
	}
	idx := atomic.AddUint64(&p.Next, 1) - 1
	return active[idx%uint64(len(active))]
}

func (p *BackendPool) SnapshotBackends() (active []string, failed []string) {
	snapshotAny := p.backends.Load()
	if snapshotAny == nil {
		return nil, nil
	}
	snapshot, ok := snapshotAny.(backendSnapshot)
	if !ok {
		return nil, nil
	}

	active = append([]string(nil), snapshot.Active...)
	failed = append([]string(nil), snapshot.Failed...)
	return active, failed
}

func (p *BackendPool) ReplaceBackends(active []string, failed []string) {
	snapshot := backendSnapshot{
		Active: append([]string(nil), active...),
		Failed: append([]string(nil), failed...),
	}
	p.backends.Store(snapshot)
}

func (p *BackendPool) InitBackends(active []string) {
	p.ReplaceBackends(active, nil)
}

func (p *BackendPool) RegisterWorker() (uint64, *WorkerMetrics) {
	id := atomic.AddUint64(&p.nextWorkerID, 1)
	wm := &WorkerMetrics{}
	p.workers.Store(id, wm)
	return id, wm
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
	live := WorkerMetrics{}
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

	active, failed := p.SnapshotBackends()
	totals.HealthyBackends = uint64(len(active))
	totals.FailedBackends = uint64(len(failed))

	return totals
}
