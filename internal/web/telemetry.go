package web

import (
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// telemetry aggregates request-level metrics for the usage dashboard. Counters
// are atomic so the hot path never takes a lock; a bounded ring of recent
// samples supports per-minute trend buckets without unbounded memory growth.
type telemetry struct {
	requests  atomic.Int64
	ok        atomic.Int64
	errs      atomic.Int64
	latencyMs atomic.Int64
	tokensIn  atomic.Int64
	tokensOut atomic.Int64
	warmHits  atomic.Int64
	warmMiss  atomic.Int64
	reuseHits atomic.Int64
	reuseMiss atomic.Int64

	mu      sync.Mutex
	samples []requestSample
}

type requestSample struct {
	at      time.Time
	ms      int64
	status  int
	model   string
	account string
}

const maxSamples = 4000

// record is called once per inbound chat request after it settles. warm reports
// a warmup-pool TLS hit; reuseHit/reuseMiss report content-key session reuse.
func (t *telemetry) record(status int, ms int64, model, account string, in, out int64, warm, reuseHit, reuseMiss bool) {
	if t == nil {
		return
	}
	t.requests.Add(1)
	if status >= 200 && status < 500 {
		t.ok.Add(1)
	} else {
		t.errs.Add(1)
	}
	t.latencyMs.Add(ms)
	t.tokensIn.Add(in)
	t.tokensOut.Add(out)
	if warm {
		t.warmHits.Add(1)
	} else {
		t.warmMiss.Add(1)
	}
	if reuseHit {
		t.reuseHits.Add(1)
	}
	if reuseMiss {
		t.reuseMiss.Add(1)
	}
	t.mu.Lock()
	t.samples = append(t.samples, requestSample{at: time.Now(), ms: ms, status: status, model: model, account: account})
	if len(t.samples) > maxSamples {
		t.samples = t.samples[len(t.samples)-maxSamples:]
	}
	t.mu.Unlock()
}

func (s *Server) adminUsage(w http.ResponseWriter, r *http.Request) {
	t := s.metrics
	warmup := 0
	if s.chat != nil {
		warmup = s.chat.WarmupSize()
	}
	if t == nil {
		jsonOut(w, map[string]any{"warmupPoolSize": warmup, "telemetry": false})
		return
	}
	totalReq := t.requests.Load()
	totalOk := t.ok.Load()
	totalErr := t.errs.Load()
	totalLat := t.latencyMs.Load()
	avg := int64(0)
	if totalReq > 0 {
		avg = totalLat / totalReq
	}
	warmHits := t.warmHits.Load()
	warmMiss := t.warmMiss.Load()
	warmTotal := warmHits + warmMiss
	reuseHits := t.reuseHits.Load()
	reuseMiss := t.reuseMiss.Load()
	reuseTotal := reuseHits + reuseMiss

	// Per-minute trend for the last 30 minutes.
	t.mu.Lock()
	samples := append([]requestSample(nil), t.samples...)
	t.mu.Unlock()
	buckets := map[string]*usageBucket{}
	cutoff := time.Now().Add(-30 * time.Minute)
	for _, sm := range samples {
		if sm.at.Before(cutoff) {
			continue
		}
		key := sm.at.Truncate(time.Minute).Format("2006-01-02T15:04")
		b, ok := buckets[key]
		if !ok {
			b = &usageBucket{Minute: key}
			buckets[key] = b
		}
		b.Requests++
		if sm.status >= 200 && sm.status < 500 {
			b.Ok++
		} else {
			b.Err++
		}
		b.LatencyMs += sm.ms
	}
	byMinute := make([]*usageBucket, 0, len(buckets))
	for _, b := range buckets {
		if b.Requests > 0 {
			b.AvgLatencyMs = b.LatencyMs / b.Requests
		}
		byMinute = append(byMinute, b)
	}
	sort.Slice(byMinute, func(i, j int) bool { return byMinute[i].Minute < byMinute[j].Minute })

	recent := samples
	if len(recent) > 50 {
		recent = recent[len(recent)-50:]
	}
	recentOut := make([]map[string]any, 0, len(recent))
	for _, sm := range recent {
		recentOut = append(recentOut, map[string]any{
			"at":      sm.at.Format(time.RFC3339),
			"ms":      sm.ms,
			"status":  sm.status,
			"model":   sm.model,
			"account": sm.account,
		})
	}

	jsonOut(w, map[string]any{
		"warmupPoolSize": warmup,
		"telemetry":      true,
		"totals": map[string]any{
			"requests":        totalReq,
			"ok":              totalOk,
			"errors":          totalErr,
			"avgLatencyMs":    avg,
			"tokensIn":        t.tokensIn.Load(),
			"tokensOut":       t.tokensOut.Load(),
			"warmHits":        warmHits,
			"warmMiss":        warmMiss,
			"warmHitRate":     rate(warmHits, warmTotal),
			"reuseHits":       reuseHits,
			"reuseMiss":       reuseMiss,
			"reuseHitRate":    rate(reuseHits, reuseTotal),
		},
		"byMinute": byMinute,
		"recent":   recentOut,
	})
}

type usageBucket struct {
	Minute       string `json:"minute"`
	Requests     int64  `json:"requests"`
	Ok           int64  `json:"ok"`
	Err          int64  `json:"errors"`
	LatencyMs    int64  `json:"latencyMs"`
	AvgLatencyMs int64  `json:"avgLatencyMs"`
}

func rate(hit, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(hit) / float64(total) * 100
}
