package lymbo

import (
	"sync/atomic"
	"time"
)

// static histogram structure
// it should be concurrent and atomic
type hist struct {
	bins  [16]atomic.Uint64
	sum   atomic.Uint64
	count atomic.Uint64
}

func toBin(dur time.Duration) int {
	mks := int(dur.Microseconds())
	if mks < 0 {
		return 0
	} else if mks <= 1000 {
		return 1
	} else if mks <= 2_000 {
		return 2
	} else if mks <= 5_000 {
		return 3
	} else if mks <= 10_000 {
		return 4
	} else if mks <= 20_000 {
		return 5
	} else if mks <= 50_000 {
		return 6
	} else if mks <= 100_000 {
		return 7
	} else if mks <= 200_000 {
		return 8
	} else if mks <= 500_000 {
		return 9
	} else if mks <= 1_000_000 {
		return 10
	} else if mks <= 2_000_000 {
		return 11
	} else if mks <= 5_000_000 {
		return 12
	} else if mks <= 10_000_000 {
		return 13
	} else if mks <= 20_000_000 {
		return 14
	} else {
		// 16 slots
		return 15
	}
}

func newHist() *hist {
	return &hist{
		bins: [16]atomic.Uint64{},
	}
}

func (h *hist) Observe(dur time.Duration) {
	idx := toBin(dur)
	ms := uint64(dur.Milliseconds())
	(&h.bins[idx]).Add(1)
	(&h.sum).Add(ms)
	(&h.count).Add(1)
}

func (h *hist) Reset() {
	for i := range 16 {
		h.bins[i].Store(0)
	}
	h.sum.Store(0)
	h.count.Store(0)
}

func (h *hist) Collect() *Histogram {
	// it is not really consistent, but good enough for stats
	r := &Histogram{
		Buckets: make([]Bucket, 16),
	}

	r.Sum = time.Duration(h.sum.Load()) * time.Millisecond
	r.Count = h.count.Load()
	for i := range 16 {
		r.Buckets[i].Count = h.bins[i].Load()
	}
	// set max durations
	r.Buckets[0].MaxDur = 0
	r.Buckets[1].MaxDur = 1 * time.Millisecond
	r.Buckets[2].MaxDur = 2 * time.Millisecond
	r.Buckets[3].MaxDur = 5 * time.Millisecond
	r.Buckets[4].MaxDur = 10 * time.Millisecond
	r.Buckets[5].MaxDur = 20 * time.Millisecond
	r.Buckets[6].MaxDur = 50 * time.Millisecond
	r.Buckets[7].MaxDur = 100 * time.Millisecond
	r.Buckets[8].MaxDur = 200 * time.Millisecond
	r.Buckets[9].MaxDur = 500 * time.Millisecond
	r.Buckets[10].MaxDur = 1 * time.Second
	r.Buckets[11].MaxDur = 2 * time.Second
	r.Buckets[12].MaxDur = 5 * time.Second
	r.Buckets[13].MaxDur = 10 * time.Second
	r.Buckets[14].MaxDur = 20 * time.Second
	r.Buckets[15].MaxDur = time.Duration(1<<63 - 1) // effectively infinite
	return r
}

type Bucket struct {
	MaxDur time.Duration
	Count  uint64
}

type Histogram struct {
	Sum     time.Duration
	Count   uint64
	Buckets []Bucket
}

type HistogramStats struct {
	Min  time.Duration `json:"min"`
	Max  time.Duration `json:"max"`
	Avg  time.Duration `json:"avg"`
	P50  time.Duration `json:"p50"`
	P90  time.Duration `json:"p90"`
	P95  time.Duration `json:"p95"`
	P99  time.Duration `json:"p99"`
	P999 time.Duration `json:"p999"`
}

func (h *Histogram) Stats() HistogramStats {
	r := HistogramStats{}
	if h.Count == 0 {
		return r
	}
	r.Avg = time.Duration(uint64(h.Sum) / h.Count)

	// min
	for _, b := range h.Buckets {
		if b.Count > 0 {
			r.Min = b.MaxDur
			break
		}
	}
	// max
	for i := len(h.Buckets) - 1; i >= 0; i-- {
		if h.Buckets[i].Count > 0 {
			r.Max = h.Buckets[i].MaxDur
			break
		}
	}

	// percentiles
	targets := []struct {
		field *time.Duration
		pct   float64
	}{
		{&r.P50, 0.50},
		{&r.P90, 0.90},
		{&r.P95, 0.95},
		{&r.P99, 0.99},
		{&r.P999, 0.999},
	}
	for _, target := range targets {
		threshold := uint64(float64(h.Count) * target.pct)
		var cumulative uint64
		for _, b := range h.Buckets {
			cumulative += b.Count
			if cumulative >= threshold {
				*target.field = b.MaxDur
				break
			}
		}
	}
	return r
}
