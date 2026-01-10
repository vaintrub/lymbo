package lymbo

import "sync/atomic"

type counter struct {
	value atomic.Int64
}

type stats struct {
	added     *counter
	polled    *counter
	scheduled *counter
	acked     *counter
	failed    *counter
	done      *counter
	retried   *counter
	canceled  *counter
	deleted   *counter
	expired   *counter
	processed *counter
}

type Stats struct {
	Added     int64 `json:"added"`
	Polled    int64 `json:"polled"`
	Scheduled int64 `json:"scheduled"`
	Acked     int64 `json:"acked"`
	Failed    int64 `json:"failed"`
	Done      int64 `json:"done"`
	Retried   int64 `json:"retried"`
	Canceled  int64 `json:"canceled"`
	Deleted   int64 `json:"deleted"`
	Expired   int64 `json:"expired"`
	Processed int64 `json:"processed"`
}

func newStats() *stats {
	return &stats{
		added:     &counter{},
		polled:    &counter{},
		scheduled: &counter{},
		acked:     &counter{},
		failed:    &counter{},
		done:      &counter{},
		retried:   &counter{},
		canceled:  &counter{},
		deleted:   &counter{},
		expired:   &counter{},
		processed: &counter{},
	}
}

func (s *stats) reset() {
	s.added.value.Store(0)
	s.polled.value.Store(0)
	s.scheduled.value.Store(0)
	s.acked.value.Store(0)
	s.failed.value.Store(0)
	s.done.value.Store(0)
	s.retried.value.Store(0)
	s.canceled.value.Store(0)
	s.deleted.value.Store(0)
	s.expired.value.Store(0)
	s.processed.value.Store(0)
}
