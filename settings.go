package lymbo

import "time"

// Settings contains configuration options for Kharon.
type Settings struct {
	// processTime is the time-to-run for tickets (prevents re-polling during processing).
	processTime time.Duration

	// maxBackoffDelay is the maximum delay between retry attempts.
	maxBackoffDelay time.Duration

	// backoffBase is the base for exponential backoff calculation.
	// The delay is calculated as: backoffBase^attempts seconds.
	// Defaults to DefaultBackoffBase.
	backoffBase float64

	// maxPollInterval is the maximum time to wait between store polls.
	// Defaults to MaxPollIntervalDefault.
	maxPollInterval time.Duration

	// minPollInterval is the minimum time to wait between store polls.
	// Defaults to MinPollIntervalDefault.
	minPollInterval time.Duration

	// batchSize is the maximum number of tickets to poll at once.
	// Defaults to 1, capped at Workers.
	batchSize int

	// workers is the number of concurrent ticket processors.
	// Defaults to 1.
	workers int

	// enableExpiration enables automatic cleanup of expired tickets.
	enableExpiration bool

	// expirationInterval
	expirationInterval time.Duration ``
}

// DefaultSettings returns a Settings instance with sensible defaults.
func DefaultSettings() *Settings {
	return &Settings{
		processTime:        30 * time.Second,
		maxPollInterval:    MaxPollIntervalDefault,
		minPollInterval:    MinPollIntervalDefault,
		maxBackoffDelay:    MaxBackoffDelay,
		backoffBase:        DefaultBackoffBase,
		batchSize:          10,
		workers:            4,
		enableExpiration:   true,
		expirationInterval: ExpirationInterval,
	}
}

func (s *Settings) WithExpiration() *Settings {
	s.enableExpiration = true
	return s
}

func (s *Settings) WithWorkers(workers int) *Settings {
	s.workers = workers
	return s
}

func (s *Settings) WithBatchSize(batchSize int) *Settings {
	s.batchSize = batchSize
	return s
}

func (s *Settings) WithoutExpiration() *Settings {
	s.enableExpiration = false
	return s
}

func (s *Settings) WithProcessTime(d time.Duration) *Settings {
	s.processTime = d
	return s
}

// WithBackoffBase sets the base for exponential backoff calculation.
// The delay is calculated as: backoffBase^attempts seconds.
func (s *Settings) WithBackoffBase(base float64) *Settings {
	s.backoffBase = base
	return s
}

func (s *Settings) WithMaxPollInterval(d time.Duration) *Settings {
	s.maxPollInterval = d
	return s
}

func (s *Settings) WithMinPollInterval(d time.Duration) *Settings {
	s.minPollInterval = d
	return s
}

// normalize applies defaults and constraints to the settings.
func (s *Settings) normalize() {
	if s.workers <= 0 {
		s.workers = 1
	}
	if s.maxPollInterval <= 0 {
		s.maxPollInterval = MaxPollIntervalDefault
	}
	if s.minPollInterval <= 0 {
		s.minPollInterval = MinPollIntervalDefault
	}
	if s.maxPollInterval < s.minPollInterval {
		s.maxPollInterval = s.minPollInterval
	}
	if s.batchSize <= 0 {
		s.batchSize = 1
	}
	if s.batchSize > s.workers {
		s.batchSize = s.workers
	}
	if s.backoffBase <= 0 {
		s.backoffBase = DefaultBackoffBase
	}
}
