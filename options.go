package lymbo

import "time"

// Option is a functional option for configuring ticket operations.
type Option func(o *Opts)

// Opts contains options for ticket operations.
type Opts struct {
	// ExpireIn sets when the ticket should expire after the operation.
	ExpireIn time.Duration

	// Keep indicates whether to retain the ticket in the store after the operation.
	// By default, completed/failed/cancelled tickets are removed.
	Keep bool

	// ErrorReason stores the reason for failure (for Fail operations).
	ErrorReason any
}

// WithExpireIn sets the expiration duration for a ticket operation.
// The ticket's Runat will be set to now + ttl.
func WithExpireIn(ttl time.Duration) Option {
	return func(o *Opts) {
		o.ExpireIn = ttl
	}
}

// WithKeep indicates that the ticket should be kept in the store after processing.
// Useful for audit trails or tracking completed work.
func WithKeep() Option {
	return func(o *Opts) {
		o.Keep = true
	}
}

// WithErrorReason sets an error reason for failed ticket operations.
// The reason will be stored in the ticket's ErrorReason field.
func WithErrorReason(reason any) Option {
	return func(o *Opts) {
		o.ErrorReason = reason
	}
}
