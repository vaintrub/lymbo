package lymbo

import (
	"time"
)

// Actually it should be a enum
// Either we have tickets to process
// Or we have to wait:
type PollResult struct {
	SleepUntil *time.Time // Can be nil, actually, meaning the store is empty
	Tickets    []Ticket
}

type Store interface {
	Get(TicketId) (Ticket, error)
	Add(Ticket) error
	Delete(TicketId) error
	Done(tid TicketId, result any) error
	Cancel(tid TicketId, reason any) error
	Fail(tid TicketId, reason any) error
	Poll(batchSize int, now time.Time, blockFor time.Duration) (PollResult, error)
}
