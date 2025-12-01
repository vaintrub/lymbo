package postgres

import "fmt"

// Pure internal
type ticketKey struct {
	slug string
}

var (
	keyTicket ticketKey = ticketKey{slug: "ticket"}
	keyFuture ticketKey = ticketKey{slug: "future_ticket"}
)

func (t *ticketKey) String() string {
	return t.slug
}

func fromStringTicketKey(s string) (ticketKey, error) {
	switch s {
	case keyTicket.slug:
		return keyTicket, nil
	case keyFuture.slug:
		return keyFuture, nil
	default:
		return ticketKey{}, fmt.Errorf("unknown ticket key: %s", s)
	}
}
