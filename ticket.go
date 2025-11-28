package lymbo

import (
	"errors"
	"time"

	"github.com/ochaton/lymbo/status"
)

type TicketId string

type Ticket struct {
	ID          TicketId
	Status      status.Status
	Runat       time.Time
	Nice        int
	Type        string // Opaque type identifier
	Ctime       time.Time
	Mtime       *time.Time
	Attempts    int
	Payload     any
	ErrorReason any
}

var ErrTidEmpty = errors.New("ticket ID cannot be empty")
var ErrTypeEmpty = errors.New("ticket type cannot be empty")

const DefaultNice = 512

func NewTicket(tid TicketId, typ string) (*Ticket, error) {
	if tid == "" {
		return nil, ErrTidEmpty
	}
	if typ == "" {
		return nil, ErrTypeEmpty
	}

	return &Ticket{
		ID:       tid,
		Runat:    time.Now(),
		Nice:     DefaultNice,
		Type:     typ,
		Ctime:    time.Now(),
		Mtime:    nil,
		Attempts: 0,
		// Payload:     nil,
		ErrorReason: nil,
	}, nil
}

func (t *Ticket) WithPayload(payload any) *Ticket {
	t.Payload = payload
	return t
}

func (t *Ticket) WithNice(nice int) *Ticket {
	t.Nice = nice
	return t
}

func (t *Ticket) WithRunat(runat time.Time) *Ticket {
	t.Runat = runat
	return t
}
