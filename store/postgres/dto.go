package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/status"
)

// pollPendingRequestDTO converts a PollRequest into pgx.NamedArgs for query execution.
func pollPendingRequestDTO(req lymbo.PollRequest) pgx.NamedArgs {
	return pgx.NamedArgs{
		"now":        req.Now,
		"limit":      req.Limit,
		"ttr":        req.TTR.Seconds(),
		"delay_base": req.BackoffBase,
		"delay_max":  req.MaxBackoffDelay.Seconds(),
	}
}

func getTicketRequestDTO(id lymbo.TicketId) pgx.NamedArgs {
	return pgx.NamedArgs{
		"id": id.String(),
	}
}

func deleteTicketRequestDTO(id lymbo.TicketId) pgx.NamedArgs {
	return pgx.NamedArgs{
		"id": id.String(),
	}
}

func expireTicketsRequestDTO(limit int, expireBefore time.Time) pgx.NamedArgs {
	return pgx.NamedArgs{
		"limit":         limit,
		"expire_before": expireBefore,
	}
}

type TicketDTO struct {
	Key         string         `db:"ticket"`
	ID          string         `db:"id"`
	Status      string         `db:"status"`
	Runat       time.Time      `db:"runat"`
	Nice        int            `db:"nice"`
	Type        string         `db:"type"`
	Ctime       time.Time      `db:"ctime"`
	Mtime       *time.Time     `db:"mtime"`
	Attempts    int            `db:"attempts"`
	Payload     sql.NullString `db:"payload"`
	ErrorReason sql.NullString `db:"error_reason"`
}

func (dto *TicketDTO) ToNamedArgs() pgx.NamedArgs {
	return pgx.NamedArgs{
		"key":          dto.Key,
		"id":           dto.ID,
		"status":       dto.Status,
		"runat":        dto.Runat,
		"nice":         dto.Nice,
		"type":         dto.Type,
		"ctime":        dto.Ctime,
		"mtime":        dto.Mtime,
		"attempts":     dto.Attempts,
		"payload":      dto.Payload,
		"error_reason": dto.ErrorReason,
	}
}

func (dto *TicketDTO) FromTicket(t lymbo.Ticket) error {
	dto.Key = keyTicket.String()
	dto.ID = t.ID.String()
	// Check for valid uuid:
	if t.ID == "" {
		return lymbo.ErrTicketIDEmpty
	}
	if _, err := uuid.Parse(dto.ID); err != nil {
		return lymbo.ErrTicketIDInvalid
	}

	dto.Status = t.Status.String()
	dto.Runat = t.Runat
	dto.Nice = t.Nice
	dto.Type = t.Type
	dto.Ctime = t.Ctime
	dto.Mtime = t.Mtime
	dto.Attempts = t.Attempts
	if t.Payload == nil {
		dto.Payload = sql.NullString{Valid: false}
	} else {
		// JsonMarshal:
		bytes, err := json.Marshal(t.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal Ticket.payload: %v", err)
		}
		dto.Payload = sql.NullString{String: string(bytes), Valid: true}
	}
	if t.ErrorReason == nil {
		dto.ErrorReason = sql.NullString{Valid: false}
	} else {
		bytes, err := json.Marshal(t.ErrorReason)
		if err != nil {
			return fmt.Errorf("failed to marshal Ticket.errorReason: %v", err)
		}
		dto.ErrorReason = sql.NullString{String: string(bytes), Valid: true}
	}
	return nil
}

func (dto *TicketDTO) toTicket() (lymbo.Ticket, error) {
	st, err := status.FromString(dto.Status)
	if err != nil {
		return lymbo.Ticket{}, err
	}

	return lymbo.Ticket{
		ID:          lymbo.TicketId(dto.ID),
		Status:      st,
		Runat:       dto.Runat,
		Nice:        dto.Nice,
		Type:        dto.Type,
		Ctime:       dto.Ctime,
		Mtime:       dto.Mtime,
		Attempts:    dto.Attempts,
		Payload:     dto.Payload.String,
		ErrorReason: dto.ErrorReason.String,
	}, nil
}
