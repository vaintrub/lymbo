package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ochaton/lymbo"
	"github.com/ochaton/lymbo/status"
	"github.com/ochaton/lymbo/store/postgres/sqlc"
)

type Tickets struct {
	db      *pgxpool.Pool
	queries *sqlc.Queries
}

var _ lymbo.Store = &Tickets{}

func NewTicketsRepository(db *pgxpool.Pool) *Tickets {
	return &Tickets{
		db:      db,
		queries: sqlc.New(db),
	}
}

func (r *Tickets) Get(ctx context.Context, id lymbo.TicketId) (lymbo.Ticket, error) {
	uuid, err := uuid.Parse(id.String())
	if err != nil {
		return lymbo.Ticket{}, lymbo.ErrTicketIDInvalid
	}
	row, err := r.queries.GetTicket(ctx, uuid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return lymbo.Ticket{}, lymbo.ErrTicketNotFound
		}
		return lymbo.Ticket{}, err
	}

	return toLymboTicket(row)
}

func (r *Tickets) Put(ctx context.Context, ticket lymbo.Ticket) error {
	req, err := toPostgresTicket(ticket)
	if err != nil {
		return err
	}
	return r.queries.PutTicket(ctx, req)
}

func (r *Tickets) Delete(ctx context.Context, id lymbo.TicketId) error {
	uuid, err := uuid.Parse(id.String())
	if err != nil {
		return lymbo.ErrTicketIDInvalid
	}
	err = r.queries.DeleteTicket(ctx, uuid)
	if errors.Is(err, pgx.ErrNoRows) {
		// not found, means was deleted sometime
		return nil
	}
	return err
}

func (r *Tickets) Update(ctx context.Context, id lymbo.TicketId, fn lymbo.UpdateFunc) error {
	uuid, err := uuid.Parse(id.String())
	if err != nil {
		return lymbo.ErrTicketIDInvalid
	}

	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	row, err := r.queries.WithTx(tx).GetTicket(ctx, uuid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return lymbo.ErrTicketNotFound
		}
		return err
	}

	ticket, err := toLymboTicket(row)
	if err != nil {
		return err
	}

	if err := fn(ctx, &ticket); err != nil {
		return err
	}

	req, err := toPostgresTicket(ticket)
	if err != nil {
		return err
	}

	if err := r.queries.WithTx(tx).PutTicket(ctx, req); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Tickets) PollPending(ctx context.Context, req lymbo.PollRequest) (lymbo.PollResult, error) {
	rows, err := r.queries.PollTickets(ctx, sqlc.PollTicketsParams{
		Now:         pgtype.Timestamptz{Time: req.Now, Valid: true},
		Ttr:         int32(req.TTR.Seconds()),
		MaxDelay:    int32(req.MaxBackoffDelay.Seconds()),
		BackoffBase: req.BackoffBase,
		Lim:         int32(req.Limit),
	})

	if err != nil {
		return lymbo.PollResult{}, err
	}

	convert := func(d sqlc.PollTicketsRow) (lymbo.Ticket, error) {
		s, err := status.FromString(d.Status)
		if err != nil {
			return lymbo.Ticket{}, err
		}
		var mtime *time.Time
		if d.Mtime.Valid {
			mtime = &d.Mtime.Time
		}
		return lymbo.Ticket{
			ID:          lymbo.TicketId(d.ID.String()),
			Status:      s,
			Runat:       d.Runat.Time,
			Nice:        int(d.Nice),
			Type:        d.Type,
			Ctime:       d.Ctime.Time,
			Mtime:       mtime,
			Attempts:    int(d.Attempts),
			Payload:     d.Payload,
			ErrorReason: d.ErrorReason,
		}, nil
	}

	var sleepUntil *time.Time
	tickets := make([]lymbo.Ticket, 0, len(rows))

	for _, row := range rows {
		switch row.Ticket {
		case "ticket":
			t, err := convert(row)
			if err != nil {
				// WARN?
				slog.WarnContext(ctx, "failed to convert ticket in PollPending", "error", err, "ticket_id", row.ID.String())
				continue
			}
			tickets = append(tickets, t)
		case "future_ticket":
			// we can evaluate sleepUntil here
			sleepUntil = &row.Runat.Time
		default:
			// unknown row type?
			slog.WarnContext(ctx, "unknown row type PollPending", "row_type", row.Ticket, "ticket_id", row.ID.String())
		}
	}

	return lymbo.PollResult{
		SleepUntil: sleepUntil,
		Tickets:    tickets,
	}, nil
}

func (r *Tickets) ExpireTickets(ctx context.Context, limit int, now time.Time) (int64, error) {
	res, err := r.queries.ExpireTickets(ctx, sqlc.ExpireTicketsParams{
		ExpireBefore: pgtype.Timestamptz{Time: now, Valid: true},
		Lim:          int32(limit),
	})
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

func toPostgresTicket(t lymbo.Ticket) (sqlc.PutTicketParams, error) {
	var id uuid.UUID
	var mtime pgtype.Timestamptz
	var payload []byte
	var errorReason []byte
	var err error
	id, err = uuid.Parse(t.ID.String())
	if err != nil {
		return sqlc.PutTicketParams{}, lymbo.ErrTicketIDInvalid
	}
	if mt := t.Mtime; mt != nil {
		mtime = pgtype.Timestamptz{Time: *mt, Valid: true}
	}
	if t.Payload != nil {
		payload, err = json.Marshal(t.Payload)
		if err != nil {
			return sqlc.PutTicketParams{}, fmt.Errorf("failed to marshal payload: %w", err)
		}
	}
	if t.ErrorReason != nil {
		errorReason, err = json.Marshal(t.ErrorReason)
		if err != nil {
			return sqlc.PutTicketParams{}, fmt.Errorf("failed to marshal error_reason: %w", err)
		}
	}
	return sqlc.PutTicketParams{
		ID:          id,
		Status:      t.Status.String(),
		Runat:       pgtype.Timestamptz{Time: t.Runat, Valid: true},
		Nice:        int16(t.Nice),
		Type:        t.Type,
		Ctime:       pgtype.Timestamptz{Time: t.Ctime, Valid: true},
		Mtime:       mtime,
		Attempts:    int32(t.Attempts),
		Payload:     payload,
		ErrorReason: errorReason,
	}, nil
}

func toLymboTicket(dto sqlc.GetTicketRow) (lymbo.Ticket, error) {
	s, err := status.FromString(dto.Status)
	if err != nil {
		return lymbo.Ticket{}, err
	}
	var mtime *time.Time
	if dto.Mtime.Valid {
		mtime = &dto.Mtime.Time
	}
	return lymbo.Ticket{
		ID:          lymbo.TicketId(dto.ID.String()),
		Status:      s,
		Runat:       dto.Runat.Time,
		Nice:        int(dto.Nice),
		Type:        dto.Type,
		Ctime:       dto.Ctime.Time,
		Mtime:       mtime,
		Attempts:    int(dto.Attempts),
		Payload:     dto.Payload,
		ErrorReason: dto.ErrorReason,
	}, nil
}
