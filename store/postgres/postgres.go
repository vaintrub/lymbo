package postgres

import (
	"context"
	"embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ochaton/lymbo"
)

//go:embed sql/*.sql
var queriesFS embed.FS

func mustReadSql(path string) string {
	data, err := queriesFS.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return string(data)
}

type Store struct {
	db        *pgxpool.Pool
	pollSql   string
	expireSql string
	schemaSql string
	getSql    string
	storeSql  string
	deleteSql string
}

func New(db *pgxpool.Pool) *Store {
	return &Store{
		db:        db,
		pollSql:   mustReadSql("sql/poll.sql"),
		expireSql: mustReadSql("sql/expire.sql"),
		schemaSql: mustReadSql("sql/schema.sql"),
		getSql:    mustReadSql("sql/get_ticket.sql"),
		storeSql:  mustReadSql("sql/store_ticket.sql"),
		deleteSql: mustReadSql("sql/delete_ticket.sql"),
	}
}

func (pg *Store) InitSchema(ctx context.Context) error {
	// TODO: proper migrations (optional table name)
	if _, err := pg.db.Exec(ctx, pg.schemaSql); err != nil {
		return err
	}
	return nil
}

func (pg *Store) Close() {
	pg.db.Close()
}

var _ lymbo.Store = (*Store)(nil)

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (pg *Store) getTicket(ctx context.Context, q querier, id lymbo.TicketId) (lymbo.Ticket, error) {
	if id == "" {
		return lymbo.Ticket{}, lymbo.ErrTicketIDEmpty
	}

	rows, err := q.Query(ctx, pg.getSql, getTicketRequestDTO(id))
	if err != nil {
		return lymbo.Ticket{}, err
	}
	defer rows.Close()

	dto, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[TicketDTO])
	if err != nil {
		return lymbo.Ticket{}, err
	}
	ticket, err := dto.toTicket()
	if err != nil {
		return lymbo.Ticket{}, err
	}
	return ticket, nil
}

type execer interface {
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
}

func (pg *Store) storeTicket(ctx context.Context, q execer, t lymbo.Ticket) error {
	dto := &TicketDTO{}
	if err := dto.FromTicket(t); err != nil {
		return err
	}

	_, err := q.Exec(ctx, pg.storeSql, dto.ToNamedArgs())
	return err
}

func (pg *Store) deleteTicket(ctx context.Context, q execer, id lymbo.TicketId) error {
	_, err := q.Exec(ctx, pg.deleteSql, deleteTicketRequestDTO(id))
	return err
}

func (pg *Store) Get(ctx context.Context, id lymbo.TicketId) (lymbo.Ticket, error) {
	tkt, err := pg.getTicket(ctx, pg.db, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return lymbo.Ticket{}, lymbo.ErrTicketNotFound
	}
	return tkt, err
}

func (pg *Store) Add(ctx context.Context, t lymbo.Ticket) error {
	return pg.storeTicket(ctx, pg.db, t)
}

func (pg *Store) Delete(ctx context.Context, id lymbo.TicketId) error {
	return pg.deleteTicket(ctx, pg.db, id)
}

func (pg *Store) Update(ctx context.Context, tid lymbo.TicketId, fn lymbo.UpdateFunc) error {
	tx, err := pg.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	t, err := pg.getTicket(ctx, tx, tid)
	if err != nil {
		return err
	}

	if err := fn(ctx, &t); err != nil {
		return err
	}

	err = pg.storeTicket(ctx, tx, t)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (pg *Store) PollPending(ctx context.Context, req lymbo.PollRequest) (lymbo.PollResult, error) {
	rows, err := pg.db.Query(ctx, pg.pollSql, pollPendingRequestDTO(req))
	if err != nil {
		return lymbo.PollResult{}, err
	}
	defer rows.Close()

	result := lymbo.PollResult{
		Tickets:    make([]lymbo.Ticket, 0),
		SleepUntil: nil,
	}

	dtos, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[TicketDTO])
	if err != nil {
		return lymbo.PollResult{}, err
	}
	for _, dto := range dtos {
		ticket, err := dto.toTicket()
		if err != nil {
			return lymbo.PollResult{}, err
		}
		if dto.Key == keyFuture.String() {
			result.SleepUntil = &ticket.Runat
		} else {
			result.Tickets = append(result.Tickets, ticket)
		}
	}

	return result, nil
}

func (pg *Store) ExpireTickets(ctx context.Context, limit int, before time.Time) (int64, error) {
	cmd, err := pg.db.Exec(ctx, pg.expireSql, expireTicketsRequestDTO(limit, before))
	if err != nil {
		return cmd.RowsAffected(), err
	}
	return cmd.RowsAffected(), nil
}
