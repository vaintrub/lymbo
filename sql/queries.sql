-- name: GetTicket :one
SELECT 'ticket' as ticket, id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason
FROM tickets
WHERE tickets.id = @id;

-- name: DeleteTicket :exec
DELETE FROM tickets
WHERE id = @id;

-- name: PutTicket :exec
INSERT INTO tickets (id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason)
VALUES (@id, @status, @runat, @nice, @type, @ctime, @mtime, @attempts, @payload, @error_reason)
ON CONFLICT (id) DO UPDATE SET
	status = EXCLUDED.status,
	runat = EXCLUDED.runat,
	nice = EXCLUDED.nice,
	type = EXCLUDED.type,
	ctime = EXCLUDED.ctime,
	mtime = EXCLUDED.mtime,
	attempts = EXCLUDED.attempts,
	payload = EXCLUDED.payload,
	error_reason = EXCLUDED.error_reason;

-- name: ExpireTickets :execresult
DELETE FROM tickets
WHERE id IN (SELECT id FROM tickets as t WHERE t.status != 'pending' AND t.runat <= @expire_before LIMIT @lim);

-- name: UpdateTicket :exec
UPDATE tickets
SET
    status = COALESCE(sqlc.narg('status'), status),
    nice = COALESCE(sqlc.narg('nice'), nice),
    runat = COALESCE(sqlc.narg('runat'), runat),
    payload = COALESCE(sqlc.narg('payload'), payload),
    error_reason = COALESCE(sqlc.narg('error_reason'), error_reason),
    mtime = NOW()
WHERE id = @id;

-- name: PollTickets :many
WITH rescheduled_tickets AS (
    UPDATE tickets as t
    SET
        attempts = attempts + 1,
        runat = sqlc.arg(now)::Timestamptz + (GREATEST(sqlc.arg(ttr), 0) + LEAST(sqlc.arg(max_delay), POWER(sqlc.arg(backoff_base), t.attempts))) * INTERVAL '1 second'
    WHERE id IN (
        SELECT t.id
        FROM tickets as t
        WHERE t.status = 'pending' AND t.runat <= sqlc.arg(now)::Timestamptz
        LIMIT sqlc.arg(lim)
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason
),
future_ticket AS (
    SELECT ft.id, ft.status, ft.runat, ft.nice, ft.type, ft.ctime, ft.mtime, ft.attempts, ft.payload, ft.error_reason
    FROM tickets as ft
    WHERE status = 'pending'
    ORDER BY ft.runat ASC, ft.nice ASC
    LIMIT 1
    FOR SHARE SKIP LOCKED
)
SELECT
    'ticket' AS ticket,
    rescheduled_tickets.id           AS id,
    rescheduled_tickets.status       AS status,
    rescheduled_tickets.runat        AS runat,
    rescheduled_tickets.nice         AS nice,
    rescheduled_tickets.type         AS type,
    rescheduled_tickets.ctime        AS ctime,
    rescheduled_tickets.mtime        AS mtime,
    rescheduled_tickets.attempts     AS attempts,
    rescheduled_tickets.payload      AS payload,
    rescheduled_tickets.error_reason AS error_reason
FROM rescheduled_tickets
UNION ALL
SELECT
    'future_ticket'            AS ticket,
    future_ticket.id           AS id,
    future_ticket.status       AS status,
    future_ticket.runat        AS runat,
    future_ticket.nice         AS nice,
    future_ticket.type         AS type,
    future_ticket.ctime        AS ctime,
    future_ticket.mtime        AS mtime,
    future_ticket.attempts     AS attempts,
    future_ticket.payload      AS payload,
    future_ticket.error_reason AS error_reason
FROM future_ticket
WHERE NOT EXISTS (SELECT 1 FROM rescheduled_tickets);
