-- Poll pending tickets for processing.
-- @now=now, @limit=limit, @ttr=ttr, @delay_base=delay_base, @delay_max=delay_max
WITH
rescheduled_tickets AS (
    UPDATE tickets
    SET
        attempts = attempts + 1,
        runat    = @now
			+ GREATEST(@ttr, 0) * INTERVAL '1 second'
			+ LEAST(POWER(@delay_base, attempts), @delay_max) * INTERVAL '1 second'
    WHERE id IN (
        SELECT id
        FROM tickets
        WHERE status = 'pending' AND runat <= @now
        ORDER BY runat ASC, nice ASC
        LIMIT @limit
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason
),
future_ticket AS (
    SELECT
        id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason
    FROM tickets
    WHERE status = 'pending'
    ORDER BY runat ASC, nice ASC
    LIMIT 1
	-- we ignore tickets that are taken by concurrent workers under "FOR UPDATE SKIP LOCKED"
	FOR SHARE SKIP LOCKED
)
SELECT
    'ticket' AS ticket,
    rescheduled_tickets.*
FROM rescheduled_tickets
UNION ALL
SELECT
    'future_ticket' AS ticket,
    future_ticket.*
FROM future_ticket
WHERE NOT EXISTS (SELECT 1 FROM rescheduled_tickets);
