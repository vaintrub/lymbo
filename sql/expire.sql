DELETE FROM tickets
WHERE id IN (
	SELECT id
	FROM tickets
	WHERE status in ('done', 'cancelled', 'failed') AND runat <= :now
	ORDER BY runat ASC
)
RETURNING id, status, runat, nice, type, ctime, mtime, attempts, payload, error_reason