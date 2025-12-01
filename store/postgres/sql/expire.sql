-- $1=now
DELETE FROM tickets
WHERE id IN (
	SELECT id
	FROM tickets
	WHERE status != 'pending' AND runat <= @expire_before
	LIMIT @limit
)
